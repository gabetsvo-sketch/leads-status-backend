"""Style Runtime v1 integration tests.

Runtime must be safe by construction: internal auth only, no live-send/CRM side
 effects, no raw PII payloads, manual_review_only on every draft, and feedback
saved as sanitized learning evidence only.
"""
import json
from pathlib import Path


BASE_REQUEST = {
    "request_id": "req-style-001",
    "deal_ref": "deal_hash_abc",
    "channel": "whatsapp",
    "client_situation_hint": "price_question",
    "last_client_message_summary": "Клиент спрашивает актуальна ли цена выбранного объекта.",
    "last_vladimir_message_summary": "Владимир ранее отправил подборку объектов.",
    "silence_days": None,
    "deal_stage": "selection",
    "client_last_message_type": "question",
    "facts_available": ["object_ref"],
    "requested_output": "client_reply_draft",
}


def test_style_runtime_draft_requires_internal_office_token(app_client, widget_headers):
    client, _ = app_client

    r = client.post("/style-runtime/v1/draft", headers=widget_headers, json=BASE_REQUEST)

    assert r.status_code == 401


def test_style_runtime_draft_blocks_missing_price_source_and_never_sends(app_client, office_headers):
    client, _ = app_client

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=BASE_REQUEST)

    # Новый контракт (2026-06-13): блокируем не «тему цены», а только когда в ТЕКСТЕ
    # черновика реально появилась конкретная цифра без подтверждённого источника.
    # Черновик на ценовую тему без выдуманных чисел показывать можно — он безопасен
    # и полезен. Прежний контракт глушил такие черновики в пустоту (жалоба Владимира).
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["request_id"] == BASE_REQUEST["request_id"]
    assert body["manual_review_only"] is True
    assert body["show_to_vladimir"] is True
    # Чистый черновик без цифр проходит; жёсткий флаг цены НЕ выставлен.
    assert "price_without_source" not in body["safety_flags"]
    # Side effects по-прежнему запрещены — рантайм ничего не отправляет и не пишет в CRM.
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_safety_gate_blocks_unsourced_price_figure_in_draft():
    """Главная гарантия безопасности сохранена: конкретная цифра (сумма/процент) без
    источника обнуляет черновик, чтобы клиенту не ушла выдуманная цена."""
    import main

    gate = main._style_safety_gate(
        {"last_client_message_summary": "клиент спрашивает про цену"},
        "Этот вариант стоит 5 млн бат, доходность около 7% годовых.",
        "price_roi_explanation",
    )
    assert gate["pass"] is False
    assert "price_without_source" in gate["flags"]
    assert "price_source_ref" in gate["missing_facts"]

    # А черновик на ту же тему без выдуманных чисел — безопасен и проходит.
    gate_clean = main._style_safety_gate(
        {"last_client_message_summary": "клиент спрашивает про цену"},
        "Подскажу по условиям и сориентирую, актуально ли это сейчас для вас.",
        "price_roi_explanation",
    )
    assert "price_without_source" not in gate_clean["flags"]


def test_style_runtime_draft_returns_manual_review_safe_low_risk_reply(app_client, office_headers):
    client, _ = app_client
    payload = {
        **BASE_REQUEST,
        "request_id": "req-style-safe-001",
        "client_situation_hint": "followup",
        "last_client_message_summary": "Клиент просит напомнить следующий шаг по подборке.",
        "deal_stage": "question",
        "client_last_message_type": "question",
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["request_id"] == "req-style-safe-001"
    assert body["manual_review_only"] is True
    assert body["safety_pass"] is True
    assert body["risk_level"] in {"low", "medium"}
    assert body["draft_text"]
    assert body["cta_count"] <= 1
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_rejects_payload_with_obvious_pii(app_client, office_headers):
    client, _ = app_client
    payload = {**BASE_REQUEST, "deal_ref": "+79991234567"}

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 400
    assert "PII" in r.json()["detail"]


def test_style_runtime_rule0_routes_transferred_not_actual_call(app_client, office_headers):
    client, _ = app_client
    payload = {
        **BASE_REQUEST,
        "request_id": "req-style-18627495",
        "client_situation_hint": "old transferred dialogue after call",
        "last_client_message_summary": "Старая переписка читаемая, но последний контакт был звонок.",
        "last_vladimir_message_summary": "В старой переписке другой менеджер отправлял материалы.",
        "dialogue_transferred": True,
        "last_significant_contact": {
            "date": "2026-06-09",
            "channel": "call",
            "meaning": "клиент сказал, что сейчас не актуально",
        },
        "deal_context_snapshot": {
            "source_coverage": {"call_transcripts": "checked", "voice_transcripts": "not_present", "zoom_transcripts": "not_present"},
            "client_state": {"demand_status": "not_actual"},
            "materials_sent_by_vladimir": [],
            "materials_sent_by_other": ["object_catalog"],
        },
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["pack_id"] == "transferred_old_dialogue_reactivation"
    assert body["dialogue_transferred"] is True
    assert body["context_status"] == "ok"
    assert body["deal_context_snapshot"]["source_coverage"]["call_transcripts"] == "checked"
    assert "Правило #0" in body["router_reason"]
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_blocks_when_required_call_context_missing(app_client, office_headers):
    client, _ = app_client
    payload = {
        **BASE_REQUEST,
        "request_id": "req-style-missing-call",
        "dialogue_transferred": True,
        "last_significant_contact": {"channel": "call", "meaning": "клиент сказал не актуально"},
        "deal_context_snapshot": {
            "source_coverage": {"call_transcripts": "missing_required"},
            "client_state": {"demand_status": "not_actual"},
        },
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["context_status"] == "needs_context_review"
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "missing_call_context" in body["safety_flags"]
    assert "call_transcripts" in body["missing_facts"]
    assert body["show_to_vladimir"] is True
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_transferred_author_confusion_is_hard_block(app_client, office_headers, monkeypatch):
    client, main = app_client
    async def bad_writer(payload, pack_id, pack_text=""):
        return "Я отправлял вам материалы, продолжим подбор по этому варианту?"
    monkeypatch.setattr(main, "_style_write_draft", bad_writer)
    payload = {
        **BASE_REQUEST,
        "request_id": "req-style-author-confusion",
        "dialogue_transferred": True,
        "last_significant_contact": {"channel": "call", "meaning": "не актуально сейчас"},
        "deal_context_snapshot": {
            "source_coverage": {"call_transcripts": "checked"},
            "client_state": {"demand_status": "not_actual"},
            "materials_sent_by_vladimir": [],
            "materials_sent_by_other": ["object_catalog"],
        },
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "author_confusion" in body["safety_flags"]
    assert "unsupported_continuity_claim" in body["safety_flags"]


def test_style_runtime_normalizes_ai_dash_in_client_draft(app_client, office_headers, monkeypatch):
    """Тире от писателя (часто от Mac/Ollama-фолбэка) теперь НЕ блокирует карточку в
    пустоту, а чинится (запятая/дефис) до гейта — иначе тире детерминированно делало
    карточку пустой (одна из причин рецидива «пустых карточек», аудит 2026-06-15).
    Инвариант Владимира сохранён: длинного/среднего тире в тексте клиенту нет; и
    по-прежнему никаких авто-отправок/записей в CRM."""
    client, main = app_client

    async def bad_writer(payload, pack_id, pack_text=""):
        return "Здравствуйте — хотел уточнить, актуален ли ещё вопрос по покупке?"

    monkeypatch.setattr(main, "_style_write_draft", bad_writer)
    payload = {
        **BASE_REQUEST,
        "request_id": "req-style-ai-dash",
        "client_situation_hint": "followup",
        "last_client_message_summary": "Клиент просит напомнить следующий шаг.",
        "deal_stage": "question",
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    # тире вычищено — текст дошёл до карточки чистым, а не обнулился
    assert body["draft_text"]
    assert "—" not in body["draft_text"] and "–" not in body["draft_text"]
    assert "ai_dash_detected" not in body["safety_flags"]
    # безопасность: ничего не отправлено и в CRM не записано
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_blocks_internal_meta_phrase_in_client_draft(app_client, office_headers, monkeypatch):
    client, main = app_client

    async def bad_writer(payload, pack_id, pack_text=""):
        return "Если вопрос ещё актуален, могу просто обновить варианты, поэтому без подборок и давления."

    monkeypatch.setattr(main, "_style_write_draft", bad_writer)
    payload = {
        **BASE_REQUEST,
        "request_id": "req-style-meta-phrase",
        "client_situation_hint": "followup",
        "last_client_message_summary": "Клиент просит напомнить следующий шаг.",
        "deal_stage": "question",
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "internal_style_meta_phrase" in body["safety_flags"]
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_loads_approved_style_memory_only_and_excludes_candidates(app_client, office_headers, monkeypatch, tmp_path):
    client, main = app_client
    memory_file = tmp_path / "style-memory.jsonl"
    memory_file.write_text("\n".join([
        json.dumps({
            "id": "sm-approved-structure",
            "pack_id": "client_asks_question",
            "type": "full_structure",
            "text": "короткий прямой ответ -> один следующий шаг",
            "stage": ["any"],
            "channel": ["whatsapp"],
            "confidence": "approved",
            "runtime_status": "approved_not_loaded",
        }, ensure_ascii=False),
        json.dumps({
            "id": "sm-candidate-phrase",
            "pack_id": "client_asks_question",
            "type": "phrase_pattern",
            "text": "candidate phrase must never be loaded",
            "confidence": "candidate",
            "runtime_status": "review_required_before_runtime_load",
        }, ensure_ascii=False),
    ]), encoding="utf-8")
    monkeypatch.setattr(main, "STYLE_MEMORY_FILE", memory_file)
    captured = {}

    async def writer(payload, pack_id, pack_text=""):
        captured["pack_text"] = pack_text
        return "Коротко отвечу по вопросу и подскажу следующий шаг."

    monkeypatch.setattr(main, "_style_write_draft", writer)
    payload = {
        **BASE_REQUEST,
        "client_situation_hint": "followup",
        "last_client_message_summary": "Клиент просит напомнить следующий шаг.",
        "deal_stage": "question",
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["style_memory_loaded"] is True
    assert body["style_memory_example_ids"] == ["sm-approved-structure"]
    assert body["style_memory_guard_ids"] == []
    assert "sm-candidate-phrase" not in body["style_memory_example_ids"]
    assert "короткий прямой ответ" in captured["pack_text"]
    assert "candidate phrase must never be loaded" not in captured["pack_text"]
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_never_uses_contraindication_as_positive_example(app_client, office_headers, monkeypatch, tmp_path):
    client, main = app_client
    memory_file = tmp_path / "style-memory.jsonl"
    memory_file.write_text(json.dumps({
        "id": "sm-no-discussed",
        "pack_id": "client_asks_question",
        "type": "contraindication",
        "text": "Не писать «как мы обсуждали» без подтвержденного разговора.",
        "confidence": "approved",
        "runtime_status": "approved_not_loaded",
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(main, "STYLE_MEMORY_FILE", memory_file)
    captured = {}

    async def bad_writer(payload, pack_id, pack_text=""):
        captured["pack_text"] = pack_text
        return "Как мы обсуждали, могу обновить варианты."

    monkeypatch.setattr(main, "_style_write_draft", bad_writer)
    payload = {
        **BASE_REQUEST,
        "client_situation_hint": "followup",
        "last_client_message_summary": "Клиент просит напомнить следующий шаг.",
        "deal_stage": "question",
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["style_memory_loaded"] is True
    assert body["style_memory_example_ids"] == []
    assert body["style_memory_guard_ids"] == ["sm-no-discussed"]
    assert "STYLE MEMORY EXAMPLES" not in captured["pack_text"]
    assert "STYLE MEMORY GUARDS" in captured["pack_text"]
    assert body["safety_pass"] is False
    assert body["draft_text"] == ""
    assert "style_memory_contraindication" in body["safety_flags"]
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


def test_style_runtime_ready_draft_has_no_forbidden_client_markers_with_style_memory(app_client, office_headers, monkeypatch, tmp_path):
    client, main = app_client
    memory_file = tmp_path / "style-memory.jsonl"
    memory_file.write_text(json.dumps({
        "id": "sm-approved-start",
        "pack_id": "client_asks_question",
        "type": "start",
        "text": "[Имя], здравствуйте.",
        "confidence": "approved",
        "runtime_status": "approved_not_loaded",
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(main, "STYLE_MEMORY_FILE", memory_file)

    async def writer(payload, pack_id, pack_text=""):
        return "Коротко отвечу по вопросу и подскажу следующий шаг."

    monkeypatch.setattr(main, "_style_write_draft", writer)
    payload = {
        **BASE_REQUEST,
        "client_situation_hint": "followup",
        "last_client_message_summary": "Клиент просит напомнить следующий шаг.",
        "deal_stage": "question",
        "facts_available": ["object_ref", "price_source_ref"],
    }

    r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["safety_pass"] is True
    assert body["draft_text"]
    assert "—" not in body["draft_text"]
    assert "–" not in body["draft_text"]
    assert "без давления" not in body["draft_text"].lower()
    assert "без подборок" not in body["draft_text"].lower()
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False



def test_style_write_draft_real_http_path_sends_merged_pack_text_with_memory(app_client, office_headers, monkeypatch, tmp_path):
    """Regression for audit blocker 1: do not monkeypatch _style_write_draft.

    The backend endpoint must call the real HTTP writer and include the merged
    pack_text (runtime pack + approved Style Memory) in POST /v1/draft.
    """
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer

    client, main = app_client
    memory_file = tmp_path / "style-memory.jsonl"
    memory_file.write_text(json.dumps({
        "id": "sm-http-approved-structure",
        "pack_id": "client_asks_question",
        "type": "full_structure",
        "text": "короткий прямой ответ -> один следующий шаг",
        "stage": ["question"],
        "channel": ["whatsapp"],
        "confidence": "approved",
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(main, "STYLE_MEMORY_FILE", memory_file)
    monkeypatch.setattr(main, "STYLE_RUNTIME_SOURCE", "local")
    monkeypatch.setattr(main, "_style_load_runtime_pack", lambda pack_id: {
        "ok": True,
        "source": "local",
        "pack_text": "BASE PACK TEXT",
        "pack_path": str(tmp_path / f"{pack_id}.md"),
        "pack_sha256": "test-sha",
    })

    received = {}

    class DraftHandler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_POST(self):
            assert self.path == "/v1/draft"
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length))
            received["body"] = body
            pack_text = body.get("pack_text") or ""
            response = {
                "ok": True,
                "draft_text": "Коротко отвечу по вопросу и подскажу следующий шаг.",
                "pack_text_source": "request" if pack_text else "disk",
                "style_memory_example_ids": ["sm-http-approved-structure"] if "sm-http-approved-structure" in pack_text else [],
            }
            data = json.dumps(response, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    server = HTTPServer(("127.0.0.1", 0), DraftHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(main, "STYLE_RUNTIME_HTTP_BASE_URL", f"http://127.0.0.1:{server.server_port}")
    try:
        payload = {
            **BASE_REQUEST,
            "request_id": "req-style-real-http-memory",
            "client_situation_hint": "followup",
            "last_client_message_summary": "Клиент просит напомнить следующий шаг.",
            "deal_stage": "question",
            "facts_available": ["object_ref", "price_source_ref"],
        }
        r = client.post("/style-runtime/v1/draft", headers=office_headers, json=payload)
    finally:
        server.shutdown()
        server.server_close()

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["style_memory_loaded"] is True
    assert body["style_memory_example_ids"] == ["sm-http-approved-structure"]
    assert body["safety_pass"] is True
    assert received["body"]["pack_id"] == "client_asks_question"
    assert received["body"]["payload"]["request_id"] == "req-style-real-http-memory"
    assert "BASE PACK TEXT" in received["body"]["pack_text"]
    assert "sm-http-approved-structure" in received["body"]["pack_text"]
    assert "короткий прямой ответ" in received["body"]["pack_text"]


def test_auto_draft_on_client_reply_wires_approved_style_memory_into_pack_and_safety(app_client, monkeypatch, tmp_path):
    client, main = app_client
    memory_file = tmp_path / "style-memory.jsonl"
    memory_file.write_text(json.dumps({
        "id": "sm-auto-no-discussed",
        "pack_id": "client_asks_question",
        "type": "contraindication",
        "text": "Не писать «как мы обсуждали» без подтвержденного разговора.",
        "stage": ["question"],
        "channel": ["telegram"],
        "confidence": "approved",
    }, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setattr(main, "STYLE_MEMORY_FILE", memory_file)
    monkeypatch.setattr(main, "_style_load_runtime_pack", lambda pack_id: {
        "ok": True,
        "source": "local",
        "pack_text": "BASE PACK TEXT",
    })
    captured = {}

    async def writer(payload, pack_id, pack_text=""):
        captured["pack_id"] = pack_id
        captured["pack_text"] = pack_text
        return "Как мы обсуждали, могу обновить варианты."

    pushes = []

    async def fake_push_to_all(**kwargs):
        pushes.append(kwargs)
        return 0

    monkeypatch.setattr(main, "_style_write_draft", writer)
    monkeypatch.setattr(main, "send_push_to_all", fake_push_to_all)

    task = {
        "task_id": "task-auto-style-memory",
        "lead_id": "lead-auto-style-memory",
        "preferred_channel": "telegram",
        "action_state": "client_replied",
        "messengers": ["telegram"],
    }
    main.asyncio.run(main._auto_draft_on_client_reply(task, "Клиент задал вопрос"))

    assert captured["pack_id"] == "client_asks_question"
    assert "BASE PACK TEXT" in captured["pack_text"]
    assert "STYLE MEMORY GUARDS" in captured["pack_text"]
    assert "sm-auto-no-discussed" in captured["pack_text"]
    assert len(main.office_drafts) == 1
    draft = main.office_drafts[0]
    assert draft["entity_id"] == "lead-auto-style-memory"
    assert draft["manual_review_only"] is True
    assert draft["text"].startswith("[Черновик заблокирован:")
    assert "style_memory_contraindication" in draft["safety_flags"]
    assert draft["block_reason"]
    assert pushes
    assert pushes[0]["body"].startswith("[Черновик заблокирован:")



def test_mac_style_runtime_server_uses_provided_pack_text_instead_of_disk(monkeypatch, tmp_path):
    """Regression for Mac/Ollama writer: request pack_text must drive prompt.

    This exercises the real HTTP /v1/draft handler. No runtime pack file is
    created on disk; success proves provided pack_text is used instead.
    """
    import http.client
    import importlib.util
    import threading
    from http.server import HTTPServer

    server_path = Path(__file__).resolve().parent.parent / "tools" / "style_runtime_server.py"
    spec = importlib.util.spec_from_file_location("style_runtime_server_test_mod", server_path)
    assert spec and spec.loader
    server_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(server_mod)

    monkeypatch.setattr(server_mod, "PUBLIC_DIR", tmp_path)
    monkeypatch.setattr(server_mod, "READ_TOKEN", "")
    captured = {}

    class FakeOllamaResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps({"message": {"content": "Коротко отвечу и предложу следующий шаг."}}, ensure_ascii=False).encode("utf-8")

    def fake_urlopen(req, timeout=60):
        captured["ollama_body"] = json.loads(req.data.decode("utf-8"))
        return FakeOllamaResponse()

    monkeypatch.setattr(server_mod.urllib.request, "urlopen", fake_urlopen)
    httpd = HTTPServer(("127.0.0.1", 0), server_mod.StyleRuntimeHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        body = json.dumps({
            "pack_id": "client_asks_question",
            "pack_text": "BASE PACK\n\nSTYLE MEMORY EXAMPLES (approved only):\n- sm-server-approved [full_structure]: коротко -> следующий шаг",
            "payload": {
                "channel": "whatsapp",
                "deal_stage": "question",
                "last_client_message_summary": "Клиент задал вопрос.",
                "facts_available": ["price_source_ref"],
            },
        }, ensure_ascii=False).encode("utf-8")
        conn = http.client.HTTPConnection("127.0.0.1", httpd.server_port, timeout=10)
        conn.request("POST", "/v1/draft", body=body, headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        response = json.loads(resp.read())
        conn.close()
    finally:
        httpd.shutdown()
        httpd.server_close()

    assert response["ok"] is True
    assert response["draft_text"] == "Коротко отвечу и предложу следующий шаг."
    assert response["pack_text_source"] == "request"
    assert response["style_memory_example_ids"] == ["sm-server-approved"]
    system_prompt = captured["ollama_body"]["messages"][0]["content"]
    assert "BASE PACK" in system_prompt
    assert "sm-server-approved" in system_prompt


def test_style_runtime_feedback_persists_sanitized_event_without_raw_text(app_client, office_headers):
    client, main = app_client
    event = {
        "event_id": "evt-style-001",
        "request_id": "req-style-safe-001",
        "deal_ref": "deal_hash_abc",
        "message_context_ref": "ctx_hash_123",
        "selected_pack_id": "client_asks_question",
        "secondary_pack_ids": ["price_roi_explanation"],
        "draft_id": "draft-style-001",
        "draft_version": 1,
        "feedback_type": "too_long",
        "feedback_text_sanitized": "Нужно короче и сразу ответить на вопрос.",
        "deal_situation_type": "transferred",
        "user_action": "edited_and_approved",
    }

    r = client.post("/style-runtime/v1/feedback", headers=office_headers, json=event)

    assert r.status_code == 200, r.text
    assert r.json()["status"] == "ok"
    lines = Path(main.STYLE_RUNTIME_FEEDBACK_FILE).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    saved = json.loads(lines[0])
    assert saved["event_id"] == "evt-style-001"
    assert saved["promotion_status"] == "candidate_observation"
    assert saved["deal_situation_type"] == "transferred"
    assert saved["no_send_side_effect"] is True
    assert saved["feedback_text_sanitized"] == "Нужно короче и сразу ответить на вопрос."


def test_style_runtime_feedback_rejects_unsanitized_pii(app_client, office_headers):
    client, _ = app_client
    event = {
        "event_id": "evt-style-pii",
        "deal_ref": "deal_hash_abc",
        "selected_pack_id": "client_asks_question",
        "draft_id": "draft-style-001",
        "feedback_type": "other",
        "feedback_text_sanitized": "Клиент Иван с телефоном +79991234567 просит цену",
        "user_action": "rejected",
    }

    r = client.post("/style-runtime/v1/feedback", headers=office_headers, json=event)

    assert r.status_code == 400
    assert "PII" in r.json()["detail"]
