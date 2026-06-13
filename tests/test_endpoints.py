"""Этап 2.4 + Packet C: backend integration tests.

Этап 2.4 invariants (incidents-driven):
1. /health без auth → 200 (Render uptime check).
2. /status требует bearer token.
3. heartbeat → health/scheduler (iOS green dot).
4. silent_ack НЕ ставит pending_status_change (инцидент Mary Land 2026-05-02).
5. /api/devices/register idempotent.

Packet C: office draft inbox.
"""
import json
from pathlib import Path


def test_health_no_auth(app_client):
    """1. /health отвечает 200 без bearer (это uptime endpoint Render'а)."""
    client, main = app_client
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_status_requires_widget_token(app_client, widget_headers):
    """2. /status без bearer → 401, c bearer → 200."""
    client, main = app_client
    r = client.get("/status")
    assert r.status_code == 401, "должен быть 401 без auth"

    r = client.get("/status", headers=widget_headers)
    assert r.status_code == 200, f"с bearer должен быть 200, получили {r.status_code}: {r.text}"
    body = r.json()
    # Schema (StatusResponse в iOS): color/updated_at/revert_at — могут быть null
    # если state.json пуст, но ключи должны существовать или быть None.
    assert "color" in body or body.get("color") is None


def test_heartbeat_then_health_scheduler(app_client, widget_headers, internal_headers):
    """3. POST heartbeat → файл создан → GET health/scheduler говорит online=True
    с age_sec < 90. Прямая связка для зелёной точки в iOS (Этап 1.1)."""
    client, main = app_client
    # До heartbeat'а файла нет → online=False
    r = client.get("/api/health/scheduler", headers=widget_headers)
    assert r.status_code == 200
    assert r.json().get("online") is False

    # POST heartbeat
    r = client.post(
        "/api/internal/heartbeat",
        headers=internal_headers,
        json={"pid": 12345, "workers": ["heartbeat", "send"]},
    )
    assert r.status_code == 200, f"heartbeat POST failed: {r.status_code} {r.text}"

    # Файл есть, age_sec < 90 → online=True
    assert Path(main.SCHEDULER_HEARTBEAT_FILE).is_file()
    r = client.get("/api/health/scheduler", headers=widget_headers)
    body = r.json()
    assert body.get("online") is True, f"должен быть online=True, body={body}"
    assert body.get("age_sec") is not None and body["age_sec"] < 90


def test_silent_ack_does_not_trigger_status_change(app_client, internal_headers):
    """4. Регрессия Mary Land 2026-05-02: silent_ack — внутренний ack без
    pending_status_change. Иначе scheduler PATCH'ит AmoCRM на V_RABOTE,
    AmoCRM Salesbot триггерится → создаёт ненужную задачу «Новое сообщение».

    Этот тест: создаём фейковый лид через /api/internal/lead, вызываем
    silent_ack, проверяем что его НЕТ в /needs_status_change."""
    client, main = app_client
    # Создаём лид
    r = client.post(
        "/api/internal/lead",
        headers=internal_headers,
        json={
            "lead_id": 999001,
            "name": "Тестовый Клиент",
            "phone": "+79991234567",
            "status_id": 67579214,
        },
    )
    assert r.status_code in (200, 201), f"lead create failed: {r.status_code} {r.text}"

    # Silent ack
    r = client.post(
        f"/api/internal/leads/999001/silent_ack",
        headers=internal_headers,
    )
    assert r.status_code == 200, f"silent_ack failed: {r.status_code} {r.text}"

    # needs_status_change должен НЕ содержать этого лида
    r = client.get("/api/internal/leads/needs_status_change", headers=internal_headers)
    assert r.status_code == 200
    pending = (r.json() or {}).get("leads") or []
    pending_ids = {p.get("lead_id") for p in pending}
    assert 999001 not in pending_ids, (
        f"silent_ack НЕ должен ставить pending_status_change "
        f"(иначе scheduler триггерит AmoCRM Salesbot). "
        f"pending_ids={pending_ids}"
    )


def test_devices_register_persists_token(app_client, widget_headers):
    """5. POST /api/devices/register сохраняет device_token в DEVICES_FILE.
    Этап 1.5 retry-механизм требует чтобы register был идемпотентным —
    можно слать повторно, токен один."""
    client, main = app_client
    token = "abcd" * 16  # 64-char fake APNs hex token
    r = client.post(
        "/api/devices/register",
        headers=widget_headers,
        json={"device_token": token, "app_version": "29(29)"},
    )
    assert r.status_code == 200, f"register failed: {r.status_code} {r.text}"

    # Файл существует, токен внутри
    assert Path(main.DEVICES_FILE).is_file()
    data = json.loads(Path(main.DEVICES_FILE).read_text())
    # Структура может быть list или dict — главное чтобы token присутствовал
    serialized = json.dumps(data)
    assert token in serialized, f"токен не найден в devices.json: {serialized[:200]}"

    # Идемпотентность: повторный POST не падает
    r2 = client.post(
        "/api/devices/register",
        headers=widget_headers,
        json={"device_token": token, "app_version": "29(29)"},
    )
    assert r2.status_code == 200, "повторный register должен быть idempotent"


def test_tasks_today_enriches_ui_context_from_leads_inbox(app_client, widget_headers, internal_headers):
    """Regression: old scheduler snapshots may miss tz/channel/draft fields.

    GET /api/tasks/today should restore display-only context from leads_inbox when
    available, without CRM writes or sending anything.
    """
    client, _ = app_client
    lead_id = 999777
    client.post(
        "/api/internal/lead",
        headers=internal_headers,
        json={
            "lead_id": lead_id,
            "name": "Наталья",
            "phone": "+79990001122",
            "client_tz_offset_min": 180,
            "client_tz_label": "Санкт-Петербург",
            "request_text": "В каком мессенджере с вами удобнее связаться?: WhatsApp",
            "start_message": "Наталья, здравствуйте. Получил вашу заявку, скоро свяжусь.",
        },
    )
    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [{
            "task_id": 991,
            "lead_id": lead_id,
            "due": "2099-01-01T10:00:00+07:00",
            "created_by": 0,
            "created_by_name": "system",
            "task_text": "Связаться",
            "lead_name": "Наталья",
            "phone": "",
            "messengers": [],
            "last_incoming_channel": "",
            "last_message_channel": "",
            "whatsapp_phone": "",
            "telegram_username": "",
            "amocrm_url": "https://example.invalid/leads/detail/999777",
            "context_summary": "",
            "suggested_message": "",
        }]},
    )
    assert r.status_code == 200

    r = client.get("/api/tasks/today", headers=widget_headers)
    assert r.status_code == 200
    task = r.json()["tasks"][0]
    assert task["client_tz_offset_min"] == 180
    assert task["client_tz_label"] == "Санкт-Петербург"
    assert task["last_message_channel"] == "whatsapp"
    assert "whatsapp" in task["messengers"]
    assert task["whatsapp_phone"]
    assert task["suggested_message"] == "", (
        "GET /api/tasks/today must not reuse new-lead start_message/timer_3min_text "
        "as a task-specific draft when scheduler supplied an empty suggested_message"
    )


def test_tasks_today_falls_back_to_phone_timezone_without_leads_inbox(app_client, widget_headers, internal_headers):
    """Regression: stale tasks without leads_inbox still show safe regional time by phone."""
    client, _ = app_client
    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [{
            "task_id": 992,
            "lead_id": 999778,
            "due": "2099-01-01T10:00:00+07:00",
            "created_by": 0,
            "created_by_name": "system",
            "task_text": "Связаться",
            "lead_name": "Клиент",
            "phone": "+79990001123",
            "messengers": [],
            "last_incoming_channel": "",
            "last_message_channel": "",
            "whatsapp_phone": "",
            "telegram_username": "",
            "amocrm_url": "https://example.invalid/leads/detail/999778",
            "context_summary": "",
            "suggested_message": "",
        }]},
    )
    assert r.status_code == 200

    r = client.get("/api/tasks/today", headers=widget_headers)
    assert r.status_code == 200
    task = r.json()["tasks"][0]
    assert task["client_tz_offset_min"] == 180
    assert task["client_tz_label"] == "Россия / Москва"


def test_tasks_today_marks_missing_contact_context_without_leads_inbox(app_client, widget_headers, internal_headers):
    """Regression: blank task cards must show an explicit contact blocker.

    If neither scheduler payload nor leads_inbox can provide contact/channel/time,
    backend must not silently return an empty action area and must not fabricate a
    task draft from new-lead fallbacks.
    """
    client, _ = app_client
    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [{
            "task_id": 993,
            "lead_id": 999779,
            "due": "2099-01-01T10:00:00+07:00",
            "created_by": 12933886,
            "created_by_name": "Владимир",
            "task_text": "Связаться",
            "lead_name": "Клиент",
            "phone": "",
            "messengers": [],
            "last_incoming_channel": "",
            "last_message_channel": "",
            "whatsapp_phone": "",
            "telegram_username": "",
            "amocrm_url": "https://example.invalid/leads/detail/999779",
            "context_summary": "",
            "suggested_message": "",
        }]},
    )
    assert r.status_code == 200

    r = client.get("/api/tasks/today", headers=widget_headers)
    assert r.status_code == 200
    task = r.json()["tasks"][0]
    assert task["contact_lookup_status"] == "contact_missing"
    assert "Контакт" in task["contact_action_blocker"]
    assert task["suggested_message"] == ""


# ---------------------------------------------------------------------------
# Packet C — office draft inbox
# ---------------------------------------------------------------------------

import time

_FUTURE_EXPIRES = "2099-01-01T00:00:00+00:00"
_PAST_EXPIRES = "2000-01-01T00:00:00+00:00"


def _draft(draft_id="d1", expires_at=_FUTURE_EXPIRES, **extra):
    return {
        "draft_id": draft_id,
        "entity_id": "lead-42",
        "text": "Добрый день! Ваша заявка рассмотрена.",
        "category": "reply",
        "expires_at": expires_at,
        **extra,
    }


def test_office_create_requires_office_token(app_client, widget_headers):
    """WIDGET_TOKEN не может создавать drafts (компрометация iOS → фальшивые drafts)."""
    client, _ = app_client
    r = client.post("/api/office/drafts", headers=widget_headers, json=_draft())
    assert r.status_code == 401, f"WIDGET_TOKEN should be rejected for office write: {r.status_code}"


def test_office_create_success(app_client, office_headers):
    """Office создаёт draft с OFFICE_TOKEN → status=created."""
    client, _ = app_client
    r = client.post("/api/office/drafts", headers=office_headers, json=_draft("d-ok"))
    assert r.status_code == 200, f"create failed: {r.status_code} {r.text}"
    body = r.json()
    assert body.get("status") == "created"
    assert body.get("draft_id") == "d-ok"


def test_office_create_duplicate_no_duplicate_pending(app_client, office_headers, widget_headers):
    """Повторный POST с тем же draft_id → update, не дублирует запись в pending."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-dup"))
    r2 = client.post("/api/office/drafts", headers=office_headers,
                     json=_draft("d-dup", text="Обновлённый текст"))
    assert r2.status_code == 200
    assert r2.json().get("status") == "updated"

    r = client.get("/api/office/drafts/pending", headers=widget_headers)
    drafts = r.json().get("drafts", [])
    ids = [d["draft_id"] for d in drafts]
    assert ids.count("d-dup") == 1, f"должна быть одна запись, получили: {ids}"


def test_office_pending_excludes_expired(app_client, office_headers, widget_headers):
    """GET pending не возвращает истёкшие drafts."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers,
                json=_draft("d-fresh", expires_at=_FUTURE_EXPIRES))
    client.post("/api/office/drafts", headers=office_headers,
                json=_draft("d-stale", expires_at=_PAST_EXPIRES))

    r = client.get("/api/office/drafts/pending", headers=widget_headers)
    assert r.status_code == 200
    ids = {d["draft_id"] for d in r.json().get("drafts", [])}
    assert "d-fresh" in ids, "свежий draft должен быть в pending"
    assert "d-stale" not in ids, "истёкший draft не должен быть в pending"


def test_approve_pending_success(app_client, office_headers, widget_headers):
    """WIDGET_TOKEN approves pending draft → status=ok, draft is approved."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-appr"))

    r = client.post("/api/office/drafts/d-appr/approve", headers=widget_headers,
                    json={"approval_id": "appr-1"})
    assert r.status_code == 200, f"approve failed: {r.status_code} {r.text}"
    assert r.json().get("status") == "ok"

    # draft должен появиться в approved internal list
    r2 = client.get("/api/internal/office/drafts/approved", headers=office_headers)
    approved_ids = {d["draft_id"] for d in r2.json().get("drafts", [])}
    assert "d-appr" in approved_ids


def test_approve_idempotent_same_approval_id(app_client, office_headers, widget_headers):
    """Повторный approve с тем же approval_id → no-op (idempotent)."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-idem"))
    client.post("/api/office/drafts/d-idem/approve", headers=widget_headers,
                json={"approval_id": "appr-x"})
    r = client.post("/api/office/drafts/d-idem/approve", headers=widget_headers,
                    json={"approval_id": "appr-x"})
    assert r.status_code == 200
    assert r.json().get("idempotent") is True


def test_approve_expired_returns_410(app_client, office_headers, widget_headers):
    """Approve истёкшего draft → 410 Gone."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers,
                json=_draft("d-exp", expires_at=_PAST_EXPIRES))
    r = client.post("/api/office/drafts/d-exp/approve", headers=widget_headers, json={})
    assert r.status_code == 410, f"должен быть 410, получили {r.status_code}: {r.text}"


def test_approve_rejected_draft_returns_409(app_client, office_headers, widget_headers):
    """Approve уже отклонённого draft → 409."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-rej"))
    client.post("/api/office/drafts/d-rej/reject", headers=widget_headers,
                json={"reject_reason": "не то"})
    r = client.post("/api/office/drafts/d-rej/approve", headers=widget_headers, json={})
    assert r.status_code == 409, f"должен быть 409, получили {r.status_code}: {r.text}"


def test_pending_excludes_approved_and_rejected(app_client, office_headers, widget_headers):
    """GET pending не возвращает одобренные и отклонённые drafts."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-p1"))
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-p2"))
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-p3"))

    client.post("/api/office/drafts/d-p2/approve", headers=widget_headers, json={})
    client.post("/api/office/drafts/d-p3/reject", headers=widget_headers, json={})

    r = client.get("/api/office/drafts/pending", headers=widget_headers)
    ids = {d["draft_id"] for d in r.json().get("drafts", [])}
    assert "d-p1" in ids
    assert "d-p2" not in ids
    assert "d-p3" not in ids


def test_approved_internal_list_excludes_pending_and_rejected(app_client, office_headers, widget_headers):
    """GET approved internal returns only approved, not pending/rejected."""
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-a1"))
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-a2"))
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-a3"))

    client.post("/api/office/drafts/d-a2/approve", headers=widget_headers, json={})
    client.post("/api/office/drafts/d-a3/reject", headers=widget_headers, json={})

    r = client.get("/api/internal/office/drafts/approved", headers=office_headers)
    assert r.status_code == 200
    ids = {d["draft_id"] for d in r.json().get("drafts", [])}
    assert "d-a2" in ids
    assert "d-a1" not in ids
    assert "d-a3" not in ids


def _variant(variant_id="v1", text="Вариант текста", **extra):
    return {
        "id": variant_id,
        "text": text,
        "rationale": "Показываем только Владимиру",
        "label": "Мягко",
        "em_dash_lint": True,
        **extra,
    }


def _get_draft(client, draft_id, widget_headers):
    r = client.get(f"/api/office/drafts/{draft_id}", headers=widget_headers)
    assert r.status_code == 200, f"get draft failed: {r.status_code} {r.text}"
    return r.json()


def test_office_create_accepts_structured_variants_and_readback_aliases(app_client, office_headers, widget_headers):
    client, _ = app_client
    variants = [_variant("soft", "Мягкий вариант"), _variant("direct", "Прямой вариант")]

    r = client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-structured", structured_variants=variants),
    )

    assert r.status_code == 200, f"create failed: {r.status_code} {r.text}"
    draft = _get_draft(client, "d-structured", widget_headers)
    assert draft["status"] == "pending"
    assert draft["structured_variants"] == variants
    assert draft["variants"] == variants

    r_pending = client.get("/api/office/drafts/pending", headers=widget_headers)
    pending = {d["draft_id"]: d for d in r_pending.json().get("drafts", [])}
    assert pending["d-structured"]["structured_variants"] == variants
    assert pending["d-structured"]["variants"] == variants


def test_office_create_accepts_legacy_variants_and_backfills_structured_alias(app_client, office_headers, widget_headers):
    client, _ = app_client
    variants = [_variant("legacy", "Старый вариант")]

    r = client.post("/api/office/drafts", headers=office_headers, json=_draft("d-legacy", variants=variants))

    assert r.status_code == 200, f"legacy create failed: {r.status_code} {r.text}"
    draft = _get_draft(client, "d-legacy", widget_headers)
    assert draft["structured_variants"] == variants
    assert draft["variants"] == variants


def test_office_create_rejects_invalid_structured_variants(app_client, office_headers):
    client, _ = app_client

    conflicting = client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft(
            "d-conflict",
            structured_variants=[_variant("a", "A")],
            variants=[_variant("b", "B")],
        ),
    )
    assert conflicting.status_code == 400

    duplicate_ids = client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-dupe-variants", structured_variants=[_variant("same", "A"), _variant("same", "B")]),
    )
    assert duplicate_ids.status_code == 400

    empty_text = client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-empty-variant", structured_variants=[_variant("empty", "")]),
    )
    assert empty_text.status_code == 400

    forbidden = client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-send-key", structured_variants=[_variant("bad", "Text", needs_send=True)]),
    )
    assert forbidden.status_code == 400


def test_office_patch_accepts_structured_variants_and_preserves_no_send_fields(app_client, office_headers, widget_headers):
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-patch-variants"))
    variants = [_variant("regen", "Новый вариант после регена")]

    r = client.patch(
        "/api/internal/office/drafts/d-patch-variants",
        headers=office_headers,
        json={"text": "Обновлённый текст", "structured_variants": variants},
    )

    assert r.status_code == 200, f"patch failed: {r.status_code} {r.text}"
    draft = _get_draft(client, "d-patch-variants", widget_headers)
    assert draft["status"] == "pending"
    assert draft["structured_variants"] == variants
    assert draft["variants"] == variants
    for key in ("send_trace_id", "claimed_at", "claimed_by", "claim_expires_at", "send_status", "consumed_at", "consumed_by_scheduler_at"):
        assert draft.get(key) is None


def test_office_patch_rejects_approval_or_send_status_side_effect(app_client, office_headers):
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-patch-no-approve"))

    r = client.patch(
        "/api/internal/office/drafts/d-patch-no-approve",
        headers=office_headers,
        json={"status": "approved"},
    )

    assert r.status_code == 400
    approved = client.get("/api/internal/office/drafts/approved", headers=office_headers).json().get("drafts", [])
    assert "d-patch-no-approve" not in {d["draft_id"] for d in approved}


def test_office_duplicate_update_refreshes_structured_variants_when_unselected(app_client, office_headers, widget_headers):
    client, _ = app_client
    client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-update-variants", structured_variants=[_variant("old", "Старый вариант")]),
    )

    r = client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-update-variants", text="Обновлённый основной текст", structured_variants=[_variant("new", "Новый вариант")]),
    )

    assert r.status_code == 200
    assert r.json().get("status") == "updated"
    draft = _get_draft(client, "d-update-variants", widget_headers)
    assert draft["text"] == "Обновлённый основной текст"
    assert draft["status"] == "pending"
    assert draft["structured_variants"] == [_variant("new", "Новый вариант")]
    pending = client.get("/api/office/drafts/pending", headers=widget_headers).json().get("drafts", [])
    assert [d["draft_id"] for d in pending].count("d-update-variants") == 1


def test_select_variant_promotes_text_but_does_not_approve_or_send(app_client, office_headers, widget_headers):
    client, _ = app_client
    variants = [_variant("a", "Первый вариант"), _variant("b", "Выбранный вариант")]
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-select", structured_variants=variants))

    r = client.post(
        "/api/office/drafts/d-select/select_variant",
        headers=widget_headers,
        json={"variant_id": "b", "reason": "лучше", "decision_id": "dec-select-1"},
    )

    assert r.status_code == 200, f"select failed: {r.status_code} {r.text}"
    body = r.json()
    assert body["status"] == "ok"
    assert body["draft_status"] == "pending"
    assert body["selected_variant_id"] == "b"
    assert body["text"] == "Выбранный вариант"
    assert body["send_performed"] is False
    assert body["backend_sent_message"] is False
    assert body["crm_mutated"] is False
    assert body["queue_cache_mutated"] is False
    assert body["approval_changed"] is False

    draft = _get_draft(client, "d-select", widget_headers)
    assert draft["status"] == "pending"
    assert draft["text"] == "Выбранный вариант"
    assert draft["selected_variant_id"] == "b"
    assert len(draft["structured_variant_decisions"]) == 1
    for key in ("send_trace_id", "claimed_at", "claimed_by", "claim_expires_at", "send_status", "consumed_at", "consumed_by_scheduler_at"):
        assert draft.get(key) is None

    approved = client.get("/api/internal/office/drafts/approved", headers=office_headers).json().get("drafts", [])
    assert "d-select" not in {d["draft_id"] for d in approved}


def test_select_variant_idempotency_and_conflict(app_client, office_headers, widget_headers):
    client, _ = app_client
    client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-select-idem", structured_variants=[_variant("a", "A"), _variant("b", "B")]),
    )
    payload = {"variant_id": "a", "reason": "same", "decision_id": "dec-1"}

    first = client.post("/api/office/drafts/d-select-idem/select_variant", headers=widget_headers, json=payload)
    second = client.post("/api/office/drafts/d-select-idem/select_variant", headers=widget_headers, json=payload)
    conflict = client.post(
        "/api/office/drafts/d-select-idem/select_variant",
        headers=widget_headers,
        json={"variant_id": "b", "reason": "same", "decision_id": "dec-1"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json().get("idempotent") is True
    assert conflict.status_code == 409
    draft = _get_draft(client, "d-select-idem", widget_headers)
    assert len(draft["structured_variant_decisions"]) == 1
    assert len(draft["variant_history"]) == 1


def test_select_variant_rejects_approved_or_unknown_without_extra_mutation(app_client, office_headers, widget_headers):
    client, _ = app_client
    client.post("/api/office/drafts", headers=office_headers, json=_draft("d-unknown", structured_variants=[_variant("a", "A")]))

    missing = client.post(
        "/api/office/drafts/d-unknown/select_variant",
        headers=widget_headers,
        json={"variant_id": "missing", "decision_id": "dec-missing"},
    )
    assert missing.status_code == 400
    draft = _get_draft(client, "d-unknown", widget_headers)
    assert draft["text"] == "Добрый день! Ваша заявка рассмотрена."
    assert draft["selected_variant_id"] is None
    assert draft["structured_variant_decisions"] == []

    client.post("/api/office/drafts/d-unknown/approve", headers=widget_headers, json={"approval_id": "appr-after-missing"})
    approved_select = client.post(
        "/api/office/drafts/d-unknown/select_variant",
        headers=widget_headers,
        json={"variant_id": "a", "decision_id": "dec-after-approve"},
    )
    assert approved_select.status_code in (400, 409)
    approved = client.get("/api/internal/office/drafts/approved", headers=office_headers).json().get("drafts", [])
    assert "d-unknown" in {d["draft_id"] for d in approved}


def test_old_stored_draft_with_only_variants_returns_structured_variants(app_client, widget_headers):
    client, main = app_client
    old_variant = _variant("legacy-only", "Только legacy")
    main.office_drafts.append({
        **_draft("d-old-storage"),
        "status": "pending",
        "created_at": "2026-01-01T00:00:00+00:00",
        "updated_at": "2026-01-01T00:00:00+00:00",
        "variants": [old_variant],
    })

    draft = _get_draft(client, "d-old-storage", widget_headers)

    assert draft["structured_variants"] == [old_variant]
    assert draft["variants"] == [old_variant]


def test_structured_variants_keep_approve_claim_consume_dry_run_contract(app_client, office_headers, widget_headers):
    client, _ = app_client
    client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft("d-send-contract", structured_variants=[_variant("a", "A")]),
    )
    approve = client.post("/api/office/drafts/d-send-contract/approve", headers=widget_headers, json={"approval_id": "appr-contract"})
    approved = client.get("/api/internal/office/drafts/approved", headers=office_headers)
    claim = client.post(
        "/api/internal/office/drafts/d-send-contract/claim",
        headers=office_headers,
        json={"claimed_by": "test-scheduler"},
    )
    trace = claim.json().get("send_trace_id")
    consume = client.post(
        "/api/internal/office/drafts/d-send-contract/consume",
        headers=office_headers,
        json={"send_trace_id": trace, "send_status": "dry_run"},
    )

    assert approve.status_code == 200
    assert approved.status_code == 200
    assert "d-send-contract" in {d["draft_id"] for d in approved.json().get("drafts", [])}
    assert claim.status_code == 200
    assert trace
    assert consume.status_code == 200
    assert consume.json().get("status") == "dry_run_consumed"

def test_office_draft_create_preserves_style_runtime_safety_fields(app_client, office_headers, widget_headers):
    client, _ = app_client
    r = client.post(
        "/api/office/drafts",
        headers=office_headers,
        json=_draft(
            "d-style-context",
            pack_id="price_roi_explanation",
            risk_level="high",
            missing_facts=["price_source_ref"],
            safety_flags=["price_without_source"],
            block_reason="Нельзя подставлять цену без источника.",
            manual_review_only=True,
        ),
    )
    assert r.status_code == 200, r.text

    draft = _get_draft(client, "d-style-context", widget_headers)
    assert draft["pack_id"] == "price_roi_explanation"
    assert draft["risk_level"] == "high"
    assert draft["missing_facts"] == ["price_source_ref"]
    assert draft["safety_flags"] == ["price_without_source"]
    assert draft["block_reason"] == "Нельзя подставлять цену без источника."
    assert draft["manual_review_only"] is True


# ---------------------------------------------------------------------------
# LS-TZ-MIKHAIL-1108 — Sakhalin timezone transport regression
# ---------------------------------------------------------------------------


def test_internal_tasks_normalizes_sakhalin_timezone_not_moscow(app_client, internal_headers, widget_headers):
    """If CRM/source city says Sakhalin, iOS payload must not show Moscow (+3)."""
    client, _ = app_client

    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={
            "tasks": [
                {
                    "task_id": 1108,
                    "lead_id": 501108,
                    "lead_name": "Михаил",
                    "phone": "+7 *** *** 1108",
                    "client_city": "Сахалин",
                    # Stale scheduler/phone fallback values that caused the incident.
                    "client_tz_offset_min": 180,
                    "client_tz_label": "Россия / Москва",
                }
            ]
        },
    )
    assert r.status_code == 200, f"internal tasks failed: {r.status_code} {r.text}"

    today = client.get("/api/tasks/today", headers=widget_headers)
    assert today.status_code == 200
    task = today.json()["tasks"][0]
    assert task["phone"].endswith("1108")
    assert task["client_city"] == "Сахалин"
    assert task["client_tz_offset_min"] == 660
    assert "Сахалин" in task["client_tz_label"]
    assert "Москва" not in task["client_tz_label"]
    assert task["client_tz_offset_min"] > 420  # Sakhalin is ahead of Phuket/Bangkok UTC+7.


def test_internal_tasks_normalizes_yuzhno_sakhalinsk_alias(app_client, internal_headers, widget_headers):
    client, _ = app_client

    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={
            "tasks": [
                {
                    "task_id": 1109,
                    "lead_id": 501109,
                    "lead_name": "Synthetic",
                    "phone": "+7 *** *** 1108",
                    "client_city": "Южно-Сахалинск",
                    "client_tz_offset_min": 180,
                    "client_tz_label": "Россия / Москва",
                }
            ]
        },
    )
    assert r.status_code == 200

    task = client.get("/api/tasks/today", headers=widget_headers).json()["tasks"][0]
    assert task["client_tz_offset_min"] == 660
    assert "Сахалин" in task["client_tz_label"]
    assert "Москва" not in task["client_tz_label"]
    assert task["client_tz_offset_min"] > 420


def test_internal_lead_normalizes_sakhalin_custom_field_timezone(app_client, internal_headers, widget_headers):
    """Lead inbox transport also uses CRM/custom-field evidence, not phone-country fallback."""
    client, _ = app_client

    r = client.post(
        "/api/internal/lead",
        headers=internal_headers,
        json={
            "lead_id": 501108,
            "name": "Михаил",
            "phone": "+7 *** *** 1108",
            "client_city": "",
            "client_tz_offset_min": 180,
            "client_tz_label": "Россия / Москва",
            "custom_fields": [
                {"field_name": "Регион", "value": "Сахалинская область"},
            ],
        },
    )
    assert r.status_code == 200, f"lead create failed: {r.status_code} {r.text}"

    leads = client.get("/api/leads", headers=widget_headers)
    assert leads.status_code == 200
    lead = next(item for item in leads.json().get("leads", []) if item.get("lead_id") == 501108)
    assert lead["client_tz_offset_min"] == 660
    assert "Сахалин" in lead["client_tz_label"]
    assert "Москва" not in lead["client_tz_label"]
    assert lead["client_tz_offset_min"] > 420



def test_internal_tasks_push_preserves_completed_today(app_client, widget_headers, internal_headers):
    """Regression: полная замена tasks не должна стирать completed_today.

    Сценарий: задача закрыта (попала в completed_today) → приходит новый
    snapshot задач → выполненные за сегодня обязаны остаться в выдаче.
    """
    client, _ = app_client

    def _task(task_id):
        return {
            "task_id": task_id,
            "lead_id": 999800 + task_id,
            "due": "2099-01-01T10:00:00+07:00",
            "created_by": 0,
            "created_by_name": "system",
            "task_text": "Связаться",
            "lead_name": "Клиент",
            "phone": "",
            "amocrm_url": f"https://example.invalid/leads/detail/{999800 + task_id}",
        }

    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [_task(7001), _task(7002)]},
    )
    assert r.status_code == 200

    r = client.post("/api/tasks/7001/close_no_followup", headers=widget_headers)
    assert r.status_code == 200
    r = client.post("/api/internal/tasks/7001/closed", headers=internal_headers)
    assert r.status_code == 200

    r = client.get("/api/tasks/today", headers=widget_headers)
    completed_ids = [t["task_id"] for t in r.json()["completed_today"]]
    assert 7001 in completed_ids

    # Новая пачка задач (полная замена) — completed_today не должен пропасть
    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [_task(7003)]},
    )
    assert r.status_code == 200

    r = client.get("/api/tasks/today", headers=widget_headers)
    body = r.json()
    assert [t["task_id"] for t in body["tasks"]] == [7003]
    completed_ids = [t["task_id"] for t in body["completed_today"]]
    assert 7001 in completed_ids, "полный пуш задач стёр completed_today"


def test_request_text_hides_empty_form_fields(app_client, widget_headers, internal_headers):
    """Феедбек Владимира 2026-06-12: пустые поля анкеты («Цель:» без ответа)
    не должны попадать в карточку — показываем только заполненное."""
    client, _ = app_client
    raw = ("Цель:\nУдобный мессенджер:\nБывали ли на Пхукете:\n\n"
           "Вопросы: Цель покупки:: Для инвестиции\n"
           "Были ли вы на Пхукете:: Нет\n"
           "Удобный Месседжер:: Max")
    client.post(
        "/api/internal/lead",
        headers=internal_headers,
        json={"lead_id": 999900, "name": "Марина", "phone": "+79990009900", "request_text": raw},
    )
    r = client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [{
            "task_id": 9901, "lead_id": 999900, "due": "2099-01-01T10:00:00+07:00",
            "created_by": 0, "created_by_name": "system", "task_text": "Связаться",
            "lead_name": "Марина", "phone": "", "amocrm_url": "https://example.invalid/leads/detail/999900",
        }]},
    )
    assert r.status_code == 200

    task = client.get("/api/tasks/today", headers=widget_headers).json()["tasks"][0]
    text = task["request_text"]
    assert "Цель:\n" not in text and "Удобный мессенджер:" not in text.split("\n")[0:1]
    for line in text.splitlines():
        assert not line.rstrip().endswith(":"), f"пустое поле анкеты просочилось: {line!r}"
    assert "Для инвестиции" in text and "Max" in text

    lead = client.get("/api/leads?only_unacked=false&limit=50", headers=widget_headers).json()["leads"][0]
    for line in (lead["request_text"] or "").splitlines():
        assert not line.rstrip().endswith(":")


def test_mark_sent_writes_sent_event_for_obsidian(app_client, widget_headers, internal_headers):
    """Запрос Владимира 2026-06-12: каждая отправка клиенту попадает в журнал,
    который Mac-воркер выгружает в Obsidian для обучения движка стиля."""
    client, _ = app_client
    client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [{
            "task_id": 9905, "lead_id": 999905, "due": "2099-01-01T10:00:00+07:00",
            "created_by": 0, "created_by_name": "system", "task_text": "Связаться",
            "lead_name": "Тест", "phone": "", "amocrm_url": "https://example.invalid/leads/detail/999905",
            "suggested_message": "Исходный черновик офиса",
            "last_message_channel": "telegram", "style_runtime_pack_id": "long_silence_reactivation",
        }]},
    )
    r = client.post(
        "/api/tasks/9905/mark_sent_manually",
        headers=widget_headers,
        json={"edited_message": "Текст, который Владимир реально отправил"},
    )
    assert r.status_code == 200 and r.json()["needs_analysis"] is True

    r = client.get("/api/internal/tasks/sent_events", headers=internal_headers)
    assert r.status_code == 200
    events = r.json()["events"]
    assert len(events) == 1
    e = events[0]
    assert e["task_id"] == 9905 and e["edited"] is True
    assert e["original_message"] == "Исходный черновик офиса"
    assert e["final_message"] == "Текст, который Владимир реально отправил"
    assert e["channel"] == "telegram" and e["pack_id"] == "long_silence_reactivation"

    # курсор after_ts отсекает уже обработанные события
    r = client.get(f"/api/internal/tasks/sent_events?after_ts={e['ts']}", headers=internal_headers)
    assert r.json()["count"] == 0

    # endpoint закрыт от widget-токена
    r = client.get("/api/internal/tasks/sent_events", headers=widget_headers)
    assert r.status_code in (401, 403)


def test_string_last_significant_contact_normalized_to_object(app_client, widget_headers, internal_headers):
    """Инцидент 2026-06-12: строка в last_significant_contact ломала decode
    всего списка задач в iOS (ждёт объект) — приложение молча показывало кэш."""
    client, _ = app_client
    client.post(
        "/api/internal/tasks",
        headers=internal_headers,
        json={"tasks": [{
            "task_id": 9910, "lead_id": 999910, "due": "2099-01-01T10:00:00+07:00",
            "created_by": 0, "created_by_name": "system", "task_text": "Связаться",
            "lead_name": "Тест", "phone": "", "amocrm_url": "https://example.invalid/leads/detail/999910",
            "last_significant_contact": "2026-05-04",
        }]},
    )
    task = client.get("/api/tasks/today", headers=widget_headers).json()["tasks"][0]
    lsc = task["last_significant_contact"]
    assert isinstance(lsc, dict) and lsc["date"] == "2026-05-04"


def test_priority_classification_flow(app_client, widget_headers, internal_headers):
    """Фича «Кому написать в первую очередь» (2026-06-13): метки приоритета
    проставляются воркером, отдаются в /api/tasks/today, переживают полный пуш."""
    client, _ = app_client

    def _task(tid):
        return {
            "task_id": tid, "lead_id": 990000 + tid, "due": "2099-01-01T10:00:00+07:00",
            "created_by": 0, "created_by_name": "system", "task_text": "Связаться",
            "lead_name": f"Клиент {tid}", "phone": "",
            "amocrm_url": f"https://example.invalid/leads/detail/{990000 + tid}",
        }

    client.post("/api/internal/tasks", headers=internal_headers,
                json={"tasks": [_task(8001), _task(8002)]})

    # триггер пересборки
    r = client.post("/api/triggers/reclassify", headers=widget_headers)
    assert r.status_code == 200
    r = client.get("/api/internal/triggers/reclassify_request", headers=internal_headers)
    assert r.status_code == 200 and r.json()["requested_at"]

    # воркер пушит метки
    r = client.post("/api/internal/tasks/priority", headers=internal_headers, json={"items": [
        {"task_id": 8001, "priority_tag": "hot", "priority_reason": "Обещан расчёт, тишина 3 дня",
         "next_step": "Прислать расчёт по рассрочке"},
        {"task_id": 8002, "priority_tag": "sleeping", "priority_reason": "Молчит 4 месяца", "next_step": "Мягкое касание"},
    ]})
    assert r.status_code == 200 and r.json()["updated"] == 2

    tasks = {t["task_id"]: t for t in client.get("/api/tasks/today", headers=widget_headers).json()["tasks"]}
    assert tasks[8001]["priority_tag"] == "hot"
    assert tasks[8001]["next_step"] == "Прислать расчёт по рассрочке"
    assert tasks[8002]["priority_tag"] == "sleeping"

    # метки переживают полный пуш задач (PRESERVE_KEYS)
    client.post("/api/internal/tasks", headers=internal_headers, json={"tasks": [_task(8001), _task(8002)]})
    tasks = {t["task_id"]: t for t in client.get("/api/tasks/today", headers=widget_headers).json()["tasks"]}
    assert tasks[8001]["priority_tag"] == "hot", "метка приоритета стёрта полным пушем"
    assert tasks[8001]["priority_reason"] == "Обещан расчёт, тишина 3 дня"

    # невалидный тег нормализуется в warm; endpoint закрыт от widget-токена
    client.post("/api/internal/tasks/priority", headers=internal_headers,
                json={"items": [{"task_id": 8002, "priority_tag": "мусор"}]})
    tasks = {t["task_id"]: t for t in client.get("/api/tasks/today", headers=widget_headers).json()["tasks"]}
    assert tasks[8002]["priority_tag"] == "warm"
    assert client.post("/api/internal/tasks/priority", headers=widget_headers, json={"items": []}).status_code in (401, 403)
