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

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["request_id"] == BASE_REQUEST["request_id"]
    assert body["manual_review_only"] is True
    assert body["show_to_vladimir"] is True
    assert body["safety_pass"] is False
    assert body["risk_level"] == "high"
    assert "price_source_ref" in body["missing_facts"]
    assert "price_without_source" in body["safety_flags"]
    assert body["block_reason"]
    assert body["draft_text"] == ""
    assert body["send_performed"] is False
    assert body["crm_mutated"] is False


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
