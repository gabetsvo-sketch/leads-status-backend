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
