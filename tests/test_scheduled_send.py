"""Отложенная отправка (решение Владимира 11.07): schedule_send → очередь
scheduled → рапорт /sent (source=scheduled). Пуши при пустом реестре — no-op."""
from datetime import datetime, timedelta, timezone


def _push_task(client, internal_headers, task_id=610001):
    client.post("/api/internal/tasks", headers=internal_headers, json={"tasks": [{
        "task_id": task_id, "lead_id": 610002, "lead_name": "Отложка-тест",
        "task_text": "связаться", "due": "сегодня",
        "phone": "+79990000061", "whatsapp_phone": "+79990000061",
        "suggested_message": "Черновик",
    }]})
    return task_id


def test_schedule_then_due_then_sent(app_client, widget_headers, internal_headers):
    client, _ = app_client
    tid = _push_task(client, internal_headers)

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    r = client.post(f"/api/tasks/{tid}/schedule_send", headers=widget_headers,
                    json={"message": "Привет, это отложенное", "channel": "whatsapp",
                          "scheduled_at": past})
    assert r.status_code == 200

    # очередь отдаёт due-задачу с pending_send
    r = client.get("/api/internal/tasks/scheduled", headers=internal_headers)
    due = [t for t in r.json()["tasks"] if t["task_id"] == tid]
    assert due and due[0]["pending_send"]["status"] == "scheduled"
    assert due[0]["pending_send"]["edited_message"] == "Привет, это отложенное" or \
           due[0]["pending_send"].get("message") == "Привет, это отложенное"

    # воркер рапортует успех (source=scheduled → пуш; реестр пуст → no-op, не падает)
    r = client.post(f"/api/internal/tasks/{tid}/sent", headers=internal_headers,
                    json={"success": True, "source": "scheduled", "channel": "whatsapp"})
    assert r.status_code == 200

    # задача ушла из очереди, статус sent, awaiting_reply
    r = client.get("/api/internal/tasks/scheduled", headers=internal_headers)
    assert not [t for t in r.json()["tasks"] if t["task_id"] == tid]
    r = client.get("/api/tasks/today", headers=widget_headers)
    task = [t for t in r.json()["tasks"] if t["task_id"] == tid][0]
    assert task["pending_send"]["status"] == "sent"
    assert task["action_state"] == "awaiting_reply"


def test_failed_report_keeps_error(app_client, widget_headers, internal_headers):
    client, _ = app_client
    tid = _push_task(client, internal_headers, task_id=610010)
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    client.post(f"/api/tasks/{tid}/schedule_send", headers=widget_headers,
                json={"message": "x", "channel": "telegram", "scheduled_at": past})
    r = client.post(f"/api/internal/tasks/{tid}/sent", headers=internal_headers,
                    json={"success": False, "error": "нет @username Telegram",
                          "source": "scheduled", "channel": "telegram"})
    assert r.status_code == 200
    r = client.get("/api/tasks/today", headers=widget_headers)
    task = [t for t in r.json()["tasks"] if t["task_id"] == tid][0]
    assert task["pending_send"]["status"] == "failed"
    assert "username" in task["pending_send"]["error"]


def test_alert_push_endpoint(app_client, internal_headers):
    client, _ = app_client
    r = client.post("/api/internal/alert_push", headers=internal_headers,
                    json={"title": "Тест", "body": "Нужен вход в WhatsApp Web"})
    assert r.status_code == 200
    r = client.post("/api/internal/alert_push", headers=internal_headers, json={"title": ""})
    assert r.status_code == 400
