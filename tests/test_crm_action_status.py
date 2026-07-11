"""Этап 3 аудита (09.07): iOS опрашивает реальный статус crm-действия.
Раньше кнопки показывали успех по факту постановки в очередь."""


def test_crm_action_lifecycle_pending_then_applied(app_client, widget_headers, internal_headers):
    client, _ = app_client
    # Ставим действие в очередь — ответ несёт id
    r = client.post("/api/tasks/crm_action", headers=widget_headers,
                    json={"crm_task_id": 111, "lead_id": 222, "action": "complete", "label": "тест"})
    assert r.status_code == 200
    aid = r.json()["id"]
    assert aid.startswith("crmact-")

    # Статус сразу после постановки — pending
    r = client.get(f"/api/crm_actions/{aid}", headers=widget_headers)
    assert r.status_code == 200
    assert r.json()["status"] == "pending"

    # Воркер рапортует applied → iOS увидит подтверждение
    r = client.post(f"/api/internal/crm_actions/{aid}/done", headers=internal_headers,
                    json={"status": "applied", "result": "task closed"})
    assert r.status_code == 200
    r = client.get(f"/api/crm_actions/{aid}", headers=widget_headers)
    assert r.json()["status"] == "applied"
    assert r.json()["result"] == "task closed"


def test_crm_action_failed_visible(app_client, widget_headers, internal_headers):
    client, _ = app_client
    aid = client.post("/api/tasks/crm_action", headers=widget_headers,
                      json={"crm_task_id": 5, "action": "complete"}).json()["id"]
    client.post(f"/api/internal/crm_actions/{aid}/done", headers=internal_headers,
                json={"status": "failed", "result": "CDP down"})
    r = client.get(f"/api/crm_actions/{aid}", headers=widget_headers)
    assert r.json()["status"] == "failed"
    assert "CDP" in r.json()["result"]


def test_crm_action_unknown_404(app_client, widget_headers):
    client, _ = app_client
    r = client.get("/api/crm_actions/crmact-nope", headers=widget_headers)
    assert r.status_code == 404


def test_lead_carries_stage_and_context_fields(app_client, widget_headers, internal_headers):
    """Этап 4: заявка несёт pipeline_id/status_id/context_summary + merge обновляет их."""
    client, _ = app_client
    client.post("/api/internal/lead", headers=internal_headers, json={
        "lead_id": 900001, "name": "Тест", "phone": "+79990000001",
        "pipeline_id": 8283982, "status_id": 82177142, "is_active_stage": True, "notify": False,
    })
    r = client.get("/api/leads?only_unacked=true&limit=10", headers=widget_headers)
    lead = [l for l in r.json()["leads"] if l["lead_id"] == 900001][0]
    assert lead["pipeline_id"] == 8283982
    assert lead["status_id"] == 82177142

    # merge: enrich-воркер дообогатил контекстом
    client.post("/api/internal/lead", headers=internal_headers, json={
        "lead_id": 900001, "name": "Тест",
        "context_summary": "Клиент спрашивал про виллы в Банг Тао.",
        "has_correspondence": True, "is_active_stage": True, "notify": False,
    })
    r = client.get("/api/leads?only_unacked=true&limit=10", headers=widget_headers)
    lead = [l for l in r.json()["leads"] if l["lead_id"] == 900001][0]
    assert "Банг Тао" in lead["context_summary"]
    assert lead["has_correspondence"] is True
