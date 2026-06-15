"""Тесты смены этапа сделки из приложения (2026-06-15).

Владимир: в карточке нужен выпадающий список смены статуса сделки, как в amoCRM;
при «Закрыто и не реализовано» — обязательный выбор причины отказа. Запись в CRM —
только по тапу Владимира (очередь crm_actions, исполняет Mac-воркер).
"""


def _action(client, headers, body):
    return client.post("/api/tasks/crm_action", headers=headers, json=body)


def test_change_status_queued(app_client, widget_headers, internal_headers):
    client, _ = app_client
    r = _action(client, widget_headers, {
        "action": "change_status", "lead_id": 12345,
        "pipeline_id": 8283982, "status_id": 82261990,  # «Контакт установлен»
    })
    assert r.status_code == 200, r.text
    # воркер видит действие в очереди с нужными полями
    q = client.get("/api/internal/crm_actions", headers=internal_headers).json()["actions"]
    a = [x for x in q if x["id"] == r.json()["id"]][0]
    assert a["action"] == "change_status"
    assert a["status_id"] == 82261990 and a["pipeline_id"] == 8283982
    assert a["lead_id"] == 12345


def test_closed_lost_requires_reason(app_client, widget_headers):
    client, _ = app_client
    # 143 = «Закрыто и не реализовано» без причины → 400
    r = _action(client, widget_headers, {
        "action": "change_status", "lead_id": 1, "pipeline_id": 8283982, "status_id": 143,
    })
    assert r.status_code == 400
    # с причиной — ок
    r2 = _action(client, widget_headers, {
        "action": "change_status", "lead_id": 1, "pipeline_id": 8283982,
        "status_id": 143, "loss_reason_id": 18028314,  # «Недозвон»
    })
    assert r2.status_code == 200, r2.text


def test_change_status_validates_ints(app_client, widget_headers):
    client, _ = app_client
    r = _action(client, widget_headers, {
        "action": "change_status", "lead_id": "x", "pipeline_id": 8283982, "status_id": 142,
    })
    assert r.status_code == 400


def test_catalog_roundtrip(app_client, widget_headers, internal_headers):
    client, _ = app_client
    cat = {
        "pipelines": [{"id": 8283982, "name": "Продажи", "is_main": True,
                       "statuses": [{"id": 82261990, "name": "Контакт установлен", "type": 0},
                                    {"id": 143, "name": "Закрыто и не реализовано", "type": 0}]}],
        "loss_reasons": [{"id": 18028314, "name": "Недозвон"}],
    }
    p = client.post("/api/internal/crm_catalog", headers=internal_headers, json=cat)
    assert p.status_code == 200, p.text
    assert p.json()["pipelines"] == 1
    got = client.get("/api/crm/catalog", headers=widget_headers).json()
    assert got["pipelines"][0]["name"] == "Продажи"
    assert got["loss_reasons"][0]["name"] == "Недозвон"


def test_catalog_empty_when_unset(app_client, widget_headers):
    client, _ = app_client
    got = client.get("/api/crm/catalog", headers=widget_headers).json()
    assert got["pipelines"] == [] and got["loss_reasons"] == []


def test_complete_still_requires_task_id(app_client, widget_headers):
    client, _ = app_client
    r = _action(client, widget_headers, {"action": "complete"})  # без crm_task_id
    assert r.status_code == 400
