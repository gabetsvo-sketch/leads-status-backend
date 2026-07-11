"""Авто-обучение стиля (решение Владимира 11.07): система сама переводит
повторяющиеся у разных клиентов правки в общие правила — без ручного одобрения.
Бэкенд-часть: приём набора авто-правил, выдача, включение в память писателя."""


def _post(client, internal_headers, rules):
    r = client.post("/api/internal/style/auto_rules", json={"rules": rules}, headers=internal_headers)
    assert r.status_code == 200
    return r.json()


def test_auto_rules_roundtrip_and_validation(app_client, widget_headers, internal_headers):
    client, _ = app_client
    res = _post(client, internal_headers, [
        {"id": "sm_auto_001", "text": "Не задавать больше одного вопроса в сообщении.",
         "evidence_leads": [1, 2, 3]},
        {"id": "bad_id_001", "text": "мимо — id не sm_auto_*"},
        {"id": "sm_auto_002", "text": ""},                      # пустой text — мимо
        {"id": "sm_auto_003", "text": "х" * 500},               # слишком длинный — мимо
        {"id": "sm_auto_004", "text": "После долгой паузы писать живее, без сухости.",
         "type": "contraindication"},
    ])
    assert res["count"] == 2
    assert res["new"] == 2

    r = client.get("/api/style/auto_rules", headers=widget_headers)
    body = r.json()
    ids = {x["id"] for x in body["rules"]}
    assert ids == {"sm_auto_001", "sm_auto_004"}
    types = {x["id"]: x["type"] for x in body["rules"]}
    assert types["sm_auto_004"] == "contraindication"
    assert all(x["confidence"] == "approved_auto" for x in body["rules"])


def test_auto_rules_idempotent_no_push_storm(app_client, internal_headers):
    """Повторная присылка того же набора → new=0 (пуш-шторм невозможен)."""
    client, _ = app_client
    rules = [{"id": "sm_auto_010", "text": "Правило без изменений."}]
    assert _post(client, internal_headers, rules)["new"] == 1
    assert _post(client, internal_headers, rules)["new"] == 0


def test_auto_rules_cap(app_client, internal_headers, widget_headers):
    client, _ = app_client
    rules = [{"id": f"sm_auto_{i:03d}", "text": f"Правило номер {i}."} for i in range(25)]
    res = _post(client, internal_headers, rules)
    assert res["count"] == 15  # AUTO_STYLE_RULES_CAP
    r = client.get("/api/style/auto_rules", headers=widget_headers)
    assert r.json()["count"] == 15


def test_auto_rules_reach_writer_memory(app_client, internal_headers):
    """Авто-правила подхватываются загрузчиком памяти и доходят до отбора
    писателя как ГЛОБАЛЬНЫЕ (без pack — не режутся лимитом сценарных)."""
    client, main = app_client
    _post(client, internal_headers, [
        {"id": "sm_auto_777", "text": "Тестовое авто-правило для писателя."},
    ])
    records = main._style_load_approved_memory_records()
    auto = [r for r in records if r["id"] == "sm_auto_777"]
    assert auto, "авто-правило не попало в память писателя"

    selected = main._style_select_memory_records(
        {"channel": "whatsapp", "deal_stage": "question"}, pack_id="client_asks_question")
    ex_ids = {r["id"] for r in selected["examples"]}
    assert "sm_auto_777" in ex_ids, "авто-правило не дошло до отбора писателя"


def test_auto_rules_replace_removes_old(app_client, internal_headers, widget_headers):
    """Полная замена: правило, исключённое учителем из набора, исчезает."""
    client, _ = app_client
    _post(client, internal_headers, [
        {"id": "sm_auto_020", "text": "Старое правило."},
        {"id": "sm_auto_021", "text": "Остающееся правило."},
    ])
    _post(client, internal_headers, [
        {"id": "sm_auto_021", "text": "Остающееся правило."},
    ])
    r = client.get("/api/style/auto_rules", headers=widget_headers)
    ids = {x["id"] for x in r.json()["rules"]}
    assert ids == {"sm_auto_021"}
