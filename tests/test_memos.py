"""Тесты памяток по проектам (Этап 2 «Улучшайзера», 2026-06-15).

Знания (15 памяток) живут в Obsidian; Mac пушит их в бэкенд, iOS показывает экраном
памятки. Карточка несёт project_memo_key (какая памятка) и promise (что обещано).
"""

MEMOS = {
    "memos": {
        "The Title Sierra (Банг Тао)": {
            "name": "The Title Sierra (Банг Тао)",
            "fits": "Инвесторам в ранний пресейл у моря.",
            "doubts": [{"doubt": "Дорого?", "answer": "Да, у моря, но и аренда выше."}],
            "phrases": ["Сейчас самая ранняя стадия пресейла"],
            "alternatives": [{"when": "нужен бюджет ниже", "target": "The Title Coralina (Камала)"}],
            "prices_verified": "2026-06-12",
            "obsidian_uri": "obsidian://open?vault=...",
        }
    }
}


def test_memos_push_and_list(app_client, internal_headers, widget_headers):
    client, _ = app_client
    p = client.post("/api/internal/knowledge/memos", headers=internal_headers, json=MEMOS)
    assert p.status_code == 200, p.text
    assert p.json()["memos"] == 1
    lst = client.get("/api/memos", headers=widget_headers).json()["memos"]
    assert lst[0]["key"] == "The Title Sierra (Банг Тао)"
    assert lst[0]["name"] == "The Title Sierra (Банг Тао)"


def test_get_one_memo(app_client, internal_headers, widget_headers):
    client, _ = app_client
    client.post("/api/internal/knowledge/memos", headers=internal_headers, json=MEMOS)
    m = client.get("/api/memo/The Title Sierra (Банг Тао)", headers=widget_headers)
    assert m.status_code == 200, m.text
    body = m.json()
    assert body["fits"].startswith("Инвесторам")
    assert body["doubts"][0]["answer"].startswith("Да")
    assert body["alternatives"][0]["target"] == "The Title Coralina (Камала)"
    assert body["prices_verified"] == "2026-06-12"


def test_memo_not_found(app_client, internal_headers, widget_headers):
    client, _ = app_client
    client.post("/api/internal/knowledge/memos", headers=internal_headers, json=MEMOS)
    assert client.get("/api/memo/Нет такой", headers=widget_headers).status_code == 404


def test_project_memo_key_and_promise_survive_push(app_client, internal_headers, widget_headers):
    client, _ = app_client
    client.post("/api/internal/tasks", headers=internal_headers, json={"tasks": [{
        "task_id": 700, "lead_name": "Сергей", "stage": "В работе",
        "suggested_message": "Сергей, добрый день.",
        "project_memo_key": "The Title Sierra (Банг Тао)", "promise": "прислать расчёт по Sierra",
    }]})
    # повторный пуш БЕЗ этих полей не должен их стереть (PRESERVE_KEYS)
    client.post("/api/internal/tasks", headers=internal_headers, json={"tasks": [{
        "task_id": 700, "lead_name": "Сергей", "stage": "В работе",
        "suggested_message": "Сергей, добрый день.",
    }]})
    t = [x for x in client.get("/api/tasks/today", headers=widget_headers).json()["tasks"] if x["task_id"] == 700][0]
    assert t["project_memo_key"] == "The Title Sierra (Банг Тао)"
    assert t["promise"] == "прислать расчёт по Sierra"
