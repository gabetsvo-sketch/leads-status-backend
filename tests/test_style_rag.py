"""Этап 3 движка стиля: база прецедентов (RAG).

Проверяем:
- запрос к базе строится по ПОСЛЕДНЕЙ реплике клиента (текущий момент), а не по сводке;
- писатель получает ПАРЫ «ситуация→ответ» с рамкой «бери ход», а не «только тон»;
- фолбэк на старый формат (только reply), если пар нет.
"""


def test_rag_query_uses_last_client_turn(app_client):
    client, main = app_client
    payload = {
        "recent_dialogue": (
            "[01.05.2026] клиент: интересует студия\n"
            "[01.05.2026] Владимир: есть варианты\n"
            "[08.05.2026] клиент: а какая рассрочка по этому проекту?"
        ),
        "last_client_message_summary": "клиент интересовался студией в целом",
    }
    q = main._style_rag_query(payload)
    # запрос = последняя реплика клиента (текущий момент), не общая сводка
    assert "рассрочка" in q
    assert "интересовался студией в целом" not in q


def test_rag_query_fallback_to_summary(app_client):
    client, main = app_client
    q = main._style_rag_query({"last_client_message_summary": "спрашивал про цену"})
    assert q == "спрашивал про цену"


def test_writer_prompt_uses_similar_pairs(app_client):
    client, main = app_client
    payload = {
        "channel": "whatsapp",
        "similar_pairs": [
            {"situation": "клиент пропал на неделю после подборки",
             "reply": "Напомнил о себе, спросил остались ли вопросы"},
        ],
    }
    _, user, _ = main._style_writer_prompts(payload, "PACK")
    assert "ПРЕЦЕДЕНТЫ" in user
    assert "клиент пропал на неделю" in user
    assert "Напомнил о себе" in user
    assert "ход" in user.lower()  # рамка «бери ход»


def test_writer_prompt_similar_examples_fallback(app_client):
    client, main = app_client
    # если пар нет, но есть старый similar_examples — используем его
    payload = {"channel": "whatsapp", "similar_examples": ["Отвечу и подскажу шаг"]}
    _, user, _ = main._style_writer_prompts(payload, "PACK")
    assert "Отвечу и подскажу шаг" in user
    assert "ПРЕЦЕДЕНТЫ" not in user  # это фолбэк-формат, не пары
