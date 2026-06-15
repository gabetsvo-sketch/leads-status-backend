"""Тесты единого барьера против заглушки обращения «Имя» (фикс 2026-06-15).

Корень рецидива «движок не работает / карточки сломаны»: обезличенные примеры
стиля содержат заглушку «[Имя]», и писатель копировал её клиенту как «Имя, добрый
день» — хотя настоящее имя есть в карточке. Здесь проверяем, что заглушка НИКОГДА
не доезжает до карточки: подставляется реальное имя, иначе срезается обращение, а
неустранимый остаток блокируется гейтом. Плюс нормализация битых полей карточки.
"""


# ---------- юнит: sanitize_outgoing_draft / валидатор / извлечение имени ----------

def test_sanitize_substitutes_real_name(app_client):
    _, main = app_client
    assert main.sanitize_outgoing_draft("Имя, добрый день. Как дела?", "Ольга") == \
        "Ольга, добрый день. Как дела?"


def test_sanitize_strips_vocative_when_no_name(app_client):
    _, main = app_client
    # Без валидного имени обращение срезается, регистр следующего слова поднимается.
    assert main.sanitize_outgoing_draft("Имя, добрый день. Как дела?", "") == \
        "Добрый день. Как дела?"


def test_sanitize_bracketed_vocative(app_client):
    _, main = app_client
    assert main.sanitize_outgoing_draft("[Имя], здравствуйте. Покажу проекты", "Mary") == \
        "Mary, здравствуйте. Покажу проекты"


def test_sanitize_brace_placeholder(app_client):
    _, main = app_client
    assert main.sanitize_outgoing_draft("{name}, привет", "Сергей") == "Сергей, привет"


def test_sanitize_clean_text_untouched(app_client):
    _, main = app_client
    clean = "Александр, добрый день! Как ваши дела по подбору?"
    assert main.sanitize_outgoing_draft(clean, "Александр") == clean


def test_sanitize_lowercase_word_not_touched(app_client):
    _, main = app_client
    # строчное «имя» в обычном предложении — НЕ заглушка, не трогаем и не флагуем.
    s = "Доброе утро! Напишите ваше имя и удобное время для звонка."
    assert main.sanitize_outgoing_draft(s, "Ольга") == s
    assert main._style_has_name_placeholder(s) is False


def test_has_placeholder_midtext_bracket(app_client):
    _, main = app_client
    # Заглушка в середине (может быть имя АГЕНТА «Меня зовут [Имя]») не подставляется,
    # а помечается — её ловит гейт.
    s = "Здравствуйте! Меня зовут [Имя], я помогу с подбором"
    assert main.sanitize_outgoing_draft(s, "Ольга") == s  # вокатив не в начале — не трогаем
    assert main._style_has_name_placeholder(s) is True


def test_has_placeholder_bare_midtext(app_client):
    _, main = app_client
    assert main._style_has_name_placeholder("Меня зовут Имя сегодня") is True


def test_valid_client_name(app_client):
    _, main = app_client
    assert main._style_valid_client_name("Ольга") == "Ольга"
    assert main._style_valid_client_name("Mary") == "Mary"
    assert main._style_valid_client_name("Анна-Мария") == "Анна-Мария"
    # битые/служебные — отвергаются (лучше без обращения, чем мусор/PII):
    assert main._style_valid_client_name("Dr.Neverov") == ""   # точка
    assert main._style_valid_client_name("@olgabondarenko") == ""  # @username (PII)
    assert main._style_valid_client_name("Клиент") == ""       # стоп-слово
    assert main._style_valid_client_name("12345") == ""        # цифры
    assert main._style_valid_client_name("") == ""


def test_name_from_lead_name(app_client):
    _, main = app_client
    assert main._style_name_from_lead_name("Александр 6967") == "Александр"
    assert main._style_name_from_lead_name("Mary Land 3177") == "Mary"
    assert main._style_name_from_lead_name("Dr.Neverov") == "Neverov"
    assert main._style_name_from_lead_name("Клиент") == ""
    assert main._style_name_from_lead_name("Заявка от (Алёна Курсакова)") == "Алёна"


# ---------- юнит: гейт помечает остаточную заглушку ----------

def test_gate_flags_name_placeholder_and_blocks(app_client):
    _, main = app_client
    safety = main._style_safety_gate(
        {"client_situation_hint": "вопрос клиента"},
        "Здравствуйте! Меня зовут [Имя], помогу с подбором.",
        "client_asks_question",
    )
    assert "name_placeholder" in safety["flags"]
    assert safety["pass"] is False  # hard-блок: не показываем как готовый


def test_gate_clean_draft_no_placeholder_flag(app_client):
    _, main = app_client
    safety = main._style_safety_gate(
        {"client_situation_hint": "вопрос клиента"},
        "Ольга, добрый день! Подскажу по подбору, актуально ещё смотреть варианты?",
        "client_asks_question",
    )
    assert "name_placeholder" not in safety["flags"]


# ---------- интеграция: полный пуш чистит заглушку и битые поля ----------

def _push(client, headers, task):
    return client.post("/api/internal/tasks", headers=headers, json={"tasks": [task]})


def test_full_push_sanitizes_preserved_placeholder_draft(app_client, internal_headers, widget_headers):
    """Гонка «черновик-до-имени»: старый черновик «Имя, ...» при пуше с актуальным
    lead_name='Ольга' должен почиститься в «Ольга, ...» (кейс задачи 21783201)."""
    client, _ = app_client
    task = {
        "task_id": 21783201, "lead_name": "Ольга", "stage": "В работе",
        "suggested_message": "Имя, добрый день. Сейчас обновились условия по рассрочке.",
    }
    assert _push(client, internal_headers, task).status_code == 200
    r = client.get("/api/tasks/today", headers=widget_headers)
    t = [x for x in r.json()["tasks"] if x["task_id"] == 21783201][0]
    assert t["suggested_message"].startswith("Ольга, добрый день")
    assert "Имя," not in t["suggested_message"]


def test_full_push_normalizes_broken_stage_and_phone(app_client, internal_headers, widget_headers):
    client, _ = app_client
    task = {
        "task_id": 555001, "lead_name": "Светлана", "stage": "82177790",
        "phone": "@svetlana_k", "whatsapp_phone": "@svetlana_k",
        "suggested_message": "Светлана, добрый день.",
    }
    assert _push(client, internal_headers, task).status_code == 200
    r = client.get("/api/tasks/today", headers=widget_headers)
    t = [x for x in r.json()["tasks"] if x["task_id"] == 555001][0]
    assert t["stage"] == ""                         # числовой статус-ID убран
    assert t["phone"] == ""                          # @username не телефон
    assert t.get("telegram_username") == "svetlana_k"  # переехал в телеграм-ник


def test_full_push_keeps_none_literal_stage_empty(app_client, internal_headers, widget_headers):
    client, _ = app_client
    task = {"task_id": 555002, "lead_name": "Игорь", "stage": "None",
            "suggested_message": "Игорь, добрый день."}
    assert _push(client, internal_headers, task).status_code == 200
    r = client.get("/api/tasks/today", headers=widget_headers)
    t = [x for x in r.json()["tasks"] if x["task_id"] == 555002][0]
    assert t["stage"] == ""


# ---------- интеграция: /regenerated чистит заглушку ----------

def test_regenerated_sanitizes_placeholder(app_client, internal_headers, widget_headers):
    client, _ = app_client
    _push(client, internal_headers,
          {"task_id": 777, "lead_name": "Жанна", "stage": "В работе",
           "suggested_message": "старый текст"})
    r = client.post("/api/internal/tasks/777/regenerated", headers=internal_headers,
                    json={"suggested_message": "Имя, здравствуйте. Подготовил информацию."})
    assert r.status_code == 200
    g = client.get("/api/tasks/today", headers=widget_headers)
    t = [x for x in g.json()["tasks"] if x["task_id"] == 777][0]
    assert t["suggested_message"].startswith("Жанна, здравствуйте")
