"""Тесты read-пути /api/tasks/today: нормализация контакта + вывод канала + блокер.

Аудит 2026-06-15: на чтении карточка не нормализовалась и канал выводился только
для лидов из leads_inbox → 8 карточек без канала, '@ник' в phone (битый wa.me/27),
ложный блокер «контакт не подтянут» при наличии телефона. Теперь read-путь чинит всё.
"""


def _push(client, headers, task):
    return client.post("/api/internal/tasks", headers=headers, json={"tasks": [task]})


def _get_card(client, widget_headers, tid):
    r = client.get("/api/tasks/today", headers=widget_headers)
    return [x for x in r.json()["tasks"] if x["task_id"] == tid][0]


def test_at_username_in_phone_cleaned_on_read(app_client, internal_headers, widget_headers):
    client, _ = app_client
    _push(client, internal_headers, {
        "task_id": 90001, "lead_name": "Ольга", "stage": "В работе",
        "phone": "@olgabondarenko27", "whatsapp_phone": "@olgabondarenko27",
        "telegram_username": "olgabondarenko27", "suggested_message": "Ольга, добрый день.",
    })
    c = _get_card(client, widget_headers, 90001)
    assert c["phone"] == ""                      # @ник убран из телефона
    assert c["whatsapp_phone"] == ""
    assert c["telegram_username"] == "olgabondarenko27"
    assert c["last_message_channel"] == "telegram"   # канал из ника
    assert "telegram" in c["messengers"]


def test_phone_only_card_gets_whatsapp_channel(app_client, internal_headers, widget_headers):
    client, _ = app_client
    _push(client, internal_headers, {
        "task_id": 90002, "lead_name": "Марина", "stage": "В работе",
        "phone": "+79091609626", "suggested_message": "Марина, добрый день.",
    })
    c = _get_card(client, widget_headers, 90002)
    assert c["last_message_channel"] == "whatsapp"   # канал выведен из телефона
    assert "whatsapp" in c["messengers"]
    assert not c.get("contact_action_blocker")        # ложный блокер снят (контакт есть)
    assert c.get("send_blocked") is False


def test_phone_with_spaces_normalized(app_client, internal_headers, widget_headers):
    client, _ = app_client
    _push(client, internal_headers, {
        "task_id": 90003, "lead_name": "Анжелика", "stage": "В работе",
        "phone": "+7 915 090 2237", "whatsapp_phone": "+7 915 090 2237",
        "suggested_message": "Анжелика, добрый день.",
    })
    c = _get_card(client, widget_headers, 90003)
    assert c["phone"] == "+79150902237"               # пробелы убраны
    assert c["whatsapp_phone"] == "+79150902237"


def test_no_contact_card_blocked_honestly(app_client, internal_headers, widget_headers):
    client, _ = app_client
    _push(client, internal_headers, {
        "task_id": 90004, "lead_name": "Илья", "stage": "В работе",
        "suggested_message": "Илья, добрый день.",
    })
    c = _get_card(client, widget_headers, 90004)
    assert c.get("send_blocked") is True
    assert c.get("contact_action_blocker")            # честный блокер: отправить нечем
    assert not c.get("messengers")


def test_telegram_preferred_over_phone(app_client, internal_headers, widget_headers):
    client, _ = app_client
    _push(client, internal_headers, {
        "task_id": 90005, "lead_name": "Сергей", "stage": "В работе",
        "phone": "+79147225870", "telegram_username": "sergeyk",
        "suggested_message": "Сергей, добрый день.",
    })
    c = _get_card(client, widget_headers, 90005)
    assert c["last_message_channel"] == "telegram"    # приоритет telegram


def test_cta_word_boundary_no_false_block(app_client):
    _, main = app_client
    # «подскажите»/«удобно» больше не считаются (граница слова) — нормальный черновик
    assert main._style_count_cta("Марина, подскажите, когда удобно? Актуально ещё?") <= 2
    # реальный перегруз призывами по-прежнему ловится
    assert main._style_count_cta("Как вы? Что решили? Когда созвон? Напишите!") > 2


def test_no_correspondence_placeholder_when_unknown(app_client, internal_headers, widget_headers):
    """Пустой контекст + has_correspondence неизвестно → на ВЫДАЧЕ плейсхолдер
    «подтягиваю переписку», а НЕ ложное «нет переписки» (фикс рецидива 2026-06-15)."""
    client, _ = app_client
    _push(client, internal_headers, {"task_id": 95001, "lead_name": "Жанна", "stage": "В работе",
                                      "suggested_message": "Жанна, добрый день."})
    c = _get_card(client, widget_headers, 95001)
    assert c["context_summary"].startswith("⏳")   # плейсхолдер, не пусто


def test_no_correspondence_honest_when_false(app_client, internal_headers, widget_headers):
    """has_correspondence=False (воркер проверил, переписки нет) → контекст пуст,
    iOS честно покажет «нет переписки»."""
    client, _ = app_client
    _push(client, internal_headers, {"task_id": 95002, "lead_name": "Без диалога", "stage": "В работе",
                                      "suggested_message": "Добрый день.", "has_correspondence": False})
    c = _get_card(client, widget_headers, 95002)
    assert (c.get("context_summary") or "") == ""   # пусто → iOS «нет переписки»


def test_real_context_not_overwritten_by_placeholder(app_client, internal_headers, widget_headers):
    """Реальный контекст показывается как есть (плейсхолдер не подменяет)."""
    client, _ = app_client
    _push(client, internal_headers, {"task_id": 95003, "lead_name": "Марина", "stage": "В работе",
                                      "suggested_message": "Марина, добрый день.",
                                      "context_summary": "Смотрели Камалу, нужна продажа квартиры в РФ."})
    c = _get_card(client, widget_headers, 95003)
    assert c["context_summary"] == "Смотрели Камалу, нужна продажа квартиры в РФ."
