"""Этап 1+2 движка стиля: живой контекст переписки + учёт паузы.

Проверяем:
- санитайзер переписки: чистит PII (телефон/почта/ник/ссылка), СОХРАНЯЕТ цены/даты;
- промпт писателя реально получает реплики диалога (recent_dialogue), а не только сводку;
- пауза градуированно попадает в промпт (долгая пауза → сильная инструкция);
- роутер: очень долгая пауза → пак возобновления (тема вторична); короткая пауза → тема.
"""


def test_sanitize_dialogue_strips_pii_keeps_prices(app_client):
    client, main = app_client
    raw = (
        "Клиент (2026-05-01): интересует студия до 12 500 000 бат, доходность 7% годовых.\n"
        "Владимир (2026-05-02): напишите на +66 81 234 5678 или почту test@mail.com, @vlad_re\n"
        "https://t.me/real_dream_phu — сдача 31.12.2026"
    )
    out = main._style_sanitize_dialogue(raw)
    # цены/проценты/даты сохранены — писателю они нужны
    assert "12 500 000" in out
    assert "7%" in out
    assert "31.12.2026" in out
    # PII вычищен
    assert "+66 81 234 5678" not in out and "[телефон]" in out
    assert "test@mail.com" not in out and "[почта]" in out
    assert "@vlad_re" not in out and "[ник]" in out
    assert "t.me/real_dream_phu" not in out and "[ссылка]" in out


def test_writer_prompt_includes_recent_dialogue(app_client):
    client, main = app_client
    payload = {
        "channel": "whatsapp",
        "client_name": "Иван",
        "recent_dialogue": "Клиент (2026-05-01): а рассрочка есть?\nВладимир (2026-05-01): да, 30/70",
        "last_client_message_summary": "клиент спрашивал про рассрочку",
    }
    system, user, _ = main._style_writer_prompts(payload, "PACK")
    assert "ПЕРЕПИСКА С КЛИЕНТОМ" in user
    assert "рассрочка есть" in user
    # при наличии реальной переписки сводка помечается как «сводка», не «последнее сообщение»
    assert "Сводка ситуации по сделке" in user


def test_writer_prompt_long_pause_instruction(app_client):
    client, main = app_client
    payload = {"channel": "whatsapp", "silence_days": 45}
    _, user, _ = main._style_writer_prompts(payload, "PACK")
    assert "45 дней" in user
    assert "долгая пауза" in user.lower()
    assert "не повторяй то, что уже отправлял" in user.lower()


def test_writer_prompt_medium_and_short_pause(app_client):
    client, main = app_client
    _, user_med, _ = main._style_writer_prompts({"channel": "whatsapp", "silence_days": 8}, "PACK")
    assert "8 дней" in user_med and "мягко напомни о себе" in user_med.lower()
    _, user_short, _ = main._style_writer_prompts({"channel": "whatsapp", "silence_days": 2}, "PACK")
    assert "2 дней" in user_short


def test_router_long_silence_beats_topic(app_client):
    client, main = app_client
    pack, secondary, reason = main._style_choose_pack({
        "deal_stage": "selection",
        "last_client_message_summary": "интересует цена объекта",
        "silence_days": 90,
    })
    assert pack == "long_silence_reactivation"
    # тема не потеряна — она во вторичных паках
    assert "price_roi_explanation" in secondary
    assert "90" in reason


def test_router_short_silence_keeps_topic(app_client):
    client, main = app_client
    pack, _, _ = main._style_choose_pack({
        "deal_stage": "selection",
        "last_client_message_summary": "интересует цена объекта",
        "silence_days": 3,
    })
    # короткая пауза не уводит из темы (цена → денежный pack)
    assert pack == "price_roi_explanation"
