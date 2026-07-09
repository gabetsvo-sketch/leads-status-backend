"""Этап 4 движка стиля: стиль + гейт.

Проверяем:
- цифра из РЕАЛЬНОЙ переписки считается подтверждённой (не блок price_without_source);
- цены/даты не считаются PII (раньше phone-регэксп ловил их → ложный hard-блок);
- память стиля не схлопывается: записи выбранного пака проходят, даже если стадия иная;
- дистиллированные правила (sm_vlad_fb_011..014) загружены как approved.
"""


def test_pii_check_allows_prices_and_dates(app_client):
    client, main = app_client
    # цены/суммы/проценты/даты — не PII
    assert main._style_text_has_pii("студия 12 500 000 бат, доходность 7%, сдача 31.12.2026") is False
    assert main._style_text_has_pii("рассрочка 30/70, взнос 1,5 млн руб") is False
    # реальный телефон/почта/ник — PII
    assert main._style_text_has_pii("звоните +66 81 234 5678") is True
    assert main._style_text_has_pii("пишите на mail@example.com") is True


def test_gate_price_confirmed_from_dialogue(app_client):
    client, main = app_client
    payload = {
        "channel": "whatsapp",
        "deal_stage": "payment",
        "recent_dialogue": "[01.05.2026] Владимир: студия в Coralina стоит 12 500 000 бат",
        "facts_available": [],
    }
    # цена 12 500 000 уже звучала в переписке → не выдумка, price_without_source НЕ ставится
    gate = main._style_safety_gate(payload, "Да, та студия за 12 500 000 бат ещё доступна.", "price_roi_explanation")
    assert "price_without_source" not in gate["flags"]
    assert gate["pass"] is True


def test_gate_price_unconfirmed_still_blocks(app_client):
    client, main = app_client
    payload = {"channel": "whatsapp", "deal_stage": "payment", "facts_available": []}
    # цена, которой НЕТ ни в переписке, ни в фактах → блок (анти-галлюцинация сохранена)
    gate = main._style_safety_gate(payload, "Есть отличный вариант за 9 900 000 бат.", "price_roi_explanation")
    assert "price_without_source" in gate["flags"]


def test_memory_not_collapsed_by_stage(app_client):
    client, main = app_client
    # пак initial_contact_after_lead имеет записи со стадией ['new_lead',...]; при иной
    # стадии payload они РАНЬШЕ отсекались. Теперь пак совпал → записи проходят.
    mem = main._style_select_memory_records(
        {"channel": "whatsapp", "deal_stage": "somethingelse"},
        "initial_contact_after_lead",
    )
    total = len(mem.get("examples", [])) + len(mem.get("guards", []))
    assert total >= 2, "записи выбранного пака должны проходить независимо от стадии"


def test_distilled_rules_loaded(app_client):
    client, main = app_client
    ids = {r.get("id") for r in main._style_load_approved_memory_records()}
    for rid in ("sm_vlad_fb_011", "sm_vlad_fb_012", "sm_vlad_fb_013", "sm_vlad_fb_014"):
        assert rid in ids, f"{rid} должно быть загружено как approved"


def test_global_rules_prioritized_over_pack(app_client):
    client, main = app_client
    # даже для пака со своими записями глобальные правила (sm_vlad_fb_*/sm_batchb_*)
    # должны попадать в выдачу, а не вытесняться сценарными при лимите.
    mem = main._style_select_memory_records(
        {"channel": "whatsapp", "deal_stage": "active"}, "initial_contact_after_lead")
    ex_ids = [r.get("id") for r in mem.get("examples", [])]
    globals_in = [i for i in ex_ids if str(i).startswith(("sm_vlad_fb", "sm_batchb"))]
    assert globals_in, "глобальные approved-правила должны быть в выдаче памяти"
