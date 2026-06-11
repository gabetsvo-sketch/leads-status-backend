"""Style Router: каждый сценарий ведёт на реально опубликованный пак.

Регрессия 2026-06-11: роутер выбирал несуществующий пак silence_reactivation,
и transferred_old_dialogue_reactivation отсутствовал в снапшоте — главные
сценарии блокировались как runtime_pack_unavailable.
"""

# Паки, опубликованные в runtime-снапшоте (source: style-runtime-index-v1.json).
PUBLISHED_PACKS = {
    "client_asks_question",
    "closing_from_selection_to_booking",
    "followup_after_silence",
    "initial_contact_after_lead",
    "legal_remote_purchase_explanation",
    "long_silence_reactivation",
    "meeting_and_visit_closing",
    "object_selection_explainer",
    "payment_and_documents_explanation",
    "price_roi_explanation",
    "soft_close_or_cold",
    "transferred_old_dialogue_reactivation",
    "zoom_or_call_scheduling",
}

ROUTER_CASES = [
    # (payload, ожидаемый pack_id)
    ({"dialogue_transferred": True,
      "last_significant_contact": {"channel": "call", "meaning": "сказал, что не актуально"}},
     "transferred_old_dialogue_reactivation"),
    ({"last_client_message_summary": "Какая цена и доходность у этого объекта?"},
     "price_roi_explanation"),
    ({"last_client_message_summary": "Можно ли купить удаленно по доверенности? Что с leasehold?"},
     "legal_remote_purchase_explanation"),
    ({"last_client_message_summary": "Как проходит оплата, какие документы нужны?"},
     "payment_and_documents_explanation"),
    ({"last_client_message_summary": "Готов внести депозит и бронировать."},
     "closing_from_selection_to_booking"),
    ({"last_client_message_summary": "Давайте созвонимся в Zoom завтра."},
     "zoom_or_call_scheduling"),
    ({"last_client_message_summary": "Прилетаю в субботу, хочу встречу и показ."},
     "meeting_and_visit_closing"),
    ({"last_client_message_summary": "Какие районы лучше для подбора, что посоветуете?"},
     "object_selection_explainer"),
    ({"deal_stage": "new_lead"},
     "initial_contact_after_lead"),
    ({"deal_stage": "cold"},
     "soft_close_or_cold"),
    ({"silence_days": 5},
     "followup_after_silence"),
    ({"silence_days": 30},
     "long_silence_reactivation"),
    ({"last_client_message_summary": "А когда сдается этот комплекс?"},
     "client_asks_question"),
]


def test_router_covers_all_cases(app_client):
    _, main = app_client
    for payload, expected in ROUTER_CASES:
        pack_id, secondary, reason = main._style_choose_pack(dict(payload))
        assert pack_id == expected, f"{payload} -> {pack_id}, ожидался {expected}"
        assert pack_id in PUBLISHED_PACKS
        for sec in secondary:
            assert sec in PUBLISHED_PACKS, f"secondary {sec} не опубликован"
        assert reason


def test_router_never_returns_unpublished_pack(app_client):
    _, main = app_client
    probes = [
        {}, {"deal_stage": "x"}, {"silence_days": 1},
        {"last_client_message_summary": "просто вопрос"},
        {"dialogue_transferred": True},
    ]
    for p in probes:
        pack_id, secondary, _ = main._style_choose_pack(dict(p))
        assert pack_id in PUBLISHED_PACKS
        assert all(s in PUBLISHED_PACKS for s in secondary)


def test_pokupka_does_not_trigger_price_pack(app_client):
    """Регрессия: «окуп» (окупаемость) ловил подстроку в «покупке/покупаем»."""
    _, main = app_client
    pack_id, _, _ = main._style_choose_pack(
        {"last_client_message_summary": "Думаем о покупке квартиры, что посоветуете по районам?"}
    )
    assert pack_id != "price_roi_explanation"
    pack_id, _, _ = main._style_choose_pack(
        {"last_client_message_summary": "Какая окупаемость у этого проекта?"}
    )
    assert pack_id == "price_roi_explanation"
