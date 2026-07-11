"""Контрактный тест iOS ↔ бэкенд (этап 5 аудита 09.07).

Главный класс рецидивов проекта — рассинхрон контракта: iOS ждёт поле, бэкенд
его не отдаёт (или перестал отдавать) → карточка «пустая», Владимир ставит
одну и ту же задачу по несколько раз. Этот тест парсит CodingKeys прямо из
Swift-моделей и сверяет с ЖИВЫМИ ответами бэкенда: убрали/переименовали поле —
тест падает ДО деплоя.

Swift-репозиторий лежит рядом (../ios); если его нет (чужой CI) — тест
пропускается, а не падает.
"""
import re
from pathlib import Path

import pytest

IOS_DIR = Path(__file__).resolve().parent.parent.parent / "ios" / "LeadsStatus"

# Поля, которые iOS резолвит НЕ из ответа бэкенда (локальные/вычисляемые),
# или которые бэкенд отдаёт только при наличии данных (опциональные надстройки
# продюсеров — их отсутствие в минимальном ответе не ломает декодер: они String?).
LEAD_KEYS_OPTIONAL_UPSTREAM = {
    "regen_feedback",       # появляется только после feedback
    "timer_3min_sent", "timer_3min_text",  # появляются после таймера/стартового
    "has_correspondence",   # появляется после обогащения enrich-воркером (Bool? в Swift)
}
TASK_KEYS_CHECK = {
    # Костяк карточки задачи, за которым тянутся жалобы «карточка пустая».
    # NB: has_correspondence здесь НЕТ — модель TodayTask его пока не читает
    # (3-состояние сообщения — отложенная iOS-задача); iOS судит по context_summary.
    "task_id", "lead_id", "lead_name", "task_text", "due",
    "suggested_message", "context_summary", "rationale",
    "pipeline_id", "status_id",
    "priority_tag", "priority_reason", "next_step",
    "promise", "project_memo_key", "caution", "caution_reason",
    "last_message_channel", "last_incoming_channel",
    "telegram_username", "telegram_id", "whatsapp_phone", "phone",
}


def _swift_coding_keys(fname: str, struct: str) -> set:
    """Достаёт raw-ключи из enum CodingKeys нужного struct'а Swift-файла."""
    src = (IOS_DIR / fname).read_text(encoding="utf-8")
    m = re.search(rf"struct {struct}\b.*?enum CodingKeys[^{{]*{{(.*?)}}", src, re.S)
    assert m, f"CodingKeys не найден в {fname}/{struct}"
    body = m.group(1)
    keys = set()
    for line in body.splitlines():
        line = line.split("//")[0].strip()
        if not line.startswith("case "):
            continue
        for part in line[5:].split(","):
            part = part.strip()
            if not part:
                continue
            if "=" in part:
                raw = part.split("=", 1)[1].strip().strip('"')
            else:
                raw = part.strip()
            keys.add(raw)
    assert keys, f"пустые CodingKeys в {fname}/{struct}"
    return keys


needs_ios = pytest.mark.skipif(not IOS_DIR.exists(), reason="ios/ рядом нет (чужой CI)")


@needs_ios
def test_lead_payload_covers_ios_model(app_client, widget_headers, internal_headers):
    """Каждый ключ, который iOS Lead читает из /api/leads, реально присутствует
    в ответе бэкенда для свежесозданного лида."""
    client, _ = app_client
    client.post("/api/internal/lead", headers=internal_headers, json={
        "lead_id": 777001, "name": "Контракт", "phone": "+79990000077",
        "pipeline_id": 1, "status_id": 2, "is_active_stage": True, "notify": False,
    })
    r = client.get("/api/leads?only_unacked=true&limit=10", headers=widget_headers)
    lead = [l for l in r.json()["leads"] if l["lead_id"] == 777001][0]

    ios_keys = _swift_coding_keys("Lead.swift", "Lead")
    missing = ios_keys - set(lead.keys()) - LEAD_KEYS_OPTIONAL_UPSTREAM
    assert not missing, (
        f"iOS Lead ждёт ключи, которых нет в ответе /api/leads: {sorted(missing)}. "
        "Либо добавить в entry/merge бэкенда, либо осознанно внести в "
        "LEAD_KEYS_OPTIONAL_UPSTREAM (поле должно быть Optional в Swift)."
    )


@needs_ios
def test_task_written_values_survive_to_ios(app_client, widget_headers, internal_headers):
    """Что продюсер ЗАПИСАЛ в задачу — то iOS и получает из /api/tasks/today.
    Ловит потерю/переименование ключа на write/read-чокпоинтах («карточка
    пустая») и рассинхрон с моделью iOS. Отсутствие НЕзаполненного ключа —
    норма (в Swift все поля Optional), поэтому пушим костяк ЗАПОЛНЕННЫМ."""
    client, _ = app_client
    payload = {
        "task_id": 555001, "lead_id": 777002, "lead_name": "Контракт-задача",
        "task_text": "связаться", "due": "сегодня 12:00",
        "pipeline_id": 1, "status_id": 2, "phone": "+79990000078",
        "suggested_message": "Черновик-проверка", "context_summary": "Контекст-проверка",
        "rationale": "Почему-проверка", "priority_tag": "hot",
        "priority_reason": "давно молчит", "next_step": "позвонить",
        "promise": "прислать планировки", "project_memo_key": "test-memo",
        "caution": True, "caution_reason": "просил паузу",
        "last_message_channel": "telegram", "last_incoming_channel": "telegram",
        "telegram_username": "@test_user", "telegram_id": "12345",
        "whatsapp_phone": "+79990000078",
    }
    client.post("/api/internal/tasks", headers=internal_headers, json={"tasks": [payload]})
    r = client.get("/api/tasks/today", headers=widget_headers)
    task = [t for t in r.json()["tasks"] if t.get("task_id") == 555001][0]

    ios_keys = _swift_coding_keys("TodayTask.swift", "TodayTask")
    not_in_ios = TASK_KEYS_CHECK - ios_keys
    assert not not_in_ios, f"костяк не покрыт моделью iOS (переименовали?): {sorted(not_in_ios)}"

    lost = {k for k in TASK_KEYS_CHECK if k in payload and task.get(k) in (None, "", [])}
    assert not lost, (
        f"записанные продюсером поля потерялись по пути к iOS: {sorted(lost)}. "
        "Это класс «карточка пустая» — чинить на write/read-чокпоинте бэкенда."
    )
