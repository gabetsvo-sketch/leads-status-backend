import asyncio
import hashlib
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import Body, FastAPI, Header, HTTPException, Query
from telethon import TelegramClient, events
from telethon.sessions import StringSession

load_dotenv()

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("leads_status")

# Этап 4.2: Sentry production monitoring (opt-in через env SENTRY_DSN).
# Inline init (а не import из assistant/sentry_helper.py) — backend деплоится
# на Render отдельно, не имеет доступа к assistant/ репо.
_sentry_dsn = os.environ.get("SENTRY_DSN", "").strip()
if _sentry_dsn:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.fastapi import FastApiIntegration

        sentry_sdk.init(
            dsn=_sentry_dsn,
            environment=os.environ.get("SENTRY_ENV", "production"),
            release=os.environ.get("BACKEND_RELEASE", "dev"),
            integrations=[
                LoggingIntegration(level=logging.WARNING, event_level=logging.ERROR),
                FastApiIntegration(),
            ],
            traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0")),
            send_default_pii=False,
            max_value_length=2048,
        )
        sentry_sdk.set_tag("component", "backend")
        log.info("sentry: активен (component=backend)")
    except ImportError:
        log.warning("sentry: SENTRY_DSN задан, но sentry-sdk не установлен")

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
CHAT_ID = int(os.environ["TG_CHAT_ID"])
WIDGET_TOKEN = os.environ["WIDGET_TOKEN"]
INTERNAL_TOKEN = os.environ.get("INTERNAL_TOKEN", "").strip()  # для Mac assistant → Render
SESSION_NAME = os.environ.get("SESSION_NAME", "leads_status")
SESSION_STRING = os.environ.get("TG_SESSION_STRING", "").strip()
STATE_FILE = Path(os.environ.get("STATE_FILE", "state.json"))
LEADS_FILE = Path(os.environ.get("LEADS_FILE", "leads.json"))
LEADS_RETENTION = int(os.environ.get("LEADS_RETENTION", "200"))  # сколько лидов хранить в памяти
TASKS_FILE = Path(os.environ.get("TASKS_FILE", "tasks_today.json"))
INSTRUCTIONS_FILE = Path(os.environ.get("INSTRUCTIONS_FILE", "instructions.json"))
ANTHROPIC_HEALTH_FILE = Path(os.environ.get("ANTHROPIC_HEALTH_FILE", "anthropic_health.json"))
NEWS_FILE = Path(os.environ.get("NEWS_FILE", "news.json"))
DEVICES_FILE = Path(os.environ.get("DEVICES_FILE", "devices.json"))
SCHEDULER_HEARTBEAT_FILE = Path(os.environ.get("SCHEDULER_HEARTBEAT_FILE", "/var/data/scheduler_heartbeat.json"))
OFFICE_TOKEN = os.environ.get("OFFICE_TOKEN", "").strip() or INTERNAL_TOKEN
OFFICE_DRAFTS_FILE = Path(os.environ.get("OFFICE_DRAFTS_FILE", "/var/data/office_drafts.json"))
DRAFT_FEEDBACK_LOG = Path(os.environ.get("DRAFT_FEEDBACK_LOG", "/var/data/draft_feedback_log.jsonl"))
STYLE_RUNTIME_DIR = Path(os.environ.get(
    "STYLE_RUNTIME_DIR",
    "/Users/vladimir/Desktop/Obsidian/Хранилище 1/Assist - Real estate/office/style-engine/runtime",
))
STYLE_RUNTIME_SOURCE = os.environ.get("STYLE_RUNTIME_SOURCE", "local").strip().lower()
STYLE_RUNTIME_R2_ENDPOINT = os.environ.get("STYLE_RUNTIME_R2_ENDPOINT", "").strip()
STYLE_RUNTIME_R2_BUCKET = os.environ.get("STYLE_RUNTIME_R2_BUCKET", "").strip()
STYLE_RUNTIME_R2_PREFIX = os.environ.get("STYLE_RUNTIME_R2_PREFIX", "style-runtime/v1/latest").strip().strip("/")
STYLE_RUNTIME_CACHE_TTL_SECONDS = int(os.environ.get("STYLE_RUNTIME_CACHE_TTL_SECONDS", "600"))
STYLE_RUNTIME_R2_ACCESS_KEY_ID = os.environ.get("STYLE_RUNTIME_R2_ACCESS_KEY_ID", "").strip()
STYLE_RUNTIME_R2_SECRET_ACCESS_KEY = os.environ.get("STYLE_RUNTIME_R2_SECRET_ACCESS_KEY", "").strip()
STYLE_RUNTIME_FEEDBACK_FILE = Path(os.environ.get("STYLE_RUNTIME_FEEDBACK_FILE", "/var/data/style_runtime_feedback.jsonl"))
STYLE_RUNTIME_HTTP_BASE_URL = os.environ.get("STYLE_RUNTIME_HTTP_BASE_URL", "").strip().rstrip("/")
STYLE_RUNTIME_HTTP_TOKEN = os.environ.get("STYLE_RUNTIME_HTTP_TOKEN", "").strip()
STYLE_MEMORY_FILE = Path(os.environ.get(
    "STYLE_MEMORY_FILE",
    str(STYLE_RUNTIME_DIR / "style-memory-v1-approved-batch-a.jsonl"),
))

# APNs config (build 20). Файл .p8 либо лежит на диске (APNS_AUTH_KEY_FILE),
# либо передаётся целиком через env (APNS_AUTH_KEY_CONTENT) — для Render.
APNS_KEY_ID = os.environ.get("APNS_KEY_ID", "").strip()
APNS_TEAM_ID = os.environ.get("APNS_TEAM_ID", "").strip()
APNS_BUNDLE_ID = os.environ.get("APNS_BUNDLE_ID", "com.gabetsvo.LeadsStatus").strip()
APNS_USE_SANDBOX = os.environ.get("APNS_USE_SANDBOX", "false").lower() in ("1", "true", "yes")
APNS_AUTH_KEY_FILE = os.environ.get("APNS_AUTH_KEY_FILE", "").strip()
APNS_AUTH_KEY_CONTENT = os.environ.get("APNS_AUTH_KEY_CONTENT", "").strip()

# Phase 2 timers (Vladimir spec):
# - 3 мин после получения лида, если не open → системное сообщение (semi-auto)
# - 15 мин после open, если в AmoCRM нет звонка → текстовое сообщение
TIMER_3MIN_SEC = int(os.environ.get("TIMER_3MIN_SEC", "180"))
TIMER_15MIN_SEC = int(os.environ.get("TIMER_15MIN_SEC", "900"))
TIMER_LOOP_INTERVAL_SEC = int(os.environ.get("TIMER_LOOP_INTERVAL_SEC", "60"))

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

RED_EMOJIS = set("🔴🟥🛑⛔🚫🚩🔻")
GREEN_EMOJIS = set("🟢🟩✅🔺")
SEND_RED = "🔴"
SEND_GREEN = "🟢"


def detect_color(text: Optional[str]) -> Optional[str]:
    """Return 'red'/'green' based on the LAST status emoji in the message, or None."""
    if not text:
        return None
    last = None
    for ch in text:
        if ch in RED_EMOJIS:
            last = "red"
        elif ch in GREEN_EMOJIS:
            last = "green"
    return last


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            d = json.loads(STATE_FILE.read_text())
            d.setdefault("revert_at", None)
            return d
        except Exception:
            log.exception("failed to load state, starting fresh")
    return {"color": None, "updated_at": None, "message_id": None, "revert_at": None}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


def load_leads() -> list:
    if LEADS_FILE.exists():
        try:
            return json.loads(LEADS_FILE.read_text())
        except Exception:
            log.exception("failed to load leads, starting fresh")
    return []


def save_leads(items: list) -> None:
    LEADS_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=2))


def load_tasks() -> dict:
    if TASKS_FILE.exists():
        try:
            data = json.loads(TASKS_FILE.read_text())
            # Backward-compat: добавляем completed_today если поля нет
            if "completed_today" not in data:
                data["completed_today"] = []
            return data
        except Exception:
            log.exception("failed to load tasks, starting fresh")
    return {"updated_at": None, "tasks": [], "completed_today": []}


def _prune_completed_today(payload: dict) -> None:
    """Удаляет из completed_today записи старше 24 часов.
    Безопасно вызывать при каждом save — не делает много работы."""
    completed = payload.get("completed_today") or []
    if not completed:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    fresh = []
    for t in completed:
        ts = t.get("closed_at")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt >= cutoff:
                fresh.append(t)
        except Exception:
            pass
    payload["completed_today"] = fresh


def save_tasks(payload: dict) -> None:
    TASKS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2))

def _normalize_tz_text(value) -> str:
    return (str(value or "")
            .strip()
            .lower()
            .replace("ё", "е")
            .replace("-", " ")
            .replace("–", " ")
            .replace("—", " "))


def _payload_mentions_sakhalin(payload: dict) -> bool:
    """Return True when CRM/source fields explicitly say Sakhalin.

    This is a transport-side safety net for stale scheduler payloads: if source
    evidence already reaches LeadsStatus as city/region/custom_fields, the app
    must not keep showing Russia/Moscow (+3) for a Sakhalin client (+11).
    """
    if not isinstance(payload, dict):
        return False

    candidates = []
    for key in (
        "client_city",
        "city",
        "region",
        "client_region",
        "client_timezone",
        "timezone",
        "tz_label",
        "client_tz_label",
    ):
        value = payload.get(key)
        if isinstance(value, str):
            candidates.append(value)

    custom_fields = payload.get("custom_fields") or []
    if isinstance(custom_fields, list):
        for field in custom_fields:
            if isinstance(field, dict):
                for key in ("value", "values", "text", "name", "field_name", "field_code"):
                    value = field.get(key)
                    if isinstance(value, str):
                        candidates.append(value)
                    elif isinstance(value, list):
                        candidates.extend(str(item) for item in value if item not in (None, ""))
            elif isinstance(field, str):
                candidates.append(field)

    normalized = " | ".join(_normalize_tz_text(item) for item in candidates)
    return any(marker in normalized for marker in (
        "сахалин",
        "южно сахалинск",
        "сахалинская область",
        "sakhalin",
        "yuzhno sakhalinsk",
    ))


def normalize_client_timezone_payload(payload: dict) -> dict:
    """Normalize client timezone fields when CRM/source evidence says Sakhalin.

    Keeps iOS/backend semantics unchanged: iOS still displays payload fields, but
    LeadsStatus refuses to transport an explicit Sakhalin city/region as Moscow.
    """
    if not isinstance(payload, dict):
        return payload
    if not _payload_mentions_sakhalin(payload):
        return payload

    normalized = dict(payload)
    normalized["client_tz_offset_min"] = 660
    normalized["client_tz_label"] = "Сахалин / UTC+11"
    normalized.setdefault("client_tz_name", "Asia/Sakhalin")
    return normalized


def _resolve_tz_from_phone(phone: Optional[str]) -> Optional[dict]:
    """Best-effort timezone fallback by phone country code for display only.

    The scheduler normally sends precise `client_tz_*` fields from CRM/city/phone
    evidence. Older snapshots and stale tasks can miss them; iOS should still show
    a safe regional time when a usable phone exists. This does not write CRM and
    does not claim city-level precision unless the country/city evidence exists.
    """
    if not phone:
        return None
    digits = "".join(ch for ch in str(phone) if ch.isdigit())
    if not digits:
        return None
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    country_tz = {
        "971": (240, "ОАЭ"),
        "972": (120, "Израиль"),
        "375": (180, "Беларусь"),
        "380": (120, "Украина"),
        "371": (120, "Латвия"),
        "372": (120, "Эстония"),
        "370": (120, "Литва"),
        "374": (240, "Армения"),
        "994": (240, "Азербайджан"),
        "995": (240, "Грузия"),
        "996": (360, "Кыргызстан"),
        "998": (300, "Узбекистан"),
        "992": (300, "Таджикистан"),
        "993": (300, "Туркменистан"),
        "420": (60, "Чехия"),
        "358": (120, "Финляндия"),
        "66": (420, "Таиланд"),
        "44": (0, "Великобритания"),
        "49": (60, "Германия"),
        "33": (60, "Франция"),
        "39": (60, "Италия"),
        "34": (60, "Испания"),
        "31": (60, "Нидерланды"),
        "84": (420, "Вьетнам"),
        "60": (480, "Малайзия"),
        "65": (480, "Сингапур"),
        "62": (420, "Индонезия / Джакарта"),
        "61": (600, "Австралия / Сидней"),
        "86": (480, "Китай"),
        "82": (540, "Корея"),
        "81": (540, "Япония"),
        "91": (330, "Индия"),
        "55": (-180, "Бразилия"),
        "52": (-360, "Мексика"),
        "48": (60, "Польша"),
        "43": (60, "Австрия"),
        "41": (60, "Швейцария"),
        "46": (60, "Швеция"),
        "47": (60, "Норвегия"),
        "30": (120, "Греция"),
        "90": (180, "Турция"),
        "7": (180, "Россия / Москва"),
        "1": (-300, "США / NY"),
    }
    for width in (3, 2, 1):
        code = digits[:width]
        if code in country_tz:
            offset, label = country_tz[code]
            return {"client_tz_offset_min": offset, "client_tz_label": label}
    return None


def load_news() -> list:
    if NEWS_FILE.exists():
        try:
            return json.loads(NEWS_FILE.read_text())
        except Exception:
            log.exception("failed to load news, starting fresh")
    return []


def save_news(items: list) -> None:
    NEWS_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=2))


def load_instructions() -> list:
    """Свободно-форматные инструкции от Vladimir'а («закрой Светлану»,
    «Анну перенеси на завтра»). Список dict со status: pending/applied/failed."""
    if INSTRUCTIONS_FILE.exists():
        try:
            return json.loads(INSTRUCTIONS_FILE.read_text())
        except Exception:
            log.exception("failed to load instructions, starting fresh")
    return []


def save_instructions(items: list) -> None:
    INSTRUCTIONS_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=2))


def load_anthropic_health() -> dict:
    """Здоровье Anthropic API: трекаем последнюю balance-ошибку.
    Простая структура: { last_balance_error_at, last_ok_at, calls_today,
    errors_today, day_key (BKK YYYY-MM-DD для сброса счётчиков) }."""
    if ANTHROPIC_HEALTH_FILE.exists():
        try:
            return json.loads(ANTHROPIC_HEALTH_FILE.read_text())
        except Exception:
            log.exception("failed to load anthropic_health, starting fresh")
    return {
        "last_balance_error_at": None,
        "last_ok_at": None,
        "calls_today": 0,
        "errors_today": 0,
        "day_key": None,
    }


def save_anthropic_health(payload: dict) -> None:
    ANTHROPIC_HEALTH_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2))


def load_devices() -> list:
    """Список зарегистрированных device tokens iOS для APNs push.
    Структура: [{device_token, registered_at, last_seen_at, app_version}]"""
    if DEVICES_FILE.exists():
        try:
            return json.loads(DEVICES_FILE.read_text())
        except Exception:
            log.exception("failed to load devices, starting empty")
    return []


def save_devices(items: list) -> None:
    DEVICES_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=2))


def load_office_drafts() -> list:
    if OFFICE_DRAFTS_FILE.exists():
        try:
            return json.loads(OFFICE_DRAFTS_FILE.read_text())
        except Exception:
            log.exception("failed to load office_drafts, starting empty")
    return []


def save_office_drafts_atomic(items: list) -> None:
    tmp = OFFICE_DRAFTS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2))
    tmp.replace(OFFICE_DRAFTS_FILE)


def _append_draft_feedback_log(entry: dict) -> None:
    try:
        with DRAFT_FEEDBACK_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        log.exception("failed to write draft_feedback_log")


state = load_state()
leads_inbox: list = load_leads()  # most-recent-first
tasks_today: dict = load_tasks()  # {"updated_at": iso, "tasks": [...]}
news_inbox: list = load_news()    # [{id, url, title, ..., status}, ...]
instructions_log: list = load_instructions()  # [{id, text, status, created_at, applied_at, result}, ...]
anthropic_health: dict = load_anthropic_health()
devices_registry: list = load_devices()
office_drafts: list = load_office_drafts()
_office_drafts_lock = asyncio.Lock()
client: Optional[TelegramClient] = None
_apns_client = None  # lazy-init


def _resolve_apns_key_path() -> Optional[str]:
    """Возвращает путь к .p8 ключу. Если задан APNS_AUTH_KEY_CONTENT (целиком
    содержимое в env, для Render) — записывает в /tmp/AuthKey.p8 и возвращает
    путь. Если задан APNS_AUTH_KEY_FILE — возвращает его. Иначе None."""
    if APNS_AUTH_KEY_CONTENT:
        path = "/tmp/AuthKey_apns.p8"
        try:
            with open(path, "w") as f:
                f.write(APNS_AUTH_KEY_CONTENT.replace("\\n", "\n"))
            os.chmod(path, 0o600)
            return path
        except Exception as e:
            log.warning(f"APNS: не удалось записать ключ из APNS_AUTH_KEY_CONTENT: {e}")
            return None
    if APNS_AUTH_KEY_FILE and Path(APNS_AUTH_KEY_FILE).exists():
        return APNS_AUTH_KEY_FILE
    return None


def _get_apns_client():
    """Lazy-initialized APNs client. Возвращает None если конфиг неполный."""
    global _apns_client
    if _apns_client is not None:
        return _apns_client
    if not (APNS_KEY_ID and APNS_TEAM_ID and APNS_BUNDLE_ID):
        return None
    key_path = _resolve_apns_key_path()
    if not key_path:
        return None
    try:
        from aioapns import APNs
        _apns_client = APNs(
            key=key_path,
            key_id=APNS_KEY_ID,
            team_id=APNS_TEAM_ID,
            topic=APNS_BUNDLE_ID,
            use_sandbox=APNS_USE_SANDBOX,
        )
        log.info(f"APNS: client initialized (sandbox={APNS_USE_SANDBOX}, topic={APNS_BUNDLE_ID})")
        return _apns_client
    except Exception as e:
        log.warning(f"APNS: client init failed: {e}")
        return None


async def send_push_to_all(title: str, body: str, payload: Optional[dict] = None) -> int:
    """Шлёт push на все зарегистрированные устройства. Возвращает кол-во успешных."""
    apns = _get_apns_client()
    if not apns:
        log.info("APNS: client not available, skipping push")
        return 0
    if not devices_registry:
        return 0
    from aioapns import NotificationRequest, PushType
    sent = 0
    failed: list = []
    for dev in list(devices_registry):
        token = dev.get("device_token") or ""
        if not token:
            continue
        try:
            msg = {
                "aps": {
                    "alert": {"title": title, "body": body},
                    "sound": "default",
                    "badge": 1,
                }
            }
            if payload:
                msg.update(payload)
            req = NotificationRequest(
                device_token=token,
                message=msg,
                push_type=PushType.ALERT,
            )
            resp = await apns.send_notification(req)
            if getattr(resp, "is_successful", False) or getattr(resp, "status", "") == "200":
                sent += 1
            else:
                reason = getattr(resp, "description", "") or str(resp)
                log.warning(f"APNS: failed for {token[:10]}…: {reason}")
                # Если токен невалиден — помечаем для удаления
                if "BadDeviceToken" in reason or "Unregistered" in reason:
                    failed.append(token)
        except Exception as e:
            log.warning(f"APNS: send error for {token[:10]}…: {e}")
    # Чистим невалидные токены
    if failed:
        devices_registry[:] = [d for d in devices_registry if d.get("device_token") not in failed]
        save_devices(devices_registry)
        log.info(f"APNS: removed {len(failed)} invalid device tokens")
    return sent


async def backfill_from_history(limit: int = 200) -> None:
    log.info("backfilling last %d messages from chat %s", limit, CHAT_ID)
    async for msg in client.iter_messages(CHAT_ID, limit=limit):
        color = detect_color(msg.message)
        if color:
            state["color"] = color
            state["updated_at"] = msg.date.astimezone(timezone.utc).isoformat()
            state["message_id"] = msg.id
            save_state(state)
            log.info("backfill found %s from message %s", color, msg.id)
            return
    log.info("backfill: no status emoji found in recent history")


async def on_new_message(event):
    color = detect_color(event.message.message)
    if not color:
        return
    state["color"] = color
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["message_id"] = event.message.id
    save_state(state)
    log.info("new status %s from message %s", color, event.message.id)


async def _generate_3min_message(lead: dict) -> str:
    """Короткое системное сообщение для нового лида (Phase 2). Генерится Claude или fallback-template."""
    name_first = (lead.get("name") or "").split(maxsplit=1)[0] or ""
    if not ANTHROPIC_API_KEY:
        # Fallback без Claude
        if name_first:
            return f"{name_first}, здравствуйте! Это Владимир из агентства RealDream на Пхукете. Получил вашу заявку, скоро свяжусь для уточнений."
        return "Здравствуйте! Это Владимир из агентства RealDream на Пхукете. Получил вашу заявку, скоро свяжусь для уточнений."
    try:
        import anthropic
        cli = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        sys_prompt = (
            "Ты пишешь от имени Владимира — агента недвижимости в RealDream на Пхукете. "
            "Это первое касание клиента сразу после заявки. "
            "Короткое сообщение 2-3 строки. Без длинных тире (только -), без пустых слов. "
            "Поприветствуй, представься, скажи что получил заявку, что свяжешься позже."
        )
        user = f"Заявка: {lead.get('name','')[:200]}\nИмя клиента: {name_first or 'не указано'}\nИсточник: {lead.get('source','')}"
        resp = await cli.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=200,
            system=sys_prompt,
            messages=[{"role": "user", "content": user}],
        )
        return resp.content[0].text.strip()
    except Exception as e:
        log.warning(f"3min-message claude failed: {e}, using fallback")
        if name_first:
            return f"{name_first}, здравствуйте! Это Владимир из агентства RealDream на Пхукете. Получил вашу заявку, скоро свяжусь."
        return "Здравствуйте! Это Владимир из агентства RealDream на Пхукете. Получил вашу заявку, скоро свяжусь."


async def timer_loop():
    """Фоновый task: каждые TIMER_LOOP_INTERVAL_SEC проверяет таймеры лидов.

    3-мин таймер: если received_at >= 3 мин назад AND acked=false AND timer_3min_sent=false →
        - генерим короткий системный текст через Claude
        - сохраняем в lead.timer_3min_text
        - шлём TG-уведомление в Saved Messages
        - mark timer_3min_sent=true
    """
    log.info(f"timer_loop: started (interval={TIMER_LOOP_INTERVAL_SEC}s, 3min={TIMER_3MIN_SEC}s, 15min={TIMER_15MIN_SEC}s)")
    while True:
        try:
            now = datetime.now(timezone.utc)
            changed = False
            for L in leads_inbox:
                if L.get("timer_3min_sent"):
                    continue
                if L.get("acked"):
                    continue
                # Build 22: seen=true → Vladimir уже открыл лид и видит draft.
                # Timer fallback не нужен, но лид остаётся в new-inbox (не ack)
                # пока он не свайпнет «взял в работу».
                if L.get("seen"):
                    continue
                rec = L.get("received_at", "")
                if not rec:
                    continue
                try:
                    rec_dt = datetime.fromisoformat(rec.replace("Z", "+00:00"))
                except Exception:
                    continue
                if (now - rec_dt).total_seconds() < TIMER_3MIN_SEC:
                    continue

                # Время сработать
                lead_id = L.get("lead_id")
                log.info(f"timer_3min triggered for lead #{lead_id}")
                msg_text = await _generate_3min_message(L)
                L["timer_3min_text"] = msg_text
                L["timer_3min_sent"] = True
                L["timer_3min_at"] = now.isoformat()
                changed = True

                # TG-уведомление
                try:
                    notify = (
                        f"⏱ Лид #{lead_id} ждёт уже 3+ мин — нужна реакция.\n"
                        f"👤 {L.get('name','(нет имени)')[:80]}\n"
                        f"📞 {L.get('phone','—')}\n"
                        f"🔗 {L.get('amocrm_url','')}\n\n"
                        f"📝 Готовый текст для отправки клиенту:\n"
                        f"{msg_text}\n\n"
                        f"Открой приложение → нажми «Открыть WhatsApp» → текст уже подставлен."
                    )
                    if client and await client.is_user_authorized():
                        await client.send_message("me", notify)
                except Exception as e:
                    log.warning(f"timer_3min TG notify failed: {e}")

            if changed:
                save_leads(leads_inbox)

            # Проверяем auto-revert красного статуса по revert_at.
            try:
                _maybe_auto_revert()
            except Exception as e:
                log.warning(f"timer_loop: auto_revert error: {e}")
        except Exception as e:
            log.error(f"timer_loop iteration error: {e}")
        await asyncio.sleep(TIMER_LOOP_INTERVAL_SEC)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    if SESSION_STRING:
        log.info("lifespan: using StringSession from env (Render mode)")
        client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
    else:
        log.info("lifespan: using file session %r (local mode)", SESSION_NAME)
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    log.info("lifespan: connecting")
    await client.connect()
    log.info("lifespan: connected, checking auth")
    if not await client.is_user_authorized():
        raise RuntimeError(
            "Telethon session is not authorized. "
            "Locally: run `python3 list_chats.py` to sign in. "
            "On Render: set TG_SESSION_STRING env var (run migrate_session.py to generate)."
        )
    me = await client.get_me()
    log.info("telethon signed in as %s (id=%s)", me.username or me.first_name, me.id)
    client.add_event_handler(on_new_message, events.NewMessage(chats=CHAT_ID))
    log.info("lifespan: starting backfill")
    await backfill_from_history()
    log.info("lifespan: backfill done, starting listener task")
    task = asyncio.create_task(client.run_until_disconnected())
    timer_task = asyncio.create_task(timer_loop())
    log.info("lifespan: ready (timers active)")
    try:
        yield
    finally:
        task.cancel()
        timer_task.cancel()
        await client.disconnect()


app = FastAPI(lifespan=lifespan)


def check_token(authorization: Optional[str]) -> None:
    if authorization != f"Bearer {WIDGET_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


def check_internal(authorization: Optional[str]) -> None:
    if not INTERNAL_TOKEN:
        raise HTTPException(status_code=503, detail="INTERNAL_TOKEN not configured")
    if authorization != f"Bearer {INTERNAL_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized (internal)")


def check_office_write(authorization: Optional[str]) -> None:
    if not OFFICE_TOKEN:
        raise HTTPException(status_code=503, detail="OFFICE_TOKEN/INTERNAL_TOKEN not configured")
    if authorization != f"Bearer {OFFICE_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized (office)")


@app.get("/health")
async def health():
    return {"ok": True}


def _maybe_auto_revert() -> bool:
    """Если color=red и revert_at прошёл — авто-отправляем зелёный.
    Возвращает True если флипнули."""
    if state.get("color") != "red":
        return False
    revert_at = state.get("revert_at")
    if not revert_at:
        return False
    try:
        rt = datetime.fromisoformat(revert_at.replace("Z", "+00:00"))
    except Exception:
        return False
    if datetime.now(timezone.utc) < rt:
        return False
    # Истёк — отправляем зелёный в чат, обновляем state
    try:
        # send_message — async, но эта функция sync. Делаем fire-and-forget.
        asyncio.create_task(client.send_message(CHAT_ID, SEND_GREEN))
    except Exception as e:
        log.warning(f"auto-revert send_message failed: {e}")
    state["color"] = "green"
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["revert_at"] = None
    state["message_id"] = None
    save_state(state)
    log.info("auto-reverted red → green (revert_at expired)")
    return True


@app.get("/status")
async def status(authorization: Optional[str] = Header(default=None)):
    check_token(authorization)
    _maybe_auto_revert()
    return {
        "color": state.get("color"),
        "updated_at": state.get("updated_at"),
        "revert_at": state.get("revert_at"),
    }


@app.post("/send")
async def send(
    color: str = Query(..., pattern="^(red|green)$"),
    revert_at: Optional[str] = Query(default=None),
    authorization: Optional[str] = Header(default=None),
):
    check_token(authorization)
    emoji = SEND_RED if color == "red" else SEND_GREEN
    msg = await client.send_message(CHAT_ID, emoji)
    state["color"] = color
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["message_id"] = msg.id
    # revert_at сохраняем только для red. Для green — обнуляем
    # (вручную поставленный зелёный — финальный, не должен авто-сбрасываться).
    if color == "red" and revert_at:
        # Валидируем ISO8601
        try:
            datetime.fromisoformat(revert_at.replace("Z", "+00:00"))
            state["revert_at"] = revert_at
        except Exception:
            log.warning(f"invalid revert_at format: {revert_at!r}, ignoring")
            state["revert_at"] = None
    else:
        state["revert_at"] = None
    save_state(state)
    log.info("sent %s as message %s; state updated (revert_at=%s)", color, msg.id, state.get("revert_at"))
    return {"sent": color, "emoji": emoji, "revert_at": state.get("revert_at")}


# ---------------------------------------------------------------------------
# AmoCRM-leads inbox
# Mac assistant scheduler детектит новые лиды (имея cookie через Chromium CDP)
# и POST'ит сюда. iOS app/widget GET'ит /api/leads. Telethon DM'ит Vladimir'у.
# ---------------------------------------------------------------------------


@app.post("/api/internal/lead")
async def internal_lead(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Принять новый лид от Mac assistant. Auth: Bearer INTERNAL_TOKEN."""
    check_internal(authorization)

    payload = normalize_client_timezone_payload(payload)
    lead_id = payload.get("lead_id")
    if not lead_id:
        raise HTTPException(status_code=400, detail="lead_id required")

    # Build 27: для существующего лида merge'им расширенные поля если они
    # пришли (client_tz_*, telegram_username, preferred_channel, request_text,
    # custom_fields, start_message). НЕ затираем acked/seen/timer_* — это
    # user-side state. Это позволяет «починить» старые лиды без удаления.
    #
    # Build 33 (2026-05-03): re-activation. Если scheduler пометил
    # `is_active_stage=true` (лид сейчас в одной из 2 стартовых стадий
    # pipeline) И в backend он уже acked=True → СБРОСИТЬ acked+seen, чтобы
    # лид снова появился в iOS. Это бывает когда AmoCRM Salesbot или коллега
    # вернул лид обратно в «Новый лид» после нашего ack'а — он опять стал
    # «новой заявкой» с нашей точки зрения. Инцидент с Бирутой 2026-05-03.
    is_active_stage = bool(payload.get("is_active_stage"))

    # Build 33.3 (2026-05-03) phone matching: Vladimir 2026-05-03: «для
    # уникальности сверяться не только с именем, но и с номером телефона».
    # Если в inbox уже есть лид с ТАКИМ ЖЕ phone (и не пустым) — это тот
    # же клиент в другой сделке/форме AmoCRM. Защищает от дубликатов в
    # iOS «Новые заявки» когда клиент написал в 2 формы.
    incoming_phone = (payload.get("phone") or "").strip()
    # Нормализуем: только цифры (избегаем +/space/-/() расхождений)
    def _normalize_phone(p: str) -> str:
        return "".join(ch for ch in p if ch.isdigit())
    incoming_phone_norm = _normalize_phone(incoming_phone)
    if incoming_phone_norm and len(incoming_phone_norm) >= 7:
        for L in leads_inbox:
            if L.get("lead_id") == lead_id:
                continue  # сам с собой не сравниваем
            existing_phone_norm = _normalize_phone(L.get("phone") or "")
            if existing_phone_norm == incoming_phone_norm:
                # Тот же клиент. Если предыдущий лид уже обработан (acked) —
                # новый тоже считается уже-обработанным (тот же человек).
                # Если предыдущий не acked — текущий лид также не появится
                # как «новая заявка» в iOS, чтобы не дублировать.
                log.info(f"lead #{lead_id} имеет тот же phone что lead #{L['lead_id']} (Бирута/тёзка) — пропускаем как дубль")
                return {"status": "duplicate_phone", "lead_id": lead_id, "matched_lead_id": L["lead_id"]}

    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            if is_active_stage and L.get("acked"):
                L["acked"] = False
                L["seen"] = False
                L.pop("acked_at", None)
                save_leads(leads_inbox)
                log.info(f"lead #{lead_id} re-activated (вернулся в стартовую стадию AmoCRM)")
            mergeable = (
                "client_city", "client_tz_offset_min", "client_tz_label",
                "telegram_username", "preferred_channel", "request_text",
                "custom_fields",
            )
            updated = False
            for k in mergeable:
                v = payload.get(k)
                if v not in (None, "", []) and L.get(k) in (None, "", []):
                    L[k] = v
                    updated = True
            # Также если scheduler сгенерил свежий start_message, а timer_3min_text
            # пуст или auto-fallback — обновим. Если Vladimir уже видел свой
            # текст (любой непустой), не трогаем.
            sm = (payload.get("start_message") or "").strip()
            if sm and not (L.get("timer_3min_text") or "").strip():
                L["timer_3min_text"] = sm
                L["timer_3min_sent"] = True
                L["timer_3min_at"] = datetime.now(timezone.utc).isoformat()
                updated = True
            # Если телефон / имя в первичном payload отсутствовали (из-за
            # старого scheduler без contact_phone) — допишем при появлении.
            if (not L.get("phone")) and payload.get("phone"):
                L["phone"] = payload["phone"]
                updated = True
            # Build 27.1: обновляем name всегда если scheduler прислал другое
            # значение — он применяет _transliterate_name (Pavel → Павел).
            # Без этого старые лиды навсегда оставались с латинским именем.
            new_name = (payload.get("name") or "").strip()
            if new_name and new_name != (L.get("name") or "").strip():
                L["name"] = new_name
                updated = True
            if updated:
                save_leads(leads_inbox)
                log.info(f"lead #{lead_id} merged extended fields (already in inbox)")
            else:
                log.info(f"lead #{lead_id} already in inbox — skipping")
            return {"status": "duplicate", "lead_id": lead_id, "merged": updated}

    received_at = datetime.now(timezone.utc).isoformat()
    entry = {
        "lead_id": lead_id,
        "name": payload.get("name", ""),
        "phone": payload.get("phone", ""),
        "source": payload.get("source", ""),
        "stage": payload.get("stage", ""),
        "pipeline": payload.get("pipeline", ""),
        "amocrm_url": payload.get("amocrm_url", f"https://realdreamthai.amocrm.ru/leads/detail/{lead_id}"),
        "created_at": payload.get("created_at", ""),
        "received_at": received_at,
        "acked": False,  # iOS отметит после показа; assistant — после записи в vault
        # Build 26: расширенные поля для UX iOS
        "client_city": payload.get("client_city", ""),
        "client_tz_offset_min": payload.get("client_tz_offset_min"),
        "client_tz_label": payload.get("client_tz_label", ""),
        "telegram_username": payload.get("telegram_username", ""),
        "preferred_channel": payload.get("preferred_channel", ""),  # "telegram"|"whatsapp"|""
        "request_text": payload.get("request_text", ""),  # что клиент написал в первичной заявке
        "custom_fields": payload.get("custom_fields") or [],  # все поля из AmoCRM как [{name,value}]
    }
    # Build 26: scheduler передал стартовое сообщение (Claude + playbook/wiki).
    # Используем его вместо fallback простого prompt в timer_loop — сообщение
    # уже сгенерено с полной базой знаний.
    sm = (payload.get("start_message") or "").strip()
    if sm:
        entry["timer_3min_text"] = sm
        entry["timer_3min_sent"] = True  # не запускаем timer_loop fallback
        entry["timer_3min_at"] = received_at
    leads_inbox.insert(0, entry)
    # Trim
    while len(leads_inbox) > LEADS_RETENTION:
        leads_inbox.pop()
    save_leads(leads_inbox)
    log.info(f"new lead #{lead_id} {entry['name']!r} from {entry['source']!r}")

    # Telegram DM в Saved Messages
    try:
        msg = (
            f"🆕 Новый лид #{lead_id}\n"
            f"👤 {entry['name'] or '(имя не указано)'}\n"
            f"📞 {entry['phone'] or '—'}\n"
            f"📍 {entry['source'] or '—'} · {entry['stage'] or ''}\n"
            f"🔗 {entry['amocrm_url']}"
        )
        await client.send_message("me", msg)
    except Exception as e:
        log.warning(f"Telegram notify failed: {e}")

    # APNs push на iOS — мгновенное уведомление о новой заявке (build 20)
    try:
        push_title = "Новая заявка"
        body_parts = []
        if entry["name"]:
            body_parts.append(entry["name"])
        if entry["source"]:
            body_parts.append(entry["source"])
        push_body = " · ".join(body_parts) or f"Лид #{lead_id}"
        sent_count = await send_push_to_all(
            title=push_title,
            body=push_body,
            payload={"kind": "new_lead", "lead_id": lead_id},
        )
        if sent_count > 0:
            log.info(f"APNS: уведомление о лиде #{lead_id} → {sent_count} устройств")
    except Exception as e:
        log.warning(f"APNS push failed: {e}")

    return {"status": "ok", "lead_id": lead_id, "inbox_size": len(leads_inbox)}


@app.post("/api/devices/register")
async def register_device(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """iOS POST'ит APNs device_token при первом запуске (PushManager).
    Сохраняем для последующих push-уведомлений о новых лидах (build 20).
    Дедуп по token — повторная регистрация не плодит записи."""
    check_token(authorization)
    token = (payload.get("device_token") or "").strip()
    if not token or len(token) < 32:
        raise HTTPException(status_code=400, detail="device_token required (hex string)")
    app_version = (payload.get("app_version") or "").strip()
    now = datetime.now(timezone.utc).isoformat()
    # Update or insert
    for d in devices_registry:
        if d.get("device_token") == token:
            d["last_seen_at"] = now
            if app_version:
                d["app_version"] = app_version
            save_devices(devices_registry)
            return {"status": "ok", "action": "refreshed", "total_devices": len(devices_registry)}
    devices_registry.append({
        "device_token": token,
        "registered_at": now,
        "last_seen_at": now,
        "app_version": app_version,
    })
    save_devices(devices_registry)
    log.info(f"device registered: {token[:10]}… app_version={app_version}, total={len(devices_registry)}")
    return {"status": "ok", "action": "registered", "total_devices": len(devices_registry)}


@app.get("/api/leads")
async def list_leads(
    limit: int = Query(50, ge=1, le=200),
    only_unacked: bool = Query(False),
    authorization: Optional[str] = Header(default=None),
):
    """Список последних лидов для iOS / assistant scheduler."""
    check_token(authorization)
    items = leads_inbox
    if only_unacked:
        items = [L for L in items if not L.get("acked")]
    return {
        "count": len(items),
        "leads": items[:limit],
    }


@app.post("/api/leads/{lead_id}/ack")
async def ack_lead(
    lead_id: int,
    authorization: Optional[str] = Header(default=None),
):
    """Пометить лид как обработанный + Build 28: запросить смену статуса
    в AmoCRM на «Взят в работу». Vladimir 2026-05-02: «свайп-вправо =
    взял в работу, должно меняться в CRM». Mac scheduler через 30 сек
    подхватит флаг и сделает API patch."""
    check_token(authorization)
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            L["acked"] = True
            L["acked_at"] = datetime.now(timezone.utc).isoformat()
            # Build 28: пометить для scheduler — нужно перевести в AmoCRM
            # status_id=STATUS_V_RABOTE (82910594). Scheduler сделает API patch
            # при следующем polling, отчитается через /status_changed.
            if not L.get("status_changed"):
                L["pending_status_change"] = "v_rabote"
            save_leads(leads_inbox)
            return {"status": "ok", "lead_id": lead_id}
    raise HTTPException(status_code=404, detail="lead not found")


@app.post("/api/triggers/force_refresh")
async def trigger_force_refresh(
    authorization: Optional[str] = Header(default=None),
):
    """Build 29: iOS pull-to-refresh запрашивает Mac scheduler сделать
    force_check_new_leads (свежий polling AmoCRM сейчас, не ждать 30s tick).
    Backend пишет timestamp в /var/data/refresh_request.json — Mac scheduler
    polling этот файл и при новом timestamp дёргает _check_new_leads."""
    check_token(authorization)
    f = Path(os.environ.get("REFRESH_REQUEST_FILE", "/var/data/refresh_request.json"))
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps({"requested_at": datetime.now(timezone.utc).isoformat()}))
    return {"status": "ok"}


@app.get("/api/internal/triggers/refresh_request")
async def get_refresh_request(
    authorization: Optional[str] = Header(default=None),
):
    """Build 29: Mac scheduler poll'ит этот endpoint каждые 5 сек и если
    timestamp обновился — запускает force_check_new_leads. Альтернатива
    file-trigger (который не доступен с Render → Mac)."""
    check_internal(authorization)
    f = Path(os.environ.get("REFRESH_REQUEST_FILE", "/var/data/refresh_request.json"))
    if not f.exists():
        return {"requested_at": None}
    try:
        return json.loads(f.read_text())
    except Exception:
        return {"requested_at": None}


@app.post("/api/internal/heartbeat")
async def scheduler_heartbeat(
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Этап 1.1: Mac scheduler пишет heartbeat каждые 30 сек.
    iOS GET /api/health/scheduler определяет online/offline по этому ts.
    """
    check_internal(authorization)
    now = datetime.now(timezone.utc).isoformat()
    workers = payload.get("workers") or []
    SCHEDULER_HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
    SCHEDULER_HEARTBEAT_FILE.write_text(json.dumps({
        "last_seen": now,
        "pid": payload.get("pid"),
        "workers_count": len(workers),
        "workers": workers,
    }, ensure_ascii=False))
    return {"status": "ok"}


@app.get("/api/health/scheduler")
async def get_scheduler_health(
    authorization: Optional[str] = Header(default=None),
):
    """Этап 1.1: iOS опрашивает чтобы показать зелёную/красную точку
    «Mac jobs: live/offline» рядом со статус-баннером.
    online = heartbeat был < 90 сек назад."""
    check_token(authorization)
    if not SCHEDULER_HEARTBEAT_FILE.exists():
        return {"online": False, "last_seen": None, "workers_count": 0}
    try:
        data = json.loads(SCHEDULER_HEARTBEAT_FILE.read_text())
        last_seen_iso = data.get("last_seen")
        if not last_seen_iso:
            return {"online": False, "last_seen": None, "workers_count": 0}
        last_seen = datetime.fromisoformat(last_seen_iso.replace("Z", "+00:00"))
        age_sec = (datetime.now(timezone.utc) - last_seen).total_seconds()
        return {
            "online": age_sec < 90,
            "last_seen": last_seen_iso,
            "workers_count": data.get("workers_count", 0),
            "age_sec": int(age_sec),
        }
    except Exception as e:
        log.error(f"scheduler_health: {e}")
        return {"online": False, "last_seen": None, "workers_count": 0}


@app.post("/api/internal/leads/{lead_id}/silent_ack")
async def silent_ack_lead(
    lead_id: int,
    authorization: Optional[str] = Header(default=None),
):
    """Build 28.3: ack лида БЕЗ запроса смены статуса в AmoCRM.
    Используется scheduler `_prune_stale_leads` для скрытия из iOS-inbox
    лидов которые УЖЕ ушли из стартовой стадии (Vladimir/Rustem перевели
    в воронке → не нужно переводить ещё раз).

    Vladimir 2026-05-02: re-process через last_ts reset вызвал
    непредвиденную автозадачу AmoCRM Salesbot — потому что prune
    использовал обычный /ack который теперь ставит pending_status_change.
    Этот endpoint решает проблему: status в AmoCRM не трогаем."""
    check_internal(authorization)
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            L["acked"] = True
            L["acked_at"] = datetime.now(timezone.utc).isoformat()
            L["ack_source"] = "prune"
            save_leads(leads_inbox)
            return {"status": "ok", "lead_id": lead_id}
    raise HTTPException(status_code=404, detail="lead not found")


@app.get("/api/internal/leads/needs_status_change")
async def list_leads_needs_status_change(
    authorization: Optional[str] = Header(default=None),
):
    """Build 28: scheduler poll'ит лидов с pending_status_change."""
    check_internal(authorization)
    items = [L for L in leads_inbox if L.get("pending_status_change")]
    return {"count": len(items), "leads": items}


@app.post("/api/internal/leads/{lead_id}/status_changed")
async def lead_status_changed(
    lead_id: int,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Build 28: scheduler рапортует что статус в AmoCRM поменян."""
    check_internal(authorization)
    success = bool(payload.get("success", True))
    error = (payload.get("error") or "").strip()
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            L["pending_status_change"] = None
            if success:
                L["status_changed"] = True
                L["status_changed_at"] = datetime.now(timezone.utc).isoformat()
            elif error:
                L["status_change_error"] = error
            save_leads(leads_inbox)
            log.info(f"lead#{lead_id}: status_changed reported success={success}")
            return {"status": "ok", "lead_id": lead_id}
    raise HTTPException(status_code=404, detail="lead not found")


@app.post("/api/leads/{lead_id}/unack")
async def unack_lead(
    lead_id: int,
    authorization: Optional[str] = Header(default=None),
):
    """Вернуть лид в «новые заявки» — снять acked. Используется assistant
    scheduler'ом когда `prune_stale_leads` ошибочно ack'нул лид (например
    статус был «Первичный контакт», а Vladimir ещё не свайпнул)."""
    check_token(authorization)
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            L["acked"] = False
            L.pop("acked_at", None)
            save_leads(leads_inbox)
            return {"status": "ok", "lead_id": lead_id}
    raise HTTPException(status_code=404, detail="lead not found")


@app.post("/api/leads/{lead_id}/feedback")
async def lead_feedback(
    lead_id: int,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Build 26: feedback на стартовое сообщение нового лида. Vladimir пишет
    что не нравится → backend ставит regen_feedback + needs_regen=True →
    Mac scheduler `_lead_regen_worker_loop` подхватит, регенерирует через
    Claude (с playbook/wiki/style) и POST'ит обновлённый текст в timer_3min_text.
    """
    check_token(authorization)
    fb = (payload.get("feedback") or "").strip()
    current_draft = (payload.get("current_draft") or "").strip()
    if not fb:
        raise HTTPException(status_code=400, detail="feedback empty")
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            L["regen_feedback"] = fb
            L["regen_current_draft"] = current_draft
            L["needs_regen"] = True
            L["regen_requested_at"] = datetime.now(timezone.utc).isoformat()
            save_leads(leads_inbox)
            log.info(f"lead#{lead_id}: regen requested ({len(fb)} chars feedback)")
            return {"status": "ok", "lead_id": lead_id}
    raise HTTPException(status_code=404, detail="lead not found")


@app.get("/api/internal/leads/needs_regen")
async def list_leads_needs_regen(
    authorization: Optional[str] = Header(default=None),
):
    """Build 26: scheduler poll'ит лидов с needs_regen=True для regenerate."""
    check_internal(authorization)
    items = [L for L in leads_inbox if L.get("needs_regen")]
    return {"count": len(items), "leads": items}


@app.post("/api/internal/leads/{lead_id}/regenerated")
async def lead_regenerated(
    lead_id: int,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Build 26: scheduler рапортует что regen завершён. Body: {start_message, error?}."""
    check_internal(authorization)
    new_text = (payload.get("start_message") or "").strip()
    error = (payload.get("error") or "").strip()
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            if new_text:
                L["timer_3min_text"] = new_text
                L["timer_3min_sent"] = True  # помечаем что текст уже сгенерён
            L["needs_regen"] = False
            L["regen_completed_at"] = datetime.now(timezone.utc).isoformat()
            if error:
                L["regen_error"] = error
            save_leads(leads_inbox)
            log.info(f"lead#{lead_id}: regen completed ({len(new_text)} chars)")
            return {"status": "ok", "lead_id": lead_id}
    raise HTTPException(status_code=404, detail="lead not found")


@app.post("/api/leads/{lead_id}/seen")
async def seen_lead(
    lead_id: int,
    authorization: Optional[str] = Header(default=None),
):
    """Vladimir открыл detail-экран лида. Останавливает 3-min timer (он уже
    видит draft и сам напишет), но лид остаётся в new-inbox — не пропадает
    с главного экрана пока Vladimir не свайпнет «взял в работу» (ack).

    Build 22: разделяет смешанную семантику старого ack — раньше открытие
    detail'a одновременно и timer останавливало, и лид прятало из inbox."""
    check_token(authorization)
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            L["seen"] = True
            L["seen_at"] = datetime.now(timezone.utc).isoformat()
            save_leads(leads_inbox)
            return {"status": "ok", "lead_id": lead_id}
    raise HTTPException(status_code=404, detail="lead not found")


# ---------------------------------------------------------------------------
# Today's tasks (push from Mac assistant scheduler, GET from iOS)
# ---------------------------------------------------------------------------


@app.post("/api/internal/tasks")
async def internal_tasks(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Mac assistant пушит сегодняшний список задач (полностью заменяет).

    Сохраняем по task_id action_state / pending_send / awaiting_since /
    client_reply_preview / needs_regen — это user-state, scheduler не знает
    о нём и не должен затирать."""
    check_internal(authorization)
    global tasks_today
    tasks = payload.get("tasks") or []
    if not isinstance(tasks, list):
        raise HTTPException(status_code=400, detail="'tasks' must be a list")

    # Извлекаем preserve-поля по task_id из текущего state
    PRESERVE_KEYS = (
        "action_state",
        "awaiting_since",
        "client_replied_at",
        "client_reply_preview",
        "pending_send",
        "needs_send",
        "needs_regen",
        "regen_feedback",
        "regen_requested_at",
        "regen_completed_at",
        "needs_close",
        "close_requested_at",
        "close_error",
    )
    prior_by_id = {
        t.get("task_id"): {k: t.get(k) for k in PRESERVE_KEYS if k in t}
        for t in (tasks_today.get("tasks") or [])
        if t.get("task_id") is not None
    }

    merged = []
    for raw_t in tasks:
        t = normalize_client_timezone_payload(raw_t)
        tid = t.get("task_id")
        prior = prior_by_id.get(tid) or {}
        # Если scheduler сам перегенерил suggested_message (regen worker), то
        # эти поля придут в новом payload — оставляем новые. Но user-action
        # поля (action_state, pending_send) — preserve.
        for k, v in prior.items():
            if v is not None and k not in t:
                t[k] = v
        merged.append(t)

    tasks_today = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "tasks": merged,
    }
    save_tasks(tasks_today)
    preserved = sum(1 for t in merged if t.get("action_state") or t.get("pending_send"))
    log.info(f"tasks_today updated: {len(merged)} items ({preserved} с action_state/pending_send preserved)")
    return {"status": "ok", "count": len(merged)}


@app.get("/api/tasks/today")
async def get_tasks_today(authorization: Optional[str] = Header(default=None)):
    """iOS GET'ит сегодняшний список задач + выполненные за последние 24 часа."""
    check_token(authorization)
    _prune_completed_today(tasks_today)

    # Build 33.7 (2026-05-03): подмешиваем `request_text` из leads_inbox по
    # lead_id. Vladimir хочет видеть «что хочет клиент простым языком» в
    # карточке задачи. Сейчас scheduler не пушит request_text в task payload,
    # но он есть в leads_inbox (от _check_new_leads). Делаем enrichment здесь.
    leads_by_id = {L["lead_id"]: L for L in leads_inbox if L.get("lead_id")}

    def _infer_channel_from_lead(L: dict) -> str:
        """Best-effort read-only display fallback for task cards.

        Older scheduler snapshots can miss messenger fields while the same lead still
        has request_text / preferred_channel in leads_inbox. This must never send
        anything; it only restores iOS labels/buttons from stored evidence.
        """
        pref = (L.get("preferred_channel") or "").strip().lower()
        if pref in ("telegram", "whatsapp"):
            return pref
        blob = " ".join(str(L.get(k) or "") for k in ("request_text", "source", "utm_source")).lower()
        if "telegram" in blob or "телеграм" in blob:
            return "telegram"
        if "whatsapp" in blob or "ватсап" in blob or "вацап" in blob:
            return "whatsapp"
        if L.get("telegram_username"):
            return "telegram"
        if L.get("phone"):
            return "whatsapp"
        return ""

    def _mark_missing_contact_context(enriched: dict) -> dict:
        has_contact = any(enriched.get(k) for k in ("phone", "whatsapp_phone", "telegram_username", "telegram", "whatsapp"))
        has_channel = bool(enriched.get("messengers") or enriched.get("last_message_channel") or enriched.get("last_incoming_channel"))
        has_tz = enriched.get("client_tz_offset_min") is not None or bool(enriched.get("client_tz_label"))
        if not (has_contact or has_channel or has_tz):
            enriched.setdefault("contact_lookup_status", "contact_missing")
            enriched.setdefault(
                "contact_action_blocker",
                "Контакт/мессенджер не подтянут. Открой AmoCRM и обнови sync перед отправкой.",
            )
        return enriched

    def _enrich(t: dict) -> dict:
        enriched = dict(t)
        lid = enriched.get("lead_id")
        if not lid:
            if enriched.get("client_tz_offset_min") is None and not enriched.get("client_tz_label"):
                tz = _resolve_tz_from_phone(enriched.get("phone") or enriched.get("whatsapp_phone"))
                if tz:
                    enriched.update(tz)
            return _mark_missing_contact_context(enriched)
        L = leads_by_id.get(lid)
        if not L:
            if enriched.get("client_tz_offset_min") is None and not enriched.get("client_tz_label"):
                tz = _resolve_tz_from_phone(enriched.get("phone") or enriched.get("whatsapp_phone"))
                if tz:
                    enriched.update(tz)
            return _mark_missing_contact_context(enriched)
        for key in ("request_text", "client_city", "client_tz_offset_min", "client_tz_label", "telegram_username"):
            if (enriched.get(key) is None or enriched.get(key) == "") and L.get(key) not in (None, ""):
                enriched[key] = L.get(key)
        if (not enriched.get("phone")) and L.get("phone"):
            enriched["phone"] = L.get("phone")
        if (not enriched.get("whatsapp_phone")) and (L.get("whatsapp_phone") or L.get("phone")):
            enriched["whatsapp_phone"] = L.get("whatsapp_phone") or L.get("phone")
        if enriched.get("client_tz_offset_min") is None and not enriched.get("client_tz_label"):
            tz = _resolve_tz_from_phone(enriched.get("phone") or enriched.get("whatsapp_phone") or L.get("phone"))
            if tz:
                enriched.update(tz)
        channel = _infer_channel_from_lead(L)
        if channel:
            if not enriched.get("last_message_channel"):
                enriched["last_message_channel"] = channel
            if not enriched.get("last_incoming_channel"):
                enriched["last_incoming_channel"] = channel
            messengers = enriched.get("messengers") or []
            if channel not in messengers:
                enriched["messengers"] = [*messengers, channel]
        has_contact = any(enriched.get(k) for k in ("phone", "whatsapp_phone", "telegram_username", "telegram", "whatsapp"))
        has_channel = bool(enriched.get("messengers") or enriched.get("last_message_channel") or enriched.get("last_incoming_channel"))
        has_tz = enriched.get("client_tz_offset_min") is not None or bool(enriched.get("client_tz_label"))
        if not (has_contact or has_channel or has_tz):
            enriched.setdefault("contact_lookup_status", "contact_missing")
            enriched.setdefault(
                "contact_action_blocker",
                "Контакт/мессенджер не подтянут. Открой AmoCRM и обнови sync перед отправкой.",
            )
        # Safety gate (2026-05-15): do NOT fill task-specific suggested_message
        # from new-lead timer_3min_text. That timer text is an emergency first-touch
        # fallback and is not grounded in the task's CRM chat history / Message KB.
        # If scheduler could not generate a task draft, iOS must show an empty draft
        # and the operator must inspect/regenerate instead of seeing a generic text.
        return enriched
    enriched_tasks = [_enrich(t) for t in (tasks_today.get("tasks") or [])]
    enriched_completed = [_enrich(t) for t in (tasks_today.get("completed_today") or [])]

    return {
        "count": len(enriched_tasks),
        "updated_at": tasks_today.get("updated_at"),
        "tasks": enriched_tasks,
        "completed_today": enriched_completed,
    }


@app.post("/api/tasks/{task_id}/feedback")
async def task_feedback(
    task_id: int,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Vladimir пишет feedback на рекомендацию агента — сохраняется на disk
    в /var/data/task_feedback.jsonl и потом подтягивается агентом для обучения."""
    check_token(authorization)
    feedback = (payload.get("feedback") or "").strip()
    if not feedback:
        raise HTTPException(status_code=400, detail="feedback empty")

    # Найти задачу для контекста
    task_meta = None
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            task_meta = t
            break

    entry = {
        "task_id": task_id,
        "lead_id": (task_meta or {}).get("lead_id"),
        "lead_name": (task_meta or {}).get("lead_name"),
        "task_text": (task_meta or {}).get("task_text"),
        "stage": (task_meta or {}).get("stage"),
        "rationale_at_feedback": (task_meta or {}).get("rationale"),
        "suggested_message_at_feedback": (task_meta or {}).get("suggested_message"),
        "feedback": feedback,
        "received_at": datetime.now(timezone.utc).isoformat(),
        # Phase E.2: prompt версия которая генерила этот draft (если scheduler
        # её прислал в /sent ранее). Дальше можно посчитать feedback_rate
        # by prompt_version и видеть какая версия prompt'а лучше работает.
        "prompt_version_at_feedback": (task_meta or {}).get("prompt_version", ""),
    }

    # JSONL: одна задача = одна строка, append-only
    fb_file = Path(os.environ.get("FEEDBACK_FILE", "/var/data/task_feedback.jsonl"))
    fb_file.parent.mkdir(parents=True, exist_ok=True)
    with open(fb_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    # Помечаем задачу как needs_regen — Mac scheduler подхватит и перегенерирует
    # suggested_message с этим feedback'ом в промпте.
    if task_meta is not None:
        task_meta["needs_regen"] = True
        task_meta["regen_feedback"] = feedback
        task_meta["regen_requested_at"] = entry["received_at"]
        save_tasks(tasks_today)

    log.info(f"feedback saved for task#{task_id}: {len(feedback)} chars; regen requested")
    return {"status": "ok", "task_id": task_id, "regen_requested": task_meta is not None}


@app.get("/api/internal/tasks/needs_regen")
async def list_tasks_needs_regen(
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler poll'ит этот endpoint, чтобы найти задачи, требующие
    перегенерации suggested_message после Vladimir's feedback."""
    check_internal(authorization)
    items = [t for t in (tasks_today.get("tasks") or []) if t.get("needs_regen")]
    return {"count": len(items), "tasks": items}


@app.post("/api/tasks/{task_id}/send")
async def request_task_send(
    task_id: int,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """iOS POST'ит финальный (возможно отредактированный) текст для отправки клиенту.

    Mac scheduler через 30 сек подхватит, отправит через Wazzup AmoCRM, отчитается.
    Если edited_message отличается от оригинального suggested_message — Claude
    проанализирует разницу и сохранит как style-edit feedback."""
    check_token(authorization)
    edited = (payload.get("edited_message") or "").strip()
    channel = (payload.get("channel") or "").strip() or None
    if not edited:
        raise HTTPException(status_code=400, detail="edited_message empty")

    target = None
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            target = t
            break
    if target is None:
        raise HTTPException(status_code=404, detail=f"task#{task_id} not found")

    original = (target.get("suggested_message") or "").strip()
    target["needs_send"] = True
    target["pending_send"] = {
        "edited_message": edited,
        "original_message": original,
        "channel": channel,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "status": "pending",
    }
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: send requested ({len(edited)} chars, edited={edited != original})")
    return {"status": "ok", "task_id": task_id, "edited": edited != original}


@app.post("/api/tasks/{task_id}/schedule_send")
async def schedule_task_send(
    task_id: int,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Build 24: отложенная отправка. iOS POST'ит {message, channel, scheduled_at}.
    scheduler `_scheduled_send_worker_loop` (30s polling) каждые 30 сек проверяет
    pending_send.scheduled_at; в назначенное время вызывает прямую отправку
    через WhatsApp Web / Telegram Web в Mac Chromium (стабильный путь),
    минуя нестабильный AmoCRM browser. Wazzup-связка AmoCRM сама подхватит
    исходящее в карточку клиента через несколько секунд.
    """
    check_token(authorization)
    edited = (payload.get("message") or payload.get("edited_message") or "").strip()
    channel = (payload.get("channel") or "").strip().lower()
    scheduled_at = (payload.get("scheduled_at") or "").strip()

    if not edited:
        raise HTTPException(status_code=400, detail="message empty")
    if channel not in ("whatsapp", "telegram"):
        raise HTTPException(status_code=400, detail="channel must be whatsapp|telegram")
    if not scheduled_at:
        raise HTTPException(status_code=400, detail="scheduled_at (ISO datetime) required")
    try:
        # Принимаем ISO 8601 с любой TZ (Z или +07:00 или naive)
        dt = datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"scheduled_at parse error: {e}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    scheduled_iso = dt.isoformat()

    target = None
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            target = t
            break
    if target is None:
        raise HTTPException(status_code=404, detail=f"task#{task_id} not found")

    original = (target.get("suggested_message") or "").strip()
    target["pending_send"] = {
        "edited_message": edited,
        "original_message": original,
        "channel": channel,
        "scheduled_at": scheduled_iso,
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "status": "scheduled",
    }
    # needs_send=False — пока не время. Scheduler scheduled-worker подхватит сам.
    target["needs_send"] = False
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: scheduled send at {scheduled_iso} via {channel} ({len(edited)} chars)")
    return {"status": "ok", "task_id": task_id, "scheduled_at": scheduled_iso, "channel": channel}


@app.post("/api/tasks/{task_id}/cancel_scheduled")
async def cancel_scheduled_send(
    task_id: int,
    authorization: Optional[str] = Header(default=None),
):
    """Build 24: отменить отложенную отправку — Vladimir передумал."""
    check_token(authorization)
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            pend = t.get("pending_send") or {}
            if pend.get("status") == "scheduled":
                t["pending_send"] = None
                t["needs_send"] = False
                save_tasks(tasks_today)
                log.info(f"task#{task_id}: scheduled send cancelled")
                return {"status": "ok", "task_id": task_id}
            raise HTTPException(status_code=400, detail="task is not scheduled")
    raise HTTPException(status_code=404, detail=f"task#{task_id} not found")


@app.get("/api/internal/tasks/scheduled")
async def list_tasks_scheduled_due(
    authorization: Optional[str] = Header(default=None),
):
    """Build 24: scheduler poll'ит scheduled задачи. Возвращаем только те,
    у которых scheduled_at <= now (пора отправлять)."""
    check_internal(authorization)
    now = datetime.now(timezone.utc)
    due = []
    for t in tasks_today.get("tasks") or []:
        pend = t.get("pending_send") or {}
        if pend.get("status") != "scheduled":
            continue
        sched = pend.get("scheduled_at")
        if not sched:
            continue
        try:
            dt = datetime.fromisoformat(sched.replace("Z", "+00:00"))
        except Exception:
            continue
        if dt <= now:
            due.append(t)
    return {"count": len(due), "tasks": due}


@app.get("/api/internal/tasks/needs_send")
async def list_tasks_needs_send(
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler poll'ит задачи, которые iOS попросил отправить."""
    check_internal(authorization)
    items = [t for t in (tasks_today.get("tasks") or []) if t.get("needs_send")]
    return {"count": len(items), "tasks": items}


@app.post("/api/internal/tasks/{task_id}/sent")
async def task_sent(
    task_id: int,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler отчитывается о результате отправки.

    Body: {success: bool, error?: str, edit_analysis?: str}.
    edit_analysis — короткий разбор Claude'а, что Vladimir изменил и почему,
    если редактировал."""
    check_internal(authorization)
    success = bool(payload.get("success"))
    error = (payload.get("error") or "").strip()
    edit_analysis = (payload.get("edit_analysis") or "").strip()
    # Phase E.2 (2026-05-03 «Супермозг»): trace prompt_version который был
    # использован при генерации этого draft'а. Дальше при /feedback на эту
    # task — мы зашьём prompt_version_at_feedback. Это даёт связку «версия
    # prompt'а X → сколько правок» для калибровки.
    prompt_version = (payload.get("prompt_version") or "").strip()

    target = None
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            target = t
            break
    if target is None:
        raise HTTPException(status_code=404, detail=f"task#{task_id} not found")

    pend = target.get("pending_send") or {}
    prior_status = pend.get("status")
    # Не затираем sent_manually — Vladimir уже сам отправил, scheduler только
    # делал edit_analysis. Сохраняем sent_manually-статус.
    if prior_status != "sent_manually":
        pend["status"] = "sent" if success else "failed"
        pend["completed_at"] = datetime.now(timezone.utc).isoformat()
    if error:
        pend["error"] = error
    if edit_analysis:
        pend["edit_analysis"] = edit_analysis
    if prompt_version:
        target["prompt_version"] = prompt_version  # для будущего /feedback корреляции
    target["pending_send"] = pend
    target["needs_send"] = False

    # Build 29: после успешной отправки задача ОСТАЁТСЯ active с
    # action_state="awaiting_reply" — iOS подсветит её зелёной рамкой
    # («работа началась, ждём ответ клиента»). В completed_today задача
    # уезжает только когда:
    #   1. client_replied_at — клиент ответил (см. /api/internal/tasks/{id}/client_replied)
    #   2. close_no_followup — Vladimir сам закрыл
    #   3. task_status_worker увидел is_completed в AmoCRM
    # Vladimir 2026-05-02: «зелёная рамка горит до момента, пока клиент не
    # ответит на первое сообщение, которое я отправил сегодня». Build 26 fix
    # «сразу в completed» эту семантику ломал.
    if success:
        target["action_state"] = "awaiting_reply"
        target["awaiting_since"] = datetime.now(timezone.utc).isoformat()

    save_tasks(tasks_today)
    log.info(f"task#{task_id}: send {pend.get('status')} (analysis={'+' if edit_analysis else '-'}, prior={prior_status}, awaiting_reply={success})")
    return {"status": "ok", "task_id": task_id}


@app.post("/api/tasks/{task_id}/close_no_followup")
async def request_task_close_no_followup(
    task_id: int,
    authorization: Optional[str] = Header(default=None),
):
    """Vladimir нажал «Завершить без новой задачи» — Mac scheduler закроет
    задачу в AmoCRM (is_completed=true) без постановки follow-up'а.
    Сразу убираем из tasks_today, чтобы iOS не показывал её больше."""
    check_token(authorization)
    target = None
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            target = t
            break
    if target is None:
        raise HTTPException(status_code=404, detail=f"task#{task_id} not found")

    target["needs_close"] = True
    target["close_requested_at"] = datetime.now(timezone.utc).isoformat()
    target["action_state"] = "closing"
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: close_no_followup requested")
    return {"status": "ok", "task_id": task_id}


@app.get("/api/internal/tasks/needs_close")
async def list_tasks_needs_close(
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler poll'ит задачи, которые iOS попросил закрыть без followup'а."""
    check_internal(authorization)
    items = [t for t in (tasks_today.get("tasks") or []) if t.get("needs_close")]
    return {"count": len(items), "tasks": items}


@app.post("/api/internal/tasks/{task_id}/closed")
async def task_closed(
    task_id: int,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler отчитывается, что задача закрыта в AmoCRM. Удаляем её
    из tasks_today — она больше не появится в iOS-списке."""
    check_internal(authorization)
    global tasks_today
    success = bool(payload.get("success", True))
    error = (payload.get("error") or "").strip()

    if success:
        before = len(tasks_today.get("tasks") or [])
        # Перемещаем закрытую задачу в completed_today вместо удаления.
        # iOS показывает её в свёрнутом блоке «Выполненные сегодня».
        closed_task = None
        remaining = []
        for t in (tasks_today.get("tasks") or []):
            if t.get("task_id") == task_id:
                closed_task = t
            else:
                remaining.append(t)
        tasks_today["tasks"] = remaining
        if closed_task is not None:
            closed_task["closed_at"] = datetime.now(timezone.utc).isoformat()
            closed_task["close_method"] = "no_followup"
            tasks_today.setdefault("completed_today", []).append(closed_task)
        _prune_completed_today(tasks_today)
        save_tasks(tasks_today)
        log.info(f"task#{task_id}: closed → completed_today (tasks {before} → {len(remaining)}, completed: {len(tasks_today.get('completed_today') or [])})")
    else:
        # Откат: убираем флаг needs_close, чтобы iOS показал задачу снова
        for t in tasks_today.get("tasks") or []:
            if t.get("task_id") == task_id:
                t["needs_close"] = False
                t["action_state"] = None
                t["close_error"] = error
                break
        save_tasks(tasks_today)
        log.warning(f"task#{task_id}: close failed: {error}")
    return {"status": "ok", "task_id": task_id}


@app.post("/api/tasks/{task_id}/mark_sent_manually")
async def task_mark_sent_manually(
    task_id: int,
    payload: Optional[dict] = Body(default=None),
    authorization: Optional[str] = Header(default=None),
):
    """Vladimir нажал «Я уже отправил вручную» — переводим в awaiting_reply +
    сохраняем edited_message (то, что Vladimir реально отправил клиенту).
    Если edited != original suggested — Mac scheduler через 30 сек запустит
    Claude edit_analysis и сохранит в feedback jsonl как стилевой урок."""
    check_token(authorization)
    target = None
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            target = t
            break
    if target is None:
        raise HTTPException(status_code=404, detail=f"task#{task_id} not found")

    note = ""
    edited = ""
    if isinstance(payload, dict):
        note = (payload.get("note") or "").strip()
        edited = (payload.get("edited_message") or "").strip()

    original = (target.get("suggested_message") or "").strip()
    is_edited = bool(edited) and edited != original
    needs_analysis = is_edited

    now_iso = datetime.now(timezone.utc).isoformat()
    # Build 29: возврат к awaiting_reply — задача остаётся active с зелёной
    # рамкой в iOS до ответа клиента или ручного закрытия. См. comment в /sent.
    target["action_state"] = "awaiting_reply"
    target["awaiting_since"] = now_iso
    pend = target.get("pending_send") or {}
    pend["status"] = "sent_manually"
    pend["completed_at"] = now_iso
    if edited:
        pend["edited_message"] = edited
        pend["original_message"] = original
    if note:
        pend["manual_note"] = note
    target["pending_send"] = pend
    # Если редактировал — флагим для send_worker'а, чтобы тот сделал edit_analysis.
    target["needs_send"] = needs_analysis

    save_tasks(tasks_today)
    log.info(f"task#{task_id}: marked sent_manually → awaiting_reply (edited={is_edited})")
    return {"status": "ok", "task_id": task_id, "needs_analysis": is_edited}


@app.post("/api/internal/tasks/{task_id}/client_replied")
async def task_client_replied(
    task_id: int,
    payload: Optional[dict] = Body(default=None),
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler детектил входящее сообщение от клиента → переключаем
    задачу в client_replied (визуально другой цвет)."""
    check_internal(authorization)
    target = None
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            target = t
            break
    if target is None:
        raise HTTPException(status_code=404, detail=f"task#{task_id} not found")
    target["action_state"] = "client_replied"
    target["client_replied_at"] = datetime.now(timezone.utc).isoformat()
    preview = ""
    if isinstance(payload, dict):
        preview = (payload.get("preview") or "")[:300]
        if preview:
            target["client_reply_preview"] = preview
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: client_replied")

    # Background: auto-generate style draft for Vladimir's inbox
    asyncio.create_task(_auto_draft_on_client_reply(dict(target), preview))

    return {"status": "ok", "task_id": task_id}


@app.post("/api/internal/tasks/{task_id}/regenerated")
async def task_regenerated(
    task_id: int,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler POST'ит сюда после регенерации с новыми suggested_message
    и rationale. Сбрасывает needs_regen, обновляет таймстемп."""
    check_internal(authorization)
    sug = (payload.get("suggested_message") or "").strip()
    rat = (payload.get("rationale") or "").strip()
    ctx = (payload.get("context_summary") or "").strip()
    found = False
    for t in tasks_today.get("tasks") or []:
        if t.get("task_id") == task_id:
            if sug:
                t["suggested_message"] = sug
            if rat:
                t["rationale"] = rat
            if ctx:
                t["context_summary"] = ctx
            t["needs_regen"] = False
            t.pop("regen_feedback", None)
            t["regen_completed_at"] = datetime.now(timezone.utc).isoformat()
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail=f"task#{task_id} not found")
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: regenerated, suggested_message len={len(sug)}")
    return {"status": "ok", "task_id": task_id}


@app.get("/api/metrics/feedback_rate")
async def feedback_rate(
    weeks: int = Query(8, ge=1, le=52),
    authorization: Optional[str] = Header(default=None),
):
    """«Супермозг» (2026-05-03): метрика обучения для iOS таб «Метрики».
    Возвращает количество правок Vladimir'а по неделям. Тренд должен идти
    ВНИЗ по мере того как векторная память делает Claude более точным."""
    check_token(authorization)
    fb_file = Path(os.environ.get("FEEDBACK_FILE", "/var/data/task_feedback.jsonl"))
    if not fb_file.exists():
        return {"weeks": []}
    try:
        items = [json.loads(ln) for ln in fb_file.read_text(encoding="utf-8").strip().splitlines() if ln.strip()]
    except Exception as e:
        log.error(f"feedback rate read failed: {e}")
        return {"weeks": []}

    from collections import Counter
    counts: Counter = Counter()
    lengths: dict = {}  # week_start_iso -> [len, len, ...]
    for it in items:
        rec_at = it.get("received_at")
        if not rec_at:
            continue
        try:
            d = datetime.fromisoformat(rec_at.replace("Z", "+00:00")).date()
        except Exception:
            continue
        # начало недели (понедельник)
        monday = d - timedelta(days=d.weekday())
        key = monday.isoformat()
        counts[key] += 1
        lengths.setdefault(key, []).append(len(it.get("feedback") or ""))

    today = datetime.now(timezone.utc).date()
    weeks_data = []
    for i in range(weeks - 1, -1, -1):
        wstart = today - timedelta(days=today.weekday() + 7 * i)
        key = wstart.isoformat()
        ls = lengths.get(key, [])
        avg_len = round(sum(ls) / len(ls), 1) if ls else 0
        weeks_data.append({
            "week_start": key,
            "feedback_count": counts.get(key, 0),
            "avg_feedback_length": avg_len,
        })
    return {"weeks": weeks_data}


@app.get("/api/internal/feedback/recent")
async def list_recent_feedback(
    limit: int = Query(20, ge=1, le=200),
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler GET'ит последние feedback'и для использования в next генерации."""
    check_internal(authorization)
    fb_file = Path(os.environ.get("FEEDBACK_FILE", "/var/data/task_feedback.jsonl"))
    if not fb_file.exists():
        return {"count": 0, "feedback": []}
    try:
        lines = fb_file.read_text(encoding="utf-8").strip().splitlines()[-limit:]
        items = [json.loads(ln) for ln in lines if ln.strip()]
        return {"count": len(items), "feedback": items}
    except Exception as e:
        log.error(f"feedback read failed: {e}")
        return {"count": 0, "feedback": []}


# ---------------------------------------------------------------------------
# News inbox: парсер пушит, iOS approve/reject
# ---------------------------------------------------------------------------


@app.post("/api/internal/news")
async def internal_news(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Парсер пушит batch новостей. Дедуп по id (хеш URL'а).

    Payload: {"items": [{"id": str, "url": str, "title": str, "title_ru": str,
                          "summary_ru": str, "one_liner_ru": str, "source": str,
                          "category": str, "score": int, "published_at": str?}, ...]}
    """
    check_internal(authorization)
    items = payload.get("items") or []
    if not isinstance(items, list):
        raise HTTPException(status_code=400, detail="'items' must be a list")

    added = 0
    for new_item in items:
        nid = new_item.get("id")
        if not nid:
            continue
        if any(n.get("id") == nid for n in news_inbox):
            continue  # dedup
        entry = {
            "id": nid,
            "url": new_item.get("url", ""),
            "source": new_item.get("source", ""),
            "title": new_item.get("title", ""),
            "title_ru": new_item.get("title_ru", ""),
            "summary_ru": new_item.get("summary_ru", ""),
            "one_liner_ru": new_item.get("one_liner_ru", ""),
            "category": new_item.get("category", "other"),
            "score": int(new_item.get("score", 0) or 0),
            "published_at": new_item.get("published_at"),
            "received_at": datetime.now(timezone.utc).isoformat(),
            "status": "pending",
            "approved_message": None,
            "decided_at": None,
            # Различаем источник: parser (RSS/HTML) vs infopovod_rustem (банк РОПа)
            "kind": new_item.get("kind", "parser"),
            # Метакластер инфоповода (taxonomy 12 кластеров — см. infopovod-classification.md)
            "cluster_id": new_item.get("cluster_id"),
            # Дополнительные поля Рустема (только для kind=infopovod_rustem)
            "rustem_for_whom": new_item.get("rustem_for_whom"),
            "rustem_was_in_phuket": new_item.get("rustem_was_in_phuket"),
            "rustem_pressure": new_item.get("rustem_pressure"),
            "rustem_type": new_item.get("rustem_type"),
            "rustem_bank": new_item.get("rustem_bank"),
        }
        news_inbox.insert(0, entry)
        added += 1

    # Trim — keep last 500
    while len(news_inbox) > 500:
        news_inbox.pop()
    save_news(news_inbox)
    log.info(f"news: добавлено {added} новых, всего в inbox: {len(news_inbox)}")
    return {"status": "ok", "added": added, "total": len(news_inbox)}


@app.get("/api/news")
async def list_news(
    status: str = Query("pending", pattern="^(pending|approved|rejected|all)$"),
    kind: str = Query("all", pattern="^(parser|infopovod_rustem|all)$"),
    cluster_id: Optional[str] = Query(default=None),
    limit: int = Query(200, ge=1, le=500),
    authorization: Optional[str] = Header(default=None),
):
    """iOS GET'ит — список новостей по статусу + типу источника + кластеру."""
    check_token(authorization)
    items = news_inbox
    if status != "all":
        items = [n for n in items if n.get("status") == status]
    if kind != "all":
        items = [n for n in items if n.get("kind", "parser") == kind]
    if cluster_id:
        items = [n for n in items if n.get("cluster_id") == cluster_id]
    return {
        "count": len(items),
        "updated_at": items[0].get("received_at") if items else None,
        "news": items[:limit],
    }


@app.post("/api/internal/news/backfill_clusters")
async def backfill_clusters(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """One-time backfill: payload {by_news_id: {nid: cluster_id, ...}}.

    Применяет cluster_id к существующим items в news_inbox. Для items, которые
    раньше пришли без cluster_id."""
    check_internal(authorization)
    mapping = payload.get("by_news_id") or {}
    if not isinstance(mapping, dict):
        raise HTTPException(status_code=400, detail="by_news_id must be dict")
    updated = 0
    for n in news_inbox:
        cid = mapping.get(n.get("id"))
        if cid and n.get("cluster_id") != cid:
            n["cluster_id"] = cid
            updated += 1
    save_news(news_inbox)
    log.info(f"backfill_clusters: updated {updated}/{len(news_inbox)} items")
    return {"status": "ok", "updated": updated, "total": len(news_inbox)}


@app.post("/api/news/{news_id}/approve")
async def approve_news(
    news_id: str,
    payload: Optional[dict] = Body(default=None),
    authorization: Optional[str] = Header(default=None),
):
    """Одобрить инфоповод. payload может содержать {"message": "..."} —
    финальную версию редактированного сообщения для клиента."""
    check_token(authorization)
    for n in news_inbox:
        if n.get("id") == news_id:
            n["status"] = "approved"
            n["decided_at"] = datetime.now(timezone.utc).isoformat()
            if payload and isinstance(payload, dict):
                msg = payload.get("message")
                if isinstance(msg, str) and msg.strip():
                    n["approved_message"] = msg.strip()
            save_news(news_inbox)
            log.info(f"news {news_id}: approved")
            return {"status": "ok", "news_id": news_id}
    raise HTTPException(status_code=404, detail="news not found")


@app.post("/api/news/{news_id}/reject")
async def reject_news(
    news_id: str,
    authorization: Optional[str] = Header(default=None),
):
    """Отклонить инфоповод."""
    check_token(authorization)
    for n in news_inbox:
        if n.get("id") == news_id:
            n["status"] = "rejected"
            n["decided_at"] = datetime.now(timezone.utc).isoformat()
            save_news(news_inbox)
            log.info(f"news {news_id}: rejected")
            return {"status": "ok", "news_id": news_id}
    raise HTTPException(status_code=404, detail="news not found")


# ---------------------------------------------------------------------------
# Free-form task instructions (build 14)
#
# Vladimir пишет в свободной форме «закрой Светлану», «Анну перенеси на завтра»,
# «Олегу поставь follow-up через неделю». Backend сохраняет инструкции в очередь;
# scheduler парсит через Claude и выполняет в AmoCRM (отдельная итерация).
# ---------------------------------------------------------------------------


@app.post("/api/tasks/instructions")
async def post_task_instruction(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """iOS POST'ит свободно-форматную инструкцию. Сохраняем со status=pending.

    task_id — опционально: если инструкция отправлена ИЗ КАРТОЧКИ задачи,
    scheduler знает контекст без парсинга имени клиента. Без task_id —
    глобальная инструкция (например, "закрой все Лагуны")."""
    check_token(authorization)
    text = (payload.get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="'text' required")
    if len(text) > 4000:
        raise HTTPException(status_code=400, detail="'text' too long (max 4000)")

    task_id = payload.get("task_id")
    try:
        task_id = int(task_id) if task_id is not None else None
    except (TypeError, ValueError):
        task_id = None

    iid = f"instr-{int(datetime.now(timezone.utc).timestamp() * 1000)}"
    entry = {
        "id": iid,
        "text": text,
        "task_id": task_id,    # int or None
        "status": "pending",    # pending | parsing | applied | failed
        "created_at": datetime.now(timezone.utc).isoformat(),
        "applied_at": None,
        "result": None,         # сюда scheduler пишет что сделано (или error)
    }
    instructions_log.append(entry)
    # Храним последние 200 инструкций
    if len(instructions_log) > 200:
        del instructions_log[: len(instructions_log) - 200]
    save_instructions(instructions_log)
    log.info(f"instruction {iid} saved (task_id={task_id}): «{text[:80]}»")
    return {"status": "ok", "id": iid}


@app.get("/api/tasks/instructions")
async def list_task_instructions(
    limit: int = 20,
    task_id: Optional[int] = None,
    authorization: Optional[str] = Header(default=None),
):
    """iOS GET'ит последние N инструкций со статусами.
    Если передан task_id — только инструкции по этой задаче (для карточки)."""
    check_token(authorization)
    items = list(instructions_log)
    if task_id is not None:
        items = [i for i in items if i.get("task_id") == task_id]
    items = items[-limit:][::-1]  # newest first
    return {"count": len(items), "instructions": items}


@app.get("/api/internal/tasks/instructions")
async def internal_list_instructions(
    authorization: Optional[str] = Header(default=None),
):
    """Mac scheduler poll'ит pending-инструкции для парсинга."""
    check_internal(authorization)
    pending = [i for i in instructions_log if i.get("status") == "pending"]
    return {"count": len(pending), "instructions": pending}


@app.post("/api/internal/tasks/instructions/{instruction_id}/done")
async def internal_instruction_done(
    instruction_id: str,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Scheduler отчитывается о выполнении: status=applied|failed + result."""
    check_internal(authorization)
    status_val = payload.get("status") or "applied"
    if status_val not in ("applied", "failed", "parsing"):
        raise HTTPException(status_code=400, detail="invalid status")
    result = payload.get("result") or ""
    for entry in instructions_log:
        if entry.get("id") == instruction_id:
            entry["status"] = status_val
            if status_val in ("applied", "failed"):
                entry["applied_at"] = datetime.now(timezone.utc).isoformat()
            entry["result"] = str(result)[:2000]
            save_instructions(instructions_log)
            log.info(f"instruction {instruction_id}: {status_val} — {str(result)[:80]}")
            return {"status": "ok"}
    raise HTTPException(status_code=404, detail="instruction not found")


# ---------------------------------------------------------------------------
# Anthropic API health (build 17)
#
# Scheduler репортит каждый Claude-вызов: ok / balance_low / other_error.
# iOS показывает виджет на главной — сразу видно когда баланс на нуле.
# ---------------------------------------------------------------------------


def _bkk_day_key() -> str:
    """YYYY-MM-DD по Bangkok timezone — для сброса дневных счётчиков."""
    bkk = timezone(timedelta(hours=7))
    return datetime.now(bkk).strftime("%Y-%m-%d")


def _reset_health_if_new_day() -> None:
    today = _bkk_day_key()
    if anthropic_health.get("day_key") != today:
        anthropic_health["day_key"] = today
        anthropic_health["calls_today"] = 0
        anthropic_health["errors_today"] = 0


@app.post("/api/internal/anthropic/event")
async def internal_anthropic_event(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Scheduler репортит каждый Claude-вызов.
    payload: {type: 'ok'|'balance_low'|'error', error?: str}
    """
    check_internal(authorization)
    _reset_health_if_new_day()
    event_type = payload.get("type") or "ok"
    now_iso = datetime.now(timezone.utc).isoformat()

    anthropic_health["calls_today"] = (anthropic_health.get("calls_today") or 0) + 1
    if event_type == "ok":
        anthropic_health["last_ok_at"] = now_iso
    elif event_type == "balance_low":
        anthropic_health["last_balance_error_at"] = now_iso
        anthropic_health["errors_today"] = (anthropic_health.get("errors_today") or 0) + 1
    elif event_type == "error":
        anthropic_health["errors_today"] = (anthropic_health.get("errors_today") or 0) + 1
    save_anthropic_health(anthropic_health)
    return {"status": "ok"}


@app.get("/api/anthropic/health")
async def get_anthropic_health(authorization: Optional[str] = Header(default=None)):
    """iOS подтягивает: status (ok/balance_low/unknown) + дневная статистика."""
    check_token(authorization)
    _reset_health_if_new_day()

    # Считаем status: balance_low если последняя balance-ошибка свежее
    # последнего успешного вызова (или ok-вызовов вообще не было).
    status_val = "unknown"
    last_err = anthropic_health.get("last_balance_error_at")
    last_ok = anthropic_health.get("last_ok_at")
    if last_err and last_ok:
        try:
            err_dt = datetime.fromisoformat(last_err.replace("Z", "+00:00"))
            ok_dt = datetime.fromisoformat(last_ok.replace("Z", "+00:00"))
            status_val = "balance_low" if err_dt > ok_dt else "ok"
        except Exception:
            status_val = "unknown"
    elif last_err and not last_ok:
        status_val = "balance_low"
    elif last_ok and not last_err:
        status_val = "ok"

    return {
        "status": status_val,
        "calls_today": anthropic_health.get("calls_today") or 0,
        "errors_today": anthropic_health.get("errors_today") or 0,
        "last_balance_error_at": last_err,
        "last_ok_at": last_ok,
    }


# ---------------------------------------------------------------------------
# Style Runtime v1 — safe draft/feedback adapter for LeadsStatus app-flow
#
# This lives inside LeadsStatus backend for V1: no live-send, no CRM writes,
# internal/office auth only, sanitized payloads only, and manual_review_only=true
# in every draft response. The implementation is intentionally deterministic so
# tests can verify the safety boundary before any model-backed writer is wired.
# ---------------------------------------------------------------------------

_PII_PATTERNS = (
    re.compile(r"\+?\d[\d\s().-]{8,}\d"),  # phone-like
    re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I),
    re.compile(r"https?://|amocrm\.ru|/leads/detail/", re.I),
    re.compile(r"@[A-Za-z0-9_]{3,}"),
)

_STYLE_FEEDBACK_TYPES = {
    "too_long", "too_short", "not_my_style", "wrong_pack", "missing_fact",
    "too_salesy", "too_cold", "unsafe_claim", "needs_direct_answer", "other",
    # Runtime spec aliases from feedback-intake-schema.md
    "too_pushy", "not_direct_answer", "wrong_fact", "not_my_words", "bad_cta",
    "needs_more_warmth", "needs_more_expertise",
}
_STYLE_USER_ACTIONS = {
    "approved", "edited_approved", "edited_and_approved", "rejected",
    "regenerated", "manual_reply_used",
}
_STYLE_DEAL_SITUATION_TYPES = {
    "transferred", "new_lead", "active", "long_silence", "cold", "unknown",
}
_STYLE_CONTEXT_SOURCE_TYPES = ("call_transcripts", "voice_transcripts", "zoom_transcripts")
_STYLE_RECENT_VOICE_CHANNELS = {"call", "voice", "zoom"}
_STYLE_NOT_ACTUAL_MARKERS = (
    "не актуально", "неактуально", "not_actual", "not actual", "не нужно",
    "пока не", "не интересно", "отлож", "uncertain", "сомне",
)


def _style_text_has_pii(value) -> bool:
    text = str(value or "")
    # Avoid false-positive on ISO dates in sanitized context snapshots.
    text = re.sub(r"\b\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?(?:Z|[+-]\d{2}:?\d{2})?)?\b", "", text)
    return any(p.search(text) for p in _PII_PATTERNS)


def _assert_style_value_no_pii(value, field: str) -> None:
    if isinstance(value, str):
        if _style_text_has_pii(value):
            raise HTTPException(status_code=400, detail=f"PII/raw data detected in {field}")
    elif isinstance(value, list):
        for item in value:
            _assert_style_value_no_pii(item, field)
    elif isinstance(value, dict):
        for item in value.values():
            _assert_style_value_no_pii(item, field)


def _assert_style_payload_no_pii(payload: dict, fields: tuple[str, ...]) -> None:
    for field in fields:
        _assert_style_value_no_pii(payload.get(field), field)


def _style_meaning_says_not_actual(*values) -> bool:
    text = " ".join(str(v or "") for v in values).lower()
    return any(marker in text for marker in _STYLE_NOT_ACTUAL_MARKERS)


def _style_normalize_source_coverage(snapshot: dict) -> dict:
    raw = snapshot.get("source_coverage") if isinstance(snapshot, dict) else None
    if not isinstance(raw, dict):
        raw = {}
    normalized = {}
    for source_type in _STYLE_CONTEXT_SOURCE_TYPES:
        status = raw.get(source_type) or raw.get(source_type.replace("_transcripts", ""))
        if isinstance(status, dict):
            status = status.get("status")
        normalized[source_type] = status or "not_present"
    for key, value in raw.items():
        normalized.setdefault(key, value.get("status") if isinstance(value, dict) else value)
    return normalized


def _style_context_missing_required(snapshot: dict) -> list[str]:
    coverage = _style_normalize_source_coverage(snapshot)
    return [source_type for source_type, status in coverage.items() if status == "missing_required"]


def _style_build_deal_context_snapshot(payload: dict) -> dict:
    """Build/normalize App Request Schema v1.1 deal_context_snapshot inline.

    The backend remains stateless and does not read raw CRM/Obsidian text here.
    Scheduler/app may pass sanitized signals; this adapter normalizes them so the
    router/safety gate does not confuse a readable old timeline with a newer call.
    """
    existing = payload.get("deal_context_snapshot")
    snapshot = dict(existing) if isinstance(existing, dict) else {}
    last_contact = payload.get("last_significant_contact") or snapshot.get("last_vladimir_contact") or {}
    if not isinstance(last_contact, dict):
        last_contact = {}
    snapshot.setdefault("last_vladimir_contact", last_contact)
    snapshot.setdefault("dialogue_transferred", bool(payload.get("dialogue_transferred") or snapshot.get("dialogue_transferred")))
    snapshot["source_coverage"] = _style_normalize_source_coverage(snapshot)
    client_state_raw = snapshot.get("client_state")
    client_state: dict = dict(client_state_raw) if isinstance(client_state_raw, dict) else {}
    last_meaning = last_contact.get("meaning") if isinstance(last_contact, dict) else None
    if not client_state.get("demand_status") and _style_meaning_says_not_actual(last_meaning, payload.get("last_client_message_summary")):
        client_state["demand_status"] = "not_actual"
    snapshot["client_state"] = client_state
    missing_required = _style_context_missing_required(snapshot)
    snapshot["context_status"] = "needs_context_review" if missing_required else snapshot.get("context_status", "ok")
    return snapshot


def _style_context_status(payload: dict) -> tuple[str, list[str], dict]:
    snapshot = _style_build_deal_context_snapshot(payload)
    missing_required = _style_context_missing_required(snapshot)
    status = "needs_context_review" if missing_required else snapshot.get("context_status", "ok")
    return status, missing_required, snapshot


def _style_is_transferred_old_not_actual(payload: dict) -> bool:
    snapshot = _style_build_deal_context_snapshot(payload)
    if not (payload.get("dialogue_transferred") or snapshot.get("dialogue_transferred")):
        return False
    client_state_raw = snapshot.get("client_state")
    client_state: dict = dict(client_state_raw) if isinstance(client_state_raw, dict) else {}
    demand_status = str(client_state.get("demand_status") or "").lower()
    if demand_status in {"not_actual", "uncertain"}:
        return True
    last_contact = payload.get("last_significant_contact") or snapshot.get("last_vladimir_contact") or {}
    if isinstance(last_contact, dict):
        channel = str(last_contact.get("channel") or "").lower()
        if channel in _STYLE_RECENT_VOICE_CHANNELS and _style_meaning_says_not_actual(last_contact.get("meaning")):
            return True
    return False


_STYLE_RUNTIME_R2_CACHE: Optional[dict] = None
_STYLE_RUNTIME_R2_LAST_GOOD: Optional[dict] = None
_STYLE_RUNTIME_HTTP_CACHE: Optional[dict] = None
_STYLE_RUNTIME_HTTP_LAST_GOOD: Optional[dict] = None


def _style_sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _style_pack_entries(index: dict) -> list[dict]:
    packs = index.get("packs") if isinstance(index, dict) else None
    if isinstance(packs, dict):
        return [dict({"pack_id": k}, **(v if isinstance(v, dict) else {})) for k, v in packs.items()]
    if isinstance(packs, list):
        return [p for p in packs if isinstance(p, dict)]
    return []


def _style_pack_in_index(index: dict, pack_id: str) -> bool:
    return any(p.get("pack_id") == pack_id for p in _style_pack_entries(index))


def _style_load_runtime_index() -> dict:
    path = STYLE_RUNTIME_DIR / "style-runtime-index-v1.json"
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            log.warning("style_runtime: failed to parse runtime index at %s", path)
    return {}


def _style_runtime_create_r2_client():
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required for STYLE_RUNTIME_SOURCE=r2") from exc
    return boto3.client(
        "s3",
        endpoint_url=STYLE_RUNTIME_R2_ENDPOINT or None,
        aws_access_key_id=STYLE_RUNTIME_R2_ACCESS_KEY_ID or None,
        aws_secret_access_key=STYLE_RUNTIME_R2_SECRET_ACCESS_KEY or None,
    )


def _style_r2_key(relative_path: str) -> str:
    rel = relative_path.strip().lstrip("/")
    return f"{STYLE_RUNTIME_R2_PREFIX}/{rel}" if STYLE_RUNTIME_R2_PREFIX else rel


def _style_r2_read_text(client, relative_path: str) -> str:
    response = client.get_object(Bucket=STYLE_RUNTIME_R2_BUCKET, Key=_style_r2_key(relative_path))
    body = response["Body"].read()
    return body.decode("utf-8") if isinstance(body, (bytes, bytearray)) else str(body)


def _style_manifest_pack(manifest: dict, pack_id: str) -> Optional[dict]:
    for pack in manifest.get("packs") or []:
        if isinstance(pack, dict) and pack.get("pack_id") == pack_id:
            return pack
    return None


def _style_guarded_runtime_state(source: str, reason: str, *, index: Optional[dict] = None) -> dict:
    return {
        "ok": False,
        "source": source,
        "index": index or {},
        "pack_text": "",
        "pack_path": None,
        "pack_sha256": None,
        "snapshot_version": None,
        "block_reason": reason,
    }


def _style_fetch_r2_snapshot(pack_id: str) -> dict:
    if not STYLE_RUNTIME_R2_BUCKET:
        raise RuntimeError("STYLE_RUNTIME_R2_BUCKET is required")
    client = _style_runtime_create_r2_client()
    manifest_text = _style_r2_read_text(client, "manifest.json")
    manifest = json.loads(manifest_text)
    if manifest.get("manual_review_only") is not True:
        raise RuntimeError("manifest manual_review_only must be true")
    index_meta = manifest.get("runtime_index") or {}
    index_path = index_meta.get("path") or "style-runtime-index-v1.json"
    index_text = _style_r2_read_text(client, index_path)
    expected_index_hash = index_meta.get("sha256")
    if expected_index_hash and _style_sha256_text(index_text) != expected_index_hash:
        raise RuntimeError("runtime index hash mismatch")
    index = json.loads(index_text)
    pack_meta = _style_manifest_pack(manifest, pack_id)
    if not pack_meta:
        raise RuntimeError(f"missing pack in manifest: {pack_id}")
    pack_path = pack_meta.get("path") or f"packs/{pack_id}.md"
    pack_text = _style_r2_read_text(client, pack_path)
    expected_pack_hash = pack_meta.get("sha256")
    actual_pack_hash = _style_sha256_text(pack_text)
    if expected_pack_hash and actual_pack_hash != expected_pack_hash:
        raise RuntimeError(f"pack hash mismatch: {pack_id}")
    return {
        "ok": True,
        "source": "r2",
        "index": index if isinstance(index, dict) else {},
        "pack_text": pack_text,
        "pack_path": pack_path,
        "pack_sha256": actual_pack_hash,
        "snapshot_version": manifest.get("published_at"),
        "block_reason": None,
    }


def _style_fetch_http_snapshot(pack_id: str) -> dict:
    import urllib.request
    if not STYLE_RUNTIME_HTTP_BASE_URL:
        raise RuntimeError("STYLE_RUNTIME_HTTP_BASE_URL is required for STYLE_RUNTIME_SOURCE=http")
    base = STYLE_RUNTIME_HTTP_BASE_URL

    def http_get(path: str) -> str:
        url = f"{base}/{path.lstrip('/')}"
        req = urllib.request.Request(url)
        if STYLE_RUNTIME_HTTP_TOKEN:
            req.add_header("X-Style-Token", STYLE_RUNTIME_HTTP_TOKEN)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.read().decode("utf-8")

    manifest_text = http_get("v1/latest/manifest.json")
    manifest = json.loads(manifest_text)
    if manifest.get("manual_review_only") is not True:
        raise RuntimeError("manifest manual_review_only must be true")
    index_meta = manifest.get("runtime_index") or {}
    index_path = index_meta.get("path") or "style-runtime-index-v1.json"
    index_text = http_get(f"v1/latest/{index_path}")
    expected_index_hash = index_meta.get("sha256")
    if expected_index_hash and _style_sha256_text(index_text) != expected_index_hash:
        raise RuntimeError("runtime index hash mismatch")
    index = json.loads(index_text)
    pack_meta = _style_manifest_pack(manifest, pack_id)
    if not pack_meta:
        raise RuntimeError(f"missing pack in manifest: {pack_id}")
    pack_path = pack_meta.get("path") or f"packs/{pack_id}.md"
    pack_text = http_get(f"v1/latest/{pack_path}")
    expected_pack_hash = pack_meta.get("sha256")
    actual_pack_hash = _style_sha256_text(pack_text)
    if expected_pack_hash and actual_pack_hash != expected_pack_hash:
        raise RuntimeError(f"pack hash mismatch: {pack_id}")
    return {
        "ok": True,
        "source": "http",
        "index": index if isinstance(index, dict) else {},
        "pack_text": pack_text,
        "pack_path": pack_path,
        "pack_sha256": actual_pack_hash,
        "snapshot_version": manifest.get("published_at"),
        "block_reason": None,
    }


def _style_load_runtime_pack(pack_id: str) -> dict:
    global _STYLE_RUNTIME_R2_CACHE, _STYLE_RUNTIME_R2_LAST_GOOD, _STYLE_RUNTIME_HTTP_CACHE, _STYLE_RUNTIME_HTTP_LAST_GOOD
    if STYLE_RUNTIME_SOURCE not in ("r2", "http"):
        index = _style_load_runtime_index()
        return {
            "ok": False,
            "source": "local",
            "index": index,
            "pack_text": "",
            "pack_path": None,
            "pack_sha256": None,
            "snapshot_version": None,
            "block_reason": None,
        }

    if STYLE_RUNTIME_SOURCE == "http":
        cache_ref = _STYLE_RUNTIME_HTTP_CACHE
        last_good_ref = _STYLE_RUNTIME_HTTP_LAST_GOOD
        fetch_fn = _style_fetch_http_snapshot
    else:
        cache_ref = _STYLE_RUNTIME_R2_CACHE
        last_good_ref = _STYLE_RUNTIME_R2_LAST_GOOD
        fetch_fn = _style_fetch_r2_snapshot

    source_name = STYLE_RUNTIME_SOURCE
    now = time.time()
    if cache_ref and cache_ref.get("pack_id") == pack_id and now - cache_ref.get("loaded_at", 0) <= STYLE_RUNTIME_CACHE_TTL_SECONDS:
        state = dict(cache_ref["state"])
        state["source"] = f"{source_name}_cache"
        return state

    try:
        state = fetch_fn(pack_id)
        new_cache = {"pack_id": pack_id, "loaded_at": now, "state": state}
        if STYLE_RUNTIME_SOURCE == "http":
            _STYLE_RUNTIME_HTTP_CACHE = new_cache
            _STYLE_RUNTIME_HTTP_LAST_GOOD = {"pack_id": pack_id, "state": state}
        else:
            _STYLE_RUNTIME_R2_CACHE = new_cache
            _STYLE_RUNTIME_R2_LAST_GOOD = {"pack_id": pack_id, "state": state}
        return state
    except Exception as exc:
        reason = str(exc)
        log.warning("style_runtime: %s snapshot unavailable/rejected: %s", source_name, reason)
        if last_good_ref and last_good_ref.get("pack_id") == pack_id:
            last_good = dict(last_good_ref["state"])
            last_good["source"] = f"{source_name}_last_good"
            return last_good
        return _style_guarded_runtime_state(source_name, reason)


def _style_choose_pack(payload: dict) -> tuple[str, list[str], str]:
    if _style_is_transferred_old_not_actual(payload):
        return (
            "transferred_old_dialogue_reactivation",
            ["silence_reactivation"],
            "Правило #0: переданная старая сделка + звонок/контакт с сигналом «не актуально», поэтому нужна мягкая проверка актуальности до любых silence-правил.",
        )
    text = " ".join(str(payload.get(k) or "") for k in (
        "client_situation_hint", "last_client_message_summary", "deal_stage", "client_last_message_type",
    )).lower()
    if any(k in text for k in ("цена", "price", "roi", "доход", "окуп", "рассроч", "payment")):
        return "price_roi_explanation", ["client_asks_question"], "Запрос связан с ценой/деньгами, поэтому выбран денежный pack с жёстким safety gate."
    if payload.get("silence_days") not in (None, "", 0) or "silence" in text or "молч" in text:
        return "silence_reactivation", [], "Сценарий похож на реактивацию после паузы."
    return "client_asks_question", [], "Клиент задаёт обычный вопрос/просит следующий шаг."


def _style_count_cta(text: str) -> int:
    if not text:
        return 0
    return min(text.count("?") + sum(1 for marker in ("напишите", "скажи", "скажите", "удобно") if marker in text.lower()), 3)


_STYLE_MEMORY_EXAMPLE_TYPES = {"phrase_pattern", "full_structure", "start", "cta", "tone", "micro_pattern"}


def _style_memory_list_contains(values, needle: str) -> bool:
    if not needle:
        return True
    if not values:
        return True
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return True
    normalized = {str(item).strip().lower() for item in values if str(item).strip()}
    if not normalized or normalized.intersection({"any", "all", "*"}):
        return True
    return needle.strip().lower() in normalized


def _style_load_approved_memory_records() -> list[dict]:
    path = STYLE_MEMORY_FILE
    candidate_paths = [path]
    if path.name == "style-memory-v1-approved-batch-a.jsonl":
        candidate_paths.extend([
            STYLE_RUNTIME_DIR / "style-memory-v1.jsonl",
            Path(__file__).resolve().with_name("style-memory-v1-approved-batch-a.jsonl"),
        ])
    for candidate in candidate_paths:
        if candidate.exists():
            path = candidate
            break
    else:
        return []
    records: list[dict] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if not isinstance(record, dict):
                continue
            if record.get("confidence") != "approved":
                continue
            if not record.get("id") or not record.get("text"):
                continue
            records.append(record)
    except Exception:
        log.warning("style_memory: failed to parse approved memory at %s", path, exc_info=True)
        return []
    return records


def _style_select_memory_records(payload: dict, pack_id: str, limit: int = 8) -> dict:
    channel = str(payload.get("channel") or "").lower()
    stage = str(payload.get("deal_stage") or payload.get("client_last_message_type") or "").lower()
    examples: list[dict] = []
    guards: list[dict] = []
    for record in _style_load_approved_memory_records():
        record_pack = record.get("pack_id") or record.get("bucket")
        if record_pack and record_pack != pack_id:
            continue
        if not _style_memory_list_contains(record.get("channel"), channel):
            continue
        if not _style_memory_list_contains(record.get("stage"), stage):
            continue
        record_type = str(record.get("type") or "").strip()
        if record_type == "contraindication":
            guards.append(record)
        elif record_type in _STYLE_MEMORY_EXAMPLE_TYPES:
            examples.append(record)
    return {"examples": examples[:limit], "guards": guards[:limit]}


def _style_format_memory_for_prompt(memory: dict) -> str:
    sections = []
    examples = memory.get("examples") or []
    guards = memory.get("guards") or []
    if examples:
        lines = ["STYLE MEMORY EXAMPLES (approved only; use as style/tone/structure hints, not as CRM facts):"]
        for item in examples:
            lines.append(f"- {item.get('id')} [{item.get('type')}]: {item.get('text')}")
        sections.append("\n".join(lines))
    if guards:
        lines = ["STYLE MEMORY GUARDS (approved contraindications; do NOT use as positive/example phrases):"]
        for item in guards:
            lines.append(f"- {item.get('id')}: {item.get('text')}")
        sections.append("\n".join(lines))
    return "\n\n".join(sections)


def _style_quoted_fragments(text: str) -> list[str]:
    fragments = []
    for left, right in (("«", "»"), ('"', '"'), ("'", "'")):
        pattern = re.escape(left) + r"([^" + re.escape(right) + r"]{3,})" + re.escape(right)
        fragments.extend(match.strip() for match in re.findall(pattern, text or "") if match.strip())
    return fragments


def _style_memory_guard_flags(draft_text: str, memory: dict) -> list[str]:
    if not draft_text:
        return []
    draft_lower = draft_text.lower()
    for guard in memory.get("guards") or []:
        fragments = _style_quoted_fragments(str(guard.get("text") or ""))
        if any(fragment.lower() in draft_lower for fragment in fragments):
            return ["style_memory_contraindication"]
    return []


def _style_client_text_forbidden_flags(draft_text: str) -> list[str]:
    """Hard style boundary for client-facing drafts.

    These are not model preferences. Vladimir explicitly treats em dashes and
    internal meta-formulas like "без давления" as AI tells in messages to clients.
    If the writer emits them, the draft must be routed to manual review instead
    of being shown as a ready-to-send message.
    """
    text = draft_text or ""
    lower = text.lower()
    flags = []
    if "—" in text or "–" in text:
        flags.append("ai_dash_detected")
    if any(marker in lower for marker in (
        "без давления",
        "без подборок",
        "без подборок и давления",
        "не буду давить",
        "не хочу давить",
    )):
        flags.append("internal_style_meta_phrase")
    return flags


def _style_safety_gate(payload: dict, draft_text: str, pack_id: str, style_memory: Optional[dict] = None) -> dict:
    facts = set(payload.get("facts_available") or [])
    summary = " ".join(str(payload.get(k) or "") for k in ("client_situation_hint", "last_client_message_summary")).lower()
    flags = []
    missing = []
    risk = "low"

    context_status, missing_sources, snapshot = _style_context_status(payload)
    if context_status == "needs_context_review":
        risk = "high"
        flags.append("missing_call_context")
        missing.extend(missing_sources)

    price_intent = pack_id == "price_roi_explanation" or any(k in summary for k in ("цена", "price", "roi", "доход", "окуп", "рассроч"))
    if price_intent:
        risk = "high"
        if "price_source_ref" not in facts:
            flags.append("price_without_source")
            missing.append("price_source_ref")
    if any(k in summary for k in ("договор", "юрид", "freehold", "leasehold", "платеж", "платёж")):
        risk = "high"
    cta_count = _style_count_cta(draft_text)
    if cta_count > 1:
        flags.append("too_many_cta")
    flags.extend(_style_client_text_forbidden_flags(draft_text))
    flags.extend(_style_memory_guard_flags(draft_text, style_memory or {}))
    if _style_text_has_pii(draft_text):
        flags.append("pii_detected")

    if payload.get("dialogue_transferred") or snapshot.get("dialogue_transferred"):
        draft_lower = (draft_text or "").lower()
        if any(marker in draft_lower for marker in ("я отправлял", "я отправил", "я присылал", "как я писал", "как я говорил")):
            flags.append("author_confusion")
        if any(marker in draft_lower for marker in ("продолжим подбор", "продолжаем подбор", "дальше по подборке", "смотрим дальше")):
            flags.append("unsupported_continuity_claim")
        client_state = snapshot.get("client_state") if isinstance(snapshot.get("client_state"), dict) else {}
        demand_status = str((client_state or {}).get("demand_status") or "").lower()
        if demand_status in {"not_actual", "uncertain"} and "актуаль" not in draft_lower:
            flags.append("not_actual_client_ignored")
        last_contact = snapshot.get("last_vladimir_contact") if isinstance(snapshot.get("last_vladimir_contact"), dict) else {}
        channel = str((last_contact or {}).get("channel") or "").lower()
        if channel in _STYLE_RECENT_VOICE_CHANNELS and payload.get("last_vladimir_message_summary") and "актуаль" not in draft_lower:
            flags.append("stale_timeline_overrides_recent_call")

    hard_block_flags = {
        "price_without_source", "pii_detected", "too_many_cta", "missing_call_context",
        "author_confusion", "unsupported_continuity_claim", "not_actual_client_ignored",
        "ai_dash_detected", "internal_style_meta_phrase", "style_memory_contraindication",
    }
    flags = list(dict.fromkeys(flags))
    missing = list(dict.fromkeys(missing))
    safety_pass = not any(f in flags for f in hard_block_flags)
    if "missing_call_context" in flags:
        block_reason = "Контекст неполный: есть обязательный источник звонка/voice/Zoom, который не прочитан. Показываем предупреждение вместо черновика."
    elif "ai_dash_detected" in flags or "internal_style_meta_phrase" in flags:
        block_reason = "Черновик похож на AI-текст или содержит внутреннюю формулировку. Нужна ручная правка вместо показа как готового сообщения."
    elif "style_memory_contraindication" in flags:
        block_reason = "Черновик нарушает approved Style Memory contraindication. Нужна ручная правка вместо показа как готового сообщения."
    else:
        block_reason = None if safety_pass else "Нельзя показывать как готовый черновик: не хватает подтверждённых фактов или есть safety-флаг."
    return {
        "pass": safety_pass,
        "risk_level": risk,
        "flags": flags,
        "missing_facts": missing,
        "cta_count": 0 if not safety_pass else cta_count,
        "show_to_vladimir": True,
        "block_reason": block_reason,
        "context_status": context_status,
    }


async def _style_write_draft(payload: dict, pack_id: str, pack_text: str = "") -> str:
    channel = (payload.get("channel") or "app").lower()
    is_messenger = channel in ("whatsapp", "telegram")
    _fallback = ("Коротко: " if is_messenger else "Здравствуйте. ") + "отвечу на вопрос и подскажу следующий шаг."

    if not pack_text:
        return _fallback

    http_base = (STYLE_RUNTIME_HTTP_BASE_URL or "").rstrip("/")
    http_token = STYLE_RUNTIME_HTTP_TOKEN or ""
    if not http_base:
        log.warning("style_draft: STYLE_RUNTIME_HTTP_BASE_URL not set, using placeholder")
        return _fallback

    import urllib.request as _req, urllib.error as _uerr

    def _call() -> str:
        request_body = {"pack_id": pack_id, "payload": payload}
        if pack_text:
            # Audit blocker fix: the backend has already merged the selected
            # runtime pack with approved-only Style Memory. Send that exact
            # merged text to the Mac/Ollama writer so the real HTTP path uses
            # the same prompt context that the safety/debug fields report.
            request_body["pack_text"] = pack_text
        body = json.dumps(request_body).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if http_token:
            headers["X-Style-Token"] = http_token
        req = _req.Request(f"{http_base}/v1/draft", data=body, headers=headers, method="POST")
        try:
            with _req.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
                if data.get("ok"):
                    return data.get("draft_text") or ""
                # Server responded but declined (bad pack, etc.) — real failure
                log.warning("style_draft: Mac server error: %s", data.get("error"))
                return ""
        except _uerr.HTTPError as e:
            log.warning("style_draft: Mac server HTTP %d: %s", e.code, e.read().decode("utf-8", errors="replace")[:200])
            return ""
        except Exception as exc:
            # Network error (Mac off, ngrok down) — use placeholder so draft_api_unavailable is not set
            log.warning("style_draft: Mac server unreachable: %s", exc)
            return _fallback

    return await asyncio.to_thread(_call)


@app.get("/style-runtime/v1/draft-health")
async def style_draft_health(authorization: Optional[str] = Header(default=None)):
    """Diagnostic: ping Mac draft server at STYLE_RUNTIME_HTTP_BASE_URL/v1/draft."""
    check_office_write(authorization)
    http_base = (STYLE_RUNTIME_HTTP_BASE_URL or "").rstrip("/")
    http_token = STYLE_RUNTIME_HTTP_TOKEN or ""
    if not http_base:
        return {"ok": False, "error": "STYLE_RUNTIME_HTTP_BASE_URL not set"}

    import urllib.request as _req, urllib.error as _uerr

    def _ping() -> dict:
        body = json.dumps({"pack_id": "client_asks_question", "payload": {
            "channel": "whatsapp", "deal_stage": "selection",
            "client_last_message_type": "question",
            "last_client_message_summary": "Клиент спрашивает об объекте.",
            "facts_available": [],
        }}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if http_token:
            headers["X-Style-Token"] = http_token
        req = _req.Request(f"{http_base}/v1/draft", data=body, headers=headers, method="POST")
        try:
            with _req.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except _uerr.HTTPError as e:
            return {"ok": False, "error": f"HTTP {e.code}", "detail": e.read().decode("utf-8", errors="replace")[:300]}
        except Exception as exc:
            return {"ok": False, "error": str(exc)[:300]}

    result = await asyncio.to_thread(_ping)
    return result


@app.post("/style-runtime/v1/draft")
async def style_runtime_draft(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    check_office_write(authorization)
    payload = dict(payload)
    payload["deal_context_snapshot"] = _style_build_deal_context_snapshot(payload)
    payload.setdefault("dialogue_transferred", bool(payload["deal_context_snapshot"].get("dialogue_transferred")))
    if "last_significant_contact" not in payload and payload["deal_context_snapshot"].get("last_vladimir_contact"):
        payload["last_significant_contact"] = payload["deal_context_snapshot"].get("last_vladimir_contact")
    required = (
        "request_id", "deal_ref", "channel", "deal_stage", "client_last_message_type", "requested_output",
    )
    missing_required = [k for k in required if payload.get(k) in (None, "")]
    if missing_required:
        raise HTTPException(status_code=400, detail=f"missing required fields: {', '.join(missing_required)}")
    if payload.get("requested_output") != "client_reply_draft":
        raise HTTPException(status_code=400, detail="requested_output must be client_reply_draft")
    _assert_style_payload_no_pii(payload, (
        "deal_ref", "client_situation_hint", "last_client_message_summary", "last_vladimir_message_summary",
        "last_significant_contact", "deal_context_snapshot", "facts_available",
    ))

    pack_id, secondary_pack_ids, router_reason = _style_choose_pack(payload)
    runtime_state = _style_load_runtime_pack(pack_id)
    style_memory = _style_select_memory_records(payload, pack_id)
    runtime_index = runtime_state.get("index") or {}
    if runtime_index and not _style_pack_in_index(runtime_index, pack_id):
        log.warning("style_runtime: selected pack %s absent from runtime index", pack_id)

    pack_text = runtime_state.get("pack_text") or ""
    memory_text = _style_format_memory_for_prompt(style_memory)
    if memory_text:
        pack_text = f"{pack_text}\n\n{memory_text}" if pack_text else memory_text
    draft_text = await _style_write_draft(payload, pack_id, pack_text=pack_text)
    draft_api_failed = bool(runtime_state.get("pack_text")) and not draft_text
    safety = _style_safety_gate(payload, draft_text, pack_id, style_memory=style_memory)
    runtime_block_reason = runtime_state.get("block_reason")
    if STYLE_RUNTIME_SOURCE in ("r2", "http") and not runtime_state.get("ok"):
        safety["pass"] = False
        safety["flags"] = list(dict.fromkeys([*safety["flags"], "runtime_pack_unavailable"]))
        safety["block_reason"] = f"Style runtime {STYLE_RUNTIME_SOURCE.upper()} snapshot unavailable: {runtime_block_reason}"
    if draft_api_failed and not draft_text:
        safety["pass"] = False
        safety["flags"] = list(dict.fromkeys([*safety["flags"], "draft_api_unavailable"]))
        safety["block_reason"] = safety.get("block_reason") or "Claude API недоступен, черновик не сгенерирован."
    if not safety["pass"]:
        draft_text = ""

    return {
        "request_id": payload["request_id"],
        "draft_text": draft_text,
        "variant": "main",
        "pack_id": pack_id,
        "secondary_pack_ids": secondary_pack_ids[:2],
        "manual_review_only": True,
        "used_facts": sorted(set(payload.get("facts_available") or []) - set(safety["missing_facts"])),
        "missing_facts": safety["missing_facts"],
        "cta_count": safety["cta_count"],
        "safety_pass": safety["pass"],
        "risk_level": safety["risk_level"],
        "safety_flags": safety["flags"],
        "router_reason": router_reason,
        "runtime_source": runtime_state.get("source"),
        "runtime_pack_loaded": bool(runtime_state.get("ok")),
        "runtime_pack_path": runtime_state.get("pack_path"),
        "runtime_pack_sha256": runtime_state.get("pack_sha256"),
        "runtime_snapshot_version": runtime_state.get("snapshot_version"),
        "runtime_block_reason": runtime_block_reason,
        "style_memory_loaded": bool((style_memory.get("examples") or []) or (style_memory.get("guards") or [])),
        "style_memory_example_ids": [item.get("id") for item in style_memory.get("examples", [])],
        "style_memory_guard_ids": [item.get("id") for item in style_memory.get("guards", [])],
        "context_status": safety.get("context_status", "ok"),
        "deal_context_snapshot": payload.get("deal_context_snapshot"),
        "last_significant_contact": payload.get("last_significant_contact"),
        "dialogue_transferred": bool(payload.get("dialogue_transferred")),
        "show_to_vladimir": safety["show_to_vladimir"],
        "block_reason": safety["block_reason"],
        "send_performed": False,
        "crm_mutated": False,
    }


@app.post("/style-runtime/v1/feedback")
async def style_runtime_feedback(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    check_office_write(authorization)
    required = ("event_id", "deal_ref", "selected_pack_id", "draft_id", "feedback_type", "user_action")
    missing_required = [k for k in required if payload.get(k) in (None, "")]
    if missing_required:
        raise HTTPException(status_code=400, detail=f"missing required fields: {', '.join(missing_required)}")
    feedback_type = payload.get("feedback_type")
    if feedback_type not in _STYLE_FEEDBACK_TYPES:
        raise HTTPException(status_code=400, detail="unknown feedback_type")
    user_action = payload.get("user_action")
    if user_action not in _STYLE_USER_ACTIONS:
        raise HTTPException(status_code=400, detail="unknown user_action")
    deal_situation_type = payload.get("deal_situation_type") or "unknown"
    if deal_situation_type not in _STYLE_DEAL_SITUATION_TYPES:
        raise HTTPException(status_code=400, detail="unknown deal_situation_type")
    _assert_style_payload_no_pii(payload, (
        "deal_ref", "message_context_ref", "selected_pack_id", "draft_id", "feedback_text_sanitized",
    ))

    now_iso = datetime.now(timezone.utc).isoformat()
    event = {
        "event_id": payload["event_id"],
        "created_at": payload.get("created_at") or now_iso,
        "app_user": payload.get("app_user") or "vladimir",
        "request_id": payload.get("request_id"),
        "deal_ref": payload["deal_ref"],
        "message_context_ref": payload.get("message_context_ref"),
        "selected_pack_id": payload["selected_pack_id"],
        "secondary_pack_ids": payload.get("secondary_pack_ids") or [],
        "deal_situation_type": deal_situation_type,
        "draft_id": payload["draft_id"],
        "draft_version": payload.get("draft_version") or 1,
        "feedback_type": feedback_type,
        "feedback_text_sanitized": (payload.get("feedback_text_sanitized") or "").strip(),
        "before_features": payload.get("before_features") or {},
        "after_features": payload.get("after_features") or {},
        "user_action": user_action,
        "promotion_status": payload.get("promotion_status") or "candidate_observation",
        "no_send_side_effect": True,
        "crm_mutated": False,
    }
    STYLE_RUNTIME_FEEDBACK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with STYLE_RUNTIME_FEEDBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")
    return {"status": "ok", "event_id": event["event_id"], "promotion_status": event["promotion_status"]}


# ---------------------------------------------------------------------------
# Packet C — Office draft inbox (approval bridge between office pipeline and iOS)
#
# POST  /api/office/drafts                         auth: OFFICE_TOKEN — office creates draft
# GET   /api/office/drafts/pending                 auth: WIDGET_TOKEN — iOS reads pending
# POST  /api/office/drafts/{id}/approve            auth: WIDGET_TOKEN — Vladimir approves
# POST  /api/office/drafts/{id}/reject             auth: WIDGET_TOKEN — Vladimir rejects
# POST  /api/office/drafts/{id}/select_variant     auth: WIDGET_TOKEN — Vladimir picks a variant
# GET   /api/internal/office/drafts/approved       auth: OFFICE_TOKEN — send_worker polls approved
# POST  /api/internal/office/drafts/{id}/claim     auth: OFFICE_TOKEN — send_worker claims draft
# POST  /api/internal/office/drafts/{id}/consume   auth: OFFICE_TOKEN — send_worker finalises draft
# ---------------------------------------------------------------------------

# --- Style lint ---
_EM_DASH = "—"


def _em_dash_lint(text: str) -> bool:
    """Return True if text passes (contains no em-dash)."""
    return _EM_DASH not in text


def _strip_em_dash(text: str) -> str:
    """Replace em-dash with colon. Logs a warning."""
    if _EM_DASH in text:
        log.warning("em_dash_lint: stripped em-dash from text snippet: %s", text[:80])
    return text.replace(_EM_DASH, ":")


def _lint_variants(variants: list) -> list:
    """Apply em-dash lint to all variants in place. Returns updated list."""
    for v in variants:
        for field in ("text", "rationale"):
            val = v.get(field, "")
            if not _em_dash_lint(val):
                v[field] = _strip_em_dash(val)
                v["em_dash_lint"] = False
            else:
                v.setdefault("em_dash_lint", True)
    return variants


_SEND_FORBIDDEN_VARIANT_KEYS = {
    "send", "send_now", "channel_send", "real_send", "needs_send", "pending_send",
    "crm_update", "status_change", "task_close", "lead_status",
    "approved_at", "send_trace_id", "claimed_at", "claimed_by", "claim_expires_at",
    "send_status", "consumed_at", "consumed_by_scheduler_at",
}


def _variants_json_equal(left, right) -> bool:
    return json.dumps(left, ensure_ascii=False, sort_keys=True) == json.dumps(right, ensure_ascii=False, sort_keys=True)


def _normalize_structured_variants(payload: dict) -> list:
    """Validate and normalize canonical structured_variants with legacy variants alias."""
    has_structured = "structured_variants" in payload
    has_legacy = "variants" in payload
    structured = payload.get("structured_variants") if has_structured else None
    legacy = payload.get("variants") if has_legacy else None

    if has_structured and has_legacy and not _variants_json_equal(structured, legacy):
        raise HTTPException(status_code=400, detail="structured_variants and variants differ")

    raw_variants = structured if has_structured else legacy
    if raw_variants in (None, []):
        return []
    if not isinstance(raw_variants, list):
        raise HTTPException(status_code=400, detail="structured_variants must be a list")

    normalized = []
    seen_ids = set()
    for idx, item in enumerate(raw_variants):
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail=f"variant[{idx}] must be an object")
        forbidden = sorted(set(item) & _SEND_FORBIDDEN_VARIANT_KEYS)
        if forbidden:
            raise HTTPException(status_code=400, detail=f"variant contains forbidden keys: {', '.join(forbidden)}")
        variant_id = item.get("id")
        text = item.get("text")
        if not isinstance(variant_id, str) or not variant_id.strip():
            raise HTTPException(status_code=400, detail=f"variant[{idx}].id required")
        if variant_id in seen_ids:
            raise HTTPException(status_code=400, detail=f"duplicate variant id: {variant_id}")
        if not isinstance(text, str) or not text.strip():
            raise HTTPException(status_code=400, detail=f"variant[{idx}].text required")
        seen_ids.add(variant_id)
        normalized.append(dict(item))

    return _lint_variants(normalized)


def _sync_structured_variants_alias(draft: dict) -> None:
    """Ensure a touched draft exposes canonical structured_variants and legacy variants."""
    structured = draft.get("structured_variants")
    legacy = draft.get("variants")
    if structured is None and legacy is None:
        structured = []
    elif structured is None:
        structured = legacy or []
    elif legacy is None:
        legacy = structured or []
    if legacy is None:
        legacy = structured or []
    draft["structured_variants"] = structured
    draft["variants"] = legacy
    draft.setdefault("structured_variant_decisions", [])


def _draft_public_response(draft: dict) -> dict:
    response = dict(draft)
    _sync_structured_variants_alias(response)
    return response

async def _auto_draft_on_client_reply(task: dict, preview: str) -> None:
    """Background: generate style-runtime draft when client replies; save as OfficeDraft."""
    task_id = task.get("task_id")
    lead_id = task.get("lead_id") or task_id
    try:
        # Determine channel
        messengers = task.get("messengers") or []
        pref = (task.get("preferred_channel") or "").lower()
        if pref in ("whatsapp", "telegram", "email"):
            channel = pref
        elif any(("telegram" in str(m).lower()) for m in messengers):
            channel = "telegram"
        elif any(("whatsapp" in str(m).lower()) for m in messengers):
            channel = "whatsapp"
        else:
            channel = "whatsapp"  # most common default

        # Infer deal_stage from task action_state
        action = (task.get("action_state") or "").lower()
        if "replied" in action or "waiting" in action:
            deal_stage = "question"
        elif task.get("silence_days", 0) and int(task.get("silence_days", 0)) > 7:
            deal_stage = "silence"
        else:
            deal_stage = "unknown"

        # Build style-runtime payload (sanitized — no PII). App Request Schema v1.1
        # fields are optional/backward-compatible but passed through when scheduler
        # already supplied sanitized context signals.
        style_payload = {
            "request_id": f"auto-{task_id}-{int(datetime.now(timezone.utc).timestamp())}",
            "deal_ref": f"task_{task_id}",
            "channel": channel,
            "last_client_message_summary": (preview or "Клиент написал сообщение.")[:300],
            "last_vladimir_message_summary": ((task.get("suggested_message") or "")[:200]) or None,
            "last_significant_contact": task.get("last_significant_contact"),
            "dialogue_transferred": bool(task.get("dialogue_transferred")),
            "deal_context_snapshot": task.get("deal_context_snapshot") or {},
            "silence_days": task.get("silence_days"),
            "deal_stage": deal_stage,
            "client_last_message_type": "question",
            "facts_available": [],
            "requested_output": "client_reply_draft",
        }
        style_payload["deal_context_snapshot"] = _style_build_deal_context_snapshot(style_payload)
        style_payload["dialogue_transferred"] = bool(style_payload["deal_context_snapshot"].get("dialogue_transferred"))

        pack_id, secondary_pack_ids, router_reason = _style_choose_pack(style_payload)
        runtime_state = _style_load_runtime_pack(pack_id)
        style_memory = _style_select_memory_records(style_payload, pack_id)
        pack_text = runtime_state.get("pack_text") or ""
        memory_text = _style_format_memory_for_prompt(style_memory)
        if memory_text:
            pack_text = f"{pack_text}\n\n{memory_text}" if pack_text else memory_text
        draft_text = await _style_write_draft(style_payload, pack_id, pack_text=pack_text)
        draft_api_failed = bool(runtime_state.get("pack_text")) and not draft_text
        safety = _style_safety_gate(style_payload, draft_text, pack_id, style_memory=style_memory)

        if draft_api_failed and not draft_text:
            safety["pass"] = False
            safety["flags"] = list(dict.fromkeys([*safety["flags"], "draft_api_unavailable"]))
            safety["block_reason"] = safety.get("block_reason") or "Mac-сервер недоступен, черновик не сгенерирован."

        # Only save draft if there is text to show
        if not draft_text and not safety["pass"]:
            log.info("auto_draft task#%s: skipped — no draft text and safety_pass=False (%s)", task_id, safety.get("flags"))
            return

        import uuid as _uuid
        draft_id = f"style-auto-{task_id}-{_uuid.uuid4().hex[:8]}"
        expires_at = (datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)).isoformat()

        visible_draft_text = draft_text if safety["pass"] else f"[Черновик заблокирован: {safety.get('block_reason', '')}]"
        draft_payload = {
            "draft_id": draft_id,
            "entity_id": str(lead_id or task_id),
            "text": visible_draft_text,
            "category": "style_runtime",
            "created_by_role": "style_engine",
            "expires_at": expires_at,
            "manual_review_only": True,
            "pack_id": pack_id,
            "risk_level": safety.get("risk_level", "low"),
            "missing_facts": safety.get("missing_facts", []),
            "safety_flags": safety.get("flags", []),
            "block_reason": safety.get("block_reason"),
            "context_status": safety.get("context_status", "ok"),
            "deal_context_snapshot": style_payload.get("deal_context_snapshot"),
            "dialogue_transferred": bool(style_payload.get("dialogue_transferred")),
        }

        async with _office_drafts_lock:
            existing = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
            if not existing:
                now_iso = datetime.now(timezone.utc).isoformat()
                draft = {
                    **draft_payload,
                    "status": "pending",
                    "created_at": now_iso,
                    "updated_at": now_iso,
                    "version": 1,
                    "approved_at": None, "approved_by": None, "approval_id": None,
                    "edited_message": None, "edit_log": None, "reject_reason": None,
                    "push_sent_count": 0, "consumed_at": None,
                    "consumed_by_scheduler_at": None, "send_trace_id": None,
                    "send_status": None, "claimed_at": None, "claimed_by": None,
                    "claim_expires_at": None, "retry_count": 0, "next_retry_at": None,
                    "expire_reason": None, "original_text": draft_payload["text"],
                    "feedback_text": None, "needs_regen": False, "regen_count": 0,
                    "last_feedback_at": None, "structured_variants": [], "variants": [],
                    "selected_variant_id": None, "selected_variant_text": None,
                    "selected_at": None, "selected_by": None, "selection_reason": None,
                    "variant_history": [], "structured_variant_decisions": [],
                }
                office_drafts.append(draft)
                save_office_drafts_atomic(office_drafts)

        push_count = await send_push_to_all(
            title="Черновик ответа",
            body=visible_draft_text[:80],
            payload={"kind": "draft_ready", "draft_id": draft_id, "entity_id": str(lead_id or task_id)},
        )
        async with _office_drafts_lock:
            target = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
            if target:
                target["push_sent_count"] = push_count
                save_office_drafts_atomic(office_drafts)

        log.info("auto_draft task#%s: created draft_id=%s pack=%s safety_pass=%s push=%s",
                 task_id, draft_id, pack_id, safety["pass"], push_count)
    except Exception as exc:
        log.warning("auto_draft task#%s: error — %s", task_id, exc)


# Statuses that block re-creation of a draft with the same draft_id.
_DRAFT_TERMINAL_STATUSES = {
    "approved", "approved_sending",
    "rejected", "consumed", "sent",
    "dry_run_consumed", "send_failed", "expired",
}

_STYLE_DRAFT_CONTEXT_FIELDS = (
    "pack_id", "risk_level", "missing_facts", "safety_flags", "block_reason", "manual_review_only",
    "context_status", "deal_context_snapshot", "dialogue_transferred",
)

_CLAIM_TTL_SECONDS = 600  # 10 minutes


@app.post("/api/office/drafts")
async def office_drafts_create(
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Office pipeline pushes a draft for Vladimir to approve on iPhone."""
    check_office_write(authorization)

    draft_id = payload.get("draft_id")
    if not draft_id:
        raise HTTPException(status_code=400, detail="draft_id required")
    text = payload.get("text")
    if not text:
        raise HTTPException(status_code=400, detail="text required")
    entity_id = payload.get("entity_id")
    if not entity_id:
        raise HTTPException(status_code=400, detail="entity_id required")
    expires_at_raw = payload.get("expires_at")
    if not expires_at_raw:
        raise HTTPException(status_code=400, detail="expires_at required")
    try:
        datetime.fromisoformat(expires_at_raw.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(status_code=400, detail="expires_at must be ISO8601")

    now_iso = datetime.now(timezone.utc).isoformat()
    has_variant_payload = "structured_variants" in payload or "variants" in payload
    incoming_structured_variants = _normalize_structured_variants(payload) if has_variant_payload else []

    async with _office_drafts_lock:
        existing = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if existing:
            if existing.get("status") in _DRAFT_TERMINAL_STATUSES:
                raise HTTPException(
                    status_code=409,
                    detail=f"draft already in terminal status: {existing['status']}",
                )
            if has_variant_payload and existing.get("selected_variant_id"):
                existing_variants = existing.get("structured_variants") or existing.get("variants") or []
                current_selected = next(
                    (v for v in existing_variants if v.get("id") == existing.get("selected_variant_id")),
                    None,
                )
                refreshed_selected = next(
                    (v for v in incoming_structured_variants if v.get("id") == existing.get("selected_variant_id")),
                    None,
                )
                if current_selected != refreshed_selected:
                    raise HTTPException(status_code=409, detail="selected variant cannot be removed or changed")
            existing["text"] = text
            existing["entity_id"] = entity_id
            existing["expires_at"] = expires_at_raw
            existing["updated_at"] = now_iso
            existing["version"] = existing.get("version", 1) + 1
            if has_variant_payload:
                existing["structured_variants"] = incoming_structured_variants
                existing["variants"] = incoming_structured_variants
                existing.setdefault("structured_variant_decisions", [])
            else:
                _sync_structured_variants_alias(existing)
            for field in ("category", "incoming_msg_id", "context_watermark", "created_by_role", "trace_id", *_STYLE_DRAFT_CONTEXT_FIELDS):
                if field in payload:
                    existing[field] = payload[field]
            save_office_drafts_atomic(office_drafts)
            log.info("office_draft update: draft_id=%s version=%s", draft_id, existing["version"])
            return {"status": "updated", "draft_id": draft_id, "version": existing["version"]}

        draft = {
            "draft_id": draft_id,
            "entity_id": entity_id,
            "text": text,
            "category": payload.get("category"),
            "incoming_msg_id": payload.get("incoming_msg_id"),
            "context_watermark": payload.get("context_watermark"),
            "created_by_role": payload.get("created_by_role"),
            "trace_id": payload.get("trace_id"),
            "expires_at": expires_at_raw,
            "status": "pending",
            "created_at": now_iso,
            "updated_at": now_iso,
            "version": 1,
            "approved_at": None,
            "approved_by": None,
            "approval_id": None,
            "edited_message": None,
            "edit_log": None,
            "reject_reason": None,
            "push_sent_count": 0,
            "consumed_at": None,
            "consumed_by_scheduler_at": None,
            "send_trace_id": None,
            "send_status": None,
            "claimed_at": None,
            "claimed_by": None,
            "claim_expires_at": None,
            "retry_count": 0,
            "next_retry_at": None,
            "expire_reason": None,
            "original_text": text,
            "feedback_text": None,
            "needs_regen": False,
            "regen_count": 0,
            "last_feedback_at": None,
            "structured_variants": incoming_structured_variants,
            "variants": incoming_structured_variants,
            "selected_variant_id": None,
            "selected_variant_text": None,
            "selected_at": None,
            "selected_by": None,
            "selection_reason": None,
            "variant_history": [],
            "structured_variant_decisions": [],
        }
        for field in _STYLE_DRAFT_CONTEXT_FIELDS:
            if field in payload:
                draft[field] = payload[field]
        office_drafts.append(draft)
        save_office_drafts_atomic(office_drafts)

    push_count = await send_push_to_all(
        title="Новый драфт",
        body=text[:80],
        payload={"kind": "draft_ready", "draft_id": draft_id, "entity_id": str(entity_id)},
    )
    async with _office_drafts_lock:
        target = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if target:
            target["push_sent_count"] = push_count
            save_office_drafts_atomic(office_drafts)

    log.info("office_draft created: draft_id=%s push_sent=%s", draft_id, push_count)
    return {"status": "created", "draft_id": draft_id, "push_sent_count": push_count}


@app.get("/api/office/drafts/pending")
async def office_drafts_pending(
    authorization: Optional[str] = Header(default=None),
):
    """iOS reads drafts waiting for approval (non-expired pending)."""
    check_token(authorization)
    now = datetime.now(timezone.utc)
    result = []
    for d in office_drafts:
        if d.get("status") != "pending":
            continue
        try:
            exp = datetime.fromisoformat(d["expires_at"].replace("Z", "+00:00"))
        except Exception:
            continue
        if exp <= now:
            continue
        result.append(_draft_public_response(d))
    result.sort(key=lambda x: x.get("created_at") or "")
    return {"drafts": result}


@app.post("/api/office/drafts/{draft_id}/approve")
async def office_drafts_approve(
    draft_id: str,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Vladimir approves (optionally edits) a pending draft."""
    check_token(authorization)
    approval_id = payload.get("approval_id")
    edited_message = payload.get("edited_message")
    now = datetime.now(timezone.utc)

    async with _office_drafts_lock:
        draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if not draft:
            raise HTTPException(status_code=404, detail="draft not found")

        if draft.get("status") == "approved" or draft.get("status") == "approved_sending":
            if approval_id and draft.get("approval_id") == approval_id:
                return {"status": "ok", "idempotent": True}
            raise HTTPException(status_code=409, detail="draft already approved")
        if draft.get("status") in ("rejected", "consumed", "sent", "dry_run_consumed", "send_failed", "expired"):
            raise HTTPException(status_code=409, detail=f"draft in terminal status: {draft['status']}")

        try:
            exp = datetime.fromisoformat(draft["expires_at"].replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=400, detail="draft has invalid expires_at")
        if exp <= now:
            raise HTTPException(status_code=410, detail="draft expired")

        draft["status"] = "approved"
        draft["approved_at"] = now.isoformat()
        draft["approved_by"] = "vladimir/ios"
        if approval_id:
            draft["approval_id"] = approval_id
        if edited_message and edited_message != draft.get("text"):
            draft["edited_message"] = edited_message
            draft["edit_log"] = {
                "original": draft.get("text"),
                "edited": edited_message,
                "edited_at": now.isoformat(),
            }
        draft["updated_at"] = now.isoformat()
        save_office_drafts_atomic(office_drafts)

    log.info("office_draft approved: draft_id=%s approval_id=%s edited=%s",
             draft_id, approval_id, bool(edited_message))
    return {"status": "ok", "draft_id": draft_id}


@app.post("/api/office/drafts/{draft_id}/reject")
async def office_drafts_reject(
    draft_id: str,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Vladimir rejects a pending draft."""
    check_token(authorization)
    reject_reason = payload.get("reject_reason")
    now = datetime.now(timezone.utc)

    async with _office_drafts_lock:
        draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if not draft:
            raise HTTPException(status_code=404, detail="draft not found")

        if draft.get("status") == "rejected":
            return {"status": "ok", "idempotent": True}
        if draft.get("status") in _DRAFT_TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"draft in terminal status: {draft['status']}")

        draft["status"] = "rejected"
        draft["reject_reason"] = reject_reason
        draft["updated_at"] = now.isoformat()
        save_office_drafts_atomic(office_drafts)

    log.info("office_draft rejected: draft_id=%s reason=%s", draft_id, (reject_reason or "")[:80])
    return {"status": "ok", "draft_id": draft_id}


@app.post("/api/office/drafts/{draft_id}/feedback")
async def office_drafts_feedback(
    draft_id: str,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Vladimir submits feedback/comment explaining why he wants a different text.
    Sets needs_regen=True so scheduler regenerates the draft with this guidance."""
    check_token(authorization)
    feedback_text = (payload.get("feedback_text") or "").strip()
    if not feedback_text:
        raise HTTPException(status_code=400, detail="feedback_text required")
    current_text = payload.get("current_text") or ""
    now = datetime.now(timezone.utc)

    async with _office_drafts_lock:
        draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if not draft:
            raise HTTPException(status_code=404, detail="draft not found")
        if draft.get("status") in ("approved", "approved_sending", "sent", "dry_run_consumed", "send_failed", "expired"):
            raise HTTPException(status_code=409, detail=f"draft in non-editable status: {draft['status']}")

        if not draft.get("original_text"):
            draft["original_text"] = draft.get("text")
        draft["feedback_text"] = feedback_text
        draft["needs_regen"] = True
        draft["last_feedback_at"] = now.isoformat()
        draft["updated_at"] = now.isoformat()
        save_office_drafts_atomic(office_drafts)

    _append_draft_feedback_log({
        "ts": now.isoformat(),
        "draft_id": draft_id,
        "entity_id": draft.get("entity_id"),
        "category": draft.get("category"),
        "original_text": draft.get("original_text"),
        "current_text": current_text or draft.get("text"),
        "feedback_text": feedback_text,
        "version_before_regen": draft.get("version", 1),
        "regen_count_before": draft.get("regen_count", 0),
    })

    log.info("office_draft feedback: draft_id=%s feedback_len=%d", draft_id, len(feedback_text))
    return {"status": "ok", "draft_id": draft_id, "needs_regen": True}


@app.post("/api/office/drafts/{draft_id}/select_variant")
async def office_drafts_select_variant(
    draft_id: str,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    check_token(authorization)

    variant_id = payload.get("variant_id")
    reason = payload.get("reason")
    decision_id = payload.get("decision_id") or payload.get("idempotency_key")
    if not variant_id:
        raise HTTPException(status_code=400, detail="variant_id required")

    async with _office_drafts_lock:
        draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if not draft:
            raise HTTPException(status_code=404, detail="Draft not found")

        _sync_structured_variants_alias(draft)
        if draft.get("status") != "pending":
            raise HTTPException(
                status_code=409,
                detail=f"Cannot select variant for draft with status={draft.get('status')}",
            )

        variants = draft.get("structured_variants") or draft.get("variants") or []
        selected = next((v for v in variants if v.get("id") == variant_id), None)
        if not selected:
            raise HTTPException(status_code=400, detail=f"variant_id={variant_id} not found")

        decisions = draft.setdefault("structured_variant_decisions", [])
        if decision_id:
            prior_decision = next((d for d in decisions if d.get("decision_id") == decision_id), None)
            if prior_decision:
                if prior_decision.get("selected_variant_id") == variant_id and prior_decision.get("reason") == reason:
                    return {
                        "status": "ok",
                        "ok": True,
                        "idempotent": True,
                        "draft_id": draft_id,
                        "selected_variant_id": variant_id,
                        "text": draft.get("text"),
                        "draft_status": draft.get("status"),
                        "send_performed": False,
                        "backend_sent_message": False,
                        "crm_mutated": False,
                        "queue_cache_mutated": False,
                        "approval_changed": False,
                    }
                raise HTTPException(status_code=409, detail="selection decision id conflict")

        now = datetime.now(timezone.utc).isoformat()
        status_before = draft.get("status")
        decision_entry = {
            "decision_id": decision_id,
            "selected_variant_id": variant_id,
            "selected_text": selected["text"],
            "reason": reason,
            "selected_at": now,
            "selected_by": "vladimir/ios",
            "draft_version_at_selection": draft.get("version", 1),
            "status_before": status_before,
            "no_send_side_effect": True,
        }
        history_entry = {
            "iteration": len(draft.get("variant_history", [])) + 1,
            "selected_variant_id": variant_id,
            "selected_text": selected["text"],
            "reason": reason,
            "selected_at": now,
            "selected_by": "vladimir/ios",
            "decision_id": decision_id,
            "all_variant_texts": {v["id"]: v["text"] for v in variants},
        }
        draft["selected_variant_id"] = variant_id
        draft["selected_variant_text"] = selected["text"]
        draft["selected_at"] = now
        draft["selected_by"] = "vladimir/ios"
        draft["selection_reason"] = reason
        draft["text"] = selected["text"]  # promote to main text for approve flow
        draft["version"] = draft.get("version", 1) + 1
        draft["updated_at"] = now
        decisions.append(decision_entry)
        draft.setdefault("variant_history", []).append(history_entry)
        save_office_drafts_atomic(office_drafts)

    log.info("draft %s: variant %s selected", draft_id, variant_id)
    return {
        "status": "ok",
        "ok": True,
        "draft_id": draft_id,
        "selected_variant_id": variant_id,
        "text": selected["text"],
        "draft_status": "pending",
        "send_performed": False,
        "backend_sent_message": False,
        "crm_mutated": False,
        "queue_cache_mutated": False,
        "approval_changed": False,
    }


@app.get("/api/internal/office/drafts/needs_regen")
async def office_drafts_needs_regen(
    authorization: Optional[str] = Header(default=None),
):
    """Regen worker polls drafts that need regeneration based on Vladimir's feedback."""
    check_office_write(authorization)
    result = [d for d in office_drafts if d.get("needs_regen") and d.get("status") in ("pending", "rejected")]
    return {"drafts": result}


@app.patch("/api/internal/office/drafts/{draft_id}")
async def office_drafts_patch(
    draft_id: str,
    payload: dict = Body(...),
    authorization: Optional[str] = Header(default=None),
):
    """Regen worker updates draft text after regeneration."""
    check_office_write(authorization)
    now = datetime.now(timezone.utc)

    async with _office_drafts_lock:
        draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if not draft:
            raise HTTPException(status_code=404, detail="draft not found")
        if payload.get("status") and payload["status"] not in ("pending", "rejected"):
            raise HTTPException(status_code=400, detail="patch status may only be pending or rejected")

        new_text = payload.get("text")
        if new_text:
            draft["text"] = new_text
            draft["version"] = draft.get("version", 1) + 1
        draft["needs_regen"] = False
        draft["regen_count"] = draft.get("regen_count", 0) + 1
        draft["updated_at"] = now.isoformat()
        if payload.get("status"):
            draft["status"] = payload["status"]
        # Store source-grounding audit fields when provided by regen worker
        _AUDIT_KEYS = (
            "source_retrieval_attempted", "requested_source_topic",
            "selected_source_id", "selected_source_title", "selected_source_status",
            "source_status", "source_relevance_pass", "source_ignored",
            "requires_supplier_approval", "no_approved_source_found",
            "no_relevant_source_found", "source_validation_failed",
        )
        for k in _AUDIT_KEYS:
            if k in payload:
                draft[k] = payload[k]
        if "structured_variants" in payload or "variants" in payload:
            incoming_structured_variants = _normalize_structured_variants(payload)
            draft["structured_variants"] = incoming_structured_variants
            draft["variants"] = incoming_structured_variants
            draft.setdefault("structured_variant_decisions", [])
        else:
            _sync_structured_variants_alias(draft)
        save_office_drafts_atomic(office_drafts)

    log.info("office_draft patched: draft_id=%s version=%s regen_count=%s",
             draft_id, draft.get("version"), draft.get("regen_count"))
    return {"status": "ok", "draft_id": draft_id, "version": draft.get("version")}


@app.get("/api/office/drafts/{draft_id}")
async def office_draft_get(
    draft_id: str,
    authorization: Optional[str] = Header(default=None),
):
    """iOS polls single draft by id (version check after feedback submit)."""
    check_token(authorization)
    draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
    if not draft:
        raise HTTPException(status_code=404, detail="draft not found")
    return _draft_public_response(draft)


@app.get("/api/internal/office/drafts/approved")
async def office_drafts_approved_internal(
    authorization: Optional[str] = Header(default=None),
):
    """Office send_worker polls approved drafts. Also returns approved_sending with expired lease."""
    check_office_write(authorization)
    now = datetime.now(timezone.utc)
    result = []
    for d in office_drafts:
        if d.get("status") == "approved":
            result.append(d)
        elif d.get("status") == "approved_sending":
            exp_raw = d.get("claim_expires_at")
            if exp_raw:
                try:
                    exp = datetime.fromisoformat(exp_raw)
                    if exp <= now:
                        result.append(d)
                except Exception:
                    pass
    result.sort(key=lambda x: x.get("approved_at") or "")
    return {"drafts": result}


@app.post("/api/internal/office/drafts/{draft_id}/claim")
async def office_drafts_claim(
    draft_id: str,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Scheduler atomically claims an approved draft before send (approved → approved_sending)."""
    check_office_write(authorization)
    claimed_by = payload.get("claimed_by", "scheduler")
    now = datetime.now(timezone.utc)
    claim_expires = now + timedelta(seconds=_CLAIM_TTL_SECONDS)

    async with _office_drafts_lock:
        draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if not draft:
            raise HTTPException(status_code=404, detail="draft not found")

        status = draft.get("status")

        if status == "approved_sending":
            exp_raw = draft.get("claim_expires_at")
            lease_expired = True
            if exp_raw:
                try:
                    lease_expired = datetime.fromisoformat(exp_raw) <= now
                except Exception:
                    pass
            if not lease_expired:
                raise HTTPException(status_code=409, detail="draft already claimed by another worker")
            # Lease expired — re-claim
            log.info("office_draft claim: re-claiming expired lease draft_id=%s", draft_id)

        elif status != "approved":
            raise HTTPException(status_code=409, detail=f"draft not claimable (status={status})")

        send_trace_id = str(uuid.uuid4())
        draft["status"] = "approved_sending"
        draft["claimed_at"] = now.isoformat()
        draft["claimed_by"] = claimed_by
        draft["claim_expires_at"] = claim_expires.isoformat()
        draft["send_trace_id"] = send_trace_id
        draft["updated_at"] = now.isoformat()
        save_office_drafts_atomic(office_drafts)

    log.info("office_draft claimed: draft_id=%s trace=%s by=%s", draft_id, send_trace_id, claimed_by)
    return {"ok": True, "send_trace_id": send_trace_id}


@app.post("/api/internal/office/drafts/{draft_id}/consume")
async def office_drafts_consume(
    draft_id: str,
    payload: dict = Body(default={}),
    authorization: Optional[str] = Header(default=None),
):
    """Scheduler finalises a claimed draft: sent / dry_run_consumed / send_failed / expired."""
    check_office_write(authorization)
    send_trace_id = payload.get("send_trace_id")
    send_status = payload.get("send_status")  # "sent" | "dry_run" | "failed" | "expired"
    expire_reason = payload.get("expire_reason")

    if send_status not in ("sent", "dry_run", "failed", "expired"):
        raise HTTPException(status_code=400, detail="send_status must be sent|dry_run|failed|expired")

    status_map = {
        "sent": "sent",
        "dry_run": "dry_run_consumed",
        "failed": "send_failed",
        "expired": "expired",
    }
    final_status = status_map[send_status]
    now = datetime.now(timezone.utc)

    async with _office_drafts_lock:
        draft = next((d for d in office_drafts if d.get("draft_id") == draft_id), None)
        if not draft:
            raise HTTPException(status_code=404, detail="draft not found")

        current_status = draft.get("status")

        # Idempotency: already in a terminal state with same trace_id
        if current_status == final_status and draft.get("send_trace_id") == send_trace_id:
            return {"ok": True, "idempotent": True}

        if current_status not in ("approved_sending", "approved"):
            raise HTTPException(status_code=409, detail=f"draft not consumable (status={current_status})")

        if send_trace_id and draft.get("send_trace_id") and draft["send_trace_id"] != send_trace_id:
            raise HTTPException(status_code=409, detail="send_trace_id mismatch (claimed by different worker)")

        draft["status"] = final_status
        draft["consumed_at"] = now.isoformat()
        draft["consumed_by_scheduler_at"] = now.isoformat()
        draft["send_status"] = send_status
        if expire_reason:
            draft["expire_reason"] = expire_reason
        draft["updated_at"] = now.isoformat()
        save_office_drafts_atomic(office_drafts)

    log.info("office_draft consumed: draft_id=%s final_status=%s trace=%s",
             draft_id, final_status, send_trace_id)
    return {"ok": True, "draft_id": draft_id, "status": final_status}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
