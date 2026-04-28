import asyncio
import json
import logging
import os
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
NEWS_FILE = Path(os.environ.get("NEWS_FILE", "news.json"))

# Phase 2 timers (Vladimir spec):
# - 3 мин после получения лида, если не open → системное сообщение (semi-auto)
# - 15 мин после open, если в AmoCRM нет звонка → текстовое сообщение
TIMER_3MIN_SEC = int(os.environ.get("TIMER_3MIN_SEC", "180"))
TIMER_15MIN_SEC = int(os.environ.get("TIMER_15MIN_SEC", "900"))
TIMER_LOOP_INTERVAL_SEC = int(os.environ.get("TIMER_LOOP_INTERVAL_SEC", "60"))

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

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


state = load_state()
leads_inbox: list = load_leads()  # most-recent-first
tasks_today: dict = load_tasks()  # {"updated_at": iso, "tasks": [...]}
news_inbox: list = load_news()    # [{id, url, title, ..., status}, ...]
instructions_log: list = load_instructions()  # [{id, text, status, created_at, applied_at, result}, ...]
client: Optional[TelegramClient] = None


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

    lead_id = payload.get("lead_id")
    if not lead_id:
        raise HTTPException(status_code=400, detail="lead_id required")

    # Дедуп по lead_id
    if any(L.get("lead_id") == lead_id for L in leads_inbox):
        log.info(f"lead #{lead_id} already in inbox — skipping")
        return {"status": "duplicate", "lead_id": lead_id}

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
    }
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

    return {"status": "ok", "lead_id": lead_id, "inbox_size": len(leads_inbox)}


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
    """Пометить лид как обработанный (iOS показал / assistant записал в vault)."""
    check_token(authorization)
    for L in leads_inbox:
        if L.get("lead_id") == lead_id:
            L["acked"] = True
            L["acked_at"] = datetime.now(timezone.utc).isoformat()
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
    for t in tasks:
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
    """iOS GET'ит сегодняшний список задач + выполненные за последние 24 часа.

    completed_today показывается в iOS в свёрнутом блоке внизу — Vladimir
    может развернуть и посмотреть что уже сделано сегодня.
    """
    check_token(authorization)
    # Чистим устаревшие completed на read (не на write — чтобы iOS-pull
    # делал одно и то же даже если scheduler пока не работал).
    _prune_completed_today(tasks_today)
    return {
        "count": len(tasks_today.get("tasks") or []),
        "updated_at": tasks_today.get("updated_at"),
        "tasks": tasks_today.get("tasks") or [],
        "completed_today": tasks_today.get("completed_today") or [],
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
    # делал edit_analysis. Сохраняем sent_manually-статус и awaiting_since.
    if prior_status != "sent_manually":
        pend["status"] = "sent" if success else "failed"
        pend["completed_at"] = datetime.now(timezone.utc).isoformat()
        if success:
            target["action_state"] = "awaiting_reply"
            target["awaiting_since"] = pend["completed_at"]
    if error:
        pend["error"] = error
    if edit_analysis:
        pend["edit_analysis"] = edit_analysis
    target["pending_send"] = pend
    target["needs_send"] = False
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: send {pend.get('status')} (analysis={'+' if edit_analysis else '-'}, prior={prior_status})")
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
    # Если редактировал — флагим для send_worker'а, чтобы тот сделал edit_analysis
    # (без реальной отправки через Wazzup, т.к. status=sent_manually).
    target["needs_send"] = needs_analysis
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: marked sent_manually (edited={is_edited}, len={len(edited)})")
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
    if isinstance(payload, dict):
        preview = (payload.get("preview") or "")[:300]
        if preview:
            target["client_reply_preview"] = preview
    save_tasks(tasks_today)
    log.info(f"task#{task_id}: client_replied")
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
