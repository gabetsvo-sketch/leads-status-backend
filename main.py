import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
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
            return json.loads(STATE_FILE.read_text())
        except Exception:
            log.exception("failed to load state, starting fresh")
    return {"color": None, "updated_at": None, "message_id": None}


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


state = load_state()
leads_inbox: list = load_leads()  # most-recent-first
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
    log.info("lifespan: ready")
    try:
        yield
    finally:
        task.cancel()
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


@app.get("/status")
async def status(authorization: Optional[str] = Header(default=None)):
    check_token(authorization)
    return {
        "color": state.get("color"),
        "updated_at": state.get("updated_at"),
    }


@app.post("/send")
async def send(
    color: str = Query(..., pattern="^(red|green)$"),
    authorization: Optional[str] = Header(default=None),
):
    check_token(authorization)
    emoji = SEND_RED if color == "red" else SEND_GREEN
    msg = await client.send_message(CHAT_ID, emoji)
    state["color"] = color
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    state["message_id"] = msg.id
    save_state(state)
    log.info("sent %s as message %s; state updated", color, msg.id)
    return {"sent": color, "emoji": emoji}


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
