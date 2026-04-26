import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query
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
SESSION_NAME = os.environ.get("SESSION_NAME", "leads_status")
SESSION_STRING = os.environ.get("TG_SESSION_STRING", "").strip()
STATE_FILE = Path(os.environ.get("STATE_FILE", "state.json"))

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


state = load_state()
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
