"""One-shot: convert file session → StringSession for Render deploy.

Reads `leads_status.session` (file-based) → prints StringSession value.
Copy the printed string to Render env var `TG_SESSION_STRING`.

Run locally once: `python3 migrate_session.py`
"""
import asyncio
import os
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_NAME = os.environ.get("SESSION_NAME", "leads_status")


async def main():
    print(f"Reading file session: {SESSION_NAME}.session")
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        print("ERROR: file session is not authorized. Sign in first via list_chats.py.")
        return
    me = await client.get_me()
    print(f"Logged in as: {me.username or me.first_name} (id={me.id})")
    string_session = StringSession.save(client.session)
    await client.disconnect()
    print()
    print("=" * 70)
    print("TG_SESSION_STRING (paste into Render → Environment → Secret env var):")
    print("=" * 70)
    print(string_session)
    print("=" * 70)
    print()
    print("⚠️  Этот string даёт ПОЛНЫЙ доступ к Telegram-аккаунту GabetsVO.")
    print("    Не коммить в git, не выкладывай никуда. Только в Render Environment.")


if __name__ == "__main__":
    asyncio.run(main())
