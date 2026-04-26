"""One-off helper: prints all your chats with their IDs so you can find the work chat."""
import asyncio
import os

from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION_NAME = os.environ.get("SESSION_NAME", "leads_status")


async def main():
    import sys
    only_groups = "--groups" in sys.argv
    async with TelegramClient(SESSION_NAME, API_ID, API_HASH) as client:
        print(f"{'ID':>15}  {'Type':<12}  Title")
        print("-" * 70)
        async for dialog in client.iter_dialogs():
            if dialog.is_channel and not dialog.is_group:
                kind = "channel"
            elif dialog.is_group:
                kind = "group"
            else:
                kind = "user"
            if only_groups and kind != "group":
                continue
            print(f"{dialog.id:>15}  {kind:<12}  {dialog.name}")


if __name__ == "__main__":
    asyncio.run(main())
