"""
One-time setup script: authenticate Telethon and save a session file.

Run from the project root (parsing-bot/) BEFORE starting any services.

Usage:
  # Authorize the bot session (used by the bot process and channel_validator):
  python -m telegram_bot.scripts.init_session bot_parser_session

  # Authorize the worker session (used by Celery parse_task):
  python -m telegram_bot.scripts.init_session session_worker_0

  # No argument → defaults to the value of TELEGRAM_SESSION_NAME in .env:
  python -m telegram_bot.scripts.init_session

Both sessions must be authorized before starting docker-compose.
The .session files are saved to sessions/ and must be accessible by both
the telegram_bot container and the celery_worker container via the
'telegram_sessions' Docker volume.
"""

import asyncio
import os
import sys

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from telegram_bot.config import settings


async def init_session(session_name: str) -> None:
    os.makedirs("sessions", exist_ok=True)
    session_path = os.path.join("sessions", session_name)

    print(f"\nSession name : {session_name}")
    print(f"Session file : {os.path.abspath(session_path)}.session")
    print(f"Phone        : {settings.TELEGRAM_PHONE}\n")

    client = TelegramClient(session_path, settings.API_ID, settings.API_HASH)
    await client.connect()

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"✓ Already authorized as @{me.username} (id={me.id}) — nothing to do.")
        await client.disconnect()
        return

    print(f"Authenticating '{session_name}'…")
    await client.send_code_request(settings.TELEGRAM_PHONE)

    code = input("Enter the code you received: ").strip()
    try:
        await client.sign_in(settings.TELEGRAM_PHONE, code)
    except SessionPasswordNeededError:
        password = input("Two-factor auth password: ").strip()
        await client.sign_in(password=password)

    if await client.is_user_authorized():
        me = await client.get_me()
        print(f"✓ Session '{session_name}' saved — authorized as @{me.username} (id={me.id})")
    else:
        print("✗ Authentication failed.")

    await client.disconnect()


if __name__ == "__main__":
    # Accept session name as positional CLI argument, fall back to env/config default.
    if len(sys.argv) >= 2:
        name = sys.argv[1]
    else:
        name = os.environ.get("SESSION_NAME", settings.TELEGRAM_SESSION_NAME)
    asyncio.run(init_session(name))
