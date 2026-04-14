"""
Debug script: verify Telethon worker session authorization and entity resolution.

Run from the project root (parsing-bot/):

  # Check with default worker session (session_worker_0):
  python -m telegram_bot.scripts.check_entity @exploitex

  # Check with a specific worker index:
  WORKER_SESSION_INDEX=1 python -m telegram_bot.scripts.check_entity @exploitex

  # Check with the bot session instead:
  USE_BOT_SESSION=1 python -m telegram_bot.scripts.check_entity @exploitex

What it does:
  1. Creates the same client that parse_task uses (make_worker_client)
  2. Connects and prints session file path + authorization status
  3. Calls get_me() to confirm identity
  4. Resolves the given @username and prints the entity
  5. Disconnects cleanly

If this fails with ResolveUsernameRequest but channel_validator succeeds,
the worker session is unauthorized — run init_session.py for session_worker_0.
"""

import asyncio
import os
import sys
import traceback

from telethon import TelegramClient
from telethon.tl.functions.contacts import ResolveUsernameRequest


def _make_client() -> tuple[TelegramClient, str]:
    """
    Return (client, session_path) using the same logic as parse_task / channel_validator.
    Set USE_BOT_SESSION=1 to use the bot session instead of the worker session.
    """
    # Import here so the script can be run without the full app being initialized
    from telegram_bot.config import settings
    from telegram_bot.parser.client import get_worker_session_path, get_bot_session_path

    if os.environ.get("USE_BOT_SESSION"):
        session_path = get_bot_session_path()
        label = "BOT session"
    else:
        session_path = get_worker_session_path()
        label = f"WORKER session (WORKER_SESSION_INDEX={os.environ.get('WORKER_SESSION_INDEX', '0')})"

    print(f"\n{'='*60}")
    print(f"Session type  : {label}")
    print(f"Session path  : {session_path}")
    print(f"File exists   : {os.path.exists(session_path + '.session') or os.path.exists(session_path)}")
    print(f"API_ID        : {settings.API_ID}")
    print(f"{'='*60}\n")

    client = TelegramClient(session_path, settings.API_ID, settings.API_HASH)
    return client, session_path


async def check(username: str) -> None:
    client, session_path = _make_client()

    try:
        print(f"[1/5] Connecting to Telegram...")
        await client.connect()
        print(f"      Connected: {client.is_connected()}")

        print(f"\n[2/5] Checking authorization...")
        authorized = await client.is_user_authorized()
        print(f"      Authorized: {authorized}")

        if not authorized:
            print(
                f"\n❌  SESSION IS NOT AUTHORIZED.\n"
                f"    Session file: {session_path}\n"
                f"\n"
                f"    To authorize this session, run:\n"
                f"      SESSION_NAME={os.path.basename(session_path)} "
                f"python -m telegram_bot.scripts.init_session\n"
                f"\n"
                f"    (Or with WORKER_SESSION_INDEX if using the worker session.)\n"
            )
            return

        print(f"\n[3/5] Getting identity (get_me)...")
        me = await client.get_me()
        print(f"      id        : {me.id}")
        print(f"      username  : @{me.username}")
        print(f"      name      : {me.first_name} {me.last_name or ''}")
        print(f"      phone     : {me.phone}")

        # Normalize the input — strip leading @ if present
        bare = username.lstrip("@")
        lookup = f"@{bare}"

        print(f"\n[4/5] Resolving {lookup} via get_entity()...")
        try:
            entity = await client.get_entity(lookup)
            print(f"      ✅ Success!")
            print(f"      type        : {type(entity).__name__}")
            print(f"      id          : {entity.id}")
            print(f"      title       : {getattr(entity, 'title', 'N/A')}")
            print(f"      username    : @{getattr(entity, 'username', 'N/A')}")
            print(f"      access_hash : {getattr(entity, 'access_hash', 'N/A')}")
        except Exception as e:
            print(f"      ❌ get_entity FAILED: {type(e).__name__}: {e}")
            traceback.print_exc()

        print(f"\n[5/5] Direct ResolveUsernameRequest(username={bare!r})...")
        try:
            result = await client(ResolveUsernameRequest(username=bare))
            peer = result.peer
            print(f"      ✅ Success!")
            print(f"      peer type   : {type(peer).__name__}")
            print(f"      chats       : {[getattr(c, 'title', '?') for c in result.chats]}")
            print(f"      users       : {[getattr(u, 'username', '?') for u in result.users]}")
        except Exception as e:
            print(f"      ❌ ResolveUsernameRequest FAILED: {type(e).__name__}: {e}")
            traceback.print_exc()

    finally:
        if client.is_connected():
            await client.disconnect()
            print(f"\nDisconnected cleanly.")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m telegram_bot.scripts.check_entity @username")
        print("       python -m telegram_bot.scripts.check_entity exploitex")
        sys.exit(1)

    username = sys.argv[1]
    asyncio.run(check(username))


if __name__ == "__main__":
    main()
