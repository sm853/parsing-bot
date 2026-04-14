"""
Telethon TelegramClient lifecycle management.

SESSION ARCHITECTURE
─────────────────────────────────────────────────────────────────────────────
Bot process  →  sessions/bot_parser_session.session
Worker       →  sessions/session_worker_0.session

Each process uses its OWN .session file so they never share a SQLite
database. Sharing one file between two OS processes causes:
  "database is locked"   (SQLite WAL contention)

Both files must be pre-authorized before first use:

  # Authorize bot session (run once):
  python -m telegram_bot.scripts.init_session bot_parser_session

  # Authorize worker session (run once):
  python -m telegram_bot.scripts.init_session session_worker_0

⚠️  Event-loop rule for workers:
  asyncio.run() creates a NEW event loop on every call.
  Telethon binds to the loop that was active at connect() time.
  Reusing a connected client across asyncio.run() calls raises:
    "The asyncio event loop must not change after connection"
  → make_worker_client() is a FACTORY. Never cache its return value.
"""

import logging
import os

from telethon import TelegramClient

from telegram_bot.config import settings

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

# Resolved once at import time, anchored to this file's location.
# client.py is at telegram_bot/parser/client.py
# ../..  →  project root  →  sessions/
# Safe regardless of the working directory when the process starts.
_SESSIONS_DIR: str = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "sessions")
)
os.makedirs(_SESSIONS_DIR, exist_ok=True)


def _abs_session_path(filename: str) -> str:
    """
    Absolute path for a session file inside sessions/.
    Anchored to __file__, NOT cwd — safe when Celery starts from any directory.
    """
    return os.path.join(_SESSIONS_DIR, filename)


def get_bot_session_path() -> str:
    """Absolute path to the bot-process session file."""
    return _abs_session_path(settings.TELEGRAM_SESSION_NAME)


def get_worker_session_path() -> str:
    """
    Absolute path to the worker session file.
    Name: session_worker_{WORKER_SESSION_INDEX}  (default index = 0)

    Each worker index gets its own file so parallel workers don't
    contend on the same SQLite database.
    """
    idx = os.environ.get("WORKER_SESSION_INDEX", "0")
    return _abs_session_path(f"session_worker_{idx}")


# ── Bot process client (singleton) ────────────────────────────────────────────

_bot_client: TelegramClient | None = None


def get_bot_client() -> TelegramClient:
    """
    Return (or lazily create) the shared bot-process Telethon client.
    Session file: sessions/{TELEGRAM_SESSION_NAME}.session
    """
    global _bot_client
    if _bot_client is None:
        session_path = get_bot_session_path()
        logger.info("get_bot_client: session_file=%r", session_path + ".session")
        _bot_client = TelegramClient(session_path, settings.API_ID, settings.API_HASH)
    return _bot_client


async def start_bot_client() -> None:
    """Connect and verify authorization. Called once at bot startup."""
    client = get_bot_client()
    session_path = get_bot_session_path()
    logger.info("start_bot_client: connecting — session_file=%r", session_path + ".session")

    if not client.is_connected():
        await client.connect()

    if not await client.is_user_authorized():
        raise RuntimeError(
            f"Bot Telegram client is NOT authorized.\n"
            f"Session file: {session_path}.session\n"
            f"Run:  python -m telegram_bot.scripts.init_session {settings.TELEGRAM_SESSION_NAME}"
        )

    me = await client.get_me()
    logger.info(
        "start_bot_client: authorized as id=%s username=@%s name=%s  session_file=%r",
        me.id, me.username, me.first_name, session_path + ".session",
    )


async def stop_bot_client() -> None:
    """Disconnect on bot shutdown."""
    global _bot_client
    if _bot_client and _bot_client.is_connected():
        await _bot_client.disconnect()
        logger.info("stop_bot_client: disconnected")
    _bot_client = None


# ── Worker client factory (NO singleton — fresh instance per task) ─────────────

def make_worker_client() -> TelegramClient:
    """
    Return a NEW, disconnected TelegramClient for use inside a Celery task.
    Session file: sessions/session_worker_{WORKER_SESSION_INDEX}.session

    This is a SEPARATE file from the bot session — no SQLite contention.
    The file must be pre-authorized:
      python -m telegram_bot.scripts.init_session session_worker_0

    Rules:
      1. NEVER cache the returned instance across asyncio.run() calls.
      2. Call this INSIDE the async function passed to asyncio.run().
      3. Always disconnect in a finally block.
    """
    session_path = get_worker_session_path()
    logger.info(
        "make_worker_client: session_file=%r  (WORKER_SESSION_INDEX=%s)",
        session_path + ".session",
        os.environ.get("WORKER_SESSION_INDEX", "0"),
    )
    return TelegramClient(session_path, settings.API_ID, settings.API_HASH)
