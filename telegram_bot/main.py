"""
Bot entry point.

Startup sequence:
  1. Create Bot + Dispatcher
  2. Register all routers
  3. Connect Telethon client (for channel validation in handlers)
  4. Start long polling
"""

import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.fsm.storage.redis import RedisStorage

from telegram_bot.config import settings
from telegram_bot.handlers import payments, parsing_flow, start
from telegram_bot.parser.client import start_bot_client, stop_bot_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def on_startup(bot: Bot) -> None:
    logger.info("Starting Telethon client…")
    await start_bot_client()
    logger.info("Bot started.")


async def on_shutdown(bot: Bot) -> None:
    logger.info("Shutting down Telethon client…")
    await stop_bot_client()


def _make_bot_session() -> AiohttpSession | None:
    if not settings.TELEGRAM_PROXY_HOST or not settings.TELEGRAM_PROXY_PORT:
        return None
    proxy_url = (
        f"socks5://{settings.TELEGRAM_PROXY_USER}:{settings.TELEGRAM_PROXY_PASS}"
        f"@{settings.TELEGRAM_PROXY_HOST}:{settings.TELEGRAM_PROXY_PORT}"
    )
    logger.info("Bot HTTP session: using SOCKS5 proxy %s:%s",
                settings.TELEGRAM_PROXY_HOST, settings.TELEGRAM_PROXY_PORT)
    return AiohttpSession(proxy=proxy_url)


async def main() -> None:
    session = _make_bot_session()
    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=session,
    )

    # Use Redis for FSM storage so state survives bot restarts
    storage = RedisStorage.from_url(settings.REDIS_URL)
    dp = Dispatcher(storage=storage)

    # Register lifecycle hooks
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Register routers — order matters for handler priority
    # payments first (pre_checkout is time-sensitive)
    dp.include_router(payments.router)
    dp.include_router(parsing_flow.router)
    dp.include_router(start.router)  # catch-all /start last

    logger.info("Starting polling…")
    await dp.start_polling(
        bot,
        allowed_updates=dp.resolve_used_update_types(),
    )


if __name__ == "__main__":
    asyncio.run(main())
