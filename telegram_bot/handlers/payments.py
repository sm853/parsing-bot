"""
Telegram Stars payment flow.

Stars are Telegram's native digital currency.
Invoice uses currency="XTR" (Stars), amount in whole Stars (not cents).
No external payment gateway needed.
"""

from aiogram import Bot, Router
from aiogram.types import (
    LabeledPrice,
    Message,
    PreCheckoutQuery,
)

from telegram_bot.config import settings
from telegram_bot.db.engine import get_async_session
from telegram_bot.db.repository_async import upsert_bot_user
from telegram_bot.services.limits import grant_paid_credits

router = Router(name="payments")


async def send_stars_invoice(chat_id: int, bot: Bot) -> None:
    """Send a Telegram Stars payment invoice to the user."""
    await bot.send_invoice(
        chat_id=chat_id,
        title="More parsing credits",
        description=f"{settings.PAID_PARSE_COUNT} channel parsings",
        payload="stars_credits",
        currency="XTR",
        prices=[LabeledPrice(label="Parsing credits", amount=settings.STARS_PRICE)],
    )


@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery) -> None:
    """Always approve — no inventory to validate."""
    await pre_checkout_query.answer(ok=True)


@router.message(lambda m: m.successful_payment is not None)
async def successful_payment_handler(message: Message) -> None:
    """Credit the user after a successful Stars payment."""
    user_id = message.from_user.id

    async with get_async_session() as session:
        # Ensure user exists (edge case: payment before /start)
        await upsert_bot_user(
            session,
            telegram_id=user_id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
        )
        await grant_paid_credits(session, user_id, settings.PAID_PARSE_COUNT)

    await message.answer(
        f"✅ <b>Payment received!</b>\n\n"
        f"Added <b>{settings.PAID_PARSE_COUNT} parsing credits</b> to your account.\n\n"
        "Send /start to begin parsing."
    )
