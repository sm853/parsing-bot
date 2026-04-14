"""
Channel validation using Telethon.

Resolves a raw user input (link or @username) to a ChannelMeta dataclass
without triggering a full parse. Used by the bot handler before asking for
post count so the user gets immediate feedback if a link is invalid.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from telethon import TelegramClient
from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import Channel

from telegram_bot.utils.text_helpers import is_invite_link, normalize_channel_input

logger = logging.getLogger(__name__)


# ── Custom exceptions ──────────────────────────────────────────────────────────

class InvalidLinkError(Exception):
    """Input could not be parsed as a Telegram channel link."""


class ChannelNotFoundError(Exception):
    """Channel username does not exist on Telegram."""


class PrivateChannelError(Exception):
    """Channel exists but is private/invite-only — bot cannot access it."""


# ── Result type ────────────────────────────────────────────────────────────────

@dataclass
class ChannelMeta:
    username: str       # bare username, lowercase, no @
    title: str
    member_count: int
    tg_id: int


# ── Main function ──────────────────────────────────────────────────────────────

async def resolve_channel(client: TelegramClient, raw_input: str) -> ChannelMeta:
    """
    Validate and resolve a channel link/username.

    Args:
        client:    Connected Telethon client (shared bot-process client).
        raw_input: User-provided string (@username, t.me/username, URL, …)

    Returns:
        ChannelMeta with resolved metadata.

    Raises:
        InvalidLinkError     — input cannot be normalised to a username
        PrivateChannelError  — invite link, or channel is not public
        ChannelNotFoundError — username doesn't exist on Telegram
    """
    # Log which session file this client is using so we can confirm
    # bot-side validation and worker-side parsing use the same authorized session.
    try:
        session_file = getattr(client.session, "filename", "unknown")
    except Exception:
        session_file = "unknown"
    logger.info(
        "resolve_channel: raw_input=%r  client_session=%r",
        raw_input,
        session_file,
    )

    # Check for invite links first — give a specific error message
    if is_invite_link(raw_input):
        logger.debug("resolve_channel: invite link detected")
        raise PrivateChannelError(
            f"'{raw_input}' is a private invite link. Only public channels are supported."
        )

    username = normalize_channel_input(raw_input)
    logger.debug("resolve_channel: normalized username=%r", username)

    if username is None:
        raise InvalidLinkError(
            f"Cannot parse '{raw_input}' as a Telegram channel username."
        )

    # Pass "@username" to get_entity so Telethon unambiguously treats it as a
    # username lookup and immediately issues ResolveUsernameRequest — no
    # internal heuristics about whether the string is a phone number etc.
    lookup = f"@{username}"
    logger.debug("resolve_channel: calling get_entity(%r)", lookup)

    # Verify the client is actually authorized before hitting the API.
    # An unauthorized client connects fine but fails on ResolveUsernameRequest.
    try:
        authorized = await client.is_user_authorized()
    except Exception:
        authorized = False
    if not authorized:
        logger.error(
            "resolve_channel: client session %r is NOT authorized — "
            "run init_session.py for this session file first",
            session_file,
        )
        raise ChannelNotFoundError(
            f"Telegram session not authorized (session={session_file}). "
            "Run: python -m telegram_bot.scripts.init_session"
        )

    try:
        entity = await client.get_entity(lookup)
        logger.debug("resolve_channel: get_entity returned type=%s", type(entity).__name__)

    except (UsernameInvalidError, UsernameNotOccupiedError, ValueError) as e:
        logger.warning("resolve_channel: username not found: %s", e)
        raise ChannelNotFoundError(f"Channel @{username} not found.") from e

    except ChannelPrivateError as e:
        logger.warning("resolve_channel: channel is private: %s", e)
        raise PrivateChannelError(f"Channel @{username} is private.") from e

    except FloodWaitError as e:
        # Do NOT wrap this as ChannelNotFoundError — it is a rate-limit, not a lookup failure.
        logger.warning("resolve_channel: FloodWait for %d seconds", e.seconds)
        raise ChannelNotFoundError(
            f"Telegram rate limit — please wait {e.seconds}s and try again."
        ) from e

    except Exception as e:
        # Log the real error before re-raising so it's visible in logs.
        logger.exception("resolve_channel: unexpected error for @%s: %s", username, e)
        raise ChannelNotFoundError(
            f"Could not access @{username}: {type(e).__name__}: {e}"
        ) from e

    # Only broadcast channels are supported, not supergroups or basic groups
    if not isinstance(entity, Channel):
        logger.warning(
            "resolve_channel: @%s is type %s, not a Channel",
            username,
            type(entity).__name__,
        )
        raise PrivateChannelError(f"@{username} is not a public broadcast channel.")

    # Fetch subscriber count (non-fatal)
    member_count = 0
    try:
        full = await client(GetFullChannelRequest(entity))
        member_count = full.full_chat.participants_count or 0
    except Exception as e:
        logger.warning("resolve_channel: could not fetch member count for @%s: %s", username, e)

    logger.info(
        "resolve_channel: resolved @%s → title=%r members=%d tg_id=%d",
        username,
        entity.title,
        member_count,
        entity.id,
    )

    return ChannelMeta(
        username=username,
        title=entity.title or username,
        member_count=member_count,
        tg_id=entity.id,
    )
