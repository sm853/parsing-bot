"""
Core channel parsing logic — refactored from backend/parser.py.

Changes from original:
  - Accepts an injected TelegramClient (no phone auth, no DB coupling)
  - Adds reaction count (sum over message.reactions.results, None-safe)
  - Adds media type detection: photo / video / none
  - Adds URL extraction from post text
  - Collects commenter usernames per post
  - Returns typed dataclasses, not raw dicts

This module has ZERO database or Telegram Bot API dependencies.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto

from telegram_bot.utils.text_helpers import extract_urls

logger = logging.getLogger(__name__)


# ── Data structures ────────────────────────────────────────────────────────────

@dataclass
class PostData:
    post_id: int
    link: str
    text: str
    media_type: str                  # "photo" | "video" | "none"
    extracted_links: list[str]       # URLs found in post text
    views: int
    reactions_count: int
    comments_count: int
    commenters: list[str]            # "@handle" or "id:{sender_id}" per commenter


@dataclass
class ChannelStats:
    title: str
    username: str
    member_count: int
    avg_views: float
    avg_reactions: float
    avg_comments: float
    total_posts_parsed: int


@dataclass
class ParseResult:
    stats: ChannelStats
    posts: list[PostData] = field(default_factory=list)


# ── Parser ─────────────────────────────────────────────────────────────────────

async def parse_channel(
    client: TelegramClient,
    channel_username: str,
    post_limit: int,
) -> ParseResult:
    """
    Parse the last `post_limit` posts from a public Telegram channel.

    Args:
        client: An already-connected Telethon TelegramClient.
        channel_username: Bare username without @.
        post_limit: Number of posts to fetch (e.g. 20, 50, 100).

    Returns:
        ParseResult with per-post data and channel aggregate stats.

    Raises:
        Exception on channel access errors (propagated to Celery task).
    """
    # Log which session this client is using — critical for diagnosing
    # "ResolveUsernameRequest failure" caused by unauthorized worker sessions.
    session_file = getattr(client.session, "filename", "unknown")
    logger.info(
        "parse_channel: starting — channel_username=%r post_limit=%d session_file=%r",
        channel_username,
        post_limit,
        session_file,
    )

    # Prefix with "@" so Telethon unambiguously issues ResolveUsernameRequest
    # rather than applying its own heuristics to the bare string.
    lookup = f"@{channel_username}"
    logger.info("parse_channel: calling get_entity(%r)", lookup)
    entity = await client.get_entity(lookup)
    logger.info(
        "parse_channel: entity resolved — type=%s id=%s title=%r",
        type(entity).__name__,
        getattr(entity, "id", "?"),
        getattr(entity, "title", "?"),
    )

    # Fetch subscriber count via GetFullChannelRequest
    try:
        full = await client(GetFullChannelRequest(entity))
        member_count = full.full_chat.participants_count or 0
    except Exception as e:
        logger.warning("parse_channel: could not fetch member count: %s", e)
        member_count = 0

    posts: list[PostData] = []

    async for msg in client.iter_messages(entity, limit=post_limit):
        # Include messages with text OR media (skip empty service messages)
        if not msg.text and not msg.media:
            continue

        text = msg.text or ""
        media_type = _detect_media_type(msg)
        extracted_links = extract_urls(text)
        reactions_count = _sum_reactions(msg)
        comments_count = msg.replies.replies if msg.replies else 0
        link = f"https://t.me/{channel_username}/{msg.id}"

        commenters: list[str] = []
        if comments_count > 0:
            commenters = await _fetch_commenters(client, entity, msg.id)

        posts.append(
            PostData(
                post_id=msg.id,
                link=link,
                text=text,
                media_type=media_type,
                extracted_links=extracted_links,
                views=msg.views or 0,
                reactions_count=reactions_count,
                comments_count=comments_count,
                commenters=commenters,
            )
        )

    # Aggregate stats
    total = len(posts)
    avg_views = sum(p.views for p in posts) / total if total else 0.0
    avg_reactions = sum(p.reactions_count for p in posts) / total if total else 0.0
    avg_comments = sum(p.comments_count for p in posts) / total if total else 0.0

    stats = ChannelStats(
        title=getattr(entity, "title", channel_username),
        username=channel_username,
        member_count=member_count,
        avg_views=round(avg_views, 1),
        avg_reactions=round(avg_reactions, 1),
        avg_comments=round(avg_comments, 1),
        total_posts_parsed=total,
    )

    return ParseResult(stats=stats, posts=posts)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _detect_media_type(msg) -> str:
    """Return "photo", "video", or "none" for a Telethon Message."""
    if isinstance(msg.media, MessageMediaPhoto):
        return "photo"
    if isinstance(msg.media, MessageMediaDocument):
        # Documents include videos, GIFs, files — check mime type
        doc = msg.media.document
        if doc and doc.mime_type and doc.mime_type.startswith("video"):
            return "video"
    return "none"


def _sum_reactions(msg) -> int:
    """Sum all reaction counts on a message. Returns 0 if no reactions."""
    try:
        results = msg.reactions.results if msg.reactions else None
        if not results:
            return 0
        return sum(r.count for r in results)
    except Exception:
        return 0


async def _fetch_commenters(
    client: TelegramClient,
    entity,
    post_id: int,
) -> list[str]:
    """
    Fetch usernames of commenters on a post.
    Returns "@handle" or "id:{user_id}" when sender has no public username.
    Errors are swallowed so a single unreachable comment thread doesn't
    abort the entire parse.
    """
    commenters: list[str] = []
    try:
        async for reply in client.iter_messages(entity, reply_to=post_id):
            sender = reply.sender
            if sender is None:
                continue
            username = getattr(sender, "username", None)
            if username:
                commenters.append(f"@{username}")
            else:
                commenters.append(f"id:{reply.sender_id}")
    except Exception as e:
        logger.warning("Could not fetch comments for post %s: %s", post_id, e)
    return commenters
