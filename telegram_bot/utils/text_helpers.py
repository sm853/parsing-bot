"""
Text utility functions for parsing and normalisation.
No external dependencies — stdlib only.
"""

import logging
import re

logger = logging.getLogger(__name__)

# ── URL extraction (used in post text parsing) ─────────────────────────────────

URL_REGEX = re.compile(r"https?://[^\s\]\[<>\"']+")


def extract_urls(text: str) -> list[str]:
    """Return all http(s) URLs found in a text string."""
    return URL_REGEX.findall(text or "")


# ── Channel input normalisation ────────────────────────────────────────────────

# t.me path segments that look like usernames but are NOT channels.
# If we normalise these we'd pass e.g. "joinchat" to get_entity and get a
# confusing ResolveUsernameRequest failure.
_RESERVED_PATHS = frozenset({
    "joinchat", "share", "addstickers", "addemoji",
    "addtheme", "boost", "invoice", "proxy", "socks",
    "login", "confirmphone", "setlanguage",
})

# Invite-link detector — must be checked BEFORE the URL pattern.
# Matches t.me/+hash  and  t.me/joinchat/hash
_INVITE_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/(?:\+|joinchat/)",
    re.IGNORECASE,
)

# t.me URL → bare username.
# Group 1 captures the username segment only.
# Explicitly strips a trailing post-ID (/123) and query/fragment (?foo, #bar).
# Rejects invite links naturally: "+" is not in [A-Za-z0-9_].
_CHANNEL_URL_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)"
    r"/([A-Za-z0-9_]{5,32})"   # username: 5–32 chars, alphanumeric + underscore
    r"(?:/\d+)?"                # optional post-ID suffix (/123) — stripped
    r"(?:[?#].*)?"              # optional query string / fragment — stripped
    r"\s*$",
    re.IGNORECASE,
)

# Bare @username or username (no URL prefix).
_AT_RE = re.compile(r"^@?([A-Za-z0-9_]{5,32})\s*$")


def is_invite_link(raw: str) -> bool:
    """
    Return True if the input looks like a private invite link
    (t.me/+hash or t.me/joinchat/hash).
    Used by channel_validator to give a specific error rather than
    the generic "invalid link" message.
    """
    return bool(_INVITE_RE.match(raw.strip()))


def normalize_channel_input(raw: str) -> str | None:
    """
    Convert any supported channel reference to a bare lowercase username.

    Accepted formats:
      @exploitex              →  "exploitex"
      exploitex               →  "exploitex"
      t.me/exploitex          →  "exploitex"
      https://t.me/exploitex  →  "exploitex"
      https://t.me/exploitex/123  →  "exploitex"  (post ID stripped)

    Rejected (returns None):
      https://t.me/+ABCdef    →  None  (invite link)
      https://t.me/joinchat/x →  None  (invite link)
      t.me/joinchat           →  None  (reserved path)
      "hi"                    →  None  (too short)
      ""                      →  None

    Returns:
        Bare lowercase username string, or None if input cannot be parsed.
    """
    raw = raw.strip()
    logger.debug("normalize_channel_input: raw=%r", raw)

    # Invite links must be rejected before the URL regex, because
    # t.me/joinchat/hash would otherwise match the URL pattern and return
    # "joinchat" as the username.
    if is_invite_link(raw):
        logger.debug("normalize_channel_input: detected invite link, returning None")
        return None

    # Try as a t.me URL first
    m = _CHANNEL_URL_RE.match(raw)
    if m:
        username = m.group(1).lower()
        if username in _RESERVED_PATHS:
            logger.debug(
                "normalize_channel_input: URL matched reserved path %r, returning None",
                username,
            )
            return None
        logger.debug("normalize_channel_input: URL match → %r", username)
        return username

    # Try as a bare @username or username
    m = _AT_RE.match(raw)
    if m:
        username = m.group(1).lower()
        logger.debug("normalize_channel_input: @/bare match → %r", username)
        return username

    logger.debug("normalize_channel_input: no pattern matched, returning None")
    return None
