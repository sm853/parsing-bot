"""
Test configuration: mock DB engine and Celery before any imports.

The production modules create SQLAlchemy engines at module-load time using
PostgreSQL URLs from the .env file.  In tests we intercept those modules
with lightweight mocks so no real DB connection is needed.

This conftest runs before any test file is collected, so the mocks are
in place by the time test modules do their imports.
"""

import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

# ── Stub out the real PostgreSQL engines ──────────────────────────────────────

# Create a mock engine module that exposes the two session factories
# used by the production code.  Both are replaced with no-op fakes here;
# individual tests that need a real session override them with fixtures.
_mock_engine = MagicMock()


@contextmanager
def _noop_async_session():
    yield MagicMock()


@contextmanager
def _noop_sync_session():
    yield MagicMock()


_mock_engine.get_async_session = _noop_async_session
_mock_engine.get_sync_session = _noop_sync_session

sys.modules.setdefault("telegram_bot.db.engine", _mock_engine)

# ── Stub out Celery so workers/tasks can be imported without a broker ─────────

_mock_celery_app = MagicMock()

# Make @celery_app.task(bind=True, ...) work as a pass-through decorator that
# keeps __wrapped__ so tests can call the raw function directly.
def _task_decorator(**kwargs):
    def _decorator(fn):
        fn.__wrapped__ = fn
        return fn
    return _decorator

_mock_celery_app.celery_app = MagicMock()
_mock_celery_app.celery_app.task = _task_decorator

sys.modules.setdefault("telegram_bot.tasks.celery_app", _mock_celery_app)

# ── Stub httpx so delivery.py can be imported without the package ─────────────
sys.modules.setdefault("httpx", MagicMock())

# ── Stub telethon so parser modules can be imported in tests ──────────────────
_telethon = MagicMock()
sys.modules.setdefault("telethon", _telethon)
sys.modules.setdefault("telethon.errors", MagicMock())
sys.modules.setdefault("telethon.tl", MagicMock())
sys.modules.setdefault("telethon.tl.functions", MagicMock())
sys.modules.setdefault("telethon.tl.functions.channels", MagicMock())
sys.modules.setdefault("telethon.tl.types", MagicMock())
