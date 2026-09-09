"""Telegram scanner-digest feature.

Named ``telegram_alerts`` rather than ``telegram`` so it cannot shadow the
PyPI ``telegram`` package for a reader.
"""

from app.services.telegram_alerts.client import (
    FileSinkTelegramClient,
    TelegramApiError,
    TelegramClient,
)
from app.services.telegram_alerts.digest import DigestResult, run_digest
from app.services.telegram_alerts.prefs_store import AlertPrefsStore
from app.services.telegram_alerts.run_state import DigestRunState

__all__ = [
    "AlertPrefsStore",
    "DigestResult",
    "DigestRunState",
    "FileSinkTelegramClient",
    "TelegramApiError",
    "TelegramClient",
    "run_digest",
]
