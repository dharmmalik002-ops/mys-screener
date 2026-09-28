"""A journal PUT replaces the whole record, so a client that has not loaded the
server copy yet must not be able to wipe the history with an empty list."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.dashboard_service import (  # noqa: E402
    JOURNAL_MAX_SILENT_TRADE_LOSS,
    DashboardService,
    JournalShrinkRefused,
)


def _trades(n: int) -> list[dict]:
    return [{"symbol": f"S{i}", "type": "Buy", "qty": 1, "price": 100.0} for i in range(n)]


class JournalShrinkGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = TemporaryDirectory()
        root = Path(self._dir.name)
        # The guard needs only the journal I/O; skip the service's heavy init.
        self.service = DashboardService.__new__(DashboardService)
        self.service._state_data_dir = lambda: root / "state"  # type: ignore[method-assign]
        self.service._legacy_data_dir = lambda: root / "legacy"  # type: ignore[method-assign]
        self.service._journal_store = SimpleNamespace(is_enabled=lambda: False)
        self.service.save_journal_data({"trades": _trades(40), "startEquity": 100000})

    def tearDown(self) -> None:
        self._dir.cleanup()

    def test_an_empty_book_cannot_replace_the_history(self) -> None:
        with self.assertRaises(JournalShrinkRefused):
            self.service.save_journal_data({"trades": []})
        self.assertEqual(len(self.service.get_journal_data()["trades"]), 40)

    def test_deleting_one_trade_at_a_time_is_allowed(self) -> None:
        self.service.save_journal_data({"trades": _trades(39)})
        self.assertEqual(len(self.service.get_journal_data()["trades"]), 39)

    def test_the_limit_is_exact(self) -> None:
        self.service.save_journal_data({"trades": _trades(40 - JOURNAL_MAX_SILENT_TRADE_LOSS)})
        with self.assertRaises(JournalShrinkRefused):
            self.service.save_journal_data({"trades": _trades(40 - 2 * JOURNAL_MAX_SILENT_TRADE_LOSS - 1)})

    def test_an_explicit_import_may_replace_it_and_the_flag_is_not_stored(self) -> None:
        self.service.save_journal_data({"trades": _trades(3), "allowShrink": True})
        stored = self.service.get_journal_data()
        self.assertEqual(len(stored["trades"]), 3)
        self.assertNotIn("allowShrink", stored)


if __name__ == "__main__":
    unittest.main()
