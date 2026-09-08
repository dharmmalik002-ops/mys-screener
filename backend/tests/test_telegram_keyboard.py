from __future__ import annotations

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.telegram_alerts import keyboard as kb
from app.services.telegram_alerts import prefs as prefs_mod
from app.services.telegram_alerts import registry


def fresh_chat() -> dict:
    return prefs_mod.chat_prefs(prefs_mod.normalize_prefs(None), 4242)


class RegistryPinTests(unittest.TestCase):
    """callback_data encodes a scanner as its INDEX here, so a reorder must
    show up as a failing test rather than as wrong scanners toggling in
    production. Update these expectations deliberately."""

    def test_digest_scan_ids_are_pinned(self) -> None:
        self.assertEqual(
            registry.DIGEST_SCAN_IDS,
            (
                "vcp", "tight-closes", "contraction", "power-base",
                "minervini-1m", "minervini-5m", "ema-expansion", "positive-earnings",
                "episodic-pivot", "rs-line-leads", "high-tight-flag", "darvas-box",
                "pivot-breakout", "clean-pullback", "breakout-ath", "breakout-52w",
                "breakout-range", "relative-strength", "volume-price", "strong-nifty",
                "strong-sector",
                "high-52w", "near-52w-high", "all-time-high", "near-ath",
                "six-month-high", "month-high", "week-high", "day-high",
                "near-day-high", "prev-day-high-break",
                "bread-butter", "volume", "fresh-stage2",
            ),
        )

    def test_no_duplicate_ids(self) -> None:
        self.assertEqual(len(registry.DIGEST_SCAN_IDS), len(set(registry.DIGEST_SCAN_IDS)))

    def test_every_id_appears_on_exactly_one_page(self) -> None:
        seen: list[str] = []
        for page in range(registry.PAGE_COUNT):
            seen.extend(scan.scan_id for scan in registry.page_scans(page))
        self.assertEqual(sorted(seen), sorted(registry.DIGEST_SCAN_IDS))

    def test_offered_ids_are_all_runnable(self) -> None:
        """Every offered id must be reachable through get_scan_results: either a
        real SCANS entry or one of the ids dashboard_service synthesizes."""
        from app.scanners.definitions import SCAN_BY_ID

        synthesized = {"bread-butter", "volume", "fresh-stage2"}
        unknown = [
            scan_id
            for scan_id in registry.DIGEST_SCAN_IDS
            if scan_id not in SCAN_BY_ID and scan_id not in synthesized
        ]
        self.assertEqual(unknown, [])

    def test_ipo_is_deliberately_not_offered(self) -> None:
        # It bypasses the snapshot staleness filter, which fights the digest's
        # data-freshness contract.
        self.assertNotIn("ipo", registry.DIGEST_SCAN_IDS)

    def test_epoch_tracks_the_tuple(self) -> None:
        self.assertEqual(len(registry.EPOCH), 6)
        self.assertTrue(all(c in "0123456789abcdef" for c in registry.EPOCH))


class CallbackCodecTests(unittest.TestCase):
    def test_round_trips_every_registry_index(self) -> None:
        for index in range(len(registry.DIGEST_SCANS)):
            with self.subTest(index=index):
                action = kb.decode(kb.encode(kb.ACTION_TOGGLE, index))
                self.assertEqual(action.action, kb.ACTION_TOGGLE)
                self.assertEqual(action.arg, index)

    def test_every_button_on_every_page_fits_the_64_byte_cap(self) -> None:
        chat = fresh_chat()
        for page in range(registry.PAGE_COUNT):
            for row in kb.render(chat, page)["inline_keyboard"]:
                for button in row:
                    with self.subTest(page=page, text=button["text"]):
                        self.assertLessEqual(
                            len(button["callback_data"].encode("utf-8")), kb.MAX_CALLBACK_BYTES
                        )

    def test_epoch_mismatch_raises_stale(self) -> None:
        stale = kb.encode(kb.ACTION_TOGGLE, 0, epoch="000000")
        with self.assertRaises(kb.StaleCallback):
            kb.decode(stale)

    def test_stale_callback_mutates_nothing(self) -> None:
        chat = fresh_chat()
        before = list(prefs_mod.effective_scan_ids(chat))
        with self.assertRaises(kb.StaleCallback):
            kb.decode(kb.encode(kb.ACTION_TOGGLE, 3, epoch="deadbe"))
        self.assertEqual(prefs_mod.effective_scan_ids(chat), before)

    def test_malformed_data_raises_bad_callback_not_index_or_value_error(self) -> None:
        for bad in ("", "s|t", "s|t|", "x|t|" + registry.EPOCH + "|0",
                    "s|z|" + registry.EPOCH + "|0",
                    "s|t|" + registry.EPOCH + "|-1",
                    "s|t|" + registry.EPOCH + "|9999",
                    "s|t|" + registry.EPOCH + "|abc",
                    "s|p|" + registry.EPOCH + "|99",
                    "s|t|" + registry.EPOCH + "|0|extra"):
            with self.subTest(data=bad):
                with self.assertRaises((kb.BadCallback, kb.StaleCallback)):
                    kb.decode(bad)

    def test_decode_none(self) -> None:
        with self.assertRaises(kb.BadCallback):
            kb.decode(None)

    def test_encode_rejects_unknown_action(self) -> None:
        with self.assertRaises(kb.BadCallback):
            kb.encode("Z", 0)


class KeyboardRenderTests(unittest.TestCase):
    def test_marks_reflect_the_selection(self) -> None:
        chat = fresh_chat()
        rows = kb.render(chat, 0)["inline_keyboard"]
        texts = [b["text"] for row in rows for b in row]
        # vcp is on by default, tight-closes is not.
        self.assertIn(f"{kb.ON_MARK} VCP", texts)
        self.assertIn(f"{kb.OFF_MARK} 3 Tight Closes", texts)

    def test_toggling_flips_the_mark(self) -> None:
        chat = fresh_chat()
        prefs_mod.toggle_scan(chat, "tight-closes")
        texts = [b["text"] for row in kb.render(chat, 0)["inline_keyboard"] for b in row]
        self.assertIn(f"{kb.ON_MARK} 3 Tight Closes", texts)

    def test_two_columns_of_scanner_buttons(self) -> None:
        rows = kb.render(fresh_chat(), 0)["inline_keyboard"]
        scanner_rows = rows[:-2]  # last two are nav + bulk actions
        self.assertTrue(all(len(row) <= kb.COLUMNS for row in scanner_rows))

    def test_first_page_has_no_live_prev_and_last_has_no_live_next(self) -> None:
        first_nav = kb.render(fresh_chat(), 0)["inline_keyboard"][-2]
        last_nav = kb.render(fresh_chat(), registry.PAGE_COUNT - 1)["inline_keyboard"][-2]
        self.assertEqual(kb.decode(first_nav[0]["callback_data"]).action, kb.ACTION_NOOP)
        self.assertEqual(kb.decode(first_nav[2]["callback_data"]).action, kb.ACTION_PAGE)
        self.assertEqual(kb.decode(last_nav[0]["callback_data"]).action, kb.ACTION_PAGE)
        self.assertEqual(kb.decode(last_nav[2]["callback_data"]).action, kb.ACTION_NOOP)

    def test_page_is_clamped_into_range(self) -> None:
        for page in (-5, 99):
            with self.subTest(page=page):
                self.assertTrue(kb.render(fresh_chat(), page)["inline_keyboard"])

    def test_run_picks_keyboard_caps_at_eight(self) -> None:
        rows = kb.render_run_picks(registry.DIGEST_SCANS)["inline_keyboard"]
        self.assertEqual(len(rows), 8)
        self.assertEqual(kb.decode(rows[0][0]["callback_data"]).action, kb.ACTION_RUN)


if __name__ == "__main__":
    unittest.main()
