from __future__ import annotations

import sys
import unittest
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.telegram_alerts import prefs as P
from app.services.telegram_alerts import registry


def chat() -> dict:
    return P.chat_prefs(P.normalize_prefs(None), 777)


class ConfiguredFlagTests(unittest.TestCase):
    def test_never_configured_uses_registry_defaults(self) -> None:
        self.assertEqual(P.effective_scan_ids(chat()), list(registry.DEFAULT_ENABLED))

    def test_deliberately_cleared_stays_empty(self) -> None:
        """The distinction that matters: 'Clear all' must not silently restore
        the defaults on the next evening."""
        c = chat()
        P.clear_all(c)
        self.assertTrue(c["configured"])
        self.assertEqual(P.effective_scan_ids(c), [])

    def test_first_toggle_materializes_the_default_set(self) -> None:
        c = chat()
        P.toggle_scan(c, "vcp")   # vcp is on by default -> turning it off
        remaining = P.effective_scan_ids(c)
        self.assertNotIn("vcp", remaining)
        # The other defaults must survive rather than everything switching on.
        self.assertEqual(remaining, [s for s in registry.DEFAULT_ENABLED if s != "vcp"])


class SelectionTests(unittest.TestCase):
    def test_toggle_is_its_own_inverse(self) -> None:
        c = chat()
        before = P.effective_scan_ids(c)
        P.toggle_scan(c, "darvas-box")
        P.toggle_scan(c, "darvas-box")
        self.assertEqual(P.effective_scan_ids(c), before)

    def test_toggle_returns_new_state(self) -> None:
        c = chat()
        self.assertTrue(P.toggle_scan(c, "darvas-box"))
        self.assertFalse(P.toggle_scan(c, "darvas-box"))

    def test_selection_is_returned_in_registry_order(self) -> None:
        c = chat()
        P.clear_all(c)
        for scan_id in ("fresh-stage2", "vcp", "high-52w"):
            P.toggle_scan(c, scan_id)
        self.assertEqual(P.effective_scan_ids(c), ["vcp", "high-52w", "fresh-stage2"])

    def test_select_and_clear_a_whole_page(self) -> None:
        c = chat()
        page_ids = [s.scan_id for s in registry.page_scans(2)]
        P.set_page_scans(c, 2, on=True)
        self.assertTrue(set(page_ids).issubset(set(P.effective_scan_ids(c))))
        P.set_page_scans(c, 2, on=False)
        self.assertFalse(set(page_ids) & set(P.effective_scan_ids(c)))


class UnknownIdTests(unittest.TestCase):
    def test_unknown_ids_are_filtered_on_read_but_kept_in_storage(self) -> None:
        c = chat()
        c["configured"] = True
        c["enabled_scans"] = ["vcp", "retired-scanner"]
        self.assertEqual(P.effective_scan_ids(c), ["vcp"])
        self.assertIn("retired-scanner", c["enabled_scans"])   # not deleted
        self.assertEqual(P.unknown_scan_ids(c), ["retired-scanner"])

    def test_unknown_ids_are_pruned_on_an_explicit_edit(self) -> None:
        c = chat()
        c["configured"] = True
        c["enabled_scans"] = ["vcp", "retired-scanner"]
        P.toggle_scan(c, "darvas-box")
        self.assertNotIn("retired-scanner", c["enabled_scans"])

    def test_normalize_survives_an_unknown_id_round_trip(self) -> None:
        c = chat()
        c["configured"] = True
        c["enabled_scans"] = ["vcp", "retired-scanner"]
        again = P.normalize_prefs({"chats": {"777": c}})
        self.assertIn("retired-scanner", again["chats"]["777"]["enabled_scans"])


class ClampingTests(unittest.TestCase):
    def test_chart_cap_is_clamped_to_the_album_ceiling(self) -> None:
        for raw, expected in ((0, 1), (-3, 1), (7, 7), (10, 10), (50, 10), ("abc", 8), (None, 8)):
            with self.subTest(raw=raw):
                self.assertEqual(P.chart_cap({"default_chart_cap": raw}, "vcp"), expected)

    def test_per_scanner_cap_overrides_the_default(self) -> None:
        c = {"default_chart_cap": 8, "chart_caps": {"vcp": 3}}
        self.assertEqual(P.chart_cap(c, "vcp"), 3)
        self.assertEqual(P.chart_cap(c, "contraction"), 8)

    def test_dedupe_sessions_clamped(self) -> None:
        for raw, expected in ((0, 1), (5, 5), (15, 15), (99, 15), ("x", 5), (None, 5)):
            with self.subTest(raw=raw):
                self.assertEqual(P.dedupe_sessions({"dedupe_sessions": raw}), expected)


class MuteTests(unittest.TestCase):
    def test_mute_until_is_inclusive(self) -> None:
        c = chat()
        P.set_mute_until(c, date(2026, 8, 20))
        self.assertTrue(P.is_muted(c, date(2026, 8, 19)))
        self.assertTrue(P.is_muted(c, date(2026, 8, 20)))    # inclusive
        self.assertFalse(P.is_muted(c, date(2026, 8, 21)))

    def test_not_muted_by_default_and_after_clearing(self) -> None:
        c = chat()
        self.assertFalse(P.is_muted(c, date(2026, 8, 20)))
        P.set_mute_until(c, date(2026, 9, 1))
        P.set_mute_until(c, None)
        self.assertFalse(P.is_muted(c, date(2026, 8, 20)))

    def test_garbage_mute_value_is_ignored(self) -> None:
        self.assertFalse(P.is_muted({"muted_until": "not-a-date"}, date(2026, 8, 20)))


class NormalizeTests(unittest.TestCase):
    def test_survives_hostile_payloads(self) -> None:
        for raw in (None, {}, [], "garbage", 42, {"chats": "garbage"},
                    {"chats": {"abc": {}}}, {"chats": {"1": "notadict"}},
                    {"version": 0}, {"last_update_id": "x"}):
            with self.subTest(raw=raw):
                out = P.normalize_prefs(raw)
                self.assertEqual(out["version"], P.SCHEMA_VERSION)
                self.assertIsInstance(out["chats"], dict)
                self.assertIsInstance(out["last_update_id"], int)

    def test_non_numeric_chat_keys_are_dropped(self) -> None:
        out = P.normalize_prefs({"chats": {"abc": {}, "123": {}}})
        self.assertEqual(list(out["chats"]), ["123"])

    def test_invalid_repeat_policy_falls_back(self) -> None:
        out = P.normalize_prefs({"chats": {"1": {"repeat_policy": "nonsense"}}})
        self.assertEqual(out["chats"]["1"]["repeat_policy"], P.DEFAULT_REPEAT_POLICY)

    def test_duplicate_enabled_scans_are_deduped(self) -> None:
        out = P.normalize_prefs({"chats": {"1": {"enabled_scans": ["vcp", "vcp", "contraction"]}}})
        self.assertEqual(out["chats"]["1"]["enabled_scans"], ["vcp", "contraction"])

    def test_chat_prefs_accepts_int_and_str_keys(self) -> None:
        prefs = P.normalize_prefs(None)
        a = P.chat_prefs(prefs, 123)
        b = P.chat_prefs(prefs, "123")
        self.assertIs(a, b)

    def test_menu_state_round_trips(self) -> None:
        c = chat()
        P.remember_menu(c, page=2, message_id=4471, epoch=registry.EPOCH)
        again = P.normalize_prefs({"chats": {"777": c}})["chats"]["777"]
        self.assertEqual(again["menu"]["message_id"], 4471)
        self.assertEqual(again["menu"]["page"], 2)
        self.assertEqual(again["menu"]["epoch"], registry.EPOCH)


if __name__ == "__main__":
    unittest.main()
