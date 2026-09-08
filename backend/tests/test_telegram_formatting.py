from __future__ import annotations

import re
import sys
import unittest
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.telegram_alerts import formatting as F


@dataclass
class FakeMatch:
    symbol: str
    change_pct: float | None = 1.5
    rs_rating: int | None = 88
    relative_volume: float | None = 2.1
    also_in: tuple[str, ...] = field(default_factory=tuple)


# Real Indian names: every one of these is a MarkdownV2 landmine, which is why
# the whole feature uses HTML.
NASTY_NAMES = [
    "Bajaj Auto Ltd.",
    "M&M Financial",
    "TVS Motor Co.",
    "Sun Pharma (Bonus)",
    "L&T Finance",
    "Dr. Reddy's",
    "3M India <test>",
]


class EscapingTests(unittest.TestCase):
    def test_escapes_the_three_html_specials_including_ampersand(self) -> None:
        self.assertEqual(F.h("M&M <Ltd>"), "M&amp;M &lt;Ltd&gt;")

    def test_none_becomes_empty(self) -> None:
        self.assertEqual(F.h(None), "")

    def test_nasty_names_leave_no_bare_specials(self) -> None:
        for name in NASTY_NAMES:
            with self.subTest(name=name):
                escaped = F.h(name)
                # No unescaped & < >
                self.assertIsNone(re.search(r"&(?!amp;|lt;|gt;|#)", escaped))
                self.assertNotIn("<", escaped.replace("&lt;", ""))
                self.assertNotIn(">", escaped.replace("&gt;", ""))


class CaptionTests(unittest.TestCase):
    def test_worst_case_caption_stays_within_the_media_limit(self) -> None:
        charted = [
            FakeMatch(
                symbol=f"VERYLONGSYMBOL{i:02d}",
                also_in=("Minervini 5 Months", "3 Tight Closes", "Relative Strengths"),
            )
            for i in range(10)
        ]
        caption = F.album_caption(
            scanner_label="Positive Earnings & Expansion <all>",
            total_matches=60,
            new_count=40,
            charted=charted,
            new_symbols={m.symbol for m in charted},
            overflow_symbols=[f"OVERFLOWSYM{i:02d}" for i in range(50)],
        )
        self.assertLessEqual(len(caption), F.CAPTION_LIMIT)

    def test_positions_are_one_based_and_sequential(self) -> None:
        charted = [FakeMatch(symbol=s) for s in ("AAA", "BBB", "CCC")]
        caption = F.album_caption(
            scanner_label="VCP", total_matches=3, new_count=0, charted=charted,
            new_symbols=set(), overflow_symbols=[],
        )
        self.assertIn("1 <b>AAA</b>", caption)
        self.assertIn("2 <b>BBB</b>", caption)
        self.assertIn("3 <b>CCC</b>", caption)

    def test_new_symbols_are_marked_and_others_are_not(self) -> None:
        charted = [FakeMatch(symbol="AAA"), FakeMatch(symbol="BBB")]
        caption = F.album_caption(
            scanner_label="VCP", total_matches=2, new_count=1, charted=charted,
            new_symbols={"AAA"}, overflow_symbols=[],
        )
        lines = {line.split()[1]: line for line in caption.splitlines() if "<b>" in line and line[0].isdigit()}
        self.assertIn("🆕", lines["<b>AAA</b>"])
        self.assertNotIn("🆕", lines["<b>BBB</b>"])

    def test_overflow_is_listed(self) -> None:
        caption = F.album_caption(
            scanner_label="VCP", total_matches=5, new_count=0,
            charted=[FakeMatch(symbol="AAA")], new_symbols=set(),
            overflow_symbols=["DIXON", "WIPRO", "CIPLA"],
        )
        self.assertIn("3 more:", caption)
        self.assertIn("DIXON", caption)

    def test_singular_plural_agreement(self) -> None:
        one = F.album_caption(
            scanner_label="VCP", total_matches=1, new_count=0,
            charted=[FakeMatch(symbol="AAA")], new_symbols=set(), overflow_symbols=[],
        )
        self.assertIn("1 match", one)
        self.assertNotIn("1 matches", one)

    def test_missing_optional_fields_do_not_crash(self) -> None:
        bare = FakeMatch(symbol="AAA", change_pct=None, rs_rating=None, relative_volume=None)
        caption = F.album_caption(
            scanner_label="VCP", total_matches=1, new_count=0, charted=[bare],
            new_symbols=set(), overflow_symbols=[],
        )
        self.assertIn("AAA", caption)


class MessageLimitTests(unittest.TestCase):
    def test_long_text_splits_on_line_boundaries(self) -> None:
        text = "\n".join(f"<b>line {i}</b> some filler text here" for i in range(600))
        chunks = F.split_message(text)
        self.assertGreater(len(chunks), 1)
        for chunk in chunks:
            self.assertLessEqual(len(chunk), F.MESSAGE_LIMIT)
            # Never split mid-entity: tags must balance within a chunk.
            self.assertEqual(chunk.count("<b>"), chunk.count("</b>"))

    def test_short_text_is_one_chunk(self) -> None:
        self.assertEqual(F.split_message("hello"), ["hello"])

    def test_a_single_overlong_line_is_truncated_not_split(self) -> None:
        chunks = F.split_message("x" * 9000, limit=1000)
        self.assertEqual(len(chunks), 1)
        self.assertLessEqual(len(chunks[0]), 1000)

    def test_overflow_table_and_text_block_respect_the_message_limit(self) -> None:
        items = [FakeMatch(symbol=f"SYMBOL{i:03d}") for i in range(60)]
        for chunk in F.split_message(F.overflow_table("VCP", items)):
            self.assertLessEqual(len(chunk), F.MESSAGE_LIMIT)
        for chunk in F.split_message(F.text_only_block("VCP", items, new_symbols=set())):
            self.assertLessEqual(len(chunk), F.MESSAGE_LIMIT)


class RegimeHeaderTests(unittest.TestCase):
    def test_renders_with_every_optional_clause_missing(self) -> None:
        header = F.regime_header(
            session_date=None, scanner_count=0, match_count=0, chart_count=0
        )
        self.assertIn("EOD Digest", header)
        self.assertNotIn("\n\n\n", header)

    def test_omits_rather_than_prints_unknown(self) -> None:
        header = F.regime_header(session_date="20 Aug 2026", scanner_count=1, match_count=1, chart_count=1)
        self.assertNotIn("unknown", header.lower().replace("session unknown", ""))
        self.assertNotIn("Regime:", header)
        self.assertNotIn("Exposure:", header)

    def test_contains_no_directional_advice(self) -> None:
        """The Markets page is measured, not predictive. The digest must not
        editorialise."""
        header = F.regime_header(
            session_date="20 Aug 2026", regime="Confirmed Uptrend", xp_score=68,
            exposure_band="Full", exposure_pct=100, advances=812, declines=431,
            scanner_count=4, match_count=34, chart_count=27,
            stop_pct=3, win_pct=5, horizon_sessions=10,
        )
        lowered = header.lower()
        for banned in (" buy", " sell", "add exposure", "reduce exposure", "should ", "we expect"):
            with self.subTest(banned=banned):
                self.assertNotIn(banned, lowered)

    def test_escapes_a_hostile_regime_label(self) -> None:
        header = F.regime_header(
            session_date="20 Aug", regime="Up & <Down>", scanner_count=1,
            match_count=1, chart_count=1,
        )
        self.assertIn("Up &amp; &lt;Down&gt;", header)


class SignedPctTests(unittest.TestCase):
    def test_uses_a_real_minus_sign(self) -> None:
        self.assertEqual(F.signed_pct(-1.25), "−1.2%")
        self.assertEqual(F.signed_pct(3.24), "+3.2%")
        self.assertEqual(F.signed_pct(0.0), "+0.0%")
        self.assertEqual(F.signed_pct(-1.25, digits=2), "−1.25%")
        self.assertEqual(F.signed_pct(None), "")


if __name__ == "__main__":
    unittest.main()
