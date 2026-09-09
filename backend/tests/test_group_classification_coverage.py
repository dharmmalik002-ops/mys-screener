"""Every stock in the shipped universe must land in a real industry group.

The Groups page used to carry an "Unclassified (Parent bucket)" row holding 19
live stocks. None of them were unclassifiable -- they all had a sector and an
industry from the vendor -- they simply had industry labels no layer of the
classifier covered ("Metal Fabrication", "Packaging", "Drug Manufacturers -
Specialty & Generic"), so the classifier returned None and the bucket caught
them. These tests hold that shut: the industry vocabulary the universe actually
uses must be covered, and every macro sector must name a fallback group so a
label we have never seen still lands with its own sector's peers.
"""

from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app.core.sector_taxonomy import NSE_MACRO_SECTORS, normalize_sector
from app.data.groups.taxonomy import GROUPS_BY_ID
from app.services import industry_classifier
from app.services.industry_classifier import (
    NEEDS_REVIEW_PATH,
    SECTOR_FALLBACK_PATH,
    IndustryClassifier,
)

UNIVERSE_PATH = Path(__file__).resolve().parent.parent / "data" / "free_universe.json"


def _universe_rows() -> list[dict]:
    if not UNIVERSE_PATH.exists():
        return []
    payload = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


class SectorFallbackTableTests(unittest.TestCase):
    def setUp(self) -> None:
        with SECTOR_FALLBACK_PATH.open(newline="", encoding="utf-8") as fh:
            self.rows = list(csv.DictReader(fh))

    def test_every_macro_sector_has_a_fallback_group(self):
        covered = {normalize_sector(r["raw_sector"]) for r in self.rows}
        # "Unclassified" is the absence of a sector, not a sector: there is
        # nothing to place such a stock next to, so it deliberately has no
        # fallback and the caller logs it instead.
        missing = (set(NSE_MACRO_SECTORS) - {"Unclassified"}) - covered
        self.assertFalse(missing, f"macro sectors with no fallback group: {sorted(missing)}")

    def test_every_fallback_target_is_a_real_group(self):
        for row in self.rows:
            self.assertIn(row["fallback_group_id"], GROUPS_BY_ID, row["raw_sector"])

    def test_a_fallback_group_sits_under_a_plausible_parent(self):
        # A fallback must not send a stock to another macro sector's peers --
        # e.g. Healthcare must not fall back to a Financials group.
        expected_parent = {
            "Automobile and Auto Components": "auto",
            "Capital Goods": "capital_goods",
            "Chemicals": "chemicals",
            "Financial Services": "financials",
            "Healthcare": "healthcare",
            "Information Technology": "technology",
            "Metals & Mining": "materials",
            "Power": "energy",
            "Realty": "real_estate",
            "Telecommunication": "telecom",
        }
        for row in self.rows:
            sector = normalize_sector(row["raw_sector"])
            if sector in expected_parent:
                parent = GROUPS_BY_ID[row["fallback_group_id"]].parent
                self.assertEqual(parent, expected_parent[sector], sector)


class UniverseClassificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rows = _universe_rows()
        cls.classifier = IndustryClassifier()

    def _classify(self, row: dict):
        return self.classifier.classify(
            symbol=row.get("symbol") or "",
            company_name=row.get("name") or "",
            raw_sector=row.get("sector") or "",
            raw_industry=row.get("sub_sector") or "",
        )

    def test_the_shipped_universe_is_not_empty(self):
        self.assertGreater(len(self.rows), 800, "free_universe.json looks truncated")

    def test_every_stock_with_a_known_sector_gets_a_group(self):
        unplaced = []
        for row in self.rows:
            if normalize_sector(row.get("sector")) == "Unclassified":
                continue  # no vendor metadata at all; nothing to place it beside
            result = self._classify(row)
            if not result.primary_group_id:
                unplaced.append((row.get("symbol"), row.get("sector"), row.get("sub_sector")))
        self.assertFalse(unplaced, f"{len(unplaced)} stock(s) would fall to Unclassified: {unplaced[:15]}")

    def test_every_assigned_group_exists_in_the_taxonomy(self):
        for row in self.rows:
            result = self._classify(row)
            if result.primary_group_id:
                self.assertIn(result.primary_group_id, GROUPS_BY_ID, row.get("symbol"))

    def test_the_industry_vocabulary_the_universe_uses_is_covered(self):
        # An industry label carried by 3+ stocks must resolve on its own, not
        # by luck of a company-name keyword: that is what the peer-alias layer
        # is for, and it is what was missing.
        counts: dict[str, int] = {}
        for row in self.rows:
            label = (row.get("sub_sector") or "").strip()
            if label and normalize_sector(row.get("sector")) != "Unclassified":
                counts[label] = counts.get(label, 0) + 1
        uncovered = []
        for label, n in counts.items():
            if n < 3:
                continue
            probe = self.classifier.classify(
                symbol="__PROBE__",
                company_name="",
                raw_sector="",
                raw_industry=label,
            )
            if probe.source_layer not in {"peer", "keyword"} or not probe.primary_group_id:
                uncovered.append((label, n))
        self.assertFalse(uncovered, f"industry labels with no alias: {sorted(uncovered, key=lambda x: -x[1])}")


class NeedsReviewQueueTests(unittest.TestCase):
    def test_a_run_that_classified_nothing_leaves_the_queue_alone(self):
        before = NEEDS_REVIEW_PATH.read_bytes() if NEEDS_REVIEW_PATH.exists() else None
        classifier = IndustryClassifier()
        classifier.reset_needs_review()
        written = classifier.write_needs_review()
        after = NEEDS_REVIEW_PATH.read_bytes() if NEEDS_REVIEW_PATH.exists() else None
        self.assertEqual(written, 0)
        self.assertEqual(before, after, "an empty run truncated the tracked curation queue")

    def test_a_run_with_rows_still_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "needs_review.csv"
            classifier = IndustryClassifier()
            classifier.reset_needs_review()
            # A label no layer covers, with a sector that has no fallback: the
            # only path that still reaches "no_match".
            classifier.classify(
                symbol="NOSUCH",
                company_name="Nothing Matching Ltd",
                raw_sector="Unclassified",
                raw_industry="Entirely Unknown Industry",
            )
            with mock.patch.object(industry_classifier, "NEEDS_REVIEW_PATH", target):
                self.assertEqual(classifier.write_needs_review(), 1)
            self.assertIn("NOSUCH", target.read_text(encoding="utf-8"))


class UnclassifiedBucketTests(unittest.TestCase):
    def test_the_bucketing_code_flags_rather_than_silently_collects(self):
        import inspect

        from app.services.industry_groups import _build_group_payload

        src = inspect.getsource(_build_group_payload)
        self.assertIn("__parent__unclassified", src)
        self.assertIn("unplaced", src)
        self.assertIn("could not be classified", src)


if __name__ == "__main__":
    unittest.main()
