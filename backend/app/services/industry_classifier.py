"""
Industry classifier — assigns each stock to one of 96 taxonomy groups.

Priority of layers (first match wins):
  1. manual_group_overrides.csv  (symbol -> group_id)
  2. keyword_group_rules.json    (positive/negative keywords + parent affinity)
  3. peer_group_aliases.csv      (raw_sector + raw_industry -> group_id)
  4. needs_review                (low confidence, written to needs_review.csv)

Output per stock: ClassificationResult(primary_group_id, confidence, source_layer, secondary_tags).
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from app.data.groups.taxonomy import GROUPS_BY_ID, GroupDef

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "groups"
OVERRIDES_PATH = DATA_DIR / "manual_group_overrides.csv"
KEYWORDS_PATH = DATA_DIR / "keyword_rules.json"
PEER_ALIASES_PATH = DATA_DIR / "peer_group_aliases.csv"
NEEDS_REVIEW_PATH = DATA_DIR / "needs_review.csv"
BUSINESS_DESC_PATH = DATA_DIR / "business_descriptions.json"

# Companion fundamentals cache populated by the chart/fundamentals path.
# We read its `about` field opportunistically so symbols a user has browsed
# at least once contribute a business description to the classifier without
# any manual upkeep.
FUNDAMENTALS_CACHE_PATH = (
    Path(__file__).resolve().parent.parent.parent / "data" / "free_fundamentals.json"
)

CONFIDENCE_THRESHOLD = 0.25


@dataclass
class ClassificationResult:
    primary_group_id: str | None
    confidence: float
    source_layer: str  # "override" | "keyword" | "peer" | "needs_review"
    secondary_tags: list[str] = field(default_factory=list)
    review_reason: str | None = None


class IndustryClassifier:
    def __init__(self) -> None:
        self._overrides: dict[str, str] = {}
        self._keyword_rules: dict[str, dict] = {}
        self._peer_aliases: dict[tuple[str, str], str] = {}
        self._peer_aliases_by_industry: dict[str, str] = {}
        self._business_descriptions: dict[str, str] = {}
        self._needs_review_rows: list[dict[str, str]] = []
        self._load()

    def _load(self) -> None:
        if OVERRIDES_PATH.exists():
            with OVERRIDES_PATH.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    symbol = (row.get("symbol") or "").strip().upper()
                    group_id = (row.get("primary_group_id") or "").strip()
                    if symbol and group_id and group_id in GROUPS_BY_ID:
                        self._overrides[symbol] = group_id

        if KEYWORDS_PATH.exists():
            with KEYWORDS_PATH.open(encoding="utf-8") as fh:
                payload = json.load(fh)
            self._keyword_rules = payload.get("rules", {}) if isinstance(payload, dict) else {}

        if PEER_ALIASES_PATH.exists():
            with PEER_ALIASES_PATH.open(newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh):
                    raw_sector = (row.get("raw_sector") or "").strip().lower()
                    raw_industry = (row.get("raw_industry") or "").strip().lower()
                    group_id = (row.get("alias_group_id") or "").strip()
                    if not group_id or group_id not in GROUPS_BY_ID:
                        continue
                    if raw_sector and raw_industry:
                        self._peer_aliases[(raw_sector, raw_industry)] = group_id
                    if raw_industry:
                        self._peer_aliases_by_industry.setdefault(raw_industry, group_id)

        # Authoritative manually-curated descriptions (highest priority).
        if BUSINESS_DESC_PATH.exists():
            try:
                with BUSINESS_DESC_PATH.open(encoding="utf-8") as fh:
                    payload = json.load(fh)
                if isinstance(payload, dict):
                    for sym, desc in payload.items():
                        if isinstance(sym, str) and isinstance(desc, str) and desc.strip():
                            self._business_descriptions[sym.strip().upper()] = desc.strip()
            except Exception as exc:
                logger.warning("Failed to load %s: %s", BUSINESS_DESC_PATH, exc)

        # Opportunistic merge from the fundamentals cache. Any symbol that has
        # been browsed at least once will have a populated `about` field, which
        # we use as a fallback when the curated JSON doesn't cover it. This is
        # how the classifier organically improves over time without manual upkeep.
        if FUNDAMENTALS_CACHE_PATH.exists():
            try:
                with FUNDAMENTALS_CACHE_PATH.open(encoding="utf-8") as fh:
                    cache_payload = json.load(fh)
                if isinstance(cache_payload, dict):
                    for sym, entry in cache_payload.items():
                        if not isinstance(entry, dict):
                            continue
                        text = entry.get("about") or entry.get("business_summary")
                        if not isinstance(text, str) or not text.strip():
                            continue
                        sym_upper = str(sym).strip().upper()
                        # Don't clobber an authoritative description.
                        if sym_upper and sym_upper not in self._business_descriptions:
                            self._business_descriptions[sym_upper] = text.strip()
            except Exception as exc:
                logger.warning("Failed to merge fundamentals descriptions from %s: %s", FUNDAMENTALS_CACHE_PATH, exc)

        logger.info(
            "industry_classifier loaded: %d overrides, %d keyword rules, %d peer aliases, %d business descriptions",
            len(self._overrides),
            len(self._keyword_rules),
            len(self._peer_aliases),
            len(self._business_descriptions),
        )

    def classify(
        self,
        *,
        symbol: str,
        company_name: str,
        raw_sector: str,
        raw_industry: str,
        business_desc: str = "",
    ) -> ClassificationResult:
        sym = (symbol or "").strip().upper()
        if sym in self._overrides:
            return ClassificationResult(
                primary_group_id=self._overrides[sym],
                confidence=1.0,
                source_layer="override",
            )

        # Fall back to the cached business description (curated JSON or
        # fundamentals cache) when the caller didn't pass one explicitly.
        if not (business_desc or "").strip() and sym in self._business_descriptions:
            business_desc = self._business_descriptions[sym]

        haystack = " ".join(
            (company_name or "", raw_sector or "", raw_industry or "", business_desc or "")
        ).lower()
        keyword_result = self._classify_by_keywords(haystack, raw_sector)
        if keyword_result and keyword_result.confidence >= CONFIDENCE_THRESHOLD:
            return keyword_result

        peer_result = self._classify_by_peer(raw_sector, raw_industry)
        if peer_result:
            return peer_result

        if keyword_result is not None:
            self._needs_review_rows.append(
                {
                    "symbol": sym,
                    "company_name": company_name or "",
                    "raw_sector": raw_sector or "",
                    "raw_industry": raw_industry or "",
                    "business_desc": (business_desc or "")[:200],
                    "suggested_group_id": keyword_result.primary_group_id or "",
                    "confidence_score": f"{keyword_result.confidence:.2f}",
                    "review_reason": "low_keyword_confidence",
                }
            )
            return ClassificationResult(
                primary_group_id=keyword_result.primary_group_id,
                confidence=keyword_result.confidence,
                source_layer="needs_review",
                review_reason="low_keyword_confidence",
            )

        self._needs_review_rows.append(
            {
                "symbol": sym,
                "company_name": company_name or "",
                "raw_sector": raw_sector or "",
                "raw_industry": raw_industry or "",
                "business_desc": (business_desc or "")[:200],
                "suggested_group_id": "",
                "confidence_score": "0.00",
                "review_reason": "no_match",
            }
        )
        return ClassificationResult(
            primary_group_id=None,
            confidence=0.0,
            source_layer="needs_review",
            review_reason="no_match",
        )

    def _classify_by_keywords(self, haystack: str, raw_sector: str) -> ClassificationResult | None:
        if not haystack.strip() or not self._keyword_rules:
            return None

        sector_lower = (raw_sector or "").lower()
        scores: list[tuple[float, str, list[str]]] = []
        for group_id, rule in self._keyword_rules.items():
            if group_id not in GROUPS_BY_ID:
                continue
            positives = [kw.lower() for kw in rule.get("positive_keywords", []) if kw]
            negatives = [kw.lower() for kw in rule.get("negative_keywords", []) if kw]
            weight = float(rule.get("confidence_weight", 1.0) or 1.0)
            preferred_parent = rule.get("preferred_parent")

            pos_hits = [kw for kw in positives if kw in haystack]
            neg_hits = [kw for kw in negatives if kw in haystack]
            raw_score = (len(pos_hits) * weight) - (len(neg_hits) * 2.0)
            if raw_score <= 0:
                continue

            if preferred_parent:
                group_def = GROUPS_BY_ID.get(group_id)
                if group_def and group_def.parent.lower() in sector_lower:
                    raw_score += 1.0

            scores.append((raw_score, group_id, pos_hits))

        if not scores:
            return None

        scores.sort(key=lambda s: s[0], reverse=True)
        best_score, best_group, best_hits = scores[0]
        runner_score = scores[1][0] if len(scores) > 1 else 0.0
        margin = best_score - runner_score
        confidence = min(1.0, (best_score / (best_score + 3.0)) + (margin / 10.0))

        secondary = [gid for _, gid, _ in scores[1:3]]
        return ClassificationResult(
            primary_group_id=best_group,
            confidence=round(confidence, 3),
            source_layer="keyword",
            secondary_tags=secondary,
        )

    def _classify_by_peer(self, raw_sector: str, raw_industry: str) -> ClassificationResult | None:
        sector_l = (raw_sector or "").strip().lower()
        industry_l = (raw_industry or "").strip().lower()
        if industry_l and (sector_l, industry_l) in self._peer_aliases:
            return ClassificationResult(
                primary_group_id=self._peer_aliases[(sector_l, industry_l)],
                confidence=0.7,
                source_layer="peer",
            )
        if industry_l and industry_l in self._peer_aliases_by_industry:
            return ClassificationResult(
                primary_group_id=self._peer_aliases_by_industry[industry_l],
                confidence=0.55,
                source_layer="peer",
            )
        return None

    def write_needs_review(self) -> int:
        NEEDS_REVIEW_PATH.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "symbol",
            "company_name",
            "raw_sector",
            "raw_industry",
            "business_desc",
            "suggested_group_id",
            "confidence_score",
            "review_reason",
        ]
        with NEEDS_REVIEW_PATH.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in self._needs_review_rows:
                writer.writerow(row)
        return len(self._needs_review_rows)

    def reset_needs_review(self) -> None:
        self._needs_review_rows = []


def classify_snapshots(
    snapshots: Iterable,
    *,
    write_review: bool = True,
) -> dict[str, ClassificationResult]:
    """Classify a sequence of StockSnapshots; returns {symbol: ClassificationResult}."""
    classifier = IndustryClassifier()
    classifier.reset_needs_review()
    results: dict[str, ClassificationResult] = {}
    for snap in snapshots:
        results[snap.symbol] = classifier.classify(
            symbol=snap.symbol,
            company_name=getattr(snap, "name", "") or "",
            raw_sector=getattr(snap, "sector", "") or "",
            raw_industry=getattr(snap, "sub_sector", "") or "",
            business_desc="",
        )
    if write_review:
        try:
            classifier.write_needs_review()
        except Exception as exc:
            logger.warning("Failed to write needs_review.csv: %s", exc)
    return results
