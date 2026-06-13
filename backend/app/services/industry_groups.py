"""
Industry-group ranking engine (96-group taxonomy).

Pipeline:
  1. Universe filter: Indian listed, market_cap_cr > 1000.
  2. Classify each stock via IndustryClassifier (override -> keyword -> peer -> needs_review).
  3. Group by primary_group_id. Groups with < 5 stocks get unstable_flag=true and are
     merged into a synthetic parent bucket (`__parent__<parent>`) for ranking only.
  4. Compute 126d / 63d / 21d / 5d total returns (using stock_return_126d / 60d / 20d / 5d as the
     pragmatic mapping over the existing snapshot fields).
  5. Winsorize each return series within the group at 5th/95th percentile.
  6. group_score = fast swing score: 5d thrust + 20d momentum + acceleration + breadth + leaders/volume.
  7. Dense rank descending by group_score so fresh moving groups surface first.
  8. rank_change_1w / 1m / 3m read from on-disk daily rank snapshots (5d / 20d / 60d ago).
"""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Iterable

from app.data.groups.taxonomy import (
    GROUPS_BY_ID,
    PARENT_LABEL,
    parent_bucket_id,
    parent_bucket_name,
)
from app.models.market import (
    IndustryGroupFilters,
    IndustryGroupMasterItem,
    IndustryGroupRankItem,
    IndustryGroupsResponse,
    IndustryGroupStockItem,
    IndustryGroupTopStock,
    StockSnapshot,
)
from app.services.industry_classifier import IndustryClassifier

logger = logging.getLogger(__name__)

GROUP_MIN_MARKET_CAP_CR = 250.0
GROUP_MIN_AVG_DAILY_VALUE_CR = 0.25
GROUP_MIN_STOCKS = 5

WINSORIZE_LOWER = 0.05
WINSORIZE_UPPER = 0.95

SCORE_WEIGHT_5D = 0.35
SCORE_WEIGHT_21D = 0.25
SCORE_WEIGHT_ACCELERATION = 0.20
SCORE_WEIGHT_BREADTH = 0.10
SCORE_WEIGHT_LEADERS_VOLUME = 0.10

RANK_HISTORY_DIR = Path(__file__).resolve().parent.parent / "data" / "rank_history"
RANK_HISTORY_LOOKBACKS = {"1w": 5, "1m": 20, "3m": 60}


def _avg_traded_value_50d_cr(snapshot: StockSnapshot) -> float:
    average_volume = snapshot.avg_volume_50d or snapshot.avg_volume_20d or 0
    if average_volume <= 0 or snapshot.last_price <= 0:
        return 0.0
    return round((average_volume * snapshot.last_price) / 10_000_000, 2)


def _safe_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _safe_mean(values: Iterable[float]) -> float:
    nums = [float(v) for v in values]
    return sum(nums) / len(nums) if nums else 0.0


def _winsorize(values: list[float], lower: float = WINSORIZE_LOWER, upper: float = WINSORIZE_UPPER) -> list[float]:
    if not values:
        return []
    if len(values) < 4:
        return list(values)
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    lo_idx = max(0, min(n - 1, int(round((n - 1) * lower))))
    hi_idx = max(0, min(n - 1, int(round((n - 1) * upper))))
    lo = sorted_vals[lo_idx]
    hi = sorted_vals[hi_idx]
    return [min(max(v, lo), hi) for v in values]


def _winsorized_median(values: list[float]) -> float:
    clipped = _winsorize(values)
    return float(median(clipped)) if clipped else 0.0


def _winsorized_mean(values: list[float]) -> float:
    return _safe_mean(_winsorize(values))


def _percentile_scores(values: list[float]) -> list[float]:
    if not values:
        return []
    if len(values) == 1:
        return [100.0]
    sorted_vals = sorted((value, index) for index, value in enumerate(values))
    scores = [0.0] * len(values)
    idx = 0
    while idx < len(sorted_vals):
        end = idx
        while end + 1 < len(sorted_vals) and sorted_vals[end + 1][0] == sorted_vals[idx][0]:
            end += 1
        rank_midpoint = (idx + end) / 2
        score = round((rank_midpoint / (len(sorted_vals) - 1)) * 100, 2)
        for _, original_index in sorted_vals[idx : end + 1]:
            scores[original_index] = score
        idx = end + 1
    return scores


def _majority_label(values: Iterable[str], fallback: str = "Unclassified") -> str:
    labels = [str(v or "").strip() for v in values if str(v or "").strip()]
    if not labels:
        return fallback
    return Counter(labels).most_common(1)[0][0]


def _snapshot_eligible(snapshot: StockSnapshot) -> bool:
    return (
        snapshot.exchange in {"NSE", "BSE"}
        and snapshot.market_cap_crore > GROUP_MIN_MARKET_CAP_CR
        and _avg_traded_value_50d_cr(snapshot) >= GROUP_MIN_AVG_DAILY_VALUE_CR
        and snapshot.last_price > 0
    )


def _resolve_strength_bucket(rank: int) -> str:
    if rank <= 10:
        return "Top 10"
    if rank <= 40:
        return "Top 40"
    if rank <= 60:
        return "Mid"
    return "Weak"


def _resolve_trend_label(score_change_1w: float | None, rank_change_1w: int | None) -> str:
    if score_change_1w is None and rank_change_1w is None:
        return "Stable"
    if (score_change_1w or 0) >= 1.5 or (rank_change_1w or 0) >= 3:
        return "Improving"
    if (score_change_1w or 0) <= -1.5 or (rank_change_1w or 0) <= -3:
        return "Weakening"
    return "Stable"


def _benchmark_returns(benchmark_snapshots: list[StockSnapshot]) -> dict[str, float]:
    if not benchmark_snapshots:
        return {"return_1w": 0.0, "return_1m": 0.0, "return_3m": 0.0, "return_6m": 0.0}
    return {
        "return_1w": round(_winsorized_mean([s.stock_return_5d for s in benchmark_snapshots]), 2),
        "return_1m": round(_winsorized_mean([s.stock_return_20d for s in benchmark_snapshots]), 2),
        "return_3m": round(_winsorized_mean([s.stock_return_60d for s in benchmark_snapshots]), 2),
        "return_6m": round(_winsorized_mean([s.stock_return_126d for s in benchmark_snapshots]), 2),
    }


def _group_description(group_name: str, market_key: str) -> str:
    market_label = "Indian" if market_key == "india" else "US"
    return f"{market_label} listed {group_name.lower()} stocks (market cap > Rs {int(GROUP_MIN_MARKET_CAP_CR)} Cr)."


_rank_store = None


def _get_rank_store():
    """Lazy shared ScanHistoryStore for group ranks. Postgres-backed when
    DATABASE_URL is configured, so rank history survives Space rebuilds
    (the on-disk snapshots below are wiped on every deploy)."""
    global _rank_store
    if _rank_store is None:
        try:
            from app.core.config import get_settings
            from app.services.scan_history_store import ScanHistoryStore

            _rank_store = ScanHistoryStore(
                get_settings().database_url,
                RANK_HISTORY_DIR.parent / "scan_history.json",
                keep_dates=70,
            )
        except Exception as exc:
            logger.info("rank store unavailable: %s", exc)
            _rank_store = False
    return _rank_store or None


def _load_rank_history(lookback_days: int, generated_at: datetime) -> dict[str, int]:
    """Read the rank snapshot from `lookback_days` trading-days ago.
    Prefers the persistent store; falls back to on-disk snapshots."""
    store = _get_rank_store()
    if store is not None:
        try:
            history = store.load("group-ranks")
            dates = sorted(history.keys())
            if dates:
                target_idx = max(0, len(dates) - 1 - lookback_days)
                payload = history[dates[target_idx]]
                ranks = {
                    str(row.get("groupId")): int(row.get("rank", 0))
                    for row in payload
                    if isinstance(row, dict) and row.get("groupId")
                }
                if ranks:
                    return ranks
        except Exception as exc:
            logger.info("rank store read failed: %s", exc)
    if not RANK_HISTORY_DIR.exists():
        return {}
    files = sorted(RANK_HISTORY_DIR.glob("ranks_*.json"))
    if not files:
        return {}
    target_idx = max(0, len(files) - 1 - lookback_days)
    target_file = files[target_idx]
    try:
        payload = json.loads(target_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to read rank history %s: %s", target_file, exc)
        return {}
    return {str(row.get("groupId")): int(row.get("rank", 0)) for row in payload if row.get("groupId")}


def _save_rank_history(rank_payload: list[dict], generated_at: datetime) -> None:
    store = _get_rank_store()
    if store is not None:
        try:
            store.record_once("group-ranks", generated_at.astimezone(timezone.utc).date().isoformat(), rank_payload)
        except Exception as exc:
            logger.info("rank store write failed: %s", exc)
    RANK_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    stamp = generated_at.astimezone(timezone.utc).strftime("%Y%m%d")
    out = RANK_HISTORY_DIR / f"ranks_{stamp}.json"
    try:
        out.write_text(json.dumps(rank_payload, separators=(",", ":")), encoding="utf-8")
    except OSError as exc:
        logger.warning("Failed to save rank history to %s: %s", out, exc)


def _build_group_payload(
    snapshots: list[StockSnapshot],
    benchmark_snapshots: list[StockSnapshot],
    market_key: str,
) -> tuple[list[dict[str, object]], list[IndustryGroupStockItem], list[IndustryGroupMasterItem]]:
    eligible = [s for s in snapshots if _snapshot_eligible(s)]
    benchmark = _benchmark_returns(benchmark_snapshots)

    classifier = IndustryClassifier()
    classifier.reset_needs_review()

    classify_results = {}
    for snap in eligible:
        classify_results[snap.symbol] = classifier.classify(
            symbol=snap.symbol,
            company_name=snap.name or "",
            raw_sector=snap.sector or "",
            raw_industry=snap.sub_sector or "",
        )
    try:
        review_count = classifier.write_needs_review()
        if review_count:
            logger.info("industry_classifier flagged %d stocks for needs_review", review_count)
    except Exception as exc:
        logger.warning("Failed to write needs_review.csv: %s", exc)

    # Bucket by primary_group_id; unclassified -> parent bucket "__unclassified__".
    raw_groups: dict[str, list[StockSnapshot]] = defaultdict(list)
    snap_classification: dict[str, tuple[str, float, str]] = {}
    for snap in eligible:
        result = classify_results.get(snap.symbol)
        gid = result.primary_group_id if result and result.primary_group_id else None
        if gid is None or gid not in GROUPS_BY_ID:
            # No primary group: bucket under unclassified parent.
            bucket = "__parent__unclassified"
        else:
            bucket = gid
        raw_groups[bucket].append(snap)
        snap_classification[snap.symbol] = (
            gid or "",
            result.confidence if result else 0.0,
            result.source_layer if result else "needs_review",
        )

    # Apply parent-bucket merge for under-5 groups.
    final_groups: dict[str, list[StockSnapshot]] = defaultdict(list)
    unstable_origin: dict[str, set[str]] = defaultdict(set)  # final_id -> set of original ids merged in
    for gid, members in raw_groups.items():
        if gid in GROUPS_BY_ID and len(members) < GROUP_MIN_STOCKS:
            parent = GROUPS_BY_ID[gid].parent
            target = parent_bucket_id(parent)
            final_groups[target].extend(members)
            unstable_origin[target].add(gid)
        else:
            final_groups[gid].extend(members)

    stock_rows: list[IndustryGroupStockItem] = []
    group_rows: list[dict[str, object]] = []
    master_rows: list[IndustryGroupMasterItem] = []

    for final_gid, members in final_groups.items():
        if final_gid in GROUPS_BY_ID:
            group_def = GROUPS_BY_ID[final_gid]
            group_name = group_def.name
            parent_sector = PARENT_LABEL.get(group_def.parent, group_def.parent.title())
            unstable = False
        elif final_gid.startswith("__parent__"):
            parent = final_gid.removeprefix("__parent__")
            group_name = parent_bucket_name(parent)
            parent_sector = PARENT_LABEL.get(parent, parent.title())
            unstable = True
        else:
            group_name = final_gid
            parent_sector = "Unclassified"
            unstable = True

        ret_126 = [s.stock_return_126d for s in members]
        ret_63 = [s.stock_return_60d for s in members]
        ret_21 = [s.stock_return_20d for s in members]
        ret_5 = [s.stock_return_5d for s in members]

        med_126 = _winsorized_median(ret_126)
        med_63 = _winsorized_median(ret_63)
        med_21 = _winsorized_median(ret_21)
        med_5 = _winsorized_median(ret_5)

        win_mean_126 = round(_winsorized_mean(ret_126), 2)
        win_mean_63 = round(_winsorized_mean(ret_63), 2)
        win_mean_21 = round(_winsorized_mean(ret_21), 2)
        win_mean_5 = round(_winsorized_mean(ret_5), 2)

        relative_1w = round(win_mean_5 - benchmark["return_1w"], 2)
        relative_1m = round(win_mean_21 - benchmark["return_1m"], 2)
        relative_3m = round(win_mean_63 - benchmark["return_3m"], 2)
        relative_6m = round(win_mean_126 - benchmark["return_6m"], 2)

        positive_1w = _safe_pct(sum(1 for s in members if s.stock_return_5d > 0), len(members))
        positive_1m = _safe_pct(sum(1 for s in members if s.stock_return_20d > 0), len(members))
        positive_3m = _safe_pct(sum(1 for s in members if s.stock_return_60d > 0), len(members))
        positive_6m = _safe_pct(sum(1 for s in members if s.stock_return_126d > 0), len(members))
        outperform_1w = _safe_pct(
            sum(1 for s in members if s.stock_return_5d > benchmark["return_1w"]), len(members)
        )
        outperform_3m = _safe_pct(
            sum(1 for s in members if s.stock_return_60d > benchmark["return_3m"]), len(members)
        )
        outperform_6m = _safe_pct(
            sum(1 for s in members if s.stock_return_126d > benchmark["return_6m"]), len(members)
        )
        above_50dma = _safe_pct(
            sum(1 for s in members if (s.sma50 or s.ema50 or 0) > 0 and s.last_price > (s.sma50 or s.ema50 or 0)),
            len(members),
        )
        above_200dma = _safe_pct(
            sum(1 for s in members if (s.sma200 or 0) > 0 and s.last_price > (s.sma200 or 0)),
            len(members),
        )
        breadth_score = round(_safe_mean([positive_3m, positive_6m, outperform_3m, outperform_6m]), 2)
        fast_breadth_score = round(_safe_mean([positive_1w, positive_1m, outperform_1w, above_50dma]), 2)
        trend_health_score = round((above_50dma * 0.6) + (above_200dma * 0.4), 2)
        fast_acceleration = round(med_5 - (med_21 / 4.0), 2)
        leader_density = _safe_pct(
            sum(1 for s in members if s.rs_eligible and s.rs_rating >= 80 and s.stock_return_5d > 0),
            len(members),
        )
        volume_thrust = _safe_pct(
            sum(1 for s in members if s.relative_volume >= 1.5 and s.change_pct > 0),
            len(members),
        )
        leaders_volume_score = round((leader_density * 0.65) + (volume_thrust * 0.35), 2)

        sorted_members = sorted(
            members,
            key=lambda s: (
                s.rs_rating if s.rs_eligible else -1,
                s.stock_return_5d,
                s.stock_return_20d,
                s.relative_volume,
                s.change_pct,
            ),
            reverse=True,
        )
        top_constituents = [
            IndustryGroupTopStock(
                symbol=s.symbol,
                company_name=s.name,
                rs_rating=s.rs_rating if s.rs_eligible else None,
                return_1m=round(s.stock_return_20d, 2),
                return_3m=round(s.stock_return_60d, 2),
                return_6m=round(s.stock_return_126d, 2),
                relative_return_3m=round(s.stock_return_60d - benchmark["return_3m"], 2),
                relative_return_6m=round(s.stock_return_126d - benchmark["return_6m"], 2),
            )
            for s in sorted_members[:5]
        ]
        laggards = sorted(
            members,
            key=lambda s: (
                s.rs_rating if s.rs_eligible else -1,
                s.stock_return_126d,
                s.stock_return_60d,
            ),
        )

        for snap in sorted_members:
            classification = snap_classification.get(snap.symbol, ("", 0.0, ""))
            stock_rows.append(
                IndustryGroupStockItem(
                    symbol=snap.symbol,
                    company_name=snap.name,
                    exchange=snap.exchange,
                    market_cap_cr=round(snap.market_cap_crore, 2),
                    avg_traded_value_50d_cr=_avg_traded_value_50d_cr(snap),
                    sector=snap.sector or "Unclassified",
                    raw_industry=snap.sub_sector or "Unclassified",
                    final_group_id=final_gid,
                    final_group_name=group_name,
                    last_price=round(snap.last_price, 2),
                    change_pct=round(snap.change_pct, 2),
                    return_1w=round(snap.stock_return_5d, 2),
                    return_1m=round(snap.stock_return_20d, 2),
                    return_3m=round(snap.stock_return_60d, 2),
                    return_6m=round(snap.stock_return_126d, 2),
                    return_1y=round(snap.stock_return_12m, 2),
                    rs_rating=snap.rs_rating if snap.rs_eligible else None,
                    classification_source=classification[2] or None,
                    classification_confidence=round(classification[1], 3) if classification[1] else None,
                )
            )

        group_rows.append(
            {
                "group_id": final_gid,
                "group_name": group_name,
                "parent_sector": parent_sector,
                "description": _group_description(group_name, market_key),
                "stock_count": len(members),
                "unstable_flag": unstable,
                "return_1w": win_mean_5,
                "return_1m": win_mean_21,
                "return_3m": win_mean_63,
                "return_6m": win_mean_126,
                "relative_return_1w": relative_1w,
                "relative_return_1m": relative_1m,
                "relative_return_3m": relative_3m,
                "relative_return_6m": relative_6m,
                "median_return_1w": round(med_5, 2),
                "median_return_1m": round(med_21, 2),
                "median_return_3m": round(med_63, 2),
                "median_return_6m": round(med_126, 2),
                "pct_above_50dma": above_50dma,
                "pct_above_200dma": above_200dma,
                "pct_outperform_benchmark_3m": outperform_3m,
                "pct_outperform_benchmark_6m": outperform_6m,
                "breadth_score": breadth_score,
                "trend_health_score": trend_health_score,
                "leaders": [s.symbol for s in sorted_members[:3]],
                "laggards": [s.symbol for s in laggards[:3]],
                "top_constituents": top_constituents,
                "symbols": [s.symbol for s in sorted_members],
                "score": 0.0,
                "_fast_return_5d": relative_1w,
                "_fast_return_20d": relative_1m,
                "_fast_acceleration": fast_acceleration,
                "_fast_breadth": fast_breadth_score,
                "_fast_leaders_volume": leaders_volume_score,
            }
        )

        master_rows.append(
            IndustryGroupMasterItem(
                group_id=final_gid,
                group_name=group_name,
                parent_sector=parent_sector,
                description=_group_description(group_name, market_key),
                stock_count=len(members),
                symbols=sorted(s.symbol for s in members),
            )
        )

    if group_rows:
        score_inputs = {
            "_fast_return_5d": _percentile_scores([float(row["_fast_return_5d"]) for row in group_rows]),
            "_fast_return_20d": _percentile_scores([float(row["_fast_return_20d"]) for row in group_rows]),
            "_fast_acceleration": _percentile_scores([float(row["_fast_acceleration"]) for row in group_rows]),
            "_fast_breadth": _percentile_scores([float(row["_fast_breadth"]) for row in group_rows]),
            "_fast_leaders_volume": _percentile_scores([float(row["_fast_leaders_volume"]) for row in group_rows]),
        }
        for idx, row in enumerate(group_rows):
            row["score"] = round(
                (SCORE_WEIGHT_5D * score_inputs["_fast_return_5d"][idx])
                + (SCORE_WEIGHT_21D * score_inputs["_fast_return_20d"][idx])
                + (SCORE_WEIGHT_ACCELERATION * score_inputs["_fast_acceleration"][idx])
                + (SCORE_WEIGHT_BREADTH * score_inputs["_fast_breadth"][idx])
                + (SCORE_WEIGHT_LEADERS_VOLUME * score_inputs["_fast_leaders_volume"][idx]),
                2,
            )
            for transient_key in score_inputs:
                row.pop(transient_key, None)

    group_rows.sort(
        key=lambda row: (
            -float(row["score"]),
            -float(row["relative_return_1w"]),
            -float(row["relative_return_1m"]),
            str(row["group_name"]),
        )
    )
    stock_rows.sort(key=lambda row: (row.final_group_name, row.symbol))
    master_rows.sort(key=lambda row: row.group_name)
    return group_rows, stock_rows, master_rows


def build_industry_groups_response(
    snapshots: list[StockSnapshot],
    benchmark_snapshots: list[StockSnapshot],
    previous_snapshots: list[StockSnapshot],
    previous_benchmark_snapshots: list[StockSnapshot],
    *,
    generated_at: datetime,
    benchmark_label: str,
    market_key: str,
) -> IndustryGroupsResponse:
    group_rows, stock_rows, master_rows = _build_group_payload(snapshots, benchmark_snapshots, market_key)

    # rank_change_1w from in-memory previous snapshots (legacy path) — fallback only
    legacy_prev_map: dict[str, dict[str, float]] = {}
    if previous_snapshots:
        prev_rows, _, _ = _build_group_payload(
            previous_snapshots, previous_benchmark_snapshots, market_key
        )
        legacy_prev_map = {
            str(r["group_id"]): {"rank": idx + 1, "score": float(r["score"])}
            for idx, r in enumerate(prev_rows)
        }

    history_1w = _load_rank_history(RANK_HISTORY_LOOKBACKS["1w"], generated_at)
    history_1m = _load_rank_history(RANK_HISTORY_LOOKBACKS["1m"], generated_at)
    history_3m = _load_rank_history(RANK_HISTORY_LOOKBACKS["3m"], generated_at)

    ranked_groups: list[IndustryGroupRankItem] = []
    rank_payload_for_history: list[dict] = []
    for index, row in enumerate(group_rows, start=1):
        gid = str(row["group_id"])

        rank_change_1w: int | None = None
        if gid in history_1w:
            rank_change_1w = history_1w[gid] - index
        elif gid in legacy_prev_map:
            rank_change_1w = int(legacy_prev_map[gid]["rank"]) - index

        rank_change_1m = (history_1m[gid] - index) if gid in history_1m else None
        rank_change_3m = (history_3m[gid] - index) if gid in history_3m else None

        score_change_1w: float | None = None
        if gid in legacy_prev_map:
            score_change_1w = round(float(row["score"]) - legacy_prev_map[gid]["score"], 2)

        ranked_groups.append(
            IndustryGroupRankItem(
                rank=index,
                rank_label=f"#{index}",
                rank_change_1w=rank_change_1w,
                rank_change_1m=rank_change_1m,
                rank_change_3m=rank_change_3m,
                score_change_1w=score_change_1w,
                strength_bucket=_resolve_strength_bucket(index),
                trend_label=_resolve_trend_label(score_change_1w, rank_change_1w),
                group_id=gid,
                group_name=str(row["group_name"]),
                parent_sector=str(row["parent_sector"]),
                description=str(row["description"]),
                stock_count=int(row["stock_count"]),
                unstable_flag=bool(row.get("unstable_flag", False)),
                score=float(row["score"]),
                return_1w=float(row["return_1w"]),
                return_1m=float(row["return_1m"]),
                return_3m=float(row["return_3m"]),
                return_6m=float(row["return_6m"]),
                relative_return_1w=float(row["relative_return_1w"]),
                relative_return_1m=float(row["relative_return_1m"]),
                relative_return_3m=float(row["relative_return_3m"]),
                relative_return_6m=float(row["relative_return_6m"]),
                median_return_1w=float(row["median_return_1w"]),
                median_return_1m=float(row["median_return_1m"]),
                median_return_3m=float(row["median_return_3m"]),
                median_return_6m=float(row["median_return_6m"]),
                pct_above_50dma=float(row["pct_above_50dma"]),
                pct_above_200dma=float(row["pct_above_200dma"]),
                pct_outperform_benchmark_3m=float(row["pct_outperform_benchmark_3m"]),
                pct_outperform_benchmark_6m=float(row["pct_outperform_benchmark_6m"]),
                breadth_score=float(row["breadth_score"]),
                trend_health_score=float(row["trend_health_score"]),
                leaders=list(row["leaders"]),
                laggards=list(row["laggards"]),
                top_constituents=list(row["top_constituents"]),
                symbols=list(row["symbols"]),
            )
        )
        rank_payload_for_history.append(
            {"groupId": gid, "rank": index, "score": float(row["score"])}
        )

    _save_rank_history(rank_payload_for_history, generated_at)

    as_of_date = generated_at.astimezone(timezone.utc).date().isoformat()
    return IndustryGroupsResponse(
        generated_at=generated_at,
        as_of_date=as_of_date,
        benchmark=benchmark_label,
        filters=IndustryGroupFilters(
            min_market_cap_cr=GROUP_MIN_MARKET_CAP_CR,
            min_avg_daily_value_cr=GROUP_MIN_AVG_DAILY_VALUE_CR,
        ),
        total_groups=len(ranked_groups),
        groups=ranked_groups,
        master=master_rows,
        stocks=stock_rows,
    )


def write_industry_group_files(
    response: IndustryGroupsResponse,
    *,
    groups_path: Path,
    ranks_path: Path,
    stocks_path: Path,
) -> None:
    groups_path.parent.mkdir(parents=True, exist_ok=True)

    groups_payload = {
        "asOfDate": response.as_of_date,
        "benchmark": response.benchmark,
        "filters": {
            "minMarketCapCr": response.filters.min_market_cap_cr,
            "minAvgDailyValueCr": response.filters.min_avg_daily_value_cr,
        },
        "master": [
            {
                "groupId": item.group_id,
                "groupName": item.group_name,
                "parentSector": item.parent_sector,
                "description": item.description,
                "stockCount": item.stock_count,
                "symbols": item.symbols,
            }
            for item in response.master
        ],
        "groups": [
            {
                "rank": item.rank,
                "rankLabel": item.rank_label,
                "rankChange1w": item.rank_change_1w,
                "rankChange1m": item.rank_change_1m,
                "rankChange3m": item.rank_change_3m,
                "scoreChange1w": item.score_change_1w,
                "strengthBucket": item.strength_bucket,
                "trendLabel": item.trend_label,
                "groupId": item.group_id,
                "groupName": item.group_name,
                "parentSector": item.parent_sector,
                "description": item.description,
                "score": item.score,
                "stockCount": item.stock_count,
                "unstableFlag": item.unstable_flag,
                "returns": {"1w": item.return_1w, "1m": item.return_1m, "3m": item.return_3m, "6m": item.return_6m},
                "relativeReturns": {
                    "1w": item.relative_return_1w,
                    "1m": item.relative_return_1m,
                    "3m": item.relative_return_3m,
                    "6m": item.relative_return_6m,
                },
                "medianReturns": {
                    "1w": item.median_return_1w,
                    "1m": item.median_return_1m,
                    "3m": item.median_return_3m,
                    "6m": item.median_return_6m,
                },
                "breadth": {
                    "above50dma": item.pct_above_50dma,
                    "above200dma": item.pct_above_200dma,
                    "positive3m": item.breadth_score,
                    "positive6m": item.pct_outperform_benchmark_6m,
                },
                "leaders": item.leaders,
                "laggards": item.laggards,
                "symbols": item.symbols,
            }
            for item in response.groups
        ],
    }
    rank_payload = [
        {
            "rank": item.rank,
            "groupId": item.group_id,
            "groupName": item.group_name,
            "score": item.score,
            "unstableFlag": item.unstable_flag,
            "return1w": item.return_1w,
            "return1m": item.return_1m,
            "return3m": item.return_3m,
            "return6m": item.return_6m,
            "relativeReturn1w": item.relative_return_1w,
            "relativeReturn1m": item.relative_return_1m,
            "relativeReturn3m": item.relative_return_3m,
            "relativeReturn6m": item.relative_return_6m,
            "above50dma": item.pct_above_50dma,
            "above200dma": item.pct_above_200dma,
            "breadthScore": item.breadth_score,
            "leaders": item.leaders,
            "topConstituents": [t.model_dump(mode="json") for t in item.top_constituents],
            "strengthBucket": item.strength_bucket,
            "trendLabel": item.trend_label,
            "scoreChange1w": item.score_change_1w,
            "rankChange1w": item.rank_change_1w,
            "rankChange1m": item.rank_change_1m,
            "rankChange3m": item.rank_change_3m,
        }
        for item in response.groups
    ]
    stock_payload = [
        {
            "symbol": item.symbol,
            "companyName": item.company_name,
            "exchange": item.exchange,
            "marketCapCr": item.market_cap_cr,
            "avgTradedValue50dCr": item.avg_traded_value_50d_cr,
            "sector": item.sector,
            "rawIndustry": item.raw_industry,
            "finalGroupId": item.final_group_id,
            "finalGroupName": item.final_group_name,
            "lastPrice": item.last_price,
            "changePct": item.change_pct,
            "return1w": item.return_1w,
            "return1m": item.return_1m,
            "return3m": item.return_3m,
            "return6m": item.return_6m,
            "return1y": item.return_1y,
            "rsRating": item.rs_rating,
            "classificationSource": item.classification_source,
            "classificationConfidence": item.classification_confidence,
        }
        for item in response.stocks
    ]

    groups_path.write_text(json.dumps(groups_payload, indent=2), encoding="utf-8")
    ranks_path.write_text(json.dumps(rank_payload, indent=2), encoding="utf-8")
    stocks_path.write_text(json.dumps(stock_payload, indent=2), encoding="utf-8")
