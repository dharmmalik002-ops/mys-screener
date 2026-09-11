"""Turn the Chart Gym drill log into measured facts about how the user trades.

Every number the coach ever states is computed here, in plain Python, from the
user's own graded cards joined to the deck's features. The AI layer is handed
this output and allowed to write prose about it — never to derive it. That is
the same split `market_regime` and `fund_review` use, and the reason those
pages' numbers can be trusted.

Two rules shape the output:

1. Every slice carries its sample size, and slices below `MIN_SAMPLE` are
   dropped rather than reported. Twenty graded cards will happily produce a
   confident-looking "you are -0.8R on deep bases" built on three trades.
2. Nothing here predicts. It reports what already happened.
"""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

# A slice needs at least this many trades before it is reported at all.
MIN_SAMPLE = 6
# The deck deals equal winners and losers, so a coin flip scores 50%. Anything
# above that on cards the user chose to take is genuine selection edge.
DECK_BASE_RATE_PCT = 50.0

CONTRACTIONS_RE = re.compile(r"(\d+)\s+contractions")
DEPTH_RE = re.compile(r"depth\s+([\d.]+)%")
DRYUP_RE = re.compile(r"5D volume\s+([\d.]+)x")
POLE_RE = re.compile(r"Pole\s+\+([\d.]+)%")
FLAG_DEPTH_RE = re.compile(r"Flag:\s+\d+\s+sessions,\s+([\d.]+)%\s+deep")


@dataclass
class Graded:
    """One graded card: the user's decision plus the card's own features."""

    card_id: str
    setup: str
    symbol: str
    graded_at: str
    took_it: bool
    waited: int | None
    risk_pct: float | None
    r: float | None
    deck_result: str
    rs_rating: int | None = None
    group_top_decile: bool | None = None
    contractions: int | None = None
    base_depth_pct: float | None = None
    dryup: float | None = None


def _num(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in (float("inf"), float("-inf")) else None


def _features(reasons: Sequence[str]) -> dict[str, float | int | None]:
    """Pull the measurable bits out of the scanner's own reason lines.

    The reasons are written for a human ("4 contractions: 16.1% -> 6.9% ..."),
    so they are parsed rather than re-derived — re-deriving would risk the
    coach describing a different base from the one the card showed.
    """
    blob = " | ".join(str(r) for r in reasons or [])
    contractions = CONTRACTIONS_RE.search(blob)
    depth = DEPTH_RE.search(blob) or FLAG_DEPTH_RE.search(blob)
    dryup = DRYUP_RE.search(blob)
    return {
        "contractions": int(contractions.group(1)) if contractions else None,
        "base_depth_pct": float(depth.group(1)) if depth else None,
        "dryup": float(dryup.group(1)) if dryup else None,
    }


def join(log: Iterable[dict[str, Any]], cards: dict[str, Any]) -> list[Graded]:
    """Pair each logged decision with the deck card it was made against."""
    out: list[Graded] = []
    for row in log or []:
        if not isinstance(row, dict):
            continue
        card_id = str(row.get("cardId") or "")
        if not card_id:
            continue
        card = cards.get(card_id)
        feats = _features(getattr(card, "reasons", None) or []) if card else {}
        action = str(row.get("action") or "")
        out.append(
            Graded(
                card_id=card_id,
                setup=str(row.get("setup") or (getattr(card, "setup", "") if card else "")),
                symbol=str(row.get("symbol") or ""),
                graded_at=str(row.get("gradedAt") or ""),
                took_it=action == "entered",
                waited=int(row["waited"]) if isinstance(row.get("waited"), (int, float)) else None,
                risk_pct=_num(row.get("riskPct")),
                r=_num(row.get("r")),
                deck_result=str(row.get("officialResult") or (getattr(card, "result", "") if card else "")),
                rs_rating=getattr(card, "rs_rating", None) if card else None,
                group_top_decile=getattr(card, "group_top_decile", None) if card else None,
                contractions=feats.get("contractions"),
                base_depth_pct=feats.get("base_depth_pct"),
                dryup=feats.get("dryup"),
            )
        )
    out.sort(key=lambda g: g.graded_at)
    return out


def _slice(label: str, rows: Sequence[Graded]) -> dict[str, Any] | None:
    """Summarise one bucket of taken trades, or None if it is too thin to mean anything."""
    scored = [g for g in rows if g.r is not None]
    if len(scored) < MIN_SAMPLE:
        return None
    rs = [g.r for g in scored if g.r is not None]
    wins = sum(1 for r in rs if r > 0)
    return {
        "label": label,
        "trades": len(scored),
        "avg_r": round(statistics.fmean(rs), 2),
        "total_r": round(sum(rs), 1),
        "hit_rate_pct": round(100.0 * wins / len(rs), 1),
    }


def _bucketed(rows: Sequence[Graded], key, buckets: Sequence[tuple[str, Any, Any]]) -> list[dict[str, Any]]:
    out = []
    for label, low, high in buckets:
        picked = []
        for g in rows:
            value = key(g)
            if value is None:
                continue
            if (low is None or value >= low) and (high is None or value < high):
                picked.append(g)
        summary = _slice(label, picked)
        if summary:
            out.append(summary)
    return out


def build(log: Iterable[dict[str, Any]], cards: dict[str, Any]) -> dict[str, Any]:
    """The whole measured picture, ready to hand to the prose layer."""
    graded = join(log, cards)
    taken = [g for g in graded if g.took_it and g.r is not None]
    passed = [g for g in graded if not g.took_it]

    if not graded:
        return {"ready": False, "reason": "no cards graded yet", "graded": 0, "min_sample": MIN_SAMPLE}

    rs = [g.r for g in taken if g.r is not None]
    wins = sum(1 for r in rs if r > 0)
    overall = {
        "graded": len(graded),
        "taken": len(taken),
        "passed": len(passed),
        "take_rate_pct": round(100.0 * len(taken) / len(graded), 1) if graded else 0.0,
        "avg_r": round(statistics.fmean(rs), 2) if rs else None,
        "total_r": round(sum(rs), 1) if rs else None,
        "hit_rate_pct": round(100.0 * wins / len(rs), 1) if rs else None,
        "best_r": round(max(rs), 2) if rs else None,
        "worst_r": round(min(rs), 2) if rs else None,
    }

    # Selection edge. The deck is 50/50 by construction, so the only honest
    # benchmark for "did you pick the right ones" is that base rate.
    selection = None
    if len(rs) >= MIN_SAMPLE:
        deck_wins = sum(1 for g in taken if g.deck_result == "win")
        selection = {
            "trades": len(taken),
            "your_hit_rate_pct": overall["hit_rate_pct"],
            "cards_that_were_winners_pct": round(100.0 * deck_wins / len(taken), 1),
            "deck_base_rate_pct": DECK_BASE_RATE_PCT,
            "edge_pts": round((100.0 * deck_wins / len(taken)) - DECK_BASE_RATE_PCT, 1),
        }

    # Did passing actually save anything? A pass is "right" when the card did
    # not go on to win under the deck's own rules.
    pass_quality = None
    resolved_passes = [g for g in passed if g.deck_result in ("win", "loss", "timeout")]
    if len(resolved_passes) >= MIN_SAMPLE:
        right = sum(1 for g in resolved_passes if g.deck_result != "win")
        pass_quality = {
            "passes": len(resolved_passes),
            "correct_pct": round(100.0 * right / len(resolved_passes), 1),
            "winners_missed": sum(1 for g in resolved_passes if g.deck_result == "win"),
        }

    # Trajectory: first half against second half, so "am I getting better" has
    # an answer rather than a feeling.
    trend = None
    if len(taken) >= MIN_SAMPLE * 2:
        half = len(taken) // 2
        early = _slice("earlier half", taken[:half])
        late = _slice("recent half", taken[half:])
        if early and late:
            trend = {
                "earlier": early,
                "recent": late,
                "avg_r_change": round(late["avg_r"] - early["avg_r"], 2),
                "direction": "improving" if late["avg_r"] > early["avg_r"] else
                             "flat" if late["avg_r"] == early["avg_r"] else "worsening",
            }

    slices = {
        "by_setup": [s for s in (_slice(name, [g for g in taken if g.setup == name])
                                 for name in sorted({g.setup for g in taken})) if s],
        "by_wait": _bucketed(taken, lambda g: g.waited, [
            ("bought the signal day", 0, 1),
            ("waited 1-2 sessions", 1, 3),
            ("waited 3-7 sessions", 3, 8),
            ("waited 8+ sessions", 8, None),
        ]),
        "by_stop_width": _bucketed(taken, lambda g: g.risk_pct, [
            ("tight stop (under 4%)", None, 4.0),
            ("normal stop (4-7%)", 4.0, 7.0),
            ("wide stop (over 7%)", 7.0, None),
        ]),
        "by_rs": _bucketed(taken, lambda g: g.rs_rating, [
            ("RS under 80", None, 80),
            ("RS 80-89", 80, 90),
            ("RS 90+", 90, None),
        ]),
        "by_base_depth": _bucketed(taken, lambda g: g.base_depth_pct, [
            ("shallow base (under 15%)", None, 15.0),
            ("normal base (15-25%)", 15.0, 25.0),
            ("deep base (over 25%)", 25.0, None),
        ]),
        "by_volume_dryup": _bucketed(taken, lambda g: g.dryup, [
            ("volume dried up hard (under 0.7x)", None, 0.7),
            ("volume quiet (0.7-0.9x)", 0.7, 0.9),
            ("volume still active (0.9x+)", 0.9, None),
        ]),
        "by_group": [s for s in (
            _slice("top-decile industry group", [g for g in taken if g.group_top_decile is True]),
            _slice("outside the leading groups", [g for g in taken if g.group_top_decile is False]),
        ) if s],
    }

    # The best and worst buckets across every dimension — the headline the
    # coach should lead with, chosen by arithmetic rather than by the model.
    everything = [s for group in slices.values() for s in group]
    best = max(everything, key=lambda s: s["avg_r"], default=None)
    worst = min(everything, key=lambda s: s["avg_r"], default=None)

    return {
        "ready": len(taken) >= MIN_SAMPLE,
        "min_sample": MIN_SAMPLE,
        "overall": overall,
        "selection": selection,
        "pass_quality": pass_quality,
        "trend": trend,
        "slices": slices,
        "strongest": best,
        "weakest": worst,
        "recent_cards": [
            {
                "symbol": g.symbol,
                "setup": g.setup,
                "took_it": g.took_it,
                "waited": g.waited,
                "risk_pct": g.risk_pct,
                "r": g.r,
                "deck_result": g.deck_result,
            }
            for g in graded[-25:][::-1]
        ],
    }
