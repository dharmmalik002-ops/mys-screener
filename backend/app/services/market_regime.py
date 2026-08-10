"""Market regime read for the Markets page.

Turns the replayed breakout statistics (see `breakout_stats`) into a written
situational-awareness brief: what the tape actually paid last week, which setups
worked, and what that implies for holding period and risk.

Division of labour, deliberately strict
---------------------------------------
Every number is computed in Python from `breakout_stats.json`. The model is
handed those numbers and asked only to write prose over them. It is told, in the
prompt and again in the schema, that it may not introduce a figure that is not in
the block it was given — because a regime brief that invents a win rate is worse
than no brief at all. The frontend renders the underlying table beside the
narrative so any claim can be checked against the data that produced it.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MIN_SAMPLE_FOR_SETUP = 8   # below this a per-setup win rate is noise, not signal
TOP_SETUPS_IN_BRIEF = 8


def load_stats(data_dir: Path) -> dict[str, Any] | None:
    path = data_dir / "breakout_stats.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        logger.warning("market-regime: breakout_stats.json unreadable: %s", exc)
        return None


def _weeks(stats: dict[str, Any]) -> list[dict[str, Any]]:
    return list(stats.get("weeks") or [])


def build_facts(stats: dict[str, Any]) -> dict[str, Any] | None:
    """Reduce the full stats file to the handful of numbers the brief is about.

    Two bases, used for different things:

    - **Levels** — the KPI strip, the setup table, the cohorts — all come from
      the newest week at the full horizon, so every figure on the page is the
      same measurement and the per-setup rows sit under a comparable total.
    - **Deltas** — "vs last week" — come from the `comparable` block, which
      scores every week at one short horizon. The newest week is still partly
      unresolved, so comparing it at the full horizon against a finished week
      would report a decline that is an artefact of recency rather than a
      change in the tape.
    """
    weeks = _weeks(stats)
    if not weeks:
        return None

    comparable = stats.get("comparable") or {}
    comp_weeks = list(comparable.get("weeks") or [])

    # Levels (the KPI strip, setup table and cohorts) all come from the newest
    # week at the FULL horizon, so everything on screen is one measurement.
    # The short common horizon is used only for week-over-week deltas, which is
    # the one place it is needed — comparing a part-resolved week against a
    # finished one at different horizons would manufacture a decline.
    # Mixing the two was showing a 4-session overall win rate directly above a
    # 10-session per-setup table.
    detail_week = weeks[-1]
    current = detail_week
    delta_basis = comp_weeks or weeks
    delta_current = delta_basis[-1] if delta_basis else None
    prior = delta_basis[-2] if len(delta_basis) >= 2 else None
    setups = [
        row for row in (detail_week.get("setups") or [])
        if int(row.get("resolved") or 0) >= MIN_SAMPLE_FOR_SETUP
    ]
    setups.sort(key=lambda r: (-float(r.get("win_rate") or 0), -int(r.get("resolved") or 0)))

    dropped = len(detail_week.get("setups") or []) - len(setups)

    overall = current.get("overall") or {}
    # Both sides of a delta come from `delta_basis`, never from `overall` —
    # otherwise the change would be measured between two different horizons.
    delta_overall = (delta_current or {}).get("overall") or {}
    prior_overall = (prior or {}).get("overall") or {}

    def delta(key: str) -> dict[str, Any] | None:
        """Signed change plus the direction spelled out.

        A bare `-6.7` / `+6.7` is too easy to misread: an early draft described
        a +6.7 point improvement as "a notable decline of 6.7 percent", which
        inverts the entire read while quoting the number correctly. Saying
        "improved" or "deteriorated" in words removes the interpretation step.
        """
        if not prior_overall or not delta_overall:
            return None
        try:
            change = round(
                float(delta_overall.get(key) or 0) - float(prior_overall.get(key) or 0), 1
            )
        except (TypeError, ValueError):
            return None
        if change > 0:
            direction = "improved"
        elif change < 0:
            direction = "deteriorated"
        else:
            direction = "unchanged"
        return {"change": change, "direction": direction, "magnitude": abs(change)}

    cohorts = detail_week.get("cohorts") or {}
    return {
        "as_of_session": stats.get("as_of_session"),
        "current_week": current.get("week"),
        "prior_week": (prior or {}).get("week"),
        "comparison_horizon_sessions": comparable.get("horizon_sessions"),
        "comparison_is_like_for_like": bool(comp_weeks),
        "universe": {
            "symbols_replayed": stats.get("symbols_replayed"),
            "sessions_replayed": stats.get("sessions_replayed"),
            "signals": stats.get("signals"),
        },
        "this_week": {
            "signals": current.get("total_signals"),
            "resolved": overall.get("resolved"),
            "still_open": overall.get("open_positions"),
            "win_rate": overall.get("win_rate"),
            "median_max_move_pct": overall.get("median_max_move_pct"),
            "median_sessions_held": overall.get("median_sessions_held"),
            "pct_closed_near_high": overall.get("pct_closed_near_high"),
            "pct_reached_big_move": overall.get("pct_reached_big_move"),
            "pct_of_big_movers_that_closed_near_high": overall.get("pct_big_move_held"),
        },
        "vs_prior_week": {
            "win_rate": delta("win_rate"),
            "median_max_move_pct": delta("median_max_move_pct"),
            "pct_closed_near_high": delta("pct_closed_near_high"),
        },
        "setups": [
            {
                "label": row.get("label"),
                "signals": row.get("resolved"),
                "win_rate": row.get("win_rate"),
                "median_max_move_pct": row.get("median_max_move_pct"),
                "median_sessions_held": row.get("median_sessions_held"),
                "pct_closed_near_high": row.get("pct_closed_near_high"),
            }
            for row in setups[:TOP_SETUPS_IN_BRIEF]
        ],
        "setups_below_sample_threshold": max(0, dropped),
        "min_sample_for_setup": MIN_SAMPLE_FOR_SETUP,
        "cohorts": {
            name: {
                "signals": (cohorts.get(name) or {}).get("resolved"),
                "win_rate": (cohorts.get(name) or {}).get("win_rate"),
                "median_max_move_pct": (cohorts.get(name) or {}).get("median_max_move_pct"),
            }
            for name in ("ipo", "leading_groups", "lagging_groups")
        },
        "best_runners": (detail_week.get("overall") or {}).get("examples") or [],
        "rules": stats.get("rules") or {},
    }


PROMPT = """You are writing the "Market Breakout Conditions & Situational Awareness" brief \
for an Indian equities swing trader who holds positions from a few days to a few weeks.

Below is a JSON block of measured statistics. Every signal was reconstructed by replaying \
the app's own scanners over historical daily bars, then simulating the trade under the rules \
in `rules`.

ABSOLUTE CONSTRAINT: you may not state any number that is not present in this JSON. Do not \
estimate, round differently, extrapolate, or infer a figure. If something is not measured, \
say it is not measured. Inventing a statistic makes this brief worse than useless.

FACTS:
{facts}

Write 5 to 8 paragraphs of flowing prose. No headings, no bullet points, no markdown. Cover:

1. The regime in one honest sentence — is the tape paying breakouts or not.
2. What the win rates actually say, naming the setups that led and the ones that failed. \
Each entry in `vs_prior_week` carries an explicit `direction` ("improved", "deteriorated" or \
"unchanged") alongside its signed `change` — use that word. Never describe an improvement as a \
decline or vice versa. \
If `comparison_is_like_for_like` is true, note that this week and last are compared over the \
same {horizon} sessions so the change is real rather than an artefact of recency.
3. The ceiling on moves: median maximum favourable excursion, how many reached the big-move \
threshold, and crucially `pct_of_big_movers_that_closed_near_high` — the share of those big \
movers that finished near their highs. It is a share of *stocks*, not a share of the move \
retained, so do not phrase it as "77% of the move was held".
4. What this means arithmetically for a trader risking the stop in `rules` — whether the \
available reward supports the observed win rate.
5. How the two cohorts differ: recent IPOs versus everything else, and stocks in \
top-decile industry groups versus the rest.
6. Concrete operational guidance, separately for a fast trader who rotates capital quickly \
and for someone who wants to hold for weeks.

Tone: direct, unsentimental, addressed to the reader as "you". Respect the data rather than \
fighting the tape. You may reference well-known trading principles about cutting losses, \
reward-to-risk, or the rarity of outlier winners, but attribute a named quote only if you are \
certain of it — otherwise state the principle without attribution.

If `still_open` is a large share of `signals`, say plainly that the most recent week is not \
yet fully resolved and the read is provisional.

Return JSON: {{"headline": "<one sentence, max 20 words>", "narrative": "<the prose, \
paragraphs separated by \\n\\n">, "stance": "<one of: constructive, mixed, defensive>"}}"""


def build_prompt(facts: dict[str, Any]) -> str:
    return PROMPT.format(
        facts=json.dumps(facts, indent=2),
        horizon=facts.get("comparison_horizon_sessions") or "measured",
    )


def deterministic_brief(facts: dict[str, Any]) -> dict[str, Any]:
    """Fallback when Gemini is unavailable or its output fails validation.

    Deliberately plain: it states the measured numbers and stops. A page that
    silently shows nothing when the AI is down is worse than one that shows the
    data without the essay.
    """
    week = facts.get("this_week") or {}
    win = week.get("win_rate")
    median_move = week.get("median_max_move_pct")
    held = week.get("median_sessions_held")
    near_high = week.get("pct_closed_near_high")
    reached = week.get("pct_reached_big_move")
    kept = week.get("pct_of_big_movers_that_closed_near_high")
    rules = facts.get("rules") or {}

    stance = "mixed"
    if isinstance(win, (int, float)):
        stance = "constructive" if win >= 50 else ("defensive" if win < 40 else "mixed")

    leaders = [s for s in (facts.get("setups") or [])][:3]
    leader_text = (
        "The setups with the highest measured win rate were "
        + ", ".join(f"{s['label']} ({s['win_rate']}% over {s['signals']} signals)" for s in leaders)
        + ". "
    ) if leaders else ""

    ipo = (facts.get("cohorts") or {}).get("ipo") or {}
    lead = (facts.get("cohorts") or {}).get("leading_groups") or {}
    lag = (facts.get("cohorts") or {}).get("lagging_groups") or {}

    narrative = (
        f"Across {week.get('resolved')} resolved breakout signals in {facts.get('current_week')}, "
        f"{win}% met the {rules.get('win_pct')}% target before the {rules.get('stop_pct')}% stop. "
        f"The median signal ran {median_move}% at its best and resolved in {held} "
        f"{'session' if held == 1 else 'sessions'}. "
        f"{near_high}% closed in the top quartile of their range.\n\n"
        f"{reached}% reached the {rules.get('big_move_pct')}% mark, and of those {kept}% "
        f"finished near their highs rather than giving the move back.\n\n"
        f"{leader_text}"
        f"Recent IPOs won {ipo.get('win_rate')}% over {ipo.get('signals')} signals, against "
        f"{lead.get('win_rate')}% for names in top-decile industry groups and "
        f"{lag.get('win_rate')}% for everything else.\n\n"
        f"These are measured outcomes under the rules shown, not projections. "
        f"{week.get('still_open')} signals from this week have not resolved yet."
    )

    return {
        "headline": f"{win}% of breakouts paid in {facts.get('current_week')}, median run {median_move}%.",
        "narrative": narrative,
        "stance": stance,
        "source": "computed",
    }


def validate_narrative(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    narrative = str(payload.get("narrative") or "").strip()
    headline = str(payload.get("headline") or "").strip()
    if len(narrative) < 200 or not headline:
        return None
    stance = str(payload.get("stance") or "mixed").strip().lower()
    if stance not in {"constructive", "mixed", "defensive"}:
        stance = "mixed"
    return {"headline": headline, "narrative": narrative, "stance": stance, "source": "ai"}


def envelope(facts: dict[str, Any], brief: dict[str, Any], stats: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stats_generated_at": stats.get("generated_at"),
        "as_of_session": facts.get("as_of_session"),
        "brief": brief,
        "facts": facts,
        "rules": stats.get("rules"),
    }
