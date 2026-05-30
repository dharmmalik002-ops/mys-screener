"""XP market breadth score engine.

Open-source proxy for the Stocksgeeks "EM" market-breadth score, computed over
the full daily bhavcopy (all listed equities). Dependency-free (stdlib only) so
it can be imported both by the FastAPI backend (HF Space) and by the GitHub
Actions bhavcopy generator without pulling in heavy deps.

Methodology (verbatim from the published XP spec):

  Step 1 - smooth the 4.5% advancer count into an EMA state:
      z_state_t = 0.162 * advancers_t + 0.838 * z_state_(t-1)

  Step 2 - log-space recursion over six inputs:
      log(XP_t) = 0.592*log(XP_(t-1))
                + 0.471*log(z_state_t)
                + 0.198*logit(ma10%)
                + 0.334
                - 0.067*log(decliners_t)
                - 0.077*logit(ma20%)
      where logit(x%) = log( x / (100 - x) )

  Step 3 - XP_t = exp(log(XP_t))

Regime bands: >25 Extremely Strong | 15-25 Swing-Friendly |
12-15 Progressive | 9.5-12 Choppy | <9.5 Avoid Longs.
"""

from __future__ import annotations

import math
from typing import Any, Iterable

# --- Calibration constants (verbatim from the XP spec) -----------------------
Z_ALPHA = 0.162  # weight on today's 4.5% advancer count
Z_BETA = 0.838  # weight on prior z_state
W_PREV_XP = 0.592
W_Z = 0.471
W_MA10 = 0.198
CONST = 0.334
W_DECLINERS = -0.067
W_MA20 = -0.077

ADVANCER_PCT_THRESHOLD = 4.5  # a stock up >= 4.5% on the day counts as a 4.5% advancer
MA_SHORT = 10
MA_LONG = 20

# Recursion seeding / warm-up. The 0.592/0.838 persistence means the series
# converges away from the seed within a couple of months; early values are
# flagged so the UI can treat them as warm-up rather than live signal.
XP_SEED = 12.0
WARMUP_DAYS = 60

# Numeric floors to keep the log-space math finite.
_EPS_PCT = 0.01  # clamp ma% into (0.01, 99.99) before the logit
_EPS_POS = 1e-6

# (low_inclusive, high_exclusive, label, color)
REGIME_BANDS: list[tuple[float, float, str, str]] = [
    (25.0, math.inf, "Extremely Strong", "#0b8f3a"),
    (15.0, 25.0, "Swing-Friendly", "#37b24d"),
    (12.0, 15.0, "Progressive Exposure", "#94d82d"),
    (9.5, 12.0, "Choppy / Spurt Only", "#f59f00"),
    (-math.inf, 9.5, "Avoid Longs", "#e03131"),
]


def regime_for(xp: float) -> tuple[str, str]:
    """Return (label, hex_color) for an XP value."""
    for low, high, label, color in REGIME_BANDS:
        if low <= xp < high:
            return label, color
    return REGIME_BANDS[-1][2], REGIME_BANDS[-1][3]


def regime_bands_public() -> list[dict[str, Any]]:
    """JSON-serialisable band definitions for the frontend (open ends -> None)."""
    out: list[dict[str, Any]] = []
    for low, high, label, color in REGIME_BANDS:
        out.append(
            {
                "label": label,
                "color": color,
                "min": None if low == -math.inf else low,
                "max": None if high == math.inf else high,
            }
        )
    return out


def _logit_pct(pct: float) -> float:
    p = min(max(float(pct), _EPS_PCT), 100.0 - _EPS_PCT)
    return math.log(p / (100.0 - p))


def _ema_alpha(period: int) -> float:
    return 2.0 / (period + 1.0)


def daily_breadth_metrics(
    date_iso: str,
    bhav: dict[str, dict[str, Any]],
    ma_state: dict[str, list[float]],
    *,
    ma_short: int = MA_SHORT,
    ma_long: int = MA_LONG,
    advancer_pct: float = ADVANCER_PCT_THRESHOLD,
    symbol_filter: set[str] | None = None,
) -> tuple[dict[str, Any], dict[str, list[float]]]:
    """Compute one day's breadth inputs from the full bhavcopy.

    The 10/20-DMA percentages use EMAs (more reactive than SMAs at turning
    points). EMA is recursive, so we persist only the per-symbol state
    ``{symbol: [ema_short, ema_long]}`` (2 floats) rather than a window of
    closes — smaller on disk and O(1) per symbol per day.

    Note: ``close > EMA(including today)`` is algebraically identical to
    ``close > prior EMA`` (since the EMA weight on the prior value is > 0), so
    seeding the EMA with the first close introduces no self-reference bias —
    a symbol's first day simply counts as "below" (warm-up).

    ``bhav``     : {symbol: {"c": close, "p": prev_close, ...}} for the day.
    ``ma_state`` : {symbol: [ema_short, ema_long]} as of the PRIOR day. Not
                   mutated; an updated copy is returned.

    Returns (metrics, updated_ma_state) where metrics has keys:
        date, total, advancers_4p5, decliners, ma10_pct, ma20_pct
    """
    adv = dec = 0
    above10 = below10 = above20 = below20 = 0
    new_state: dict[str, list[float]] = dict(ma_state)
    alpha_s = _ema_alpha(ma_short)
    alpha_l = _ema_alpha(ma_long)
    total = 0

    for sym, rec in bhav.items():
        if symbol_filter is not None and sym not in symbol_filter:
            continue
        try:
            close = float(rec.get("c") or 0.0)
            prev = float(rec.get("p") or 0.0)
        except (TypeError, ValueError):
            continue
        if close <= 0:
            continue
        total += 1

        if prev > 0:
            chg_pct = (close - prev) / prev * 100.0
            if chg_pct >= advancer_pct:
                adv += 1
            if close < prev:
                dec += 1

        st = new_state.get(sym)
        if not st or len(st) < 2 or st[0] is None or st[1] is None:
            ema_s = close  # seed on first sight
            ema_l = close
        else:
            ema_s = alpha_s * close + (1.0 - alpha_s) * float(st[0])
            ema_l = alpha_l * close + (1.0 - alpha_l) * float(st[1])
        new_state[sym] = [round(ema_s, 4), round(ema_l, 4)]

        if close > ema_s:
            above10 += 1
        else:
            below10 += 1
        if close > ema_l:
            above20 += 1
        else:
            below20 += 1

    ma10_pct = (above10 / (above10 + below10) * 100.0) if (above10 + below10) else 0.0
    ma20_pct = (above20 / (above20 + below20) * 100.0) if (above20 + below20) else 0.0

    metrics = {
        "date": date_iso,
        "total": total,
        "advancers_4p5": adv,
        "decliners": dec,
        "ma10_pct": round(ma10_pct, 2),
        "ma20_pct": round(ma20_pct, 2),
    }
    return metrics, new_state


def compute_xp_series(
    daily_rows: Iterable[dict[str, Any]],
    *,
    xp_seed: float = XP_SEED,
    warmup_days: int = WARMUP_DAYS,
    const: float = CONST,
) -> list[dict[str, Any]]:
    """Run the XP recursion over a date-sorted sequence of daily metrics.

    Each input row needs: date, advancers_4p5, decliners, ma10_pct, ma20_pct.
    Returns the rows augmented with: z_state, xp_score, regime, regime_color,
    warmup. Idempotent — safe to recompute over the full history each run.

    ``const`` is the spec's calibration offset (default 0.334). Because the
    z_state / decliners terms scale with universe size, this offset is the
    knob used to re-align the regime bands when the universe differs from the
    author's ~2000-stock NSE base (e.g. the ~4000-stock all-bhavcopy base).
    """
    rows = sorted(daily_rows, key=lambda r: str(r.get("date") or ""))
    out: list[dict[str, Any]] = []
    z_state: float | None = None
    prev_xp: float | None = None

    for i, r in enumerate(rows):
        adv = float(r.get("advancers_4p5") or 0.0)
        dec = max(float(r.get("decliners") or 0.0), 1.0)
        ma10 = float(r.get("ma10_pct") or 0.0)
        ma20 = float(r.get("ma20_pct") or 0.0)

        # Step 1: z_state EMA of 4.5% advancers
        if z_state is None:
            z_state = max(adv, _EPS_POS)
        else:
            z_state = Z_ALPHA * adv + Z_BETA * z_state
        z_eff = max(z_state, _EPS_POS)

        # Step 2: log-space recursion
        if prev_xp is None:
            prev_xp = float(xp_seed)
        log_xp = (
            W_PREV_XP * math.log(prev_xp)
            + W_Z * math.log(z_eff)
            + W_MA10 * _logit_pct(ma10)
            + const
            + W_DECLINERS * math.log(dec)
            + W_MA20 * _logit_pct(ma20)
        )
        # Step 3: back to normal scale
        xp = math.exp(log_xp)
        prev_xp = xp

        label, color = regime_for(xp)
        enriched = dict(r)
        enriched.update(
            {
                "z_state": round(z_state, 4),
                "xp_score": round(xp, 3),
                "regime": label,
                "regime_color": color,
                "warmup": i < warmup_days,
            }
        )
        out.append(enriched)

    return out


OUTPUT_CLAMP = (0.0, 35.0)  # keep the calibrated score sane at market extremes


def fit_output_calibration(
    series: Iterable[dict[str, Any]],
    anchors: dict[str, float],
) -> tuple[float, float]:
    """Least-squares fit of an affine map ``EM ≈ scale*XP + offset`` from the
    computed (const-calibrated) XP onto the author's published EM values. This
    stretches/shifts the proxy so its numbers line up with EM. Returns
    (scale, offset); (1.0, 0.0) if too few anchors overlap.
    """
    by_date = {str(r["date"]): float(r["xp_score"]) for r in series}
    xs: list[float] = []
    ys: list[float] = []
    for d, em in anchors.items():
        v = by_date.get(str(d))
        if v is not None:
            xs.append(v)
            ys.append(float(em))
    if len(xs) < 2:
        return (1.0, 0.0)
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return (1.0, round(my - mx, 6))
    scale = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den
    offset = my - scale * mx
    return (round(scale, 6), round(offset, 6))


def apply_output_calibration(
    series: list[dict[str, Any]],
    scale: float,
    offset: float,
    *,
    clamp: tuple[float, float] = OUTPUT_CLAMP,
) -> list[dict[str, Any]]:
    """Apply the affine EM map to each day's score and re-derive the regime.
    Keeps the raw score under ``xp_raw`` (the recursion is rebuilt from metric
    inputs each run, so overwriting xp_score for display is safe). A no-op when
    scale==1 and offset==0.
    """
    lo, hi = clamp
    out: list[dict[str, Any]] = []
    for r in series:
        raw = float(r["xp_score"])
        val = max(lo, min(hi, scale * raw + offset))
        label, color = regime_for(val)
        nr = dict(r)
        nr["xp_raw"] = round(raw, 3)
        nr["xp_score"] = round(val, 3)
        nr["regime"] = label
        nr["regime_color"] = color
        out.append(nr)
    return out


def calibrate_const(
    daily_rows: Iterable[dict[str, Any]],
    anchors: dict[str, float],
    *,
    lo: float = -12.0,
    hi: float = 12.0,
    iters: int = 80,
    xp_seed: float = XP_SEED,
    warmup_days: int = WARMUP_DAYS,
) -> float:
    """Fit the calibration ``const`` so the computed XP matches known published
    values. ``anchors`` maps date -> author's published XP on that date. Returns
    the best-fit const (minimising mean squared error in log space).

    Because const -> log(XP) is monotonic, a ternary search converges quickly.
    """
    rows = list(daily_rows)

    def err(c: float) -> float:
        series = compute_xp_series(rows, const=c, xp_seed=xp_seed, warmup_days=warmup_days)
        by_date = {str(r["date"]): float(r["xp_score"]) for r in series}
        total = 0.0
        n = 0
        for d, target in anchors.items():
            got = by_date.get(str(d))
            if got and got > 0 and target > 0:
                total += (math.log(got) - math.log(target)) ** 2
                n += 1
        return total / n if n else float("inf")

    for _ in range(iters):
        m1 = lo + (hi - lo) / 3.0
        m2 = hi - (hi - lo) / 3.0
        if err(m1) < err(m2):
            hi = m2
        else:
            lo = m1
    return round((lo + hi) / 2.0, 4)
