"""Server-side candlestick PNG rendering.

Pure ``ChartBar`` in, PNG ``bytes`` out. No Telegram knowledge, no provider
access, no I/O beyond an in-memory buffer -- so every piece is unit-testable
without a running app.

The geometry and indicator maths are deliberate ports of the frontend's
hand-rolled SVG grid chart (``frontend/src/components/ChartGridModal.tsx``),
so a digest chart matches what the site draws. The colours come from
``ChartPanel``'s dark theme (``frontend/src/components/ChartPanel.tsx``),
which is the chart the user actually reads -- the grid modal defaults to a
light theme with a blue-up palette and is only the maths reference.

Three rules this module must keep (they are why rendering is thread-safe):

1. Never import ``matplotlib.pyplot``. Its global figure manager is not
   thread-safe and the digest renders from a thread pool.
2. Never mutate ``matplotlib.rcParams`` or use ``rc_context`` -- both are
   process-global and would interleave across render threads. Every artist is
   therefore styled explicitly at construction.
3. Callers must go through :func:`render_candles_png_async`, which bounds
   concurrency and hands the CPU-bound work to a thread.
"""

from __future__ import annotations

import asyncio
import io
import logging
import math
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Sequence
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

# Must be set before matplotlib is imported -- it reads both at import time.
# The Dockerfile already sets MPLCONFIGDIR for the container; this fallback only
# applies it when /code is actually writable, so local dev and CI keep
# matplotlib's own default instead of failing on a read-only path.
if not os.environ.get("MPLCONFIGDIR") and os.access("/code", os.W_OK):
    os.environ["MPLCONFIGDIR"] = "/code/.mplcache"
os.environ.setdefault("MPLBACKEND", "Agg")

try:  # pragma: no cover - exercised by the import-guard test
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.collections import LineCollection, PolyCollection
    from matplotlib.figure import Figure

    CHART_RENDER_AVAILABLE = True
    CHART_RENDER_ERROR: str | None = None
except Exception as exc:  # pragma: no cover - only when the wheel is missing
    CHART_RENDER_AVAILABLE = False
    CHART_RENDER_ERROR = str(exc)


# --------------------------------------------------------------------------
# Specs
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class MaSpec:
    key: str
    label: str
    kind: str  # "ema" | "sma"
    length: int
    color: str


# The grid chart's overlay set and colours (ChartGridModal.tsx MA_OVERLAYS).
# 10/21 rather than ChartPanel's 10/20 is deliberate: 10 and 21 are the pair
# the Momentum Burst scanner itself reports distances from.
DEFAULT_MAS: tuple[MaSpec, ...] = (
    MaSpec("e10", "10 EMA", "ema", 10, "#ef4444"),
    MaSpec("e21", "21 EMA", "ema", 21, "#22c55e"),
    MaSpec("s50", "50 SMA", "sma", 50, "#3b82f6"),
    MaSpec("s200", "200 SMA", "sma", 200, "#f4f6fb"),
)


@dataclass(frozen=True)
class ChartTheme:
    """ChartPanel's "current" dark palette, with the app's rgba() values
    flattened onto the opaque background (one less thing to get wrong than
    passing alpha through a collection)."""

    background: str = "#0d1117"
    text: str = "#e8edf4"
    text_soft: str = "#8b949e"
    text_dim: str = "#64748b"
    grid: str = "#0d2028"
    up: str = "#089981"
    down: str = "#f23645"
    volume_up: str = "#1d4a44"
    volume_down: str = "#4b2027"
    positive: str = "#22c55e"
    negative: str = "#ef4444"
    chip_bg: str = "#152233"


@dataclass(frozen=True)
class RenderSpec:
    # 1280 on the long side is exact: Telegram re-encodes bot photo uploads and
    # caps the largest retained size around 1280px, so anything bigger costs
    # bandwidth and loses text crispness to the downscale.
    width_px: int = 1280
    height_px: int = 800
    dpi: int = 100
    # Display ~6 months (matches the app's "6M": 132 preset) but compute the
    # moving averages over a much longer series -- see render_candles_png.
    display_bars: int = 130
    show_volume: bool = True
    show_trade_levels: bool = True
    right_pad_bars: float = 3.0
    # Index-space pad so the first month label (centred on bar 0) is not
    # clipped by the axes edge.
    left_pad_bars: float = 2.5
    mas: tuple[MaSpec, ...] = DEFAULT_MAS
    theme: ChartTheme = field(default_factory=ChartTheme)

    # Figure-fraction rectangles. The volume axes' top touches the price axes'
    # bottom (0.130 + 0.170 == 0.300) so the two read as one pane.
    price_rect: tuple[float, float, float, float] = (0.010, 0.300, 0.940, 0.545)
    volume_rect: tuple[float, float, float, float] = (0.010, 0.130, 0.940, 0.170)

    @property
    def px(self) -> float:
        """Points per pixel. matplotlib linewidths and fontsizes are in points,
        so every stroke/font constant below is expressed as ``n_px * px``."""
        return 72.0 / float(self.dpi)


@dataclass(frozen=True)
class ChartAnnotation:
    symbol: str
    name: str | None = None
    exchange: str | None = None
    sector: str | None = None
    market_cap_crore: float | None = None
    last_price: float | None = None
    change_pct: float | None = None
    rs_rating: int | None = None
    relative_volume: float | None = None
    scanner_name: str | None = None
    is_new: bool = False
    also_in: tuple[str, ...] = ()
    session_date: str | None = None


# --------------------------------------------------------------------------
# Pure ports of the frontend maths
# --------------------------------------------------------------------------


def sanitize_bars(bars: Sequence[object]) -> list[dict]:
    """Port of ``sanitizeChartBars`` (frontend/src/lib/chartData.ts:3-33).

    Drops non-finite and non-positive-close bars, enforces OHLC consistency,
    de-duplicates by timestamp keeping the LAST occurrence (JS ``Map.set``
    overwrites), and sorts ascending by time.
    """
    deduped: dict[int, dict] = {}
    for bar in bars:
        try:
            time_ = int(_attr(bar, "time"))
            open_ = float(_attr(bar, "open"))
            high = float(_attr(bar, "high"))
            low = float(_attr(bar, "low"))
            close = float(_attr(bar, "close"))
            volume = float(_attr(bar, "volume") or 0)
        except (TypeError, ValueError):
            continue
        values = (open_, high, low, close, volume)
        if not all(math.isfinite(v) for v in values):
            continue
        if close <= 0:
            continue
        deduped[time_] = {
            "time": time_,
            "open": open_,
            "high": max(high, open_, close),
            "low": min(low, open_, close),
            "close": close,
            "volume": volume,
        }
    return [deduped[t] for t in sorted(deduped)]


def _attr(bar: object, name: str):
    if isinstance(bar, dict):
        return bar.get(name)
    return getattr(bar, name)


def sma_overlay(values: Sequence[float], window: int) -> list[float | None]:
    """Port of ``smaOverlay`` (ChartGridModal.tsx:285-298). Rolling sum; emits
    ``None`` until index ``window - 1``."""
    out: list[float | None] = [None] * len(values)
    if window <= 0:
        return out
    total = 0.0
    for i, value in enumerate(values):
        total += value
        if i >= window:
            total -= values[i - window]
        if i >= window - 1:
            out[i] = total / window
    return out


def ema_overlay(values: Sequence[float], span: int) -> list[float | None]:
    """Port of ``emaOverlay`` (ChartGridModal.tsx:300-317).

    Seeds with the SMA of the first ``span`` values, writes that seed at index
    ``span - 1``, then walks forward with ``k = 2 / (span + 1)``. Returns all
    ``None`` when there is less history than ``span`` -- a partial line would
    be wrong, not merely short.
    """
    out: list[float | None] = [None] * len(values)
    if span <= 0 or len(values) < span:
        return out
    ema = sum(values[:span]) / span
    out[span - 1] = ema
    k = 2.0 / (span + 1.0)
    for i in range(span, len(values)):
        ema = values[i] * k + ema * (1.0 - k)
        out[i] = ema
    return out


def _indian_group(value: int) -> str:
    """Indian digit grouping: 1942100 -> '19,42,100'.

    The frontend uses ``toLocaleString("en-IN")``; Python's ``f"{v:,}"`` gives
    Western grouping and ``locale.setlocale`` is process-global, not
    thread-safe, and needs an ``en_IN`` locale that python:*-slim lacks.
    """
    sign = "-" if value < 0 else ""
    digits = str(abs(int(value)))
    if len(digits) <= 3:
        return sign + digits
    head, tail = digits[:-3], digits[-3:]
    parts: list[str] = []
    while len(head) > 2:
        parts.insert(0, head[-2:])
        head = head[:-2]
    if head:
        parts.insert(0, head)
    return sign + ",".join(parts) + "," + tail


def format_scale_price(value: float) -> str:
    """Port of ``formatScalePrice`` (ChartGridModal.tsx:341-352). The branch is
    chosen on the raw value, so 9999.6 formats as '10000' (the >=1000 branch)
    while 10000.0 formats as '10,000' (the grouped branch)."""
    if not math.isfinite(value):
        return ""
    if value >= 10_000:
        return _indian_group(round(value))
    if value >= 1_000:
        return f"{value:.0f}"
    if value >= 100:
        return f"{value:.1f}"
    return f"{value:.2f}"


def price_ticks(low: float, high: float, count: int = 4) -> list[float]:
    """Port of ``priceTicks`` (ChartGridModal.tsx:354-365). Nice-step of
    1/2/5 x 10^floor(log10(rawStep))."""
    spread = max(high - low, 1e-6)
    raw_step = spread / (count + 1)
    if raw_step <= 0 or not math.isfinite(raw_step):
        return []
    magnitude = 10.0 ** math.floor(math.log10(raw_step))
    residual = raw_step / magnitude
    nice_step = (5.0 if residual >= 5 else 2.0 if residual >= 2 else 1.0) * magnitude
    ticks: list[float] = []
    tick = math.ceil(low / nice_step) * nice_step
    # Guard against a pathological step producing an unbounded loop.
    for _ in range(count + 8):
        if tick > high:
            break
        ticks.append(tick)
        tick += nice_step
    return ticks[: count + 2]


def month_axis_labels(
    bars: Sequence[dict],
    x_of: Callable[[int], float],
    min_gap_px: float = 34.0,
) -> list[tuple[int, str]]:
    """Port of ``MonthAxis`` (ChartGridModal.tsx:380-421).

    One label per month change (plus index 0), a two-digit year appended when
    the window spans more than ~360 days, then thinned so no two labels sit
    closer than ``min_gap_px``. Returns ``[(bar_index, text), ...]``.

    Bar times are epoch seconds UTC; the browser's ``getMonth()`` is local
    time (IST for this user), so the timezone is made explicit here.
    """
    if not bars:
        return []
    span_days = (bars[-1]["time"] - bars[0]["time"]) / 86_400 if len(bars) > 1 else 0
    labels: list[tuple[int, str]] = []
    previous_month = -1
    for index, bar in enumerate(bars):
        try:
            moment = datetime.fromtimestamp(bar["time"], tz=IST)
        except (OSError, OverflowError, ValueError):
            continue
        month = moment.month
        if index == 0 or month != previous_month:
            show_year = span_days > 360 and (month == 1 or index == 0)
            text = moment.strftime("%b %y") if show_year else moment.strftime("%b")
            labels.append((index, text.upper()))
            previous_month = month
    spaced: list[tuple[int, str]] = []
    for index, text in labels:
        if not spaced or (x_of(index) - x_of(spaced[-1][0])) >= min_gap_px:
            spaced.append((index, text))
    return spaced


def _ma_segments(values: Sequence[float | None]) -> list[list[tuple[float, float]]]:
    """Split an overlay into contiguous runs so a ``None`` gap breaks the line
    instead of interpolating across it (the frontend's pen-up/pen-down)."""
    segments: list[list[tuple[float, float]]] = []
    current: list[tuple[float, float]] = []
    for index, value in enumerate(values):
        if value is None or not math.isfinite(value):
            if len(current) > 1:
                segments.append(current)
            current = []
        else:
            current.append((float(index), float(value)))
    if len(current) > 1:
        segments.append(current)
    return segments


# --------------------------------------------------------------------------
# Renderer
# --------------------------------------------------------------------------

# Absolute pixel sizes at 1280px wide. Text scales with the image (the PNG is
# ~4x the on-screen card) but strokes deliberately do not -- a 5px wick reads
# as a fence post. Telegram re-encodes to JPEG, whose chroma subsampling
# smears thin saturated strokes on near-black, so nothing here is under 1.5px.
_WICK_PX = 1.6
_MA_PX = 2.0
_GRID_PX = 1.0
_LEVEL_PX = 1.4
_SYMBOL_PX = 40.0
_META_PX = 22.0
_BADGE_PX = 21.0
_SCALE_PX = 20.0
_MONTH_PX = 18.0
_LEGEND_PX = 17.0
_FOOT_PX = 19.0

_BODY_WIDTH = 0.65   # ChartGridModal.tsx candle body width, in index units
_VOLUME_WIDTH = 0.60

# Rendering is pure CPU; the Space is ~2 vCPU. Shared by the digest and any
# debug endpoint so an accidental hammering cannot starve the digest.
_RENDER_SEMAPHORE = asyncio.Semaphore(2)

MIN_RENDERABLE_BARS = 2


def _fmt_compact_crore(value: float | None) -> str | None:
    if value is None or not math.isfinite(value) or value <= 0:
        return None
    return f"₹{_indian_group(round(value))} cr"


def _fmt_signed_pct(value: float | None) -> str | None:
    if value is None or not math.isfinite(value):
        return None
    # U+2212 MINUS SIGN reads better than a hyphen; it is present in DejaVu Sans.
    sign = "+" if value >= 0 else "−"
    return f"{sign}{abs(value):.2f}%"


def render_candles_png(
    bars: Sequence[object],
    annotation: ChartAnnotation,
    spec: RenderSpec | None = None,
) -> bytes:
    """Render a dark-theme candlestick chart as PNG bytes.

    ``bars`` should carry MORE history than ``spec.display_bars`` -- the
    moving averages are computed over everything supplied and only then
    sliced to the display window, so a 200 SMA is correct at the left edge.
    Pass ~520 bars to display 130.

    Raises ``ValueError`` when there is too little usable history to draw.
    Callers treat that as "skip this symbol", never as a failure.
    """
    if not CHART_RENDER_AVAILABLE:  # pragma: no cover - guarded by callers
        raise RuntimeError(f"matplotlib unavailable: {CHART_RENDER_ERROR}")

    spec = spec or RenderSpec()
    theme = spec.theme
    px = spec.px

    full = sanitize_bars(bars)
    if len(full) < MIN_RENDERABLE_BARS:
        raise ValueError(f"{annotation.symbol}: {len(full)} usable bars, need {MIN_RENDERABLE_BARS}")

    closes = [b["close"] for b in full]

    # Compute over the FULL series, then slice the RESULT. Slicing the closes
    # first would leave the 200 SMA entirely empty and the 50 SMA blank for 49
    # bars (frontend does the same -- ChartGridModal.tsx:708-724).
    overlays_full = [
        (ma, ema_overlay(closes, ma.length) if ma.kind == "ema" else sma_overlay(closes, ma.length))
        for ma in spec.mas
    ]

    count = min(max(int(spec.display_bars), MIN_RENDERABLE_BARS), len(full))
    window = full[-count:]
    overlays = [(ma, values[-count:]) for ma, values in overlays_full]

    # Only the visible window's MA values widen the axis. Using the whole
    # 520-bar range would let an off-screen 200 SMA stretch the scale.
    ma_values = [v for _, values in overlays for v in values if v is not None and math.isfinite(v)]
    low = min([b["low"] for b in window] + ma_values)
    high = max([b["high"] for b in window] + ma_values)
    spread = max(high - low, 1e-6)

    fig = Figure(
        figsize=(spec.width_px / spec.dpi, spec.height_px / spec.dpi),
        dpi=spec.dpi,
        facecolor=theme.background,
    )
    FigureCanvasAgg(fig)  # bind a canvas; never touch pyplot

    price_ax = fig.add_axes(spec.price_rect)
    vol_ax = fig.add_axes(spec.volume_rect, sharex=price_ax)
    for axes in (price_ax, vol_ax):
        axes.set_facecolor(theme.background)
        for spine in axes.spines.values():
            spine.set_visible(False)

    x_min = -0.5 - spec.left_pad_bars
    x_max = count - 0.5 + spec.right_pad_bars
    price_ax.set_xlim(x_min, x_max)
    price_ax.set_ylim(low, high)
    price_ax.set_autoscale_on(False)  # collections do not update datalim reliably
    vol_ax.set_autoscale_on(False)

    # ---- gridlines (behind everything) ----
    ticks = price_ticks(low, high, 4)
    for tick in ticks:
        price_ax.axhline(tick, color=theme.grid, linewidth=_GRID_PX * px, zorder=0)

    # ---- candles: one LineCollection for wicks, one PolyCollection for bodies ----
    price_axes_height_px = spec.price_rect[3] * spec.height_px
    min_body = spread / max(price_axes_height_px, 1.0)   # the frontend's 1px floor
    half_body = _BODY_WIDTH / 2.0

    body_verts: list[list[tuple[float, float]]] = []
    body_colors: list[str] = []
    wick_segments: list[list[tuple[float, float]]] = []
    wick_colors: list[str] = []

    for index, bar in enumerate(window):
        rising = bar["close"] >= bar["open"]        # >= is the frontend's tie-break
        color = theme.up if rising else theme.down
        body_low, body_high = (
            (bar["open"], bar["close"]) if rising else (bar["close"], bar["open"])
        )
        if body_high - body_low < min_body:
            # Doji: centre the minimum-height body so it does not visually shift.
            middle = (bar["open"] + bar["close"]) / 2.0
            body_low, body_high = middle - min_body / 2.0, middle + min_body / 2.0
        body_verts.append(
            [
                (index - half_body, body_low),
                (index + half_body, body_low),
                (index + half_body, body_high),
                (index - half_body, body_high),
            ]
        )
        body_colors.append(color)
        wick_segments.append([(index, bar["low"]), (index, bar["high"])])
        wick_colors.append(color)

    price_ax.add_collection(
        LineCollection(
            wick_segments,
            colors=wick_colors,
            linewidths=_WICK_PX * px,
            # The default "projecting" cap overshoots every high/low by half a
            # linewidth -- visible on all 130 candles.
            capstyle="butt",
            zorder=2,
        )
    )
    price_ax.add_collection(
        PolyCollection(body_verts, facecolors=body_colors, edgecolors="none", zorder=3)
    )

    # ---- moving averages ----
    legend_entries: list[tuple[str, str]] = []
    for ma, values in overlays:
        segments = _ma_segments(values)
        if not segments:
            continue
        price_ax.add_collection(
            LineCollection(
                segments, colors=ma.color, linewidths=_MA_PX * px, alpha=0.9, zorder=4
            )
        )
        legend_entries.append((ma.label, ma.color))

    # ---- trade levels from the user's own risk parameters ----
    if spec.show_trade_levels:
        _draw_trade_levels(price_ax, window[-1]["close"], low, high, x_min, theme, px)

    # ---- right-hand price scale ----
    price_ax.yaxis.tick_right()
    price_ax.set_yticks(ticks)
    price_ax.set_yticklabels(
        [format_scale_price(t) for t in ticks], fontsize=_SCALE_PX * px, color=theme.text_soft
    )
    price_ax.tick_params(axis="y", length=0, pad=6)
    price_ax.tick_params(axis="x", length=0, labelbottom=False)

    # ---- volume band ----
    volumes = [b["volume"] or 0.0 for b in window]
    if spec.show_volume and any(v > 0 for v in volumes):
        half_vol = _VOLUME_WIDTH / 2.0
        vol_verts = [
            [(i - half_vol, 0.0), (i + half_vol, 0.0), (i + half_vol, v), (i - half_vol, v)]
            for i, v in enumerate(volumes)
        ]
        vol_colors = [
            theme.volume_up if b["close"] >= b["open"] else theme.volume_down for b in window
        ]
        vol_ax.add_collection(
            PolyCollection(vol_verts, facecolors=vol_colors, edgecolors="none", zorder=2)
        )
        vol_ax.set_ylim(0, max(volumes) * 1.02)
    else:
        vol_ax.set_ylim(0, 1)
    vol_ax.set_yticks([])

    # ---- month axis (on the shared bottom axis) ----
    plot_width_px = spec.price_rect[2] * spec.width_px
    px_per_index = plot_width_px / max(x_max - x_min, 1e-6)
    month_labels = month_axis_labels(window, lambda i: i * px_per_index, min_gap_px=34.0)
    vol_ax.set_xticks([i for i, _ in month_labels])
    vol_ax.set_xticklabels(
        [text for _, text in month_labels], fontsize=_MONTH_PX * px, color=theme.text_dim
    )
    vol_ax.tick_params(axis="x", length=0, pad=6)

    _draw_legend(price_ax, legend_entries, theme, px)
    _draw_annotation(fig, annotation, theme, px, bars_available=len(full))

    buffer = io.BytesIO()
    fig.savefig(
        buffer, format="png", facecolor=theme.background, edgecolor="none", dpi=spec.dpi
    )
    fig.clf()  # release the Agg buffer deterministically
    return buffer.getvalue()


def _draw_trade_levels(axes, last_close, low, high, x_min, theme: ChartTheme, px: float) -> None:
    """Dashed stop/target/ceiling lines from the user's own parameters."""
    try:
        from app.services.breakout_stats import BIG_MOVE_PCT, STOP_PCT, WIN_PCT
    except Exception:  # pragma: no cover - constants moved or renamed
        return
    levels = (
        (-abs(STOP_PCT), theme.negative, f"−{abs(STOP_PCT):.0f}% stop"),
        (abs(WIN_PCT), theme.positive, f"+{abs(WIN_PCT):.0f}% win"),
        (abs(BIG_MOVE_PCT), theme.text_soft, f"+{abs(BIG_MOVE_PCT):.0f}%"),
    )
    for pct, color, label in levels:
        value = last_close * (1.0 + pct / 100.0)
        if not (low <= value <= high):
            continue
        axes.axhline(
            value, color=color, linewidth=_LEVEL_PX * px, linestyle=(0, (4, 4)), alpha=0.35, zorder=1
        )
        axes.annotate(
            label,
            xy=(x_min, value),
            xycoords="data",
            ha="left",
            va="bottom",
            fontsize=_LEGEND_PX * px,
            color=color,
            alpha=0.75,
        )


def _draw_legend(axes, entries: list[tuple[str, str]], theme: ChartTheme, px: float) -> None:
    """Inline MA legend, top-left of the price pane. Only lists overlays that
    actually produced a line."""
    if not entries:
        return
    x = 0.008
    for label, color in entries:
        axes.text(
            x,
            0.972,
            f"— {label}",
            transform=axes.transAxes,
            ha="left",
            va="top",
            fontsize=_LEGEND_PX * px,
            color=color,
            zorder=6,
            bbox=dict(facecolor=theme.background, alpha=0.72, edgecolor="none", pad=1.6),
        )
        x += 0.072


def _draw_annotation(
    fig, note: ChartAnnotation, theme: ChartTheme, px: float, *, bars_available: int
) -> None:
    """Header and footer bands.

    Every field is burned into the image rather than left to a caption: in a
    Telegram album only ONE item's caption is visible in the timeline, so a
    photo has to identify itself.
    """
    # --- header row 1: symbol (left), change % badge (right) ---
    fig.text(
        0.014, 0.952, note.symbol, ha="left", va="center",
        fontsize=_SYMBOL_PX * px, fontweight="bold", color=theme.text,
    )
    change = _fmt_signed_pct(note.change_pct)
    if change:
        positive = (note.change_pct or 0) >= 0
        fig.text(
            0.986, 0.952, change, ha="right", va="center",
            fontsize=_BADGE_PX * px, fontweight="bold",
            color=theme.positive if positive else theme.negative,
            bbox=dict(
                facecolor="#0f2e20" if positive else "#2e1418",
                edgecolor=theme.positive if positive else theme.negative,
                linewidth=0.8, boxstyle="round,pad=0.32",
            ),
        )

    # --- header row 2: identity line (left), RS chip (right) ---
    meta = [
        note.name,
        note.exchange,
        note.sector,
        _fmt_compact_crore(note.market_cap_crore),
        f"₹{note.last_price:,.2f}" if note.last_price else None,
    ]
    line = " · ".join(p for p in meta if p)
    if line:
        fig.text(
            0.014, 0.900, line[:150], ha="left", va="center",
            fontsize=_META_PX * px, color=theme.text_soft,
        )
    if note.rs_rating is not None:
        fig.text(
            0.986, 0.900, f"RS {note.rs_rating}", ha="right", va="center",
            fontsize=_BADGE_PX * px, color=theme.text,
            bbox=dict(facecolor=theme.chip_bg, edgecolor="none", boxstyle="round,pad=0.3"),
        )

    # --- footer row 1: scanner (left), NEW badge (right) ---
    if note.scanner_name:
        fig.text(
            0.014, 0.062, note.scanner_name, ha="left", va="center",
            fontsize=_FOOT_PX * px, fontweight="bold", color=theme.text,
        )
    if note.is_new:
        # Text, not an emoji: DejaVu Sans has no emoji glyphs.
        fig.text(
            0.986, 0.062, "NEW", ha="right", va="center",
            fontsize=_FOOT_PX * px, fontweight="bold", color=theme.positive,
            bbox=dict(
                facecolor="#1a2e1f", edgecolor=theme.positive,
                linewidth=0.8, boxstyle="round,pad=0.3",
            ),
        )

    # --- footer row 2: confluence (left), rvol + session (right) ---
    left_bits: list[str] = []
    if note.also_in:
        left_bits.append("Also in: " + " · ".join(note.also_in[:3]))
    if bars_available < 200:
        left_bits.append(f"limited history ({bars_available} bars)")
    if left_bits:
        fig.text(
            0.014, 0.022, "   ".join(left_bits)[:150], ha="left", va="center",
            fontsize=_LEGEND_PX * px, color=theme.text_dim,
        )
    right_bits = []
    if note.relative_volume:
        right_bits.append(f"RVOL {note.relative_volume:.1f}x")
    if note.session_date:
        right_bits.append(note.session_date)
    if right_bits:
        fig.text(
            0.986, 0.022, " · ".join(right_bits), ha="right", va="center",
            fontsize=_LEGEND_PX * px, color=theme.text_dim,
        )


async def render_candles_png_async(
    bars: Sequence[object],
    annotation: ChartAnnotation,
    spec: RenderSpec | None = None,
) -> bytes:
    """Bounded, off-the-event-loop wrapper. Always use this from request or
    scheduler code -- rendering is CPU-bound and would block the loop."""
    async with _RENDER_SEMAPHORE:
        return await asyncio.to_thread(render_candles_png, bars, annotation, spec)
