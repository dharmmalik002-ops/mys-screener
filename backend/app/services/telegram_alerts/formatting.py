"""Message and caption text. Pure functions.

``parse_mode=HTML`` throughout, deliberately not MarkdownV2. MarkdownV2 needs
``_ * [ ] ( ) ~ ` > # + - = | { } . !`` escaped in ALL text, and these captions
are full of ``+3.24%``, ``-1.2``, ``Rs 1,412.60``, ``2026-08-20`` and names
like ``M&M Financial Services Ltd.``. One miss returns
``400 can't parse entities`` and kills the whole album. HTML needs only
``& < >``, via the single :func:`h` helper that every string goes through.
"""

from __future__ import annotations

import html
from typing import Any, Iterable, Sequence

# Telegram hard limits.
CAPTION_LIMIT = 1024
MESSAGE_LIMIT = 4096
# Leave headroom so a long confluence list can never push a caption over.
CAPTION_BUDGET = 1000
MESSAGE_BUDGET = 3800


def h(value: Any) -> str:
    """Escape for HTML parse_mode. ``&`` must be escaped too -- that is the one
    people forget, and ``M&M`` is a real Indian ticker name."""
    return html.escape("" if value is None else str(value), quote=False)


def truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def signed_pct(value: float | None, *, digits: int = 1) -> str:
    if value is None:
        return ""
    sign = "+" if value >= 0 else "−"
    return f"{sign}{abs(value):.{digits}f}%"


def split_message(text: str, limit: int = MESSAGE_BUDGET) -> list[str]:
    """Split on line boundaries only. Splitting mid-entity (inside a ``<b>``)
    produces a message Telegram cannot parse."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current: list[str] = []
    length = 0
    for line in text.split("\n"):
        line_length = len(line) + 1
        if length + line_length > limit and current:
            chunks.append("\n".join(current))
            current, length = [], 0
        # A single line longer than the limit is hard-truncated rather than
        # split mid-tag.
        if line_length > limit:
            line = truncate(line, limit - 1)
            line_length = len(line) + 1
        current.append(line)
        length += line_length
    if current:
        chunks.append("\n".join(current))
    return chunks


def regime_header(
    *,
    session_date: str | None,
    regime: str | None = None,
    xp_score: float | None = None,
    exposure_band: str | None = None,
    exposure_pct: float | None = None,
    advances: int | None = None,
    declines: int | None = None,
    scanner_count: int = 0,
    match_count: int = 0,
    chart_count: int = 0,
    stop_pct: float | None = None,
    win_pct: float | None = None,
    horizon_sessions: int | None = None,
) -> str:
    """The digest's opening message.

    Every clause is independently optional -- omit rather than print
    "unknown". Deliberately measured, never predictive: no buy/sell/add/reduce
    wording, consistent with the Markets page.
    """
    lines = [f"<b>📊 EOD Digest</b> · {h(session_date or 'session unknown')}", ""]

    context: list[str] = []
    if regime:
        context.append(
            f"Regime: <b>{h(regime)}</b>" + (f" (XP {xp_score:.0f})" if xp_score is not None else "")
        )
    if exposure_band:
        exposure = f"Exposure: <b>{h(exposure_band)}</b>"
        if exposure_pct is not None:
            exposure += f" ({exposure_pct:.0f}%)"
        context.append(exposure)
    if context:
        lines.append(" · ".join(context))

    if advances is not None and declines is not None:
        lines.append(f"Breadth: {advances} up / {declines} down")

    rules = []
    if stop_pct is not None:
        rules.append(f"{stop_pct:.0f}% stop")
    if win_pct is not None:
        rules.append(f"{win_pct:.0f}% win")
    if horizon_sessions is not None:
        rules.append(f"{horizon_sessions} sessions")
    if rules:
        lines.append(f"<i>Rules: {' · '.join(rules)}</i>")

    lines.append("")
    lines.append(
        f"{scanner_count} scanner{'s' if scanner_count != 1 else ''} · "
        f"{match_count} match{'es' if match_count != 1 else ''} · {chart_count} charts"
    )
    return _collapse_blanks(lines)


def _collapse_blanks(lines: Sequence[str]) -> str:
    """Join, collapsing runs of blank lines, so an omitted optional clause does
    not leave a double gap."""
    out: list[str] = []
    for line in lines:
        if not line and (not out or not out[-1]):
            continue
        out.append(line)
    return "\n".join(out).strip("\n")


def _item_row(position: int, item: Any, *, is_new: bool) -> str:
    """One line of the album caption. Position maps to the album's reading
    order so the grid is legible without opening a single photo."""
    bits = [f"{position} <b>{h(getattr(item, 'symbol', ''))}</b>"]
    rs = getattr(item, "rs_rating", None)
    if rs is not None:
        bits.append(f"RS {int(rs)}")
    change = getattr(item, "change_pct", None)
    if change is not None:
        bits.append(signed_pct(change))
    row = "  ".join(b for b in bits if b)
    if is_new:
        row += " 🆕"
    also_in = tuple(getattr(item, "also_in", ()) or ())
    if also_in:
        row += f" · also {h(also_in[0])}"
    return row


def album_caption(
    *,
    scanner_label: str,
    total_matches: int,
    new_count: int,
    charted: Sequence[Any],
    new_symbols: Iterable[str],
    overflow_symbols: Sequence[str],
) -> str:
    """The album's ONLY caption -- it goes on item 0.

    A media group with two or more captioned items shows no text at all in the
    timeline (per-item captions appear only in the fullscreen viewer), so all
    scanner-level context has to live here.
    """
    fresh = set(new_symbols)
    headline = f"<b>{h(scanner_label)}</b> — {total_matches} match" + (
        "es" if total_matches != 1 else ""
    )
    if new_count:
        headline += f", {new_count} new"
    if len(charted) < total_matches:
        headline += f", top {len(charted)} charted"

    lines = [headline, ""]
    for position, item in enumerate(charted, start=1):
        lines.append(_item_row(position, item, is_new=getattr(item, "symbol", "") in fresh))

    if overflow_symbols:
        shown = [h(s) for s in overflow_symbols[:40]]
        extra = len(overflow_symbols) - len(shown)
        tail = ", ".join(shown) + (f" +{extra} more" if extra > 0 else "")
        lines.extend(["", f"<i>{len(overflow_symbols)} more:</i> {tail}"])

    return truncate("\n".join(lines), CAPTION_BUDGET)


def overflow_table(scanner_label: str, items: Sequence[Any]) -> str:
    """Monospace table for matches that did not get a chart. ``<code>`` rather
    than ``<pre>`` -- same monospace font, less vertical padding and no copy
    button."""
    header = f"<b>{h(scanner_label)}</b> · {len(items)} more (no chart)"
    rows = [f"{'SYMBOL':<14}{'RS':>4}{'CHG':>9}{'RVOL':>7}"]
    for item in items:
        symbol = str(getattr(item, "symbol", ""))[:13]
        rs = getattr(item, "rs_rating", None)
        change = getattr(item, "change_pct", None)
        rvol = getattr(item, "relative_volume", None)
        rows.append(
            f"{symbol:<14}"
            f"{(str(int(rs)) if rs is not None else '-'):>4}"
            f"{(signed_pct(change) if change is not None else '-'):>9}"
            f"{(f'{rvol:.1f}x' if rvol else '-'):>7}"
        )
    return f"{header}\n<code>{h(chr(10).join(rows))}</code>"


def text_only_block(scanner_label: str, items: Sequence[Any], *, new_symbols: Iterable[str]) -> str:
    """Fallback when charts are unavailable (matplotlib missing, or the global
    image budget is exhausted). The digest still delivers information."""
    fresh = set(new_symbols)
    lines = [f"<b>{h(scanner_label)}</b> — {len(items)} match" + ("es" if len(items) != 1 else "")]
    lines.append("")
    for position, item in enumerate(items, start=1):
        lines.append(_item_row(position, item, is_new=getattr(item, "symbol", "") in fresh))
    return "\n".join(lines)


def closing_summary(
    *,
    confluence: Sequence[str],
    fresh_count: int,
    skipped: Sequence[str],
    photos_sent: int,
    photo_budget: int,
    failed_scanners: Sequence[str],
    app_url: str | None = None,
) -> str:
    lines: list[str] = []
    if confluence:
        lines.append(f"<b>Confluence (3+ scanners):</b> {h(', '.join(confluence[:12]))}")
    lines.append(f"Fresh today: {fresh_count}")
    if skipped:
        # Never silent: a cold-cache miss or a fetch failure must be visible.
        lines.append(f"Skipped (no chart data): {h(', '.join(skipped[:12]))}")
    lines.append(f"Images: {photos_sent} / {photo_budget}")
    if photos_sent >= photo_budget:
        lines.append("<i>Image budget reached — later scanners sent as text.</i>")
    if failed_scanners:
        lines.append(f"⚠ {len(failed_scanners)} scanner(s) failed: {h(', '.join(failed_scanners))}")
    if app_url:
        lines.extend(["", f'Open in app → {h(app_url)}'])
    return "\n".join(lines)
