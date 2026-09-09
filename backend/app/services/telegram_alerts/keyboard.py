"""Inline-keyboard rendering and callback_data codec. Pure -- no httpx, no
Postgres -- because this is the most bug-prone part of the feature and must be
exhaustively unit-testable.

Wire format:  ``s|<action>|<epoch>|<arg>``  (pipe-delimited, positional)

    s|t|be8d1d|27    toggle registry index 27
    s|p|be8d1d|1     show page 1
    s|A|be8d1d|0     select every scanner on the current page
    s|C|be8d1d|0     clear all
    s|D|be8d1d|0     done -- collapse the keyboard
    s|r|be8d1d|27    /run disambiguation pick
    s|n|be8d1d|0     no-op (page indicator / disabled arrow)

Worst case is ~18 bytes against Telegram's 64-byte callback_data cap, so there
is 3.5x headroom even if scanner ids grow. The arg is an INDEX into
``registry.DIGEST_SCANS``, never a scan id: compact, and constant-size. Prefs
are always keyed by scan id -- the index is purely a wire encoding.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.services.telegram_alerts import prefs as prefs_mod
from app.services.telegram_alerts import registry

PREFIX = "s"
MAX_CALLBACK_BYTES = 64

ACTION_TOGGLE = "t"
ACTION_PAGE = "p"
ACTION_ALL = "A"
ACTION_CLEAR = "C"
ACTION_DONE = "D"
ACTION_RUN = "r"
ACTION_NOOP = "n"

_ACTIONS = frozenset(
    {ACTION_TOGGLE, ACTION_PAGE, ACTION_ALL, ACTION_CLEAR, ACTION_DONE, ACTION_RUN, ACTION_NOOP}
)

ON_MARK = "✅"    # white heavy check mark
OFF_MARK = "▫"   # white small square
COLUMNS = 2           # 3 columns truncates labels to uselessness on a phone


class BadCallback(ValueError):
    """Malformed callback_data -- answer politely, never 500."""


class StaleCallback(ValueError):
    """A keyboard from before a registry change. Telegram keeps old messages
    tappable forever, so this must be detected and refused rather than acted on
    against shifted indices."""


@dataclass(frozen=True)
class CallbackAction:
    action: str
    arg: int


def encode(action: str, arg: int = 0, *, epoch: str | None = None) -> str:
    if action not in _ACTIONS:
        raise BadCallback(f"unknown action {action!r}")
    return f"{PREFIX}|{action}|{epoch or registry.EPOCH}|{int(arg)}"


def decode(data: str | None) -> CallbackAction:
    parts = str(data or "").split("|")
    if len(parts) != 4 or parts[0] != PREFIX:
        raise BadCallback(f"unparseable callback_data {data!r}")
    _, action, epoch, raw_arg = parts
    if action not in _ACTIONS:
        raise BadCallback(f"unknown action {action!r}")
    try:
        arg = int(raw_arg)
    except ValueError as exc:
        raise BadCallback(f"non-numeric arg {raw_arg!r}") from exc
    if epoch != registry.EPOCH:
        raise StaleCallback(f"epoch {epoch!r} != {registry.EPOCH!r}")
    if action in (ACTION_TOGGLE, ACTION_RUN):
        if registry.scan_at(arg) is None:
            raise BadCallback(f"index {arg} out of range")
    if action == ACTION_PAGE and not (0 <= arg < registry.PAGE_COUNT):
        raise BadCallback(f"page {arg} out of range")
    return CallbackAction(action=action, arg=arg)


def _button(text: str, action: str, arg: int = 0) -> dict:
    return {"text": text, "callback_data": encode(action, arg)}


def render(chat: dict, page: int = 0) -> dict:
    """Build the reply_markup for ``/scanners`` on ``page``."""
    page = max(0, min(int(page), registry.PAGE_COUNT - 1))
    selected = set(prefs_mod.effective_scan_ids(chat))

    rows: list[list[dict]] = []
    row: list[dict] = []
    for scan in registry.page_scans(page):
        mark = ON_MARK if scan.scan_id in selected else OFF_MARK
        index = registry.index_of(scan.scan_id)
        row.append(_button(f"{mark} {scan.label}", ACTION_TOGGLE, index or 0))
        if len(row) == COLUMNS:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    # Disabled arrows render as no-ops so the row keeps its shape instead of
    # the buttons jumping around between pages.
    prev_button = (
        _button("‹ Prev", ACTION_PAGE, page - 1) if page > 0 else _button(" ", ACTION_NOOP)
    )
    next_button = (
        _button("Next ›", ACTION_PAGE, page + 1)
        if page < registry.PAGE_COUNT - 1
        else _button(" ", ACTION_NOOP)
    )
    rows.append(
        [
            prev_button,
            _button(f"{registry.page_title(page)} {page + 1}/{registry.PAGE_COUNT}", ACTION_NOOP),
            next_button,
        ]
    )
    rows.append(
        [
            _button("All on page", ACTION_ALL, page),
            _button("Clear all", ACTION_CLEAR),
            _button("✔ Done", ACTION_DONE),
        ]
    )
    return {"inline_keyboard": rows}


def render_run_picks(matches) -> dict:
    """Disambiguation keyboard for an ambiguous ``/run <query>``."""
    rows = [
        [_button(scan.label, ACTION_RUN, registry.index_of(scan.scan_id) or 0)]
        for scan in matches[:8]
    ]
    return {"inline_keyboard": rows}
