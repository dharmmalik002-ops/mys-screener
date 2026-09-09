"""Preference schema -- pure functions, no I/O, no psycopg.

Every read goes through :func:`normalize_prefs`, which is also the migration
seam: it fills defaults, coerces types and clamps ranges, so a hand-edited or
half-written blob can never crash the digest.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from app.services.telegram_alerts import registry

SCHEMA_VERSION = 1

# 10 is the hard sendMediaGroup album ceiling, so a cap above it is meaningless.
MIN_CHART_CAP, MAX_CHART_CAP = 1, 10
# 15 is ScanHistoryStore's default retention.
MIN_DEDUPE_SESSIONS, MAX_DEDUPE_SESSIONS = 1, 15

DEFAULT_CHART_CAP = 8
DEFAULT_DEDUPE_SESSIONS = 5

# "flag_only" shows every hit and marks the fresh ones; "new_only" sends only
# newcomers plus a repeat count. flag_only is the safer default while the
# dedupe window is still being calibrated.
REPEAT_POLICIES = ("flag_only", "new_only")
DEFAULT_REPEAT_POLICY = "flag_only"


def _clamp_int(value: Any, low: int, high: int, fallback: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return fallback
    return max(low, min(high, parsed))


def empty_prefs() -> dict:
    return {"version": SCHEMA_VERSION, "last_update_id": 0, "chats": {}}


def normalize_prefs(raw: Any) -> dict:
    """Coerce any stored payload into the current schema. Never raises."""
    if not isinstance(raw, dict):
        return empty_prefs()

    chats_raw = raw.get("chats")
    chats: dict[str, dict] = {}
    if isinstance(chats_raw, dict):
        for key, value in chats_raw.items():
            try:
                chat_key = str(int(str(key).strip()))
            except (TypeError, ValueError):
                continue
            chats[chat_key] = _normalize_chat(value)

    return {
        "version": SCHEMA_VERSION,
        "last_update_id": _clamp_int(raw.get("last_update_id"), 0, 2**63 - 1, 0),
        "chats": chats,
    }


def _normalize_chat(raw: Any) -> dict:
    raw = raw if isinstance(raw, dict) else {}

    enabled_raw = raw.get("enabled_scans")
    enabled: list[str] = []
    if isinstance(enabled_raw, (list, tuple)):
        for scan_id in enabled_raw:
            text = str(scan_id or "").strip()
            # Unknown ids are KEPT in storage (see effective_scan_ids) so a
            # scanner temporarily hidden and later restored keeps the choice.
            if text and text not in enabled:
                enabled.append(text)

    caps_raw = raw.get("chart_caps")
    caps: dict[str, int] = {}
    if isinstance(caps_raw, dict):
        for scan_id, value in caps_raw.items():
            text = str(scan_id or "").strip()
            if text:
                caps[text] = _clamp_int(value, MIN_CHART_CAP, MAX_CHART_CAP, DEFAULT_CHART_CAP)

    policy = str(raw.get("repeat_policy") or DEFAULT_REPEAT_POLICY)
    if policy not in REPEAT_POLICIES:
        policy = DEFAULT_REPEAT_POLICY

    menu_raw = raw.get("menu") if isinstance(raw.get("menu"), dict) else {}

    return {
        "configured": bool(raw.get("configured", False)),
        "enabled_scans": enabled,
        "default_chart_cap": _clamp_int(
            raw.get("default_chart_cap"), MIN_CHART_CAP, MAX_CHART_CAP, DEFAULT_CHART_CAP
        ),
        "chart_caps": caps,
        "charts_enabled": bool(raw.get("charts_enabled", True)),
        "dedupe_sessions": _clamp_int(
            raw.get("dedupe_sessions"),
            MIN_DEDUPE_SESSIONS,
            MAX_DEDUPE_SESSIONS,
            DEFAULT_DEDUPE_SESSIONS,
        ),
        "repeat_policy": policy,
        "muted_until": _normalize_date(raw.get("muted_until")),
        "menu": {
            "epoch": str(menu_raw.get("epoch") or ""),
            "page": _clamp_int(menu_raw.get("page"), 0, max(registry.PAGE_COUNT - 1, 0), 0),
            "message_id": _clamp_int(menu_raw.get("message_id"), 0, 2**63 - 1, 0),
        },
    }


def _normalize_date(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def chat_prefs(prefs: dict, chat_id: int | str) -> dict:
    """Return (creating if absent) the mutable per-chat block."""
    key = str(int(chat_id))
    chats = prefs.setdefault("chats", {})
    if key not in chats:
        chats[key] = _normalize_chat({})
    return chats[key]


def effective_scan_ids(chat: dict) -> list[str]:
    """The scanners to actually run, in registry order.

    Unknown ids are filtered here rather than deleted from storage -- silent
    deletion is unrecoverable, silent filtering is not. ``configured`` is what
    distinguishes "never set up" (use defaults) from "deliberately cleared"
    (send the header only).
    """
    if not chat.get("configured"):
        selected = set(registry.DEFAULT_ENABLED)
    else:
        selected = set(chat.get("enabled_scans") or ())
    return [scan_id for scan_id in registry.DIGEST_SCAN_IDS if scan_id in selected]


def unknown_scan_ids(chat: dict) -> list[str]:
    """Saved ids the registry no longer offers -- surfaced by /status so the
    state is never invisible."""
    return [
        scan_id
        for scan_id in (chat.get("enabled_scans") or ())
        if not registry.is_known(scan_id)
    ]


def is_selected(chat: dict, scan_id: str) -> bool:
    return scan_id in set(effective_scan_ids(chat))


def toggle_scan(chat: dict, scan_id: str) -> bool:
    """Flip one scanner. Returns its new state.

    The first explicit edit materializes the effective set and marks the chat
    ``configured``, so toggling one scanner off a default set does not silently
    enable everything else.
    """
    current = effective_scan_ids(chat)
    if scan_id in current:
        current.remove(scan_id)
        now_on = False
    else:
        current.append(scan_id)
        now_on = True
    _commit(chat, current)
    return now_on


def set_page_scans(chat: dict, page: int, *, on: bool) -> list[str]:
    current = set(effective_scan_ids(chat))
    page_ids = [scan.scan_id for scan in registry.page_scans(page)]
    for scan_id in page_ids:
        if on:
            current.add(scan_id)
        else:
            current.discard(scan_id)
    _commit(chat, list(current))
    return page_ids


def clear_all(chat: dict) -> None:
    _commit(chat, [])


def _commit(chat: dict, scan_ids: list[str]) -> None:
    """Persist a selection in registry order. An explicit edit is the right
    moment to garbage-collect ids the registry no longer offers."""
    keep = set(scan_ids)
    chat["enabled_scans"] = [s for s in registry.DIGEST_SCAN_IDS if s in keep]
    chat["configured"] = True


def chart_cap(chat: dict, scan_id: str) -> int:
    caps = chat.get("chart_caps") or {}
    if scan_id in caps:
        return _clamp_int(caps[scan_id], MIN_CHART_CAP, MAX_CHART_CAP, DEFAULT_CHART_CAP)
    return _clamp_int(
        chat.get("default_chart_cap"), MIN_CHART_CAP, MAX_CHART_CAP, DEFAULT_CHART_CAP
    )


def dedupe_sessions(chat: dict) -> int:
    return _clamp_int(
        chat.get("dedupe_sessions"),
        MIN_DEDUPE_SESSIONS,
        MAX_DEDUPE_SESSIONS,
        DEFAULT_DEDUPE_SESSIONS,
    )


def is_muted(chat: dict, today: date) -> bool:
    """``muted_until`` is inclusive."""
    until = _normalize_date(chat.get("muted_until"))
    if not until:
        return False
    return today <= date.fromisoformat(until)


def set_mute_until(chat: dict, until: date | None) -> None:
    chat["muted_until"] = until.isoformat() if until else None


def remember_menu(chat: dict, *, page: int, message_id: int, epoch: str) -> None:
    chat["menu"] = {"epoch": epoch, "page": int(page), "message_id": int(message_id)}
