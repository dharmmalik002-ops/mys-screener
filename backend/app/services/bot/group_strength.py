"""Industry-group strength at entry — the "L" and "I" of buying leaders.

Every signal is tagged with how strong its industry group was on the signal
day: the group's median 63-session return, ranked against every other group
that day (1.0 = strongest). Causal by construction — each day's rank reads
closes up to that day only.

It is the one new entry feature in a long search that earned a place in the
book (CLAUDE.md gotcha 113). Inside the band the bot actually trades, stocks
in the weakest 40% of groups returned roughly half the R of the rest, in both
halves of the history, and as a confidence-score component it beat all ten
matched random-noise controls on the yearly-rebuild test.

**Group definition.** `sub_sector` when the universe holds at least
`MIN_GROUP_SIZE` names in it, otherwise the broader `sector`. Sub-sectors
alone are too small to read (median 4 names), and a median over three stocks
is one stock's news.

**What this does not fix.** Membership is today's classification applied to
all of history, and the universe is today's listed companies (gotcha 31). The
group a stock belongs to is a static fact rather than a performance record,
so this is a much milder bias than survivorship itself — but it is a bias.
"""

from __future__ import annotations

import collections
import json
from pathlib import Path
from typing import Mapping

import numpy as np

MIN_GROUP_SIZE = 8          # below this, fall back to the sector
MIN_LIVE_MEMBERS = 3        # a group needs this many priced names to be read
LOOKBACK = 63               # sessions — the same horizon as the momentum input


def group_map(data_dir: Path) -> dict[str, str]:
    rows = json.loads((Path(data_dir) / "free_universe.json").read_text())
    size = collections.Counter(r.get("sub_sector") for r in rows)
    out: dict[str, str] = {}
    for r in rows:
        g = r.get("sub_sector") if size[r.get("sub_sector")] >= MIN_GROUP_SIZE else r.get("sector")
        if g:
            out[str(r["symbol"])] = str(g)
    return out


def build_ranks(data_dir: Path, symbols=None) -> "GroupRanks":
    """Daily percentile rank of every group. ~1 minute over the full store."""
    import pandas as pd
    from .history import available_symbols, read_bars

    groups = group_map(data_dir)
    series = {}
    for sym in (symbols or available_symbols(data_dir)):
        if sym not in groups:
            continue
        b = read_bars(data_dir, sym)
        if b is None or len(b.close) <= LOOKBACK:
            continue
        c = pd.Series(np.asarray(b.close, float), index=pd.to_datetime(list(b.dates)))
        series[sym] = c / c.shift(LOOKBACK) - 1.0
    frame = pd.DataFrame(series)
    medians = {}
    by_group = collections.defaultdict(list)
    for sym in frame.columns:
        by_group[groups[sym]].append(sym)
    for g, members in by_group.items():
        sub = frame[members]
        m = sub.median(axis=1)
        m[sub.notna().sum(axis=1) < MIN_LIVE_MEMBERS] = np.nan
        medians[g] = m
    pct = pd.DataFrame(medians).rank(axis=1, pct=True)
    table = {g: {d.date(): float(v) for d, v in pct[g].dropna().items()} for g in pct.columns}
    return GroupRanks(groups, table)


class GroupRanks:
    def __init__(self, groups: Mapping[str, str], table: Mapping[str, Mapping]):
        self.groups = dict(groups)
        self.table = {g: dict(v) for g, v in table.items()}

    def rank(self, symbol: str, day) -> float | None:
        """1.0 = the strongest group that day; None when it cannot be read."""
        from datetime import date as _date
        if isinstance(day, str):
            day = _date.fromisoformat(day)
        g = self.groups.get(str(symbol))
        return self.table.get(g, {}).get(day) if g else None
