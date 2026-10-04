"""Turn a library of newsletter charts into (ticker, date) references.

The newsletter names every chart `TICKER-MM-DD-YY.gif` (for example
`NVDA-08-12-26.gif` is NVDA on 12 Aug 2026), so the filename alone carries all
we need and the image itself is never opened. Three inputs are accepted: a
folder of image files, saved newsletter HTML pages (the chart filenames are read
out of the page), and plain text with one filename or `TICKER,YYYY-MM-DD` per
line.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

IMAGE_EXTENSIONS = {".gif", ".png", ".jpg", ".jpeg", ".webp"}

# TICKER-MM-DD-YY, optionally followed by a suffix like "-2" or "_a" before the
# extension. Tickers may carry a dot or dash class suffix (BRK.B, BF-B), which
# is why the date is anchored to the END of the stem.
_FILENAME = re.compile(
    r"^(?P<ticker>[A-Za-z][A-Za-z0-9.\-]{0,11}?)-(?P<m>\d{1,2})-(?P<d>\d{1,2})-(?P<y>\d{2}|\d{4})(?:[-_][A-Za-z0-9]{1,4})?$"
)
_IN_HTML = re.compile(r"([A-Za-z][A-Za-z0-9.\-]{0,11}-\d{1,2}-\d{1,2}-\d{2,4})\.(?:gif|png|jpe?g|webp)", re.I)
_CSV_LINE = re.compile(r"^\s*(?P<ticker>[A-Za-z][A-Za-z0-9.\-]{0,11})\s*[,;\t ]\s*(?P<iso>\d{4}-\d{2}-\d{2})\s*$")


@dataclass(frozen=True)
class Reference:
    ticker: str
    day: date
    source: str
    # Whose setups these are ("minervini", "zanger"). Each style is learned on
    # its own: one trader's charts are not evidence about another's.
    style: str = ""

    @property
    def key(self) -> str:
        return f"{self.ticker}@{self.day.isoformat()}"


def parse_filename(name: str) -> tuple[str, date] | None:
    """`NVDA-08-12-26.gif` -> ("NVDA", 2026-08-12). None when it does not parse
    or the date is impossible, never a guess."""
    stem = Path(name).name
    for ext in IMAGE_EXTENSIONS:
        if stem.lower().endswith(ext):
            stem = stem[: -len(ext)]
            break
    match = _FILENAME.match(stem)
    if not match:
        return None
    year = int(match["y"])
    if year < 100:
        # Two-digit years: the newsletter has run since the 1990s, so 90-99 are
        # the 1900s and everything else the 2000s.
        year += 1900 if year >= 90 else 2000
    try:
        day = date(year, int(match["m"]), int(match["d"]))
    except ValueError:
        return None
    return match["ticker"].upper(), day


def _from_text(text: str, source: str) -> list[Reference]:
    found: list[Reference] = []
    for line in text.splitlines():
        csv = _CSV_LINE.match(line)
        if csv:
            try:
                found.append(Reference(csv["ticker"].upper(), date.fromisoformat(csv["iso"]), source))
            except ValueError:
                pass
            continue
        parsed = parse_filename(line.strip())
        if parsed:
            found.append(Reference(parsed[0], parsed[1], source))
    return found


def _from_html(text: str, source: str) -> list[Reference]:
    found: list[Reference] = []
    for stem in _IN_HTML.findall(text):
        parsed = parse_filename(stem)
        if parsed:
            found.append(Reference(parsed[0], parsed[1], source))
    return found


def collect(paths: Iterable[Path]) -> tuple[list[Reference], list[str]]:
    """Every reference found under `paths`, de-duplicated on (ticker, date), plus
    the names that looked like charts but did not parse — reported so a folder
    with an unexpected naming scheme fails loudly instead of shrinking quietly."""
    refs: dict[str, Reference] = {}
    unparsed: list[str] = []

    def add(items: list[Reference]) -> None:
        for ref in items:
            refs.setdefault(ref.key, ref)

    for root in paths:
        files = sorted(p for p in root.rglob("*") if p.is_file()) if root.is_dir() else [root]
        for path in files:
            suffix = path.suffix.lower()
            if suffix in IMAGE_EXTENSIONS:
                parsed = parse_filename(path.name)
                if parsed:
                    add([Reference(parsed[0], parsed[1], path.name)])
                else:
                    unparsed.append(path.name)
            elif suffix in {".html", ".htm", ".cfm"}:
                add(_from_html(path.read_text(errors="ignore"), path.name))
            elif suffix in {".txt", ".csv"}:
                add(_from_text(path.read_text(errors="ignore"), path.name))
    return sorted(refs.values(), key=lambda r: (r.day, r.ticker)), unparsed


def from_html_text(text: str, source: str) -> list[Reference]:
    """For a newsletter page fetched over the network rather than saved to disk."""
    out: dict[str, Reference] = {}
    for ref in _from_html(text, source):
        out.setdefault(ref.key, ref)
    return sorted(out.values(), key=lambda r: (r.day, r.ticker))


SETUP_NAMES = {
    "cup_handle": "Cup & handle", "flag": "Flag & pennant", "triangle": "Triangle", "wedge": "Wedge",
    "channel": "Channel", "head_shoulders": "Head & shoulders", "double_bottom": "Double bottom",
    "base": "Base", "trendline": "Trendline break", "gap": "Gap",
}


def style_name(style: str) -> str:
    """`zanger_cup_handle` -> "Zanger · Cup & handle"; `minervini` -> "Minervini"."""
    trader, _, setup = style.partition("_")
    name = trader.title()
    return f"{name} · {SETUP_NAMES.get(setup, setup.replace('_', ' ').capitalize())}" if setup else name


REF_SHARDS = 64


def ref_shard(key: str) -> str:
    """Which `lookalike_refs/<shard>.json` holds a public reference row. Tens of
    thousands of before-and-after charts are too many for one file the Space
    reloads on every change, so they are spread over a fixed set of files."""
    import zlib

    return f"{zlib.crc32(key.encode()) % REF_SHARDS:02x}"


SOURCES_DIR = "sources"


def sources_root(data_dir: Path) -> Path:
    return data_dir / "lookalike" / SOURCES_DIR


def collect_sources(data_dir: Path) -> tuple[list[Reference], list[str]]:
    """Every saved reference list under `data/lookalike/sources/<style>/`, the
    folder name being the style. The library is always rebuilt from all of
    them, so adding one trader's charts can never silently drop another's."""
    root = sources_root(data_dir)
    refs: list[Reference] = []
    unparsed: list[str] = []
    if not root.exists():
        return refs, unparsed
    for style_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        found, bad = collect([style_dir])
        refs.extend(Reference(r.ticker, r.day, r.source, style_dir.name) for r in found)
        unparsed.extend(bad)
    return refs, unparsed


def save_source(data_dir: Path, style: str, name: str, refs: list[Reference]) -> Path:
    """Store a reference list as plain `TICKER,YYYY-MM-DD` lines. Only the names
    are kept — never the images."""
    safe_style = re.sub(r"[^a-z0-9_-]", "", style.lower()) or "default"
    safe_name = re.sub(r"[^A-Za-z0-9_.-]", "_", name)[:80] or "list"
    path = sources_root(data_dir) / safe_style / f"{safe_name}.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{r.ticker},{r.day.isoformat()}\n" for r in refs))
    return path
