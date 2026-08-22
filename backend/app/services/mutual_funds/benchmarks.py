"""Which index each fund category is measured against, and where to get it.

SEBI makes every scheme name a benchmark, and Groww reports it verbatim
(`benchmark_name`), but those names are TRI indices whose series are not
freely available. So each SEBI sub-category is mapped to the closest index we
can actually fetch, from one of two sources:

* ``mf`` — the NAV of a large passive index fund tracking that index, pulled
  from the same AMFI feed as the fund itself. This is the *preferred* source:
  it is total-return (dividends included, which a price index is not), and it
  shares the fund's exact trading calendar so the comparison needs no
  interpolation. Its cost is a ~0.2% p.a. tracking drag and a start date no
  earlier than the index fund's launch.
* ``yahoo`` — a price index via the provider already wired into this app.
  Longer history, but excludes dividends, so it understates the index by
  roughly 1.2% a year on large caps. Used where no index fund goes back far
  enough, and always labelled as a price index in the UI.

Every fund also gets compared to its own category average, which is computed
from our own universe and needs no external source at all. For hybrid
categories, whose official CRISIL benchmarks have no free series, the category
average *is* the primary comparison and the index line is explicitly a
reference only.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Benchmark:
    key: str
    label: str
    source: str  # "mf" | "yahoo"
    scheme_code: str | None = None
    yahoo_symbol: str | None = None
    # Longer-history stand-in when the primary is an index fund that launched
    # recently and the user asks for a 10-year chart.
    fallback_yahoo_symbol: str | None = None
    fallback_label: str | None = None
    total_return: bool = True
    # True where the index is a stand-in rather than the scheme's actual SEBI
    # benchmark, so the UI can say so instead of implying an official compare.
    is_reference_only: bool = False
    notes: str = ""


NIFTY_50 = Benchmark(
    key="nifty50",
    label="Nifty 50",
    source="yahoo",
    yahoo_symbol="^NSEI",
    total_return=False,
    notes="Price index — excludes dividends, so it understates the index by roughly 1.2% a year.",
)
NIFTY_100 = Benchmark(
    key="nifty100",
    label="Nifty 100",
    source="yahoo",
    yahoo_symbol="^CNX100",
    total_return=False,
    notes="Price index — excludes dividends.",
)
NIFTY_500 = Benchmark(
    key="nifty500",
    label="Nifty 500",
    source="yahoo",
    yahoo_symbol="^CRSLDX",
    total_return=False,
    notes="Price index — excludes dividends.",
)
NIFTY_NEXT_50 = Benchmark(
    key="niftynext50",
    label="Nifty Next 50",
    source="yahoo",
    yahoo_symbol="^NSMIDCP",
    total_return=False,
    notes="Price index — excludes dividends.",
)
# Yahoo's ^CNXSC (Nifty Smallcap 100) returns a single bar with no history, so
# small caps — the category where the benchmark question matters most — have to
# come from an index fund's NAV.
NIFTY_SMALLCAP_250 = Benchmark(
    key="niftysmallcap250",
    label="Nifty Smallcap 250 (index fund NAV)",
    source="mf",
    scheme_code="148519",
    notes="Tracked via Nippon India Nifty Smallcap 250 Index Fund (Direct, Growth) — "
          "total-return, net of ~0.3% tracking cost. History starts Oct 2020; "
          "Yahoo's Nifty Smallcap price series has no usable history.",
)
NIFTY_MIDCAP_150 = Benchmark(
    key="niftymidcap150",
    label="Nifty Midcap 150 (index fund NAV)",
    source="mf",
    scheme_code="148726",
    fallback_yahoo_symbol="^NSEMDCP50",
    fallback_label="Nifty Midcap 50 (price)",
    notes="Tracked via Nippon India Nifty Midcap 150 Index Fund (Direct, Growth). "
          "History starts Feb 2021; longer ranges fall back to the Nifty Midcap 50 price index.",
)
NIFTY_LARGEMIDCAP_250 = Benchmark(
    key="niftylargemidcap250",
    label="Nifty LargeMidcap 250 (index fund NAV)",
    source="mf",
    scheme_code="149343",
    fallback_yahoo_symbol="^CNX100",
    fallback_label="Nifty 100 (price)",
    notes="Tracked via Edelweiss Nifty Large Midcap 250 Index Fund (Direct, Growth).",
)

# Hybrid schemes are benchmarked to CRISIL blended indices, which have no free
# series. Nifty 50 is shown as a rough equity reference and flagged as such —
# a 65/35 hybrid is not expected to track it.
HYBRID_REFERENCE = Benchmark(
    key="nifty50",
    label="Nifty 50 (reference)",
    source="yahoo",
    yahoo_symbol="^NSEI",
    total_return=False,
    is_reference_only=True,
    notes="This scheme's official benchmark is a CRISIL blended index with no free data series. "
          "Nifty 50 is shown as an equity reference only — a hybrid fund holds debt too and is "
          "not meant to track it. Compare against the category average instead.",
)

# Keys are lowercased Groww `sub_category` values.
_BY_SUB_CATEGORY: dict[str, Benchmark] = {
    # Equity
    "large cap": NIFTY_100,
    "mid cap": NIFTY_MIDCAP_150,
    "small cap": NIFTY_SMALLCAP_250,
    "large & midcap": NIFTY_LARGEMIDCAP_250,
    "large and mid cap": NIFTY_LARGEMIDCAP_250,
    "multi cap": NIFTY_500,
    "flexi cap": NIFTY_500,
    "value": NIFTY_500,
    "contra": NIFTY_500,
    "value oriented": NIFTY_500,
    "focused": NIFTY_500,
    "dividend yield": NIFTY_500,
    "elss": NIFTY_500,
    "sectoral / thematic": NIFTY_500,
    "sectoral/thematic": NIFTY_500,
    "thematic": NIFTY_500,
    "sectoral": NIFTY_500,
    "index funds": NIFTY_50,
    "index": NIFTY_50,
    "etf": NIFTY_50,
    "fofs (overseas)": NIFTY_500,
    "fof": NIFTY_500,
    # Hybrid — no free official series
    "aggressive hybrid": HYBRID_REFERENCE,
    "balanced hybrid": HYBRID_REFERENCE,
    "conservative hybrid": HYBRID_REFERENCE,
    "dynamic asset allocation": HYBRID_REFERENCE,
    "balanced advantage": HYBRID_REFERENCE,
    "multi asset allocation": HYBRID_REFERENCE,
    "equity savings": HYBRID_REFERENCE,
    "arbitrage": HYBRID_REFERENCE,
    "retirement": HYBRID_REFERENCE,
    "children's": HYBRID_REFERENCE,
    "childrens fund": HYBRID_REFERENCE,
}

ALL_BENCHMARKS: dict[str, Benchmark] = {
    bench.key: bench
    for bench in (
        NIFTY_50,
        NIFTY_100,
        NIFTY_500,
        NIFTY_NEXT_50,
        NIFTY_SMALLCAP_250,
        NIFTY_MIDCAP_150,
        NIFTY_LARGEMIDCAP_250,
    )
}

DEFAULT_BENCHMARK = NIFTY_500


def _normalise(value: str | None) -> str:
    return str(value or "").strip().lower()


def resolve(sub_category: str | None, *, category: str | None = None) -> Benchmark:
    """Best available benchmark for a SEBI sub-category.

    Falls back on substring matching before defaulting, because Groww's
    sub-category strings drift ("Sectoral / Thematic" vs "Thematic") and a
    silent default to Nifty 500 for a small-cap fund would be a wrong number
    on screen, not a missing one.
    """
    key = _normalise(sub_category)
    if key in _BY_SUB_CATEGORY:
        return _BY_SUB_CATEGORY[key]

    if "small" in key:
        return NIFTY_SMALLCAP_250
    if "mid" in key and "large" in key:
        return NIFTY_LARGEMIDCAP_250
    if "mid" in key:
        return NIFTY_MIDCAP_150
    if "large" in key:
        return NIFTY_100
    if "hybrid" in key or "arbitrage" in key or "asset allocation" in key or "savings" in key:
        return HYBRID_REFERENCE
    if _normalise(category) == "hybrid":
        return HYBRID_REFERENCE
    return DEFAULT_BENCHMARK
