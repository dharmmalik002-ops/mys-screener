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

import re
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


# ---------------------------------------------------------------- sector
# A sectoral or thematic fund measured against Nifty 500 answers the wrong
# question. A pharma fund that lost 8% in a year when pharma lost 15% did its
# job; against Nifty 500 it just looks bad. These are the Nifty sector indices
# with usable history on the price feed, so a themed fund can be judged against
# the thing it actually invests in.
#
# All are price indices (no dividends), so alpha against them carries the same
# ~1.2%/yr flattery as the broad ones and is flagged the same way.
def _sector(key: str, label: str, symbol: str) -> Benchmark:
    return Benchmark(
        key=key,
        label=label,
        source="yahoo",
        yahoo_symbol=symbol,
        total_return=False,
        notes=f"{label} is a price index — it excludes dividends. The fund is compared "
              f"to its own sector rather than the broad market, which is the only fair "
              f"read on a themed fund.",
    )


NIFTY_IT = _sector("niftyit", "Nifty IT", "^CNXIT")
NIFTY_BANK = _sector("niftybank", "Nifty Bank", "^NSEBANK")
NIFTY_PSU_BANK = _sector("niftypsubank", "Nifty PSU Bank", "^CNXPSUBANK")
NIFTY_PHARMA = _sector("niftypharma", "Nifty Pharma", "^CNXPHARMA")
NIFTY_AUTO = _sector("niftyauto", "Nifty Auto", "^CNXAUTO")
NIFTY_FMCG = _sector("niftyfmcg", "Nifty FMCG", "^CNXFMCG")
NIFTY_METAL = _sector("niftymetal", "Nifty Metal", "^CNXMETAL")
NIFTY_REALTY = _sector("niftyrealty", "Nifty Realty", "^CNXREALTY")
NIFTY_ENERGY = _sector("niftyenergy", "Nifty Energy", "^CNXENERGY")
NIFTY_INFRA = _sector("niftyinfra", "Nifty Infrastructure", "^CNXINFRA")
NIFTY_MEDIA = _sector("niftymedia", "Nifty Media", "^CNXMEDIA")
NIFTY_CONSUMPTION = _sector("niftyconsumption", "Nifty India Consumption", "^CNXCONSUM")
NIFTY_COMMODITIES = _sector("niftycommodities", "Nifty Commodities", "^CNXCMDT")
NIFTY_SERVICES = _sector("niftyservices", "Nifty Services Sector", "^CNXSERVICE")
NIFTY_PSE = _sector("niftypse", "Nifty PSE", "^CNXPSE")
NIFTY_MNC = _sector("niftymnc", "Nifty MNC", "^CNXMNC")

# Matched against the fund's *name*, most specific first — "Banking and PSU"
# must not fall to the plain bank index, and "Pharma and Healthcare" must not
# be caught by a bare "health" rule placed above it. Order is load-bearing.
_THEME_PATTERNS: tuple[tuple[str, Benchmark], ...] = (
    (r"psu\s*bank|public\s*sector\s*bank", NIFTY_PSU_BANK),
    (r"\bit\b|technolog|digital|software", NIFTY_IT),
    (r"pharma|healthcare|health\s*care|medic", NIFTY_PHARMA),
    (r"bank|financial\s*servic|\bbfsi\b|finserv", NIFTY_BANK),
    (r"auto|mobilit|transport", NIFTY_AUTO),
    (r"fmcg|consumer\s*stapl", NIFTY_FMCG),
    (r"metal|mining|steel", NIFTY_METAL),
    (r"realt|real\s*estate|housing", NIFTY_REALTY),
    (r"energ|power|oil|gas|utilit", NIFTY_ENERGY),
    (r"infra|capital\s*good|manufactur|defence|defense", NIFTY_INFRA),
    (r"media|entertain", NIFTY_MEDIA),
    (r"consum|retail|fmcg", NIFTY_CONSUMPTION),
    (r"commodit|natural\s*resourc", NIFTY_COMMODITIES),
    (r"\bpsu\b|public\s*sector|\bcpse\b", NIFTY_PSE),
    (r"\bmnc\b|multinational", NIFTY_MNC),
    (r"servic", NIFTY_SERVICES),
)

# Groww's coarse holdings sectors, used only when the name says nothing.
_HOLDINGS_SECTOR_MAP: dict[str, Benchmark] = {
    "technology": NIFTY_IT,
    "financial": NIFTY_BANK,
    "healthcare": NIFTY_PHARMA,
    "energy & utilities": NIFTY_ENERGY,
    "consumer staples": NIFTY_FMCG,
    "consumer discretionary": NIFTY_CONSUMPTION,
    "materials": NIFTY_COMMODITIES,
    "industrials": NIFTY_INFRA,
    "real estate": NIFTY_REALTY,
    "communication": NIFTY_MEDIA,
}

SECTOR_BENCHMARKS: tuple[Benchmark, ...] = (
    NIFTY_IT, NIFTY_BANK, NIFTY_PSU_BANK, NIFTY_PHARMA, NIFTY_AUTO, NIFTY_FMCG,
    NIFTY_METAL, NIFTY_REALTY, NIFTY_ENERGY, NIFTY_INFRA, NIFTY_MEDIA,
    NIFTY_CONSUMPTION, NIFTY_COMMODITIES, NIFTY_SERVICES, NIFTY_PSE, NIFTY_MNC,
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
        *SECTOR_BENCHMARKS,
    )
}

DEFAULT_BENCHMARK = NIFTY_500


def _normalise(value: str | None) -> str:
    return str(value or "").strip().lower()


def _is_themed(sub_category: str | None) -> bool:
    key = _normalise(sub_category)
    return "thematic" in key or "sectoral" in key or "sector" in key


def resolve_theme(name: str | None, *, dominant_sector: str | None = None) -> Benchmark | None:
    """The sector index a themed fund actually tracks, if we can tell.

    Reads the fund's name first — Indian scheme names state the theme outright
    ("ICICI Prudential Technology Fund") — and falls back to the largest sector
    in the disclosed portfolio. Returns None when neither is conclusive, so the
    caller keeps the broad-market benchmark rather than guessing: a confidently
    wrong sector is a wrong number on screen, which is worse than a broad one.
    """
    haystack = _normalise(name)
    if haystack:
        for pattern, bench in _THEME_PATTERNS:
            if re.search(pattern, haystack):
                return bench
    sector = _normalise(dominant_sector)
    if sector and sector in _HOLDINGS_SECTOR_MAP:
        return _HOLDINGS_SECTOR_MAP[sector]
    return None


def resolve(
    sub_category: str | None,
    *,
    category: str | None = None,
    name: str | None = None,
    dominant_sector: str | None = None,
) -> Benchmark:
    """Best available benchmark for a fund.

    Falls back on substring matching before defaulting, because Groww's
    sub-category strings drift ("Sectoral / Thematic" vs "Thematic") and a
    silent default to Nifty 500 for a small-cap fund would be a wrong number
    on screen, not a missing one.
    """
    # Themed funds resolve to their own sector where identifiable. Checked
    # before the sub-category table, which would otherwise send every one of
    # them to Nifty 500.
    if _is_themed(sub_category):
        themed = resolve_theme(name, dominant_sector=dominant_sector)
        if themed is not None:
            return themed

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
