"""The index and macro series the regime engine reads.

Two groups, and the distinction matters for how they are used:

*Domestic* series describe the tape the bot actually trades — the Nifty for
trend, the mid and small cap indices for where risk appetite sits, India VIX
for how violent the sessions are. These drive the regime label directly.

*External* series describe the weather blowing in: the dollar, crude, US rates,
US equities. They do not classify the regime on their own — an Indian uptrend
is an Indian uptrend whatever the dollar is doing — but they load the dice, and
the engine measures whether they do rather than assuming it (see
`macro.py::measure_macro_edge`). Every one of them is a *headwind* direction
written down in advance, so the study cannot quietly relabel a series as
bullish after seeing the returns.

`ticker` is the Yahoo symbol; `key` is the stable internal name used in the
store and in every payload, so a vendor symbol change touches this file only.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContextSeries:
    key: str
    ticker: str
    label: str
    group: str            # "domestic" | "external"
    # Sign of the series' move that historically pressures Indian equities.
    # -1: falls are the headwind (Nifty, S&P). +1: rises are the headwind
    # (VIX, crude, dollar, US yields). Declared here, before any measurement.
    headwind_when: int
    note: str


CONTEXT_SERIES: tuple[ContextSeries, ...] = (
    # --- Domestic: these define the regime -------------------------------
    ContextSeries("NIFTY", "^NSEI", "Nifty 50", "domestic", -1,
                  "The trend reference. Every regime label starts here."),
    ContextSeries("NIFTY500", "^CRSLDX", "Nifty 500", "domestic", -1,
                  "Broad market — the universe the bot actually picks from."),
    ContextSeries("MIDCAP", "^NSEMDCP50", "Nifty Midcap 50", "domestic", -1,
                  "Risk appetite. Mid caps lead on the way up and break first."),
    ContextSeries("MIDCAP100", "^NSMIDCP", "Nifty Midcap 100", "domestic", -1,
                  "Broader mid-cap read; confirms or contradicts Midcap 50."),
    ContextSeries("BANKNIFTY", "^NSEBANK", "Bank Nifty", "domestic", -1,
                  "Domestic credit and liquidity conditions in one price."),
    ContextSeries("INDIAVIX", "^INDIAVIX", "India VIX", "domestic", +1,
                  "How violent sessions are. High VIX widens stops and kills breakouts."),

    # No small-cap index here on purpose: Yahoo serves ^CNXSC an empty frame
    # (the same symbol the mutual-fund benchmarks already route around). Risk
    # appetite is measured instead from the universe's own bars in
    # `breadth.py` — percentage above the 50 and 200 DMA, new highs against new
    # lows — which is a finer read than a 100-name index and comes from exactly
    # the stocks the bot trades.

    # --- External: measured, never assumed --------------------------------
    ContextSeries("SP500", "^GSPC", "S&P 500", "external", -1,
                  "Global risk appetite."),
    ContextSeries("NASDAQ", "^IXIC", "Nasdaq Composite", "external", -1,
                  "Global growth/tech bid; leads Indian IT."),
    ContextSeries("VIX", "^VIX", "CBOE VIX", "external", +1,
                  "Global fear. Spikes transmit to Indian equities within days."),
    ContextSeries("DXY", "DX-Y.NYB", "US Dollar Index", "external", +1,
                  "A strong dollar pulls foreign money out of emerging markets."),
    ContextSeries("USDINR", "USDINR=X", "USD/INR", "external", +1,
                  "Rupee weakness is FII selling made visible."),
    ContextSeries("US10Y", "^TNX", "US 10-Year Yield", "external", +1,
                  "The discount rate for every risk asset on earth."),
    ContextSeries("CRUDE", "BZ=F", "Brent Crude", "external", +1,
                  "India imports ~85% of its oil; crude is an inflation tax."),
    ContextSeries("GOLD", "GC=F", "Gold", "external", +1,
                  "Defensive rotation shows up here first."),
    ContextSeries("COPPER", "HG=F", "Copper", "external", -1,
                  "Global industrial demand; falls with a slowing cycle."),
    ContextSeries("EM", "EEM", "MSCI Emerging Markets ETF", "external", -1,
                  "The flow India competes with for the same allocation."),
)

BY_KEY = {s.key: s for s in CONTEXT_SERIES}

# The trend reference. Hard-coded rather than picked at runtime: the regime
# label must mean the same thing across every run and every stored artifact.
BENCHMARK_KEY = "NIFTY"
BREADTH_UNIVERSE_KEY = "NIFTY500"


def domestic() -> tuple[ContextSeries, ...]:
    return tuple(s for s in CONTEXT_SERIES if s.group == "domestic")


def external() -> tuple[ContextSeries, ...]:
    return tuple(s for s in CONTEXT_SERIES if s.group == "external")
