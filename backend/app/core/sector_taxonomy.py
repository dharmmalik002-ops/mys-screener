"""The app's single sector vocabulary.

Two vendors label the universe and they do not agree. NSE/BSE metadata returns
NSE macro-sector names ("Oil Gas & Consumable Fuels", "Realty", "Capital
Goods"); Yahoo returns GICS-style names ("Energy", "Real Estate",
"Industrials"). Whichever reaches a row last used to win, so the same real
sector appeared under two names and every consumer that grouped by sector --
market map, breadth, sector returns -- silently split it in two.

This module owns the mapping. `normalize_sector` is applied by a validator on
`StockSnapshot.sector`, which is the choke point every row passes through, so
no assembly path can bypass it. That matters: `free.py` builds sector labels in
seven different places, and patching them one by one left 40 stocks (of 1,271)
still on GICS labels in production -- the paths I had not found.
"""

from __future__ import annotations

# The NSE macro sectors the universe actually uses. Anything outside this set is
# a label we have invented.
NSE_MACRO_SECTORS: frozenset[str] = frozenset({
    "Automobile and Auto Components",
    "Capital Goods",
    "Chemicals",
    "Construction",
    "Construction Materials",
    "Consumer Durables",
    "Consumer Services",
    "Diversified",
    "Fast Moving Consumer Goods",
    "Financial Services",
    "Forest Materials",
    "Healthcare",
    "Information Technology",
    "Media Entertainment & Publication",
    "Metals & Mining",
    "Oil Gas & Consumable Fuels",
    "Power",
    "Realty",
    "Services",
    "Telecommunication",
    "Textiles",
    "Unclassified",
})

# GICS / Yahoo name -> NSE macro sector.
#
# Two of these are approximations and deliberately so. Yahoo's "Basic
# Materials" spans NSE's Metals & Mining, Chemicals, Construction Materials and
# Forest Materials, and "Consumer Cyclical" spans Consumer Durables, Consumer
# Services and Retail. A single coarse label cannot preserve that; the precise
# classification already lives in the industry group (`final_group_name`),
# which is what the Groups page and the market map group by. This label only
# has to stop inventing sectors that do not exist.
SECTOR_ALIASES: dict[str, str] = {
    # Unambiguous one-to-one landings.
    "Technology": "Information Technology",
    "Communication Services": "Telecommunication",
    "Energy": "Oil Gas & Consumable Fuels",
    "Utilities": "Power",
    "Real Estate": "Realty",
    "Industrials": "Capital Goods",
    "Consumer Defensive": "Fast Moving Consumer Goods",
    "Consumer Staples": "Fast Moving Consumer Goods",
    # Approximations -- see the note above.
    "Basic Materials": "Metals & Mining",
    "Materials": "Metals & Mining",
    "Consumer Cyclical": "Consumer Durables",
    "Consumer Discretionary": "Consumer Durables",
}

# Case- and spacing-insensitive lookup, so "real estate" and "Real  Estate"
# both land. Vendor payloads are not consistent about either.
_ALIAS_LOOKUP: dict[str, str] = {
    " ".join(key.lower().split()): value for key, value in SECTOR_ALIASES.items()
}
_CANONICAL_LOOKUP: dict[str, str] = {
    " ".join(name.lower().split()): name for name in NSE_MACRO_SECTORS
}


def normalize_sector(value: object) -> str:
    """Map any vendor sector label onto the app's single vocabulary.

    Idempotent, and safe on None/blank/non-string input (returns
    "Unclassified"). An unrecognised label is returned trimmed rather than
    forced to "Unclassified" -- a new NSE sector should show up as itself, not
    disappear.
    """
    if not isinstance(value, str):
        return "Unclassified"
    label = " ".join(value.split())
    if not label:
        return "Unclassified"
    key = label.lower()
    aliased = _ALIAS_LOOKUP.get(key)
    if aliased:
        return aliased
    canonical = _CANONICAL_LOOKUP.get(key)
    if canonical:
        return canonical
    return label
