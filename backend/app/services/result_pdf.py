"""Read a quarter's figures from the results PDF a company files on BSE.

Why: BSE publishes the structured figures (`Corp_detailedResult_Transpose_ng`)
and NSE the XBRL some hours after the PDF — sometimes the next day — so a
result declared at 5 PM (HCLTECH, 2026-10-12) sat as "awaiting figures" while
its numbers were already public in the filing.

How it stays honest: a results statement prints three quarter columns side by
side — this quarter, the previous quarter and the same quarter a year ago (the
SEBI format). Every figure is accepted only when the PDF's previous-quarter and
year-ago columns on the same line MATCH the values already stored for those
quarters from the exchanges' own data. That check also tells which table is
consolidated and which standalone, and the unit (crore, lakh, million or
rupees). A line that does not match is ignored, and a PDF with no line that
matches yields nothing — the card keeps waiting for the exchange's figures,
which replace these as soon as they arrive. Nothing here estimates a number.
"""

from __future__ import annotations

import io
import re
from typing import Any

# Values in the PDF divided by this give crore.
SCALES = (1.0, 100.0, 10.0, 1e7, 1e5)
# Printed figures are rounded to the statement's unit: half a unit, in crore,
# is the most two honest copies of one number can differ (0.5 crore for a
# statement in crore, 0.005 for one in lakh), plus 0.1% for restatement noise.
REL_TOLERANCE = 0.001
ABS_TOLERANCE_EPS = 0.011


def _rounding(scale: float) -> float:
    return 0.51 / scale if scale >= 1 else 0.51


LABELS: dict[str, tuple[str, ...]] = {
    "sales": ("revenue from operations", "income from operations", "interest earned", "net sales",
              "total revenue from operations"),
    "profit_before_tax": ("profit before tax", "profit/(loss) before tax", "profit / (loss) before tax",
                          "profit (loss) before tax", "profit before exceptional items and tax",
                          "profit from ordinary activities before tax"),
    "net_profit": ("attributable to owners", "attributable to the owners", "attributable to equity holders",
                   "attributable to shareholders", "net profit", "profit for the period", "profit for the quarter",
                   "profit/(loss) for the period", "profit / (loss) for the period", "profit (loss) for the period",
                   "profit after tax"),
    "eps": ("basic",),
    "expenses": ("total expenses", "total expenditure"),
    "depreciation": ("depreciation",),
    "finance": ("finance cost", "finance costs", "interest expended"),
    "bank_operating": ("operating profit before provision",),
}
MONETARY = ("sales", "profit_before_tax", "net_profit", "expenses", "depreciation", "finance", "bank_operating")

_NUMBER = re.compile(r"\(?-?\d[\d,]*(?:\.\d+)?\)?")
_NOTE = re.compile(r"\(?\s*(?:refer\s+)?note\s*(?:no\.?)?\s*\d+[a-z]?\s*\)?", re.I)


def extract_text(pdf_bytes: bytes) -> str:
    """The PDF's text, or "" (a scanned PDF has none: it is simply not read)."""
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(pdf_bytes))
        return "\n".join((page.extract_text() or "") for page in reader.pages[:12])
    except Exception:  # noqa: BLE001 - an unreadable PDF reads as nothing
        return ""


def _numbers(text: str) -> list[float]:
    out: list[float] = []
    for token in _NUMBER.findall(_NOTE.sub(" ", text)):
        negative = token.startswith("(") and token.endswith(")") or token.startswith("-") or token.startswith("(-")
        digits = token.strip("()").lstrip("-").replace(",", "")
        if not digits or digits.count(".") > 1:
            continue
        try:
            value = float(digits)
        except ValueError:
            continue
        out.append(-value if negative else value)
    return out


def _candidate_lines(lines: list[str], metric: str) -> list[list[float]]:
    """Numbers following each line that carries one of the metric's labels
    (the next line's too, when the label line holds fewer than three)."""
    found: list[list[float]] = []
    for index, raw in enumerate(lines):
        low = re.sub(r"\s+", " ", raw.lower())
        for label in LABELS[metric]:
            at = low.find(label)
            if at < 0:
                continue
            if metric == "eps" and not any(word in low for word in ("basic", "eps", "earning")):
                continue
            nums = _numbers(raw[at + len(label):])
            if len(nums) < 3 and index + 1 < len(lines):
                nums += _numbers(lines[index + 1])
            if len(nums) >= 3:
                found.append(nums)
            break
    return found


def _error(value: float, stored: float | None, eps: bool, scale: float) -> float | None:
    """How far a printed comparative is from the stored figure, as a share of
    what rounding allows; None when it is further than that."""
    if stored is None:
        return 0.0
    tolerance = max(abs(stored) * REL_TOLERANCE, ABS_TOLERANCE_EPS if eps else _rounding(scale))
    gap = abs(value - stored)
    return gap / tolerance if gap <= tolerance else None


def _best(lines: list[list[float]], prev: float | None, year_ago: float | None,
          scales: tuple[float, ...], eps: bool) -> tuple[float, float, int] | None:
    """(current value, scale, line index) of the line whose (current,
    previous, year-ago) window matches both stored quarters most closely."""
    if prev is None and year_ago is None:
        return None
    best: tuple[float, float, float, int] | None = None
    for index, nums in enumerate(lines):
        for scale in scales:
            for start in range(0, min(3, len(nums) - 2)):
                cur, p, y = (n / scale for n in nums[start:start + 3])
                e1, e2 = _error(p, prev, eps, scale), _error(y, year_ago, eps, scale)
                if e1 is None or e2 is None:
                    continue
                if best is None or e1 + e2 < best[0]:
                    best = (e1 + e2, round(cur, 2), scale, index)
    return None if best is None else best[1:]


def read_quarter(text: str, period: str, priors: dict[str, tuple[dict | None, dict | None]]) -> dict[str, dict]:
    """{basis: row} for each basis whose revenue line matches its stored
    previous and year-ago quarters. `priors` maps basis -> (previous quarter
    row, year-ago row), rows as stored (crore, `sales_crore` etc.)."""
    lines = [line for line in text.splitlines() if line.strip()]
    candidates = {metric: _candidate_lines(lines, metric) for metric in LABELS}
    out: dict[str, dict] = {}
    used_sales: set[int] = set()
    for basis, (prev, year_ago) in priors.items():
        if not prev and not year_ago:
            continue

        def stored(row: dict | None, key: str) -> float | None:
            return None if row is None else row.get(key)

        open_lines = [nums if i not in used_sales else [] for i, nums in enumerate(candidates["sales"])]
        found = _best(open_lines, stored(prev, "sales_crore"), stored(year_ago, "sales_crore"), SCALES, False)
        if found is None:
            continue
        sales, scale, line = found
        used_sales.add(line)

        def money(metric: str, key: str) -> float | None:
            hit = _best(candidates[metric], stored(prev, key), stored(year_ago, key), (scale,), False)
            return None if hit is None else hit[0]

        row: dict[str, Any] = {"period": period, "sales_crore": sales}
        row["profit_before_tax_crore"] = money("profit_before_tax", "profit_before_tax_crore")
        row["net_profit_crore"] = money("net_profit", "net_profit_crore")
        hit = _best(candidates["eps"], stored(prev, "eps"), stored(year_ago, "eps"), (1.0,), True)
        row["eps"] = None if hit is None else hit[0]
        operating = money("bank_operating", "operating_profit_crore")
        if operating is None:
            operating = _derived_operating(candidates, scale, prev, year_ago)
        row["operating_profit_crore"] = operating
        row["operating_margin_pct"] = round(operating / sales * 100, 2) if operating is not None and sales else None
        out[basis] = row
    return out


def _derived_operating(candidates: dict, scale: float, prev: dict | None, year_ago: dict | None) -> float | None:
    """Revenue less expenses before depreciation and interest — the stored
    definition — accepted only when the same arithmetic on the previous and
    year-ago columns gives the stored operating profit."""
    want_prev = None if prev is None else prev.get("operating_profit_crore")
    want_year = None if year_ago is None else year_ago.get("operating_profit_crore")
    if want_prev is None and want_year is None:
        return None
    for sales in candidates["sales"]:
        for expenses in candidates["expenses"]:
            for dep in candidates["depreciation"] or [[0.0, 0.0, 0.0]]:
                for fin in candidates["finance"] or [[0.0, 0.0, 0.0]]:
                    cols = [
                        (sales[i] - (abs(expenses[i]) - abs(dep[i]) - abs(fin[i]))) / scale
                        for i in range(3)
                    ]
                    if (_error(cols[1], want_prev, False, scale) is not None
                            and _error(cols[2], want_year, False, scale) is not None):
                        return round(cols[0], 2)
    return None
