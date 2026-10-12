"""The results-PDF reader takes a figure only when the PDF's previous-quarter
and year-ago columns match what the exchanges already published."""

from __future__ import annotations

from app.services import result_pdf as rp

# Shaped like a real statement's extracted text: consolidated first (₹ crore),
# then standalone in ₹ lakh, with serial numbers and a note reference.
STATEMENT = """
HCL Technologies Limited
Statement of Consolidated Results for the quarter ended September 30, 2026 (Rs. in crores)
Particulars Quarter ended 30.09.2026 30.06.2026 30.09.2025 Half year ended
1 Revenue from operations (Refer note 3) 33,872 32,503 30,349 66,375 59,211
2 Other income 610 515 480 1,125 900
Total expenses 26,400 25,880 24,200 52,280 47,000
Depreciation and amortisation expense 1,200 1,150 1,100 2,350 2,180
Finance costs 180 170 150 350 300
Profit before tax 8,082 7,138 6,629 15,220 12,980
Net profit for the period 6,100 5,400 4,600 11,500 9,800
Attributable to owners of the Company 6,090 5,390 4,590 11,480 9,780
Earnings per share
(a) Basic (Rs.) 22.45 19.87 16.92 42.32 36.10
Statement of Standalone Results (Rs. in lakhs)
Revenue from operations 1,520,000 1,433,700 1,286,200 2,953,700 2,560,000
Profit before tax 640,000 415,800 405,500 1,055,800 820,000
Net profit for the period 501,000 318,200 312,600 819,200 640,000
Basic 18.46 11.73 11.52 30.19 23.60
"""

CONS_PREV = {"period": "Jun 2026", "sales_crore": 32503.0, "operating_profit_crore": 7943.0,
             "profit_before_tax_crore": 7138.0, "net_profit_crore": 5390.0, "eps": 19.87}
CONS_YEAR = {"period": "Sep 2025", "sales_crore": 30349.0, "operating_profit_crore": 7399.0,
             "profit_before_tax_crore": 6629.0, "net_profit_crore": 4590.0, "eps": 16.92}
STAND_PREV = {"period": "Jun 2026", "sales_crore": 14337.0, "net_profit_crore": 3182.0, "eps": 11.73,
              "profit_before_tax_crore": 4158.0}
STAND_YEAR = {"period": "Sep 2025", "sales_crore": 12862.0, "net_profit_crore": 3126.0, "eps": 11.52,
              "profit_before_tax_crore": 4055.0}


def test_reads_each_basis_only_where_its_stored_quarters_match():
    rows = rp.read_quarter(STATEMENT, "Sep 2026", {
        "consolidated": (CONS_PREV, CONS_YEAR), "standalone": (STAND_PREV, STAND_YEAR)})
    cons = rows["consolidated"]
    assert (cons["sales_crore"], cons["profit_before_tax_crore"], cons["eps"]) == (33872.0, 8082.0, 22.45)
    # The owners' line matches the stored owners' profit; the period line does not.
    assert cons["net_profit_crore"] == 6090.0
    # Operating profit by the stored definition, checked on both comparatives:
    # 33,872 - (26,400 - 1,200 - 180) = 8,852.
    assert cons["operating_profit_crore"] == 8852.0
    # Standalone in lakh: found by the unit that makes its comparatives match.
    stand = rows["standalone"]
    assert (stand["sales_crore"], stand["net_profit_crore"], stand["eps"]) == (15200.0, 5010.0, 18.46)
    assert stand["operating_profit_crore"] is None  # no expense lines in that table: not guessed


def test_nothing_is_read_when_the_comparatives_do_not_match():
    wrong = dict(CONS_PREV, sales_crore=31000.0)
    rows = rp.read_quarter(STATEMENT, "Sep 2026", {"consolidated": (wrong, dict(CONS_YEAR, sales_crore=29000.0))})
    assert rows == {}


def test_a_figure_whose_own_comparatives_differ_is_left_out():
    # Revenue matches; the stored EPS differs (e.g. restated for a bonus): EPS
    # is left blank rather than taken from the PDF unchecked.
    rows = rp.read_quarter(STATEMENT, "Sep 2026", {
        "consolidated": (dict(CONS_PREV, eps=9.93), dict(CONS_YEAR, eps=8.46))})
    assert rows["consolidated"]["sales_crore"] == 33872.0 and rows["consolidated"]["eps"] is None


def test_no_stored_quarters_means_nothing_to_check_against():
    assert rp.read_quarter(STATEMENT, "Sep 2026", {"consolidated": (None, None)}) == {}


def test_negative_figures_in_brackets():
    text = "Revenue from operations 100.50 90.00 80.00\nNet profit for the period (12.40) (3.10) 2.00\n"
    rows = rp.read_quarter(text, "Sep 2026", {"standalone": (
        {"sales_crore": 90.0, "net_profit_crore": -3.1}, {"sales_crore": 80.0, "net_profit_crore": 2.0})})
    assert rows["standalone"]["net_profit_crore"] == -12.4


def test_a_scanned_pdf_reads_as_nothing():
    assert rp.extract_text(b"%PDF-1.4 not really a pdf") == ""
