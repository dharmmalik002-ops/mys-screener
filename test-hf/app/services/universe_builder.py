import argparse
import csv
import gzip
import json
from pathlib import Path
from typing import Any


def normalize_key(value: str) -> str:
    return "".join(char for char in value.upper() if char.isalnum())


def find_symbol_column(headers: list[str]) -> str:
    normalized = {header: normalize_key(header) for header in headers}
    for header, key in normalized.items():
        if key in {"SYMBOL", "TICKER", "TRADINGSYMBOL"}:
            return header
    raise ValueError("Could not find a symbol column in the market-cap CSV.")


def find_market_cap_column(headers: list[str]) -> str:
    normalized = {header: normalize_key(header) for header in headers}
    for header, key in normalized.items():
        if "MARKETCAP" in key or "MARKETCAPITALIZATION" in key:
            return header
    raise ValueError("Could not find a market cap column in the market-cap CSV.")


def load_market_caps(csv_path: Path) -> dict[str, float]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("Market-cap CSV is missing headers.")
        symbol_column = find_symbol_column(reader.fieldnames)
        market_cap_column = find_market_cap_column(reader.fieldnames)

        market_caps: dict[str, float] = {}
        for row in reader:
            symbol = normalize_key(row.get(symbol_column, ""))
            raw_cap = (row.get(market_cap_column) or "").replace(",", "").strip()
            if not symbol or not raw_cap:
                continue
            try:
                market_caps[symbol] = float(raw_cap)
            except ValueError:
                continue
        return market_caps


def load_upstox_instruments(instrument_path: Path) -> list[dict[str, Any]]:
    with gzip.open(instrument_path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def build_free_universe(
    market_cap_csv: Path,
    nse_json_gz: Path,
    bse_json_gz: Path,
    threshold_crore: float,
    output_path: Path,
) -> int:
    market_caps = load_market_caps(market_cap_csv)
    universe: list[dict[str, Any]] = []
    seen: set[str] = set()

    for exchange, path in (("NSE", nse_json_gz), ("BSE", bse_json_gz)):
        for item in load_upstox_instruments(path):
            segment = (item.get("segment") or "").upper()
            instrument_type = (item.get("instrument_type") or "").upper()
            if "EQ" not in segment or instrument_type not in {"EQ", "EQUITY"}:
                continue

            symbol = normalize_key(item.get("trading_symbol") or item.get("symbol") or "")
            if not symbol or symbol in seen:
                continue

            market_cap = market_caps.get(symbol)
            if market_cap is None or market_cap < threshold_crore:
                continue

            universe.append(
                {
                    "symbol": symbol,
                    "name": item.get("company_name") or item.get("name") or symbol,
                    "exchange": exchange,
                    "sector": item.get("sector") or "Unknown",
                    "market_cap_crore": market_cap,
                    "last_price": float(item.get("last_price") or 0),
                    "change_pct": 0,
                    "volume": int(item.get("volume") or 0),
                    "avg_volume_20d": int(item.get("volume") or 0),
                    "day_high": float(item.get("last_price") or 0),
                    "day_low": float(item.get("last_price") or 0),
                    "ath": float(item.get("last_price") or 0),
                    "high_52w": float(item.get("last_price") or 0),
                    "range_high_20d": float(item.get("last_price") or 0),
                    "ema20": float(item.get("last_price") or 0),
                    "ema50": float(item.get("last_price") or 0),
                    "ema200": float(item.get("last_price") or 0),
                    "benchmark_return_20d": 0,
                    "sector_return_20d": 0,
                    "stock_return_20d": 0,
                    "stock_return_60d": 0,
                    "pivot_high": float(item.get("last_price") or 0),
                    "darvas_high": float(item.get("last_price") or 0),
                    "darvas_low": float(item.get("last_price") or 0),
                    "pullback_depth_pct": 0,
                    "trend_strength": 0,
                    "instrument_key": item.get("instrument_key"),
                }
            )
            seen.add(symbol)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(universe, indent=2), encoding="utf-8")
    return len(universe)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a free local scanner universe from market-cap CSV and Upstox instruments.")
    parser.add_argument("--market-cap-csv", required=True, help="CSV exported from the official quarterly NSE market-cap sheet.")
    parser.add_argument("--nse-json-gz", required=True, help="Path to NSE.json.gz from Upstox instruments.")
    parser.add_argument("--bse-json-gz", required=True, help="Path to BSE.json.gz from Upstox instruments.")
    parser.add_argument("--threshold", type=float, default=1000, help="Minimum market cap in crore.")
    parser.add_argument("--output", default="data/live_universe.json", help="Output path for the merged universe.")
    args = parser.parse_args()

    count = build_free_universe(
        market_cap_csv=Path(args.market_cap_csv),
        nse_json_gz=Path(args.nse_json_gz),
        bse_json_gz=Path(args.bse_json_gz),
        threshold_crore=args.threshold,
        output_path=Path(args.output),
    )
    print(f"Wrote {count} symbols to {args.output}")


if __name__ == "__main__":
    main()
