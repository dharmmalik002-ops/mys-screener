import csv
import io
import json
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import holidays
import pandas as pd
import yfinance as yf

from app.providers.free import FreeMarketDataProvider, USER_AGENT


US_ET = ZoneInfo("America/New_York")
US_MARKET_OPEN_MINUTES = (9 * 60) + 30
US_MARKET_CLOSE_MINUTES = 16 * 60
USD_TO_INR = 83.0
NASDAQ_TRADED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
NASDAQ_STOCK_SCREENER_URL = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&download=true"
NASDAQ_ETF_SCREENER_URL = "https://api.nasdaq.com/api/screener/etf?tableonly=true&download=true"
US_MIN_PRICE = 15.0
US_MIN_LIQUIDITY_SHARES = 400_000
US_SNAPSHOT_HISTORY_PERIOD = "2y"
US_SNAPSHOT_HISTORY_SYMBOL_LIMIT = 2500
US_SNAPSHOT_HISTORY_BATCH_SIZE = 200
US_MIN_HISTORY_READY_ROWS = 250
US_HISTORICAL_BREADTH_SYMBOL_LIMIT = 250
US_HISTORICAL_BREADTH_BATCH_SIZE = 64


class USFreeMarketDataProvider(FreeMarketDataProvider):
    def __init__(self, gemini_api_key: str | None = None) -> None:
        super().__init__(gemini_api_key=gemini_api_key)
        self.universe_cache_path = self.backend_root / "data" / "free_universe_us.json"
        self.snapshot_cache_path = self.backend_root / "data" / "free_snapshots_us.json"
        self.company_metadata_path = self.backend_root / "data" / "free_company_metadata_us.json"
        self.fundamentals_cache_path = self.backend_root / "data" / "free_fundamentals_us.json"
        self.bulk_fundamentals_cache_path = self.backend_root / "data" / "free_fundamental_cache_us.json"
        self.historical_breadth_cache_path = self.backend_root / "data" / "free_historical_breadth_us.json"
        self.chart_cache_dir = self.backend_root / "data" / "chart_cache_us"
        self.chart_cache_dir.mkdir(parents=True, exist_ok=True)
        self._last_refresh_metadata = self._default_refresh_metadata()

    def _benchmark_symbol(self) -> str:
        return "^GSPC"

    async def get_index_quotes(self, symbols: list[str]):
        normalized_symbols = [symbol.strip() for symbol in symbols if symbol.strip()]
        if not normalized_symbols:
            return []
        items = self._fetch_index_quotes(normalized_symbols)
        if items:
            return items
        return await self.demo.get_index_quotes(normalized_symbols)

    def _write_snapshot_rows(self, rows: list[dict[str, Any]]) -> None:
        universe_size = len(self._load_json_rows(self.universe_cache_path))
        if universe_size >= 500 and len(rows) < min(500, max(universe_size // 2, 1)):
            raise ValueError(f"Refusing to write undersized US snapshot cache: {len(rows)} rows for universe {universe_size}")

        existing_rows = self._load_json_rows(self.snapshot_cache_path)
        if rows and not self._snapshot_schema_ok(rows) and self._snapshot_schema_ok(existing_rows):
            raise ValueError("Refusing to overwrite valid US snapshot cache with invalid rows")

        super()._write_snapshot_rows(rows)

    def _history_backed_snapshot_cache_ok(self, rows: list[dict[str, Any]]) -> bool:
        if not rows:
            return False
        minimum_ready = min(len(rows), max(US_MIN_HISTORY_READY_ROWS, len(rows) // 10))
        history_ready = 0
        for row in rows:
            if str(row.get("history_source") or "").strip().lower() != "history":
                continue
            if self._to_float(row.get("ema20")) is None:
                continue
            if int(row.get("history_bars") or 0) < 200:
                continue
            history_ready += 1
            if history_ready >= minimum_ready:
                return True
        return False

    def _snapshot_schema_ok(self, rows: list[dict[str, Any]]) -> bool:
        return super()._snapshot_schema_ok(rows) and self._history_backed_snapshot_cache_ok(rows)

    def _should_rebuild_snapshot_history(self, rows: list[dict[str, Any]], force_refresh: bool = False) -> bool:
        return (not self._history_backed_snapshot_cache_ok(rows)) or super()._should_rebuild_snapshot_history(rows, force_refresh)

    def _history_listing_date(self, instrument: dict[str, Any], history: pd.DataFrame) -> str | None:
        listing_date = super()._history_listing_date(instrument, history)
        if listing_date:
            return listing_date
        if history.empty or not isinstance(history.index, pd.DatetimeIndex):
            return None
        return history.index[0].date().isoformat()

    @staticmethod
    def _derived_listing_date_from_cached_row(row: dict[str, Any]) -> str | None:
        listing_date = str(row.get("listing_date") or "").strip()
        if listing_date:
            return listing_date

        history_source = str(row.get("history_source") or "").strip().lower()
        history_bars = int(row.get("history_bars") or 0)
        history_as_of_date = str(row.get("history_as_of_date") or "").strip()
        if history_source not in {"history", "chart_cache", "legacy_chart_cache"}:
            return None
        if history_bars <= 0 or history_bars > 260 or not history_as_of_date:
            return None

        try:
            history_index = pd.bdate_range(end=pd.Timestamp(history_as_of_date), periods=history_bars)
        except Exception:
            return None
        if len(history_index) == 0:
            return None
        return history_index[0].date().isoformat()

    def _backfill_listing_dates(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
        changed = False
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            next_row = dict(row)
            derived_listing_date = self._derived_listing_date_from_cached_row(next_row)
            if derived_listing_date and next_row.get("listing_date") != derived_listing_date:
                next_row["listing_date"] = derived_listing_date
                changed = True
            normalized_rows.append(next_row)
        return normalized_rows, changed

    def _load_or_refresh_snapshots(self, market_cap_min_crore: float, force_refresh: bool) -> list[dict[str, Any]]:
        rows = super()._load_or_refresh_snapshots(market_cap_min_crore, force_refresh)
        normalized_rows, changed = self._backfill_listing_dates(rows)
        if not changed:
            return normalized_rows

        cached_rows = self._load_json_rows(self.snapshot_cache_path)
        normalized_cache_rows, cache_changed = self._backfill_listing_dates(cached_rows)
        if cache_changed:
            self._write_snapshot_rows(normalized_cache_rows)
        return normalized_rows

    def _benchmark_label(self) -> str:
        return "S&P 500"

    def _default_exchange(self) -> str:
        return "NASDAQ"

    def _historical_breadth_universes(self, snapshots: list[dict[str, Any]]) -> list[tuple[str, set[str]]]:
        equities = [
            snapshot
            for snapshot in sorted(snapshots, key=lambda row: row.get("market_cap_crore", 0), reverse=True)
            if str(snapshot.get("sector") or "").strip().lower() != "exchange traded funds"
            and str(snapshot.get("sub_sector") or "").strip().lower() != "etf"
        ]

        def snapshot_symbol(row: dict[str, Any]) -> str:
            return self._normalize_us_symbol(row.get("symbol") or row.get("ticker") or row.get("instrument_key"))

        def build_exchange_set(exchange: str) -> set[str]:
            selected: list[str] = []
            for snapshot in equities:
                if str(snapshot.get("exchange") or "").strip().upper() != exchange:
                    continue
                symbol = snapshot_symbol(snapshot)
                if not symbol:
                    continue
                selected.append(symbol)
                if len(selected) >= US_HISTORICAL_BREADTH_SYMBOL_LIMIT:
                    break
            return set(selected)

        nyse = build_exchange_set("NYSE")
        nasdaq = build_exchange_set("NASDAQ")
        return [("NYSE", nyse), ("NASDAQ", nasdaq)]

    @staticmethod
    def _history_frame_from_download(payload: pd.DataFrame, symbol: str) -> pd.DataFrame:
        if payload.empty:
            return pd.DataFrame()

        if isinstance(payload.columns, pd.MultiIndex):
            if symbol not in payload.columns.get_level_values(0):
                return pd.DataFrame()
            history = payload[symbol].copy()
        else:
            history = payload.copy()

        if history.empty:
            return pd.DataFrame()

        history.columns = [str(column) for column in history.columns]
        if "Adj Close" not in history.columns and "Close" in history.columns:
            history["Adj Close"] = history["Close"]
        if "Stock Splits" not in history.columns:
            history["Stock Splits"] = 0.0
        required_columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Stock Splits"]
        if any(column not in history.columns for column in required_columns[:5]):
            return pd.DataFrame()
        return history[required_columns].dropna(how="all")

    def _rebuild_historical_breadth(self, snapshots: list[dict[str, Any]]) -> bool:
        universe_specs = self._historical_breadth_universes(snapshots)
        symbols = sorted({symbol for _, tickers in universe_specs for symbol in tickers if symbol})
        if not symbols:
            return False

        breadth_dfs: dict[str, pd.DataFrame] = {}
        for index in range(0, len(symbols), US_HISTORICAL_BREADTH_BATCH_SIZE):
            batch = symbols[index:index + US_HISTORICAL_BREADTH_BATCH_SIZE]
            try:
                payload = yf.download(
                    tickers=batch,
                    period="3y",
                    interval="1d",
                    auto_adjust=False,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
            except Exception:
                continue

            for symbol in batch:
                history = self._history_frame_from_download(payload, symbol)
                if history.empty:
                    continue
                breadth_df = self._compute_historical_breadth_for_stock(history)
                if breadth_df is not None and not breadth_df.empty:
                    breadth_dfs[symbol] = breadth_df

        if not breadth_dfs:
            return False

        self._aggregate_and_save_historical_breadth(snapshots, breadth_dfs)
        return True

    def _build_snapshot_cache(self, universe: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not universe:
            return []

        try:
            benchmark_history = self._download_history_frame(
                ticker=self._benchmark_symbol(),
                period=US_SNAPSHOT_HISTORY_PERIOD,
                interval="1d",
            )
            benchmark_close = self._extract_close_series(benchmark_history, self._benchmark_symbol())
        except Exception:
            benchmark_close = pd.Series(dtype=float)

        results: list[dict[str, Any]] = []
        breadth_dfs: dict[str, pd.DataFrame] = {}
        cached_results, cached_breadth, remaining_universe = self._build_snapshot_rows_from_fresh_cached_bars(
            universe,
            benchmark_close,
        )
        if cached_results:
            results.extend(cached_results)
            breadth_dfs.update(cached_breadth)

        history_universe = remaining_universe[:US_SNAPSHOT_HISTORY_SYMBOL_LIMIT]
        quote_only_candidates = remaining_universe[US_SNAPSHOT_HISTORY_SYMBOL_LIMIT:]
        unresolved: list[dict[str, Any]] = []
        for start in range(0, len(history_universe), US_SNAPSHOT_HISTORY_BATCH_SIZE):
            batch = history_universe[start : start + US_SNAPSHOT_HISTORY_BATCH_SIZE]
            tickers = [str(item.get("ticker") or item.get("symbol") or "").upper() for item in batch if str(item.get("ticker") or item.get("symbol") or "").strip()]
            if not tickers:
                continue

            try:
                payload = yf.download(
                    tickers=tickers,
                    period=US_SNAPSHOT_HISTORY_PERIOD,
                    interval="1d",
                    auto_adjust=False,
                    actions=True,
                    progress=False,
                    group_by="ticker",
                    threads=True,
                )
            except Exception:
                unresolved.extend(batch)
                continue

            for instrument in batch:
                ticker = str(instrument.get("ticker") or instrument.get("symbol") or "").upper()
                history = self._history_frame_from_download(payload, ticker)
                if not self._history_has_minimum_bars(history):
                    unresolved.append(instrument)
                    continue

                snapshot = self._history_to_snapshot(instrument, history, benchmark_close)
                if snapshot is None:
                    unresolved.append(instrument)
                    continue

                results.append(snapshot)
                breadth_df = self._compute_historical_breadth_for_stock(self._split_adjusted_history(history))
                if breadth_df is not None and not breadth_df.empty:
                    breadth_dfs[ticker] = breadth_df

        built_symbols = {
            str(row.get("symbol") or "").upper()
            for row in results
            if str(row.get("symbol") or "").strip()
        }
        quote_only_rows = self._build_quote_only_snapshots(
            [
                *unresolved,
                *quote_only_candidates,
            ] if unresolved or quote_only_candidates else [],
            benchmark_close,
        )

        merged_results = [*results, *quote_only_rows]
        merged_results = self._apply_sector_benchmarks(merged_results)
        final_snapshots = self._apply_rs_rating(merged_results)
        try:
            if breadth_dfs:
                self._aggregate_and_save_historical_breadth(final_snapshots, breadth_dfs)
        except Exception:
            pass
        return final_snapshots

    @staticmethod
    def _normalize_us_symbol(value: Any) -> str:
        symbol = str(value or "").strip().upper()
        if not symbol:
            return ""
        return symbol.replace(".", "-").replace("/", "-")

    @staticmethod
    def _market_cap_to_crore(value: Any) -> float | None:
        try:
            market_cap_usd = float(value)
        except (TypeError, ValueError):
            return None
        if market_cap_usd <= 0:
            return None
        return round((market_cap_usd * USD_TO_INR) / 10_000_000, 2)

    @staticmethod
    def _liquidity_proxy_to_crore(price: float, avg_volume: float) -> float:
        return round((price * avg_volume * USD_TO_INR) / 10_000_000, 2)

    @staticmethod
    def _yahoo_listing_date(info: dict[str, Any]) -> str | None:
        first_trade = info.get("firstTradeDateEpochUtc")
        if first_trade in (None, ""):
            return None
        try:
            return datetime.fromtimestamp(int(float(first_trade)), tz=timezone.utc).date().isoformat()
        except (TypeError, ValueError, OSError):
            return None

    def _current_ist_date(self) -> date:
        return datetime.now(timezone.utc).astimezone(US_ET).date()

    def _market_holiday_dates(self, years: tuple[int, ...]) -> set[date]:
        cached = self._holiday_date_cache.get(years)
        if cached is not None:
            return cached
        calendar = holidays.financial_holidays("NYSE", years=list(years))
        holiday_dates = {holiday_date for holiday_date in calendar.keys()}
        self._holiday_date_cache[years] = holiday_dates
        return holiday_dates

    def _current_or_previous_trading_day_ist(self) -> date:
        trading_date = self._current_ist_date()
        while not self._is_trading_day_ist(trading_date):
            trading_date -= timedelta(days=1)
        return trading_date

    def _is_trading_day_ist(self, target: date | None = None) -> bool:
        trading_date = target or self._current_ist_date()
        return trading_date.weekday() < 5 and not self._is_market_holiday(trading_date)

    def _is_market_open_ist(self) -> bool:
        now = datetime.now(timezone.utc).astimezone(US_ET)
        if not self._is_trading_day_ist(now.date()):
            return False
        total_minutes = (now.hour * 60) + now.minute
        return US_MARKET_OPEN_MINUTES <= total_minutes <= US_MARKET_CLOSE_MINUTES

    def _market_timezone(self):
        return US_ET

    def _market_close_minutes(self) -> int:
        return US_MARKET_CLOSE_MINUTES

    def _post_close_refresh_grace_minutes(self) -> int:
        return 15

    def _is_live_quote_recent(self, updated_at: datetime | None) -> bool:
        if updated_at is None:
            return False
        now_utc = datetime.now(timezone.utc)
        quote_session_date = updated_at.astimezone(US_ET).date()
        current_session_date = self._current_or_previous_trading_day_ist()
        age_seconds = max((now_utc - updated_at).total_seconds(), 0.0)
        if self._is_market_open_ist():
            return quote_session_date == current_session_date and age_seconds <= 20 * 60
        return quote_session_date == current_session_date and age_seconds <= 72 * 60 * 60

    def _resolve_ticker(self, symbol: str) -> str:
        if symbol.startswith("^"):
            return symbol
        normalized_symbol = self._normalize_us_symbol(symbol)
        if self.snapshot_cache_path.exists():
            try:
                rows = json.loads(self.snapshot_cache_path.read_text(encoding="utf-8"))
            except Exception:
                rows = []
            row = next((item for item in rows if str(item.get("symbol") or "").upper() == normalized_symbol), None)
            if row and row.get("instrument_key"):
                return str(row["instrument_key"])
        return normalized_symbol

    @staticmethod
    def _metadata_needs_refresh(metadata: dict[str, Any] | None) -> bool:
        payload = metadata or {}
        sector = str(payload.get("sector") or "").strip()
        sub_sector = str(payload.get("sub_sector") or "").strip()
        listing_date = payload.get("listing_date")
        return not listing_date or not sector or sector == "Unclassified" or not sub_sector or sub_sector == "Unclassified"

    def _should_fetch_metadata_for_item(self, item: dict[str, Any]) -> bool:
        return False

    @staticmethod
    def _nasdaq_headers() -> dict[str, str]:
        return {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Origin": "https://www.nasdaq.com",
            "Referer": "https://www.nasdaq.com/",
        }

    @staticmethod
    def _parse_numeric_text(value: Any) -> float | None:
        raw = str(value or "").strip()
        if not raw or raw in {"--", "N/A"}:
            return None
        cleaned = raw.replace("$", "").replace(",", "").replace("%", "").strip()
        suffix_multiplier = 1.0
        if cleaned.endswith("K"):
            suffix_multiplier = 1_000.0
            cleaned = cleaned[:-1]
        elif cleaned.endswith("M"):
            suffix_multiplier = 1_000_000.0
            cleaned = cleaned[:-1]
        elif cleaned.endswith("B"):
            suffix_multiplier = 1_000_000_000.0
            cleaned = cleaned[:-1]
        elif cleaned.endswith("T"):
            suffix_multiplier = 1_000_000_000_000.0
            cleaned = cleaned[:-1]
        try:
            return float(cleaned) * suffix_multiplier
        except ValueError:
            return None

    def _fetch_nasdaq_screener_rows(self, url: str) -> list[dict[str, Any]]:
        with httpx.Client(timeout=30, headers=self._nasdaq_headers(), follow_redirects=True, http2=False) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()

        data = payload.get("data") or {}
        nested = data.get("data") if isinstance(data.get("data"), dict) else data
        rows = nested.get("rows") or []
        return [row for row in rows if isinstance(row, dict)]

    def _fetch_us_listing_map(self) -> dict[str, dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}

        for row in self._fetch_us_directory_rows(NASDAQ_TRADED_URL):
            symbol = self._normalize_us_symbol(row.get("NASDAQ Symbol") or row.get("Symbol"))
            if not symbol or symbol == "FILE CREATION TIME":
                continue
            if str(row.get("Test Issue") or "").upper() == "Y":
                continue
            rows[symbol] = {
                "symbol": symbol,
                "exchange": "NASDAQ",
                "ticker": symbol,
                "is_etf": str(row.get("ETF") or "").upper() == "Y",
            }

        exchange_code_map = {
            "N": "NYSE",
            "A": "NYSE",
            "P": "NYSE",
            "Z": "NASDAQ",
            "V": "NASDAQ",
        }
        for row in self._fetch_us_directory_rows(OTHER_LISTED_URL):
            exchange_code = str(row.get("Exchange") or "").strip().upper()
            if exchange_code not in exchange_code_map:
                continue
            if str(row.get("Test Issue") or "").upper() == "Y":
                continue
            symbol = self._normalize_us_symbol(row.get("NASDAQ Symbol") or row.get("ACT Symbol") or row.get("CQS Symbol"))
            if not symbol or symbol == "FILE CREATION TIME":
                continue
            rows[symbol] = {
                "symbol": symbol,
                "exchange": exchange_code_map[exchange_code],
                "ticker": symbol,
                "is_etf": str(row.get("ETF") or "").upper() == "Y",
            }

        return rows

    def _fetch_company_metadata_chunk(self, universe: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        metadata: dict[str, dict[str, Any]] = {}
        if not universe:
            return metadata

        for item in universe:
            symbol = str(item.get("symbol") or "").upper()
            ticker = str(item.get("ticker") or symbol)
            try:
                yf_ticker = yf.Ticker(ticker)
                try:
                    info = yf_ticker.get_info()
                except Exception:
                    info = yf_ticker.info
            except Exception:
                info = {}

            quote_type = str(info.get("quoteType") or item.get("asset_type") or "").strip().upper()
            is_etf = quote_type == "ETF" or bool(item.get("is_etf"))
            sector = str(info.get("sectorDisp") or info.get("sector") or "").strip()
            sub_sector = str(info.get("industryDisp") or info.get("industry") or "").strip()
            if not sector:
                sector = "Exchange Traded Funds" if is_etf else "Unclassified"
            if not sub_sector:
                sub_sector = "ETF" if is_etf else "Unclassified"
            metadata[symbol] = {
                "market_cap_crore": self._market_cap_to_crore(info.get("marketCap")) or item.get("market_cap_crore"),
                "sector": sector,
                "sub_sector": sub_sector,
                "listing_date": self._yahoo_listing_date(info) or item.get("listing_date"),
                "circuit_band_label": None,
                "upper_circuit_limit": None,
                "lower_circuit_limit": None,
            }

        return metadata

    def _fetch_us_directory_rows(self, url: str) -> list[dict[str, str]]:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/plain,text/csv,*/*",
            "Referer": "https://www.nasdaqtrader.com/",
        }
        with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            text = response.text
        reader = csv.DictReader(io.StringIO(text), delimiter="|")
        return [{str(key).strip(): str(value or "").strip() for key, value in row.items()} for row in reader]

    def _fetch_us_listed_universe(self) -> list[dict[str, Any]]:
        listing_map = self._fetch_us_listing_map()
        deduped: dict[str, dict[str, Any]] = {}

        for row in self._fetch_nasdaq_screener_rows(NASDAQ_STOCK_SCREENER_URL):
            symbol = self._normalize_us_symbol(row.get("symbol"))
            price = self._parse_numeric_text(row.get("lastsale"))
            last_session_volume = int(self._parse_numeric_text(row.get("volume")) or 0)
            net_change = self._parse_numeric_text(row.get("netchange")) or 0.0
            previous_close = price - net_change if price is not None else None
            if (
                not symbol
                or price is None
                or price < US_MIN_PRICE
                or last_session_volume < US_MIN_LIQUIDITY_SHARES
                or previous_close in (None, 0)
            ):
                continue
            listing = listing_map.get(symbol, {})
            deduped[symbol] = {
                "symbol": symbol,
                "name": str(row.get("name") or symbol).strip(),
                "exchange": str(listing.get("exchange") or "NASDAQ").upper(),
                "ticker": str(listing.get("ticker") or symbol),
                "is_etf": bool(listing.get("is_etf")),
                "asset_type": "ETF" if bool(listing.get("is_etf")) else "EQUITY",
                "last_price": round(price, 2),
                "previous_close": round(float(previous_close), 2),
                "last_session_volume": last_session_volume,
                "market_cap_crore": self._market_cap_to_crore(row.get("marketCap")),
                "listing_date": None,
                "sector": str(row.get("sector") or "Unclassified").strip() or "Unclassified",
                "sub_sector": str(row.get("industry") or "Unclassified").strip() or "Unclassified",
            }

        for row in self._fetch_nasdaq_screener_rows(NASDAQ_ETF_SCREENER_URL):
            symbol = self._normalize_us_symbol(row.get("symbol"))
            price = self._parse_numeric_text(row.get("lastSalePrice"))
            net_change = self._parse_numeric_text(row.get("netChange")) or 0.0
            previous_close = price - net_change if price is not None else None
            if not symbol or price is None or price < US_MIN_PRICE:
                continue
            listing = listing_map.get(symbol, {})
            deduped[symbol] = {
                "symbol": symbol,
                "name": str(row.get("companyName") or symbol).strip(),
                "exchange": str(listing.get("exchange") or "NASDAQ").upper(),
                "ticker": str(listing.get("ticker") or symbol),
                "is_etf": True,
                "asset_type": "ETF",
                "last_price": round(price, 2),
                "previous_close": round(float(previous_close), 2) if previous_close not in (None, 0) else round(price, 2),
                "last_session_volume": 0,
                "market_cap_crore": None,
                "listing_date": None,
                "sector": "Exchange Traded Funds",
                "sub_sector": "ETF",
            }

        return list(deduped.values())

    def _fetch_market_cap_universe(self, market_cap_min_crore: float) -> list[dict[str, Any]]:
        del market_cap_min_crore
        cached_metadata: dict[str, dict[str, Any]] = {}
        if self.company_metadata_path.exists():
            try:
                cached_metadata = json.loads(self.company_metadata_path.read_text(encoding="utf-8"))
            except Exception:
                cached_metadata = {}

        session_date = self._current_or_previous_trading_day_ist().isoformat()
        rows: list[dict[str, Any]] = []
        for row in self._fetch_us_listed_universe():
            symbol = str(row.get("symbol") or "").upper()
            metadata = cached_metadata.get(symbol, {})
            market_cap_crore = metadata.get("market_cap_crore") or row.get("market_cap_crore")
            if market_cap_crore in (None, 0):
                seeded_volume = float(row.get("last_session_volume") or US_MIN_LIQUIDITY_SHARES)
                market_cap_crore = self._liquidity_proxy_to_crore(float(row.get("last_price") or 0), seeded_volume)
            rows.append(
                {
                    "symbol": symbol,
                    "name": str(row.get("name") or symbol).strip(),
                    "exchange": str(row.get("exchange") or "NASDAQ").upper(),
                    "market_cap_crore": market_cap_crore or 0.0,
                    "ticker": str(row.get("ticker") or symbol),
                    "listing_date": metadata.get("listing_date") or row.get("listing_date"),
                    "sector": metadata.get("sector") or row.get("sector") or "Unclassified",
                    "sub_sector": metadata.get("sub_sector") or row.get("sub_sector") or "Unclassified",
                    "last_price": row.get("last_price"),
                    "previous_close": row.get("previous_close") or row.get("last_price"),
                    "last_session_volume": int(row.get("last_session_volume") or 0),
                    "isin": None,
                    "bse_code": None,
                    "asset_type": row.get("asset_type") or ("ETF" if row.get("is_etf") else "EQUITY"),
                    "is_etf": bool(row.get("is_etf")),
                    "universe_version": 3,
                    "universe_session_date": session_date,
                    "universe_market_cap_floor_crore": 0,
                }
            )

        rows.sort(key=lambda item: (-float(item.get("market_cap_crore", 0) or 0), str(item.get("symbol") or "")))
        return rows

    def _load_or_refresh_universe(self, market_cap_min_crore: float, force_refresh: bool) -> list[dict[str, Any]]:
        rows = self._load_json_rows(self.universe_cache_path)
        cache_has_seed_quotes = bool(rows) and all(
            row.get("last_price") not in (None, 0) and "last_session_volume" in row
            for row in rows[: min(len(rows), 50)]
        )
        if cache_has_seed_quotes and self._universe_cache_ok(rows, market_cap_min_crore) and (
            not force_refresh or self._is_fresh(self.universe_cache_path, max_age_hours=24 * 7)
        ):
            filtered = [row for row in rows if float(row.get("market_cap_crore", 0) or 0) >= market_cap_min_crore]
            if filtered:
                return sorted(filtered, key=lambda item: (-float(item.get("market_cap_crore", 0) or 0), str(item.get("symbol") or "")))

        universe = self._fetch_market_cap_universe(market_cap_min_crore)
        self.universe_cache_path.write_text(json.dumps(universe, indent=2), encoding="utf-8")
        return universe

    @staticmethod
    def _seed_quote_from_universe_item(item: dict[str, Any]) -> dict[str, Any] | None:
        last_price = float(item.get("last_price") or 0)
        if last_price <= 0:
            return None
        previous_close = float(item.get("previous_close") or last_price)
        timestamp = int(datetime.now(timezone.utc).timestamp())
        return {
            "regularMarketPrice": last_price,
            "regularMarketPreviousClose": previous_close if previous_close > 0 else last_price,
            "regularMarketDayHigh": last_price,
            "regularMarketDayLow": last_price,
            "regularMarketOpen": previous_close if previous_close > 0 else last_price,
            "regularMarketVolume": int(item.get("last_session_volume") or 0),
            "regularMarketTime": timestamp,
            "fiftyTwoWeekHigh": last_price,
            "fiftyTwoWeekLow": last_price,
        }

    def _load_or_refresh_snapshots(self, market_cap_min_crore: float, force_refresh: bool) -> list[dict[str, Any]]:
        rows = super()._load_or_refresh_snapshots(market_cap_min_crore, force_refresh)
        normalized_rows, changed = self._backfill_listing_dates(rows)
        if not changed:
            return normalized_rows

        cached_rows = self._load_json_rows(self.snapshot_cache_path)
        normalized_cache_rows, cache_changed = self._backfill_listing_dates(cached_rows)
        if cache_changed:
            self._write_snapshot_rows(normalized_cache_rows)
        return normalized_rows