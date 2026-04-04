import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import io
import json
import os
import re
import subprocess
import time
from datetime import date, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
import holidays
import requests
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from pypdf import PdfReader

from app.models.market import (
    BalanceSheetItem,
    BusinessTrigger,
    CashFlowItem,
    ChartBar,
    CompanyFundamentals,
    CompanyUpdateItem,
    DetailedNews,
    FinancialRatios,
    GrowthDriver,
    GrowthSnapshot,
    IndexQuoteItem,
    InsiderTransaction,
    ManagementGuidance,
    ProfitLossItem,
    QuarterlyResultItem,
    ShareholdingDelta,
    ShareholdingPatternItem,
    StockSnapshot,
    ValuationSnapshot,
)
from app.providers.demo import DemoMarketDataProvider
from app.services.ai_analysis_service import AIAnalysisService, enrich_fundamentals_with_ai

NSE_MARKET_CAP_URL = "https://www.nseindia.com/static/regulations/listing-compliance/nse-market-capitalisation-all-companies"
NSE_LISTED_EQUITIES_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_QUOTE_EQUITY_URL = "https://www.nseindia.com/api/quote-equity"
NSE_ALL_INDICES_URL = "https://www.nseindia.com/api/allIndices"
NSE_INDEX_PE_HISTORY_URL = "https://www.nseindia.com/api/index-pe-history"
NSE_INDEX_CHART_URL = "https://www.nseindia.com/api/chart-databyindex"
BSE_LIST_SCRIPS_PAGE_URL = "https://www.bseindia.com/corporates/List_Scrips.aspx"
BSE_LIST_SCRIPS_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
FRED_WTI_HISTORY_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"
NIFTY_TICKER = "^NSEI"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
SCREENER_URL = "https://www.screener.in/company"
SCREENER_BASE_URL = "https://www.screener.in"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
FUNDAMENTALS_CACHE_VERSION = 10
SNAPSHOT_CACHE_VERSION = 12
CHART_CACHE_VERSION = 4
IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN_MINUTES_IST = (9 * 60) + 15
MARKET_CLOSE_MINUTES_IST = (15 * 60) + 30
LIVE_SNAPSHOT_MAX_AGE_SECONDS = 120
MAX_ACCEPTABLE_SESSION_PRICE_RATIO = 5.0
MAX_ACCEPTABLE_DAILY_CLOSE_JUMP_RATIO = 5.0
SNAPSHOT_SESSION_RANGE_BUFFER_RATIO = 0.25
SNAPSHOT_SESSION_FALLBACK_RATIO = 2.0
QUOTE_DOWNLOAD_BATCH_SIZE = 200
NSE_DIRECT_QUOTE_FALLBACK_MAX_SYMBOLS = 60
UNIVERSE_CACHE_VERSION = 3
UNIVERSE_FORCE_REFRESH_MAX_AGE_HOURS = 2
SNAPSHOT_HISTORY_PERIOD = "3y"
RELIABLE_HISTORY_SOURCES = {"history", "chart_cache", "legacy_chart_cache"}
MACRO_CHART_SYMBOLS = {"CL=F", "BZ=F", "^NSEI", "^CNXSC", "^NSEMDCP50", "^GSPC", "^IXIC", "^DJI", "SPY", "QQQ", "DIA"}
RS_LOOKBACKS: tuple[tuple[int, float], ...] = ((63, 0.4), (126, 0.2), (189, 0.2), (252, 0.2))
RETURN_1W_BARS = 5
RETURN_1M_BARS = 21
RETURN_5M_BARS = RETURN_1M_BARS * 5
RETURN_3M_BARS = 63
RETURN_6M_BARS = 126
RETURN_9M_BARS = 189
RETURN_1Y_BARS = 252
RETURN_2Y_BARS = 504
MIN_ADAPTIVE_RS_HISTORY_BARS = RETURN_3M_BARS
MATERIAL_UPDATE_KEYWORDS = (
    "revenue",
    "sales",
    "profit",
    "margin",
    "ebitda",
    "earnings",
    "guidance",
    "outlook",
    "demand",
    "order",
    "contract",
    "deal",
    "approval",
    "capacity",
    "plant",
    "capex",
    "expansion",
    "acquisition",
    "merger",
    "commissioning",
    "launch",
    "pricing",
    "market share",
    "bookings",
    "pipeline",
    "cost",
    "operating leverage",
    "roce",
    "roe",
)
MANAGEMENT_HIGHLIGHT_KEYWORDS = (
    "revenue",
    "growth",
    "margin",
    "profit",
    "sales",
    "demand",
    "capacity",
    "order",
    "book",
    "pricing",
    "ebitda",
    "capex",
    "guidance",
    "outlook",
)
SOURCE_HIGHLIGHT_SKIP_PHRASES = (
    "safe harbor",
    "forward-looking statements",
    "for further details",
    "audio recording",
    "operator",
    "moderator",
    "question-and-answer session",
    "question and answer session",
    "thank you",
    "ladies and gentlemen",
    "good day",
)
MANUAL_SECTOR_OVERRIDES: dict[str, dict[str, str]] = {
    "LTIM": {
        "sector": "Information Technology",
        "sub_sector": "Computers - Software & Consulting",
    },
    "SEQUENT": {
        "sector": "Healthcare",
        "sub_sector": "Pharmaceuticals",
    },
    "INFIBEAM": {
        "sector": "Financial Services",
        "sub_sector": "Payment Services",
    },
    "SABTNL": {
        "sector": "Media Entertainment & Publication",
        "sub_sector": "Television Broadcasting & Content",
    },
    "HEUBACHIND": {
        "sector": "Chemicals",
        "sub_sector": "Specialty Chemicals",
    },
    "ARISINFRA": {
        "sector": "Construction Materials",
        "sub_sector": "Construction Material Distribution",
    },
    "MEGASOFT": {
        "sector": "Capital Goods",
        "sub_sector": "Aerospace & Defense",
    },
}

YAHOO_SECTOR_ALIASES: dict[str, str] = {
    "Basic Materials": "Materials",
    "Communication Services": "Telecommunication",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Technology": "Information Technology",
}

INDEX_SYMBOL_TO_NSE_NAME = {
    "^NSEI": "NIFTY 50",
    "^CNX500": "NIFTY 500",
    "^CNXSC": "NIFTY SMALLCAP 250",
    "^NSEMDCP50": "NIFTY MIDCAP 50",
    "^NSEBANK": "NIFTY BANK",
    "^CNXIT": "NIFTY IT",
    "^CNXAUTO": "NIFTY AUTO",
    "^CNXFMCG": "NIFTY FMCG",
    "^CNXPHARMA": "NIFTY PHARMA",
    "^CNXMETAL": "NIFTY METAL",
    "^CNXREALTY": "NIFTY REALTY",
}


class FreeMarketDataProvider:
    def __init__(self, gemini_api_key: str | None = None) -> None:
        self.backend_root = Path(__file__).resolve().parents[2]
        self.universe_cache_path = self.backend_root / "data" / "free_universe.json"
        self.snapshot_cache_path = self.backend_root / "data" / "free_snapshots.json"
        self.company_metadata_path = self.backend_root / "data" / "free_company_metadata.json"
        self.fundamentals_cache_path = self.backend_root / "data" / "free_fundamentals.json"
        self.bulk_fundamentals_cache_path = self.backend_root / "data" / "free_fundamental_cache.json"
        self.historical_breadth_cache_path = self.backend_root / "data" / "free_historical_breadth.json"
        self.chart_cache_dir = self.backend_root / "data" / "chart_cache"
        self.chart_cache_dir.mkdir(parents=True, exist_ok=True)
        self._snapshots_memory_cache: dict[float, tuple[float, float, list[StockSnapshot]]] = {}
        self._snapshot_request_tasks: dict[float, asyncio.Task[list[StockSnapshot]]] = {}
        self._background_snapshot_refresh_tasks: dict[float, asyncio.Task[list[StockSnapshot]]] = {}
        self._live_snapshot_refresh_tasks: dict[float, asyncio.Task[list[StockSnapshot]]] = {}
        self._fundamentals_memory_cache: dict[str, CompanyFundamentals] = {}
        self._bulk_fundamentals_memory_cache: dict[str, dict[str, Any]] | None = None
        self._chart_symbol_scale_cache: tuple[float, dict[str, dict[str, Any]]] | None = None
        self._holiday_date_cache: dict[tuple[int, ...], set[date]] = {}
        self._last_refresh_metadata = self._default_refresh_metadata()
        self.demo = DemoMarketDataProvider()
        self.ai_service = AIAnalysisService(api_key=gemini_api_key, cache_dir=self.backend_root / "data")

    def _benchmark_symbol(self) -> str:
        return NIFTY_TICKER

    def _benchmark_label(self) -> str:
        return "Nifty 50"

    def _default_exchange(self) -> str:
        return "NSE"

    def _should_fetch_metadata_for_item(self, item: dict[str, Any]) -> bool:
        return str(item.get("exchange") or "").upper() == "NSE"

    @staticmethod
    def _default_refresh_metadata() -> dict[str, Any]:
        return {
            "applied_quote_count": 0,
            "historical_rebuild": False,
            "quote_source": None,
        }

    @staticmethod
    def _merge_quote_sources(sources: set[str]) -> str | None:
        normalized = {str(source).strip().lower() for source in sources if str(source).strip()}
        if not normalized:
            return None
        if len(normalized) == 1:
            return next(iter(normalized))
        return "mixed"

    def get_last_refresh_metadata(self) -> dict[str, Any]:
        return dict(self._last_refresh_metadata)

    @staticmethod
    def _price_ratio(left: Any, right: Any) -> float | None:
        try:
            left_value = abs(float(left))
            right_value = abs(float(right))
        except (TypeError, ValueError):
            return None
        if left_value <= 0 or right_value <= 0:
            return None
        return max(left_value / right_value, right_value / left_value)

    def _row_has_sane_session_price_scale(self, row: dict[str, Any]) -> bool:
        ratio = self._price_ratio(row.get("last_price"), row.get("previous_close"))
        return ratio is None or ratio <= MAX_ACCEPTABLE_SESSION_PRICE_RATIO

    def _history_has_sane_price_scale(self, history: pd.DataFrame) -> bool:
        if history.empty or "Close" not in history.columns:
            return False

        close = pd.to_numeric(history["Close"], errors="coerce").replace(0, pd.NA).dropna()
        if len(close) < 2:
            return True

        previous_close = close.shift(1)
        ratios = pd.concat([(close / previous_close), (previous_close / close)], axis=1).max(axis=1)
        ratios = ratios.replace([float("inf"), float("-inf")], pd.NA).dropna()
        return ratios.empty or float(ratios.max()) <= MAX_ACCEPTABLE_DAILY_CLOSE_JUMP_RATIO

    def _live_quote_matches_cached_scale(self, cached_price: Any, live_price: Any) -> bool:
        ratio = self._price_ratio(cached_price, live_price)
        return ratio is None or ratio <= MAX_ACCEPTABLE_SESSION_PRICE_RATIO

    @staticmethod
    def _normalize_classification_label(value: Any) -> str:
        label = str(value or "").strip()
        if not label or label.upper() in {"NA", "N/A", "NONE", "NULL", "NIL", "-"}:
            return "Unclassified"
        return label

    @classmethod
    def _normalize_sector_label(cls, value: Any) -> str:
        label = cls._normalize_classification_label(value)
        return YAHOO_SECTOR_ALIASES.get(label, label)

    @staticmethod
    def _current_ist_date() -> date:
        return datetime.now(timezone.utc).astimezone(IST).date()

    def _market_holiday_dates(self, years: tuple[int, ...]) -> set[date]:
        cached = self._holiday_date_cache.get(years)
        if cached is not None:
            return cached
        calendar = holidays.country_holidays("IN", years=list(years))
        holiday_dates = {holiday_date for holiday_date in calendar.keys()}
        self._holiday_date_cache[years] = holiday_dates
        return holiday_dates

    def _is_market_holiday(self, target: date | None = None) -> bool:
        trading_date = target or self._current_ist_date()
        years = tuple(sorted({trading_date.year - 1, trading_date.year, trading_date.year + 1}))
        return trading_date in self._market_holiday_dates(years)

    def _current_or_previous_trading_day_ist(self) -> date:
        trading_date = self._current_ist_date()
        while not self._is_trading_day_ist(trading_date):
            trading_date -= timedelta(days=1)
        return trading_date

    def _is_trading_day_ist(self, target: date | None = None) -> bool:
        trading_date = target or self._current_ist_date()
        return trading_date.weekday() < 5 and not self._is_market_holiday(trading_date)

    @staticmethod
    def _parse_row_date(value: Any) -> date | None:
        if value in (None, ""):
            return None
        try:
            return date.fromisoformat(str(value))
        except ValueError:
            return None

    async def get_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        cache_key = float(market_cap_min_crore)
        cache_signature = self._snapshot_memory_signature()
        cached = self._snapshots_memory_cache.get(cache_key)
        if cached and cached[0] == cache_signature[0] and cached[1] == cache_signature[1]:
            return cached[2]

        cached_rows = await asyncio.to_thread(self._load_cached_snapshot_rows, market_cap_min_crore)
        if cached_rows:
            snapshots = await asyncio.to_thread(self._materialize_snapshot_rows, cached_rows)
            self._snapshots_memory_cache[cache_key] = (*self._snapshot_memory_signature(), snapshots)
            return snapshots

        return await self._get_or_create_snapshot_request_task(cache_key, market_cap_min_crore)

    async def get_index_quotes(self, symbols: list[str]) -> list[IndexQuoteItem]:
        if not symbols:
            return []

        normalized_symbols = [symbol.strip() for symbol in symbols if symbol and symbol.strip()]
        if not normalized_symbols:
            return []

        try:
            items = await asyncio.to_thread(self._fetch_index_quotes, normalized_symbols)
        except Exception:
            items = []

        items_by_symbol = {item.symbol.upper(): item for item in items}
        missing_symbols = [symbol for symbol in normalized_symbols if symbol.upper() not in items_by_symbol]
        if missing_symbols:
            try:
                fallback_items = await self.demo.get_index_quotes(missing_symbols)
            except Exception:
                fallback_items = []
            for item in fallback_items:
                items_by_symbol[item.symbol.upper()] = item

        return [items_by_symbol[symbol.upper()] for symbol in normalized_symbols if symbol.upper() in items_by_symbol]

    async def refresh_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        self._snapshots_memory_cache.clear()
        self._snapshot_request_tasks.clear()
        self._background_snapshot_refresh_tasks.clear()
        self._live_snapshot_refresh_tasks.clear()
        if not self._is_market_open_ist():
            self._fundamentals_memory_cache.clear()
            if self.fundamentals_cache_path.exists():
                self.fundamentals_cache_path.unlink(missing_ok=True)
        try:
            payload = await asyncio.to_thread(self._load_or_refresh_snapshots, market_cap_min_crore, True)
        except Exception:
            cached_rows = self._load_json_rows(self.snapshot_cache_path)
            if not cached_rows or not self._snapshot_schema_ok(cached_rows):
                raise
            payload = [row for row in cached_rows if float(row.get("market_cap_crore", 0) or 0) >= market_cap_min_crore]
            self._last_refresh_metadata = self._default_refresh_metadata()
        snapshots = await asyncio.to_thread(self._materialize_snapshot_rows, payload)
        self._snapshots_memory_cache[float(market_cap_min_crore)] = (*self._snapshot_memory_signature(), snapshots)
        return snapshots

    async def refresh_live_snapshots(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        cache_key = float(market_cap_min_crore)

        background_task = self._background_snapshot_refresh_tasks.get(cache_key)
        if background_task is not None and not background_task.done():
            return await background_task

        current_task = self._live_snapshot_refresh_tasks.get(cache_key)
        if current_task is not None:
            return await current_task

        task = asyncio.create_task(self._refresh_live_snapshots_internal(market_cap_min_crore))
        self._live_snapshot_refresh_tasks[cache_key] = task

        def clear_task(completed_task: asyncio.Task[list[StockSnapshot]]) -> None:
            if self._live_snapshot_refresh_tasks.get(cache_key) is completed_task:
                self._live_snapshot_refresh_tasks.pop(cache_key, None)

        task.add_done_callback(clear_task)
        return await task

    def get_snapshot_updated_at(self) -> datetime | None:
        if not self.snapshot_cache_path.exists():
            return None
        return datetime.fromtimestamp(self.snapshot_cache_path.stat().st_mtime, tz=timezone.utc)

    def _load_valid_cached_snapshot_rows(self) -> list[dict[str, Any]]:
        rows = self._load_json_rows(self.snapshot_cache_path)
        if not rows or not self._snapshot_schema_ok(rows):
            return []
        normalized_rows, changed = self._normalize_cached_snapshot_rows(rows)
        if changed:
            self._write_snapshot_rows(normalized_rows)
        return normalized_rows

    def _load_cached_snapshot_rows(self, market_cap_min_crore: float) -> list[dict[str, Any]]:
        rows = self._load_valid_cached_snapshot_rows()
        return [row for row in rows if float(row.get("market_cap_crore", 0) or 0) >= market_cap_min_crore]

    def _materialize_snapshot_rows(self, rows: list[dict[str, Any]]) -> list[StockSnapshot]:
        return [StockSnapshot.model_validate(self._with_snapshot_fallbacks(row)) for row in rows]

    async def _load_snapshots_with_fallback(
        self,
        market_cap_min_crore: float,
        force_refresh: bool = False,
    ) -> list[StockSnapshot]:
        cache_key = float(market_cap_min_crore)
        try:
            payload = await asyncio.to_thread(self._load_or_refresh_snapshots, market_cap_min_crore, force_refresh)
            snapshots = await asyncio.to_thread(self._materialize_snapshot_rows, payload)
            self._snapshots_memory_cache[cache_key] = (*self._snapshot_memory_signature(), snapshots)
            return snapshots
        except Exception:
            cached_rows = await asyncio.to_thread(self._load_cached_snapshot_rows, market_cap_min_crore)
            if cached_rows:
                snapshots = await asyncio.to_thread(self._materialize_snapshot_rows, cached_rows)
                self._snapshots_memory_cache[cache_key] = (*self._snapshot_memory_signature(), snapshots)
                return snapshots
            return await self.demo.get_snapshots(market_cap_min_crore)

    async def _get_or_create_snapshot_request_task(
        self,
        cache_key: float,
        market_cap_min_crore: float,
        *,
        force_refresh: bool = False,
    ) -> list[StockSnapshot]:
        current_task = self._snapshot_request_tasks.get(cache_key)
        if current_task is not None:
            snapshots = await current_task
            if force_refresh and self._market_close_refresh_due():
                return await self._load_snapshots_with_fallback(market_cap_min_crore, True)
            return snapshots

        task = asyncio.create_task(self._load_snapshots_with_fallback(market_cap_min_crore, force_refresh))
        self._snapshot_request_tasks[cache_key] = task

        def clear_task(completed_task: asyncio.Task[list[StockSnapshot]]) -> None:
            if self._snapshot_request_tasks.get(cache_key) is completed_task:
                self._snapshot_request_tasks.pop(cache_key, None)

        task.add_done_callback(clear_task)
        return await task

    async def _refresh_live_snapshots_internal(self, market_cap_min_crore: float) -> list[StockSnapshot]:
        cache_key = float(market_cap_min_crore)
        self._snapshots_memory_cache.clear()

        if not self._is_market_open_ist():
            self._last_refresh_metadata = self._default_refresh_metadata()
            return await self.get_snapshots(market_cap_min_crore)

        cached_rows = await asyncio.to_thread(self._load_valid_cached_snapshot_rows)
        if not cached_rows:
            return await self.refresh_snapshots(market_cap_min_crore)

        try:
            refreshed_rows, refresh_metadata = await asyncio.to_thread(self._refresh_snapshot_rows_live, cached_rows)
            self._last_refresh_metadata = self._default_refresh_metadata()
            self._last_refresh_metadata.update(refresh_metadata)

            active_rows = refreshed_rows
            if int(refresh_metadata.get("applied_quote_count", 0) or 0) > 0:
                await asyncio.to_thread(self._write_snapshot_rows, refreshed_rows)
            else:
                active_rows = cached_rows

            filtered_rows = [
                row
                for row in active_rows
                if float(row.get("market_cap_crore", 0) or 0) >= market_cap_min_crore
            ]
            snapshots = await asyncio.to_thread(self._materialize_snapshot_rows, filtered_rows)
            self._snapshots_memory_cache[cache_key] = (*self._snapshot_memory_signature(), snapshots)
            return snapshots
        except Exception:
            return await self.get_snapshots(market_cap_min_crore)

    def _schedule_background_snapshot_refresh(
        self,
        cache_key: float,
        market_cap_min_crore: float,
        *,
        force_refresh: bool = False,
    ) -> None:
        if self._snapshot_request_tasks.get(cache_key) is not None:
            return

        current_task = self._background_snapshot_refresh_tasks.get(cache_key)
        if current_task is not None and not current_task.done():
            current_force_refresh = bool(getattr(current_task, "_force_refresh", False))
            if not force_refresh or current_force_refresh:
                return
            current_task.cancel()

        async def refresh() -> list[StockSnapshot]:
            return await self._load_snapshots_with_fallback(market_cap_min_crore, force_refresh)

        task = asyncio.create_task(refresh())
        setattr(task, "_force_refresh", force_refresh)
        self._background_snapshot_refresh_tasks[cache_key] = task

        def clear_task(completed_task: asyncio.Task[list[StockSnapshot]]) -> None:
            if self._background_snapshot_refresh_tasks.get(cache_key) is completed_task:
                self._background_snapshot_refresh_tasks.pop(cache_key, None)

        task.add_done_callback(clear_task)

    def _chart_cache_path(self, symbol: str, timeframe: str) -> Path:
        safe_symbol = re.sub(r"[^A-Za-z0-9._-]+", "_", symbol.upper())
        safe_timeframe = re.sub(r"[^A-Za-z0-9._-]+", "_", timeframe.upper())
        return self.chart_cache_dir / f"{safe_symbol}__{safe_timeframe}.json"

    def _chart_symbol_scale_lookup(self) -> dict[str, dict[str, Any]]:
        snapshot_mtime = self.snapshot_cache_path.stat().st_mtime if self.snapshot_cache_path.exists() else -1.0
        cached = self._chart_symbol_scale_cache
        if cached and cached[0] == snapshot_mtime:
            return cached[1]

        snapshot_file_session_date = (
            datetime.fromtimestamp(snapshot_mtime, tz=timezone.utc).astimezone(self._market_timezone()).date()
            if snapshot_mtime > 0
            else None
        )
        latest_closed_session_date = self._latest_clock_closed_market_session_date()
        normalized_rows, _ = self._normalize_cached_snapshot_rows(self._load_json_rows(self.snapshot_cache_path))
        lookup: dict[str, dict[str, Any]] = {}
        for row in normalized_rows:
            if not isinstance(row, dict) or not self._row_has_sane_session_price_scale(row):
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            stored_session_date = self._parse_row_date(row.get("history_session_date"))
            effective_session_date = stored_session_date
            if (
                snapshot_file_session_date is not None
                and self._is_trading_day_ist(snapshot_file_session_date)
                and snapshot_file_session_date <= latest_closed_session_date
                and (effective_session_date is None or snapshot_file_session_date > effective_session_date)
            ):
                effective_session_date = snapshot_file_session_date
            lookup[symbol] = {
                "last_price": self._to_float(row.get("last_price")),
                "previous_close": self._to_float(row.get("previous_close")),
                "day_high": self._to_float(row.get("day_high")),
                "day_low": self._to_float(row.get("day_low")),
                "volume": self._to_float(row.get("volume")),
                "history_session_date": effective_session_date.isoformat() if effective_session_date else None,
                "history_source": str(row.get("history_source") or "").strip().lower() or None,
                "instrument_key": str(row.get("instrument_key") or row.get("ticker") or "").strip().upper() or None,
            }

        self._chart_symbol_scale_cache = (snapshot_mtime, lookup)
        return lookup

    def _chart_cache_identity_matches(self, symbol: str, timeframe: str, payload: dict[str, Any]) -> bool:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_timeframe = str(timeframe or "").strip().upper()

        cached_symbol = str(payload.get("symbol") or "").strip().upper()
        if cached_symbol and cached_symbol != normalized_symbol:
            return False

        cached_timeframe = str(payload.get("timeframe") or "").strip().upper()
        if cached_timeframe and cached_timeframe != normalized_timeframe:
            return False

        cached_ticker = str(payload.get("ticker") or "").strip().upper()
        expected_ticker = str(
            (self._chart_symbol_scale_lookup().get(normalized_symbol) or {}).get("instrument_key") or ""
        ).strip().upper()
        if cached_ticker and expected_ticker and cached_ticker != expected_ticker:
            return False

        return True

    def _chart_bars_match_symbol_scale(self, symbol: str, bars: list[ChartBar]) -> bool:
        if not bars:
            return True

        latest_close = self._to_float(getattr(bars[-1], "close", None))
        if latest_close in (None, 0):
            return False

        scale = self._chart_symbol_scale_lookup().get(str(symbol or "").strip().upper()) or {}
        references = [
            self._to_float(scale.get("last_price")),
            self._to_float(scale.get("previous_close")),
        ]
        references = [reference for reference in references if reference not in (None, 0)]
        if not references:
            return True

        if not any(self._live_quote_matches_cached_scale(reference, latest_close) for reference in references):
            return False

        return self._bar_matches_snapshot_session(symbol, bars[-1])

    def _chart_cache_covers_snapshot_session(self, symbol: str, timeframe: str, bars: list[ChartBar]) -> bool:
        if not bars or str(timeframe or "").strip().upper() not in {"1D", "1W"}:
            return True

        snapshot_session_date = self._snapshot_session_date(symbol)
        if snapshot_session_date is None:
            return True

        return self._chart_bar_trade_date(bars[-1]) >= snapshot_session_date

    @staticmethod
    def _history_source_is_reliable(value: Any) -> bool:
        return str(value or "").strip().lower() in RELIABLE_HISTORY_SOURCES

    def _normalize_snapshot_volume_baselines(self, row: dict[str, Any]) -> dict[str, Any]:
        if self._history_source_is_reliable(row.get("history_source")):
            return row
        return {
            **row,
            "avg_volume_20d": 0,
            "avg_volume_30d": 0,
            "avg_volume_50d": 0,
            "recent_volumes": [],
        }

    def _snapshot_session_bounds(self, symbol: str) -> tuple[float | None, float | None]:
        scale = self._chart_symbol_scale_lookup().get(str(symbol or "").strip().upper()) or {}
        day_low = self._to_float(scale.get("day_low"))
        day_high = self._to_float(scale.get("day_high"))
        references = [
            day_low,
            day_high,
            self._to_float(scale.get("last_price")),
            self._to_float(scale.get("previous_close")),
        ]
        references = [reference for reference in references if reference not in (None, 0)]
        if not references:
            return None, None

        lower = min(references)
        upper = max(references)
        if day_low not in (None, 0) and day_high not in (None, 0):
            return (
                lower * (1 - SNAPSHOT_SESSION_RANGE_BUFFER_RATIO),
                upper * (1 + SNAPSHOT_SESSION_RANGE_BUFFER_RATIO),
            )
        return (lower / SNAPSHOT_SESSION_FALLBACK_RATIO, upper * SNAPSHOT_SESSION_FALLBACK_RATIO)

    def _snapshot_session_date(self, symbol: str) -> date | None:
        scale = self._chart_symbol_scale_lookup().get(str(symbol or "").strip().upper()) or {}
        return self._parse_row_date(scale.get("history_session_date"))

    def _snapshot_rows_session_date(self, rows: list[dict[str, Any]]) -> date | None:
        session_dates = [self._parse_row_date(row.get("history_session_date")) for row in rows]
        session_dates = [session_date for session_date in session_dates if session_date is not None]
        if not session_dates:
            return None
        return max(session_dates)

    def _latest_available_chart_session_date(self, symbol: str) -> date | None:
        try:
            cached_bars = self._read_chart_cache(symbol, "1D", 10)
        except Exception:
            cached_bars = []
        if cached_bars:
            return self._chart_bar_trade_date(cached_bars[-1])

        try:
            fetched_bars = self._fetch_chart_bars(symbol, "1D", 10)
        except Exception:
            fetched_bars = []
        if fetched_bars:
            return self._chart_bar_trade_date(fetched_bars[-1])

        cached_rows = self._load_valid_cached_snapshot_rows()
        return self._snapshot_rows_session_date(cached_rows)

    def _latest_completed_market_session_date(self) -> date:
        market_timezone = self._market_timezone()
        market_now = datetime.now(timezone.utc).astimezone(market_timezone)
        session_date = market_now.date()
        refresh_cutoff = datetime.combine(session_date, datetime.min.time(), tzinfo=market_timezone) + timedelta(
            minutes=self._market_close_minutes() + self._post_close_refresh_grace_minutes()
        )

        if not self._is_trading_day_ist(session_date) or market_now < refresh_cutoff:
            return self._previous_trading_day(session_date)

        latest_chart_session_date = self._latest_available_chart_session_date(self._benchmark_symbol())
        if latest_chart_session_date is not None:
            return latest_chart_session_date

        return self._previous_trading_day(session_date)

    @staticmethod
    def _chart_bar_trade_date(bar: ChartBar) -> date:
        timestamp = datetime.fromtimestamp(int(bar.time), tz=timezone.utc)
        if timestamp.hour == 0 and timestamp.minute == 0 and timestamp.second == 0:
            return timestamp.date()
        return timestamp.astimezone(IST).date()

    def _values_fit_snapshot_session(self, symbol: str, values: list[Any]) -> bool:
        lower, upper = self._snapshot_session_bounds(symbol)
        if lower is None or upper is None:
            return True
        normalized_values = [self._to_float(value) for value in values]
        normalized_values = [value for value in normalized_values if value not in (None, 0)]
        if not normalized_values:
            return True
        return all(lower <= value <= upper for value in normalized_values)

    def _quote_matches_snapshot_session(self, symbol: str, quote: dict[str, Any] | None) -> bool:
        return self._values_fit_snapshot_session(
            symbol,
            [
                self._quote_number(quote, "regularMarketPrice"),
                self._quote_number(quote, "regularMarketDayHigh"),
                self._quote_number(quote, "regularMarketDayLow"),
            ],
        )

    def _bar_matches_snapshot_session(self, symbol: str, bar: ChartBar) -> bool:
        snapshot_session_date = self._snapshot_session_date(symbol)
        if snapshot_session_date is None:
            return True
        bar_date = self._chart_bar_trade_date(bar)
        if bar_date != snapshot_session_date:
            return True
        return self._values_fit_snapshot_session(symbol, [bar.open, bar.high, bar.low, bar.close])

    def _snapshot_session_bar(self, symbol: str) -> ChartBar | None:
        scale = self._chart_symbol_scale_lookup().get(str(symbol or "").strip().upper()) or {}
        close_value = self._to_float(scale.get("last_price"))
        if close_value in (None, 0):
            return None

        session_date = self._parse_row_date(scale.get("history_session_date")) or self._current_or_previous_trading_day_ist()
        open_value = self._to_float(scale.get("previous_close")) or close_value
        high_value = self._to_float(scale.get("day_high")) or max(open_value, close_value)
        low_value = self._to_float(scale.get("day_low")) or min(open_value, close_value)
        if high_value >= low_value:
            open_value = min(max(open_value, low_value), high_value)
        volume_value = int(self._to_float(scale.get("volume")) or 0)
        timestamp = int(datetime.combine(session_date, datetime.min.time(), tzinfo=timezone.utc).timestamp())
        return ChartBar(
            time=timestamp,
            open=round(float(open_value), 2),
            high=round(float(max(high_value, open_value, close_value)), 2),
            low=round(float(min(low_value, open_value, close_value)), 2),
            close=round(float(close_value), 2),
            volume=volume_value,
        )

    def _patch_daily_chart_cache_with_snapshot(self, symbol: str) -> None:
        cached_bars = self._read_chart_cache(symbol, "1D", 520)
        if not cached_bars:
            return

        snapshot_bar = self._snapshot_session_bar(symbol)
        if snapshot_bar is None or not self._bar_matches_snapshot_session(symbol, snapshot_bar):
            return

        patched_bars = list(cached_bars)
        snapshot_date = self._chart_bar_trade_date(snapshot_bar)
        last_bar_date = self._chart_bar_trade_date(patched_bars[-1])
        if last_bar_date == snapshot_date:
            patched_bars[-1] = snapshot_bar
        else:
            patched_bars.append(snapshot_bar)

        self._write_chart_cache(symbol, "1D", patched_bars[-520:])

    def _apply_session_bar_to_daily_history(self, history: pd.DataFrame, bar: ChartBar) -> pd.DataFrame:
        patched = history.copy()
        trade_date = self._chart_bar_trade_date(bar)
        row_payload = {
            "Open": float(bar.open),
            "High": float(bar.high),
            "Low": float(bar.low),
            "Close": float(bar.close),
            "Adj Close": float(bar.close),
            "Volume": int(bar.volume),
            "Stock Splits": 0.0,
        }

        if isinstance(patched.index, pd.DatetimeIndex) and patched.index.tz is not None:
            trade_index = pd.Timestamp(datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc))
            last_trade_date = patched.index[-1].astimezone(IST).date() if len(patched.index) else None
        else:
            trade_index = pd.Timestamp(trade_date)
            last_trade_date = patched.index[-1].date() if len(patched.index) else None

        if last_trade_date == trade_date and len(patched.index):
            for column, value in row_payload.items():
                patched.at[patched.index[-1], column] = value
        else:
            patched.loc[trade_index] = row_payload

        return patched[~patched.index.duplicated(keep="last")].sort_index()

    def _apply_snapshot_row_to_daily_history(self, symbol: str, history: pd.DataFrame) -> pd.DataFrame:
        snapshot_bar = self._snapshot_session_bar(symbol)
        if snapshot_bar is None or not self._bar_matches_snapshot_session(symbol, snapshot_bar):
            return history
        return self._apply_session_bar_to_daily_history(history, snapshot_bar)

    def _sanitize_live_quote_ohlc(
        self,
        *,
        anchor_close: float | None,
        previous_close: float | None,
        current_price: float,
        session_open: float | None,
        session_high: float | None,
        session_low: float | None,
    ) -> tuple[float, float, float]:
        reference = anchor_close or previous_close or current_price
        open_value = (
            float(session_open)
            if session_open not in (None, 0) and self._live_quote_matches_cached_scale(reference, session_open)
            else float(previous_close or current_price)
        )
        high_value = (
            float(session_high)
            if session_high not in (None, 0) and self._live_quote_matches_cached_scale(reference, session_high)
            else float(max(open_value, current_price))
        )
        low_value = (
            float(session_low)
            if session_low not in (None, 0) and self._live_quote_matches_cached_scale(reference, session_low)
            else float(min(open_value, current_price))
        )

        return (
            open_value,
            float(max(high_value, open_value, current_price)),
            float(min(low_value, open_value, current_price)),
        )

    def _write_chart_cache(self, symbol: str, timeframe: str, bars: list[ChartBar]) -> None:
        if not bars:
            return
        payload = {
            "cache_version": CHART_CACHE_VERSION,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "symbol": str(symbol or "").strip().upper(),
            "timeframe": str(timeframe or "").strip().upper(),
            "ticker": str(self._resolve_ticker(symbol)).strip().upper(),
            "bars": [bar.model_dump(mode="json") for bar in bars],
        }
        self._chart_cache_path(symbol, timeframe).write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _read_chart_cache(
        self,
        symbol: str,
        timeframe: str,
        bars: int,
        *,
        allow_legacy: bool = False,
    ) -> list[ChartBar]:
        path = self._chart_cache_path(symbol, timeframe)
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        if isinstance(payload, list):
            return []
        if not isinstance(payload, dict):
            return []
        cache_version = int(payload.get("cache_version", 0) or 0)
        if cache_version != CHART_CACHE_VERSION and not allow_legacy:
            return []
        if not self._chart_cache_identity_matches(symbol, timeframe, payload):
            return []
        bar_payload = payload.get("bars")
        if not isinstance(bar_payload, list):
            return []
        try:
            cached_bars = [ChartBar.model_validate(item) for item in bar_payload]
        except Exception:
            return []
        if str(timeframe or "").strip().upper() == "1D" and cached_bars and not self._is_trading_day_ist(
            self._chart_bar_trade_date(cached_bars[-1])
        ):
            return []
        if not self._chart_bars_match_symbol_scale(symbol, cached_bars):
            return []
        return cached_bars[-bars:]

    def _seed_daily_chart_cache(self, symbol: str, history: pd.DataFrame) -> None:
        series = history.dropna(subset=["Close"]).tail(520)
        if series.empty:
            return
        self._write_chart_cache(symbol, "1D", self._history_to_chart_bars(series))

    def _patch_daily_chart_cache_with_quote(self, symbol: str, quote: dict[str, Any]) -> None:
        cached_bars = self._read_chart_cache(symbol, "1D", 520)
        if not cached_bars:
            return

        quote_updated_at = self._quote_updated_at_from_quote(quote)
        if quote_updated_at is None:
            return
        if not self._quote_matches_snapshot_session(symbol, quote):
            self._patch_daily_chart_cache_with_snapshot(symbol)
            return
        current_price = self._quote_number(quote, "regularMarketPrice")
        if current_price in (None, 0):
            return
        cached_close = self._to_float(getattr(cached_bars[-1], "close", None))
        if not self._live_quote_matches_cached_scale(cached_close, current_price):
            return

        previous_close = self._quote_number(quote, "regularMarketPreviousClose") or current_price
        session_open, session_high, session_low = self._sanitize_live_quote_ohlc(
            anchor_close=cached_close,
            previous_close=self._to_float(previous_close),
            current_price=float(current_price),
            session_open=self._quote_number(quote, "regularMarketOpen"),
            session_high=self._quote_number(quote, "regularMarketDayHigh"),
            session_low=self._quote_number(quote, "regularMarketDayLow"),
        )
        session_volume = int(self._quote_number(quote, "regularMarketVolume") or 0)
        trade_date = quote_updated_at.astimezone(IST).date()
        trade_timestamp = int(datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc).timestamp())

        next_bar = ChartBar(
            time=trade_timestamp,
            open=round(float(session_open), 2),
            high=round(float(max(session_high, current_price, session_open)), 2),
            low=round(float(min(session_low, current_price, session_open)), 2),
            close=round(float(current_price), 2),
            volume=session_volume,
        )

        patched_bars = list(cached_bars)
        last_bar_date = self._chart_bar_trade_date(patched_bars[-1])
        if last_bar_date == trade_date:
            previous_bar = patched_bars[-1]
            patched_bars[-1] = ChartBar(
                time=trade_timestamp,
                open=round(float(previous_bar.open if previous_bar.open not in (None, 0) else session_open), 2),
                high=round(float(max(previous_bar.high, next_bar.high)), 2),
                low=round(float(min(previous_bar.low, next_bar.low)), 2),
                close=next_bar.close,
                volume=session_volume,
            )
        else:
            patched_bars.append(next_bar)

        self._write_chart_cache(symbol, "1D", patched_bars[-520:])

    def _snapshot_memory_signature(self) -> tuple[float, float]:
        snapshot_mtime = self.snapshot_cache_path.stat().st_mtime if self.snapshot_cache_path.exists() else -1.0
        metadata_mtime = self.company_metadata_path.stat().st_mtime if self.company_metadata_path.exists() else -1.0
        return (snapshot_mtime, metadata_mtime)

    def _get_bulk_fundamentals(self) -> dict[str, dict[str, Any]]:
        if self._bulk_fundamentals_memory_cache is not None:
            return self._bulk_fundamentals_memory_cache
        if not self.bulk_fundamentals_cache_path.exists():
            self._bulk_fundamentals_memory_cache = {}
            return self._bulk_fundamentals_memory_cache
        try:
            payload = json.loads(self.bulk_fundamentals_cache_path.read_text(encoding="utf-8"))
            self._bulk_fundamentals_memory_cache = payload if isinstance(payload, dict) else {}
        except Exception:
            self._bulk_fundamentals_memory_cache = {}
        return self._bulk_fundamentals_memory_cache

    def _with_snapshot_fallbacks(self, row: dict[str, Any]) -> dict[str, Any]:
        symbol = str(row.get("symbol") or "").upper()
        funds = self._get_bulk_fundamentals().get(symbol) or {}
        if funds:
            for k, v in funds.items():
                if k not in row and k != "symbol":
                    row[k] = v
        
        if "rs_rating_1m_ago" in row:
            return self._normalize_snapshot_volume_baselines({
                **row,
                "previous_close": row.get("previous_close", row.get("last_price")),
                "circuit_band_label": row.get("circuit_band_label"),
                "upper_circuit_limit": row.get("upper_circuit_limit"),
                "lower_circuit_limit": row.get("lower_circuit_limit"),
                "rs_rating_1d_ago": row.get("rs_rating_1d_ago", row.get("rs_rating", 0)),
                "rs_rating_1w_ago": row.get("rs_rating_1w_ago", row.get("rs_rating", 0)),
                "stock_return_12m_1d_ago": row.get("stock_return_12m_1d_ago", row.get("stock_return_12m", 0)),
                "stock_return_12m_1w_ago": row.get("stock_return_12m_1w_ago", row.get("stock_return_12m", 0)),
                "rs_weighted_score": row.get("rs_weighted_score", row.get("stock_return_12m", 0)),
                "rs_weighted_score_1d_ago": row.get("rs_weighted_score_1d_ago", row.get("stock_return_12m_1d_ago", 0)),
                "rs_weighted_score_1w_ago": row.get("rs_weighted_score_1w_ago", row.get("stock_return_12m_1w_ago", 0)),
                "rs_weighted_score_1m_ago": row.get("rs_weighted_score_1m_ago", row.get("stock_return_12m_1m_ago", 0)),
                "rs_eligible": row.get("rs_eligible", False),
                "rs_eligible_1d_ago": row.get("rs_eligible_1d_ago", False),
                "rs_eligible_1w_ago": row.get("rs_eligible_1w_ago", False),
                "rs_eligible_1m_ago": row.get("rs_eligible_1m_ago", False),
                "recent_highs": row.get("recent_highs", []),
                "recent_lows": row.get("recent_lows", []),
                "recent_volumes": row.get("recent_volumes", []),
                "sma20": row.get("sma20", row.get("ema20")),
                "sma50": row.get("sma50", row.get("ema50")),
                "sma150": row.get("sma150", row.get("sma200", row.get("ema200"))),
                "sma200": row.get("sma200", row.get("ema200")),
                "sma200_1m_ago": row.get("sma200_1m_ago", row.get("sma200", row.get("ema200"))),
                "sma200_5m_ago": row.get("sma200_5m_ago", row.get("sma200_1m_ago", row.get("sma200", row.get("ema200")))),
                "avg_volume_50d": row.get("avg_volume_50d", row.get("avg_volume_30d", row.get("avg_volume_20d", 0))),
                "weekly_ema20": row.get("weekly_ema20", row.get("ema20")),
                "multi_year_high": row.get("multi_year_high", row.get("ath", row.get("high_52w", row.get("last_price", 0)))),
                "high_3y": row.get("high_3y", row.get("multi_year_high", row.get("ath", row.get("high_52w", row.get("last_price", 0))))),
                "stock_return_40d": row.get("stock_return_40d", row.get("stock_return_20d", 0)),
                "stock_return_189d": row.get("stock_return_189d", row.get("stock_return_126d", row.get("stock_return_60d", 0))),
                "stock_return_504d": row.get("stock_return_504d", row.get("stock_return_12m", 0)),
                "atr14": row.get("atr14", 0),
                "adr_pct_20": row.get("adr_pct_20", row.get("long_base_avg_range_pct", row.get("day_range_pct", 0))),
                "long_base_avg_range_pct": row.get("long_base_avg_range_pct", row.get("day_range_pct", 0)),
                "long_base_span_pct": row.get("long_base_span_pct", row.get("pct_from_52w_high", 0)),
                "long_base_window_days": row.get("long_base_window_days", 60),
                "ma200_type": row.get("ma200_type", "ema"),
                "baseline_close_504d": row.get("baseline_close_504d", row.get("baseline_close_252d", row.get("last_price"))),
            })
        return self._normalize_snapshot_volume_baselines({
            **row,
            "previous_close": row.get("previous_close", row.get("last_price")),
            "circuit_band_label": row.get("circuit_band_label"),
            "upper_circuit_limit": row.get("upper_circuit_limit"),
            "lower_circuit_limit": row.get("lower_circuit_limit"),
            "stock_return_12m_1d_ago": row.get("stock_return_12m", 0),
            "stock_return_12m_1w_ago": row.get("stock_return_12m", 0),
            "rs_weighted_score": row.get("stock_return_12m", 0),
            "rs_weighted_score_1d_ago": row.get("stock_return_12m", 0),
            "rs_weighted_score_1w_ago": row.get("stock_return_12m", 0),
            "rs_weighted_score_1m_ago": row.get("stock_return_12m", 0),
            "rs_rating_1m_ago": row.get("rs_rating", 0),
            "rs_rating_1d_ago": row.get("rs_rating", 0),
            "rs_rating_1w_ago": row.get("rs_rating", 0),
            "rs_eligible": False,
            "rs_eligible_1d_ago": False,
            "rs_eligible_1w_ago": False,
            "rs_eligible_1m_ago": False,
            "recent_highs": row.get("recent_highs", []),
            "recent_lows": row.get("recent_lows", []),
            "recent_volumes": row.get("recent_volumes", []),
            "sma20": row.get("sma20", row.get("ema20")),
            "sma50": row.get("sma50", row.get("ema50")),
            "sma150": row.get("sma150", row.get("sma200", row.get("ema200"))),
            "sma200": row.get("sma200", row.get("ema200")),
            "sma200_1m_ago": row.get("sma200_1m_ago", row.get("sma200", row.get("ema200"))),
            "sma200_5m_ago": row.get("sma200_5m_ago", row.get("sma200_1m_ago", row.get("sma200", row.get("ema200")))),
            "avg_volume_50d": row.get("avg_volume_50d", row.get("avg_volume_30d", row.get("avg_volume_20d", 0))),
            "weekly_ema20": row.get("weekly_ema20", row.get("ema20")),
            "multi_year_high": row.get("multi_year_high", row.get("ath", row.get("high_52w", row.get("last_price", 0)))),
            "high_3y": row.get("high_3y", row.get("multi_year_high", row.get("ath", row.get("high_52w", row.get("last_price", 0))))),
            "stock_return_40d": row.get("stock_return_40d", row.get("stock_return_20d", 0)),
            "stock_return_189d": row.get("stock_return_189d", row.get("stock_return_126d", row.get("stock_return_60d", 0))),
            "stock_return_504d": row.get("stock_return_504d", row.get("stock_return_12m", 0)),
            "atr14": row.get("atr14", 0),
            "adr_pct_20": row.get("adr_pct_20", row.get("long_base_avg_range_pct", row.get("day_range_pct", 0))),
            "long_base_avg_range_pct": row.get("long_base_avg_range_pct", row.get("day_range_pct", 0)),
            "long_base_span_pct": row.get("long_base_span_pct", row.get("pct_from_52w_high", 0)),
            "long_base_window_days": row.get("long_base_window_days", 60),
            "ma200_type": row.get("ma200_type", "ema"),
            "baseline_close_504d": row.get("baseline_close_504d", row.get("baseline_close_252d", row.get("last_price"))),
        })

    def _enrich_snapshot_rows(
        self,
        rows: list[dict[str, Any]],
        market_cap_min_crore: float,
        force_refresh: bool,
        fetch_missing_metadata: bool = True,
    ) -> list[dict[str, Any]]:
        if not rows:
            return rows

        universe = self._load_or_refresh_universe(market_cap_min_crore, False)
        metadata_by_symbol = self._load_or_refresh_company_metadata(universe, force_refresh, fetch_missing_metadata)

        enriched = []
        for row in rows:
            metadata = metadata_by_symbol.get(str(row.get("symbol", "")), {})
            enriched.append(
                {
                    **self._with_snapshot_fallbacks(row),
                    "market_cap_crore": metadata.get("market_cap_crore", row.get("market_cap_crore")),
                    "listing_date": metadata.get("listing_date") or row.get("listing_date"),
                    "sector": self._normalize_classification_label(metadata.get("sector") or row.get("sector")),
                    "sub_sector": self._normalize_classification_label(metadata.get("sub_sector") or row.get("sub_sector")),
                    "circuit_band_label": metadata.get("circuit_band_label") or row.get("circuit_band_label"),
                    "upper_circuit_limit": metadata.get("upper_circuit_limit", row.get("upper_circuit_limit")),
                    "lower_circuit_limit": metadata.get("lower_circuit_limit", row.get("lower_circuit_limit")),
                }
            )

        return self._apply_sector_benchmarks(enriched)

    def _metadata_seed_from_item(self, item: dict[str, Any]) -> dict[str, Any]:
        sector = str(item.get("sector") or "").strip() or "Unclassified"
        sub_sector = str(item.get("sub_sector") or "").strip() or "Unclassified"
        listing_date = item.get("listing_date")
        return {
            "market_cap_crore": item.get("market_cap_crore"),
            "sector": sector,
            "sub_sector": sub_sector,
            "listing_date": listing_date,
            "circuit_band_label": item.get("circuit_band_label"),
            "upper_circuit_limit": item.get("upper_circuit_limit"),
            "lower_circuit_limit": item.get("lower_circuit_limit"),
            "circuit_updated_on": item.get("circuit_updated_on"),
        }

    def _metadata_needs_refresh(self, metadata: dict[str, Any] | None) -> bool:
        payload = metadata or {}
        sector = str(payload.get("sector") or "").strip()
        sub_sector = str(payload.get("sub_sector") or "").strip()
        listing_date = payload.get("listing_date")
        circuit_present = any(
            payload.get(field) not in (None, "")
            for field in ("circuit_band_label", "upper_circuit_limit", "lower_circuit_limit")
        )
        circuit_updated_on = self._parse_row_date(payload.get("circuit_updated_on"))
        circuit_stale = circuit_present and circuit_updated_on != self._current_or_previous_trading_day_ist()
        return (
            not listing_date
            or not sector
            or sector == "Unclassified"
            or not sub_sector
            or sub_sector == "Unclassified"
            or not circuit_present
            or circuit_stale
        )

    def _load_or_refresh_company_metadata(
        self,
        universe: list[dict[str, Any]],
        force_refresh: bool,
        fetch_missing_metadata: bool = True,
    ) -> dict[str, dict[str, Any]]:
        cached: dict[str, dict[str, Any]] = {}
        if not force_refresh and self._is_fresh(self.company_metadata_path, max_age_hours=24 * 30):
            try:
                cached = json.loads(self.company_metadata_path.read_text(encoding="utf-8"))
            except Exception:
                cached = {}

        cache_changed = False
        for item in universe:
            symbol = str(item.get("symbol") or "").upper()
            if not symbol:
                continue

            seeded = self._metadata_seed_from_item(item)
            existing = dict(cached.get(symbol) or {})
            merged = {
                "market_cap_crore": existing.get("market_cap_crore"),
                "sector": existing.get("sector") or "Unclassified",
                "sub_sector": existing.get("sub_sector") or "Unclassified",
                "listing_date": existing.get("listing_date"),
                "circuit_band_label": existing.get("circuit_band_label"),
                "upper_circuit_limit": existing.get("upper_circuit_limit"),
                "lower_circuit_limit": existing.get("lower_circuit_limit"),
                "circuit_updated_on": existing.get("circuit_updated_on"),
            }
            if seeded["market_cap_crore"] is not None:
                merged["market_cap_crore"] = seeded["market_cap_crore"]
            if seeded["sector"] != "Unclassified":
                merged["sector"] = seeded["sector"]
            if seeded["sub_sector"] != "Unclassified":
                merged["sub_sector"] = seeded["sub_sector"]
            if seeded["listing_date"]:
                merged["listing_date"] = seeded["listing_date"]
            if seeded["circuit_band_label"] not in (None, ""):
                merged["circuit_band_label"] = seeded["circuit_band_label"]
            if seeded["upper_circuit_limit"] is not None:
                merged["upper_circuit_limit"] = seeded["upper_circuit_limit"]
            if seeded["lower_circuit_limit"] is not None:
                merged["lower_circuit_limit"] = seeded["lower_circuit_limit"]
            if seeded.get("circuit_updated_on"):
                merged["circuit_updated_on"] = seeded["circuit_updated_on"]

            if existing != merged:
                cached[symbol] = merged
                cache_changed = True

        if not fetch_missing_metadata:
            if cache_changed:
                self.company_metadata_path.write_text(json.dumps(cached, indent=2), encoding="utf-8")
            return cached

        missing = [
            item
            for item in universe
            if self._should_fetch_metadata_for_item(item)
            and (
                force_refresh
                or self._metadata_needs_refresh(cached.get(str(item.get("symbol") or "").upper()))
            )
        ]

        if missing:
            fetched = self._fetch_company_metadata(missing)
            for symbol, payload in fetched.items():
                merged = {
                    **(cached.get(symbol) or {}),
                    **payload,
                }
                if cached.get(symbol) != merged:
                    cached[symbol] = merged
                    cache_changed = True

        if cache_changed:
            self.company_metadata_path.write_text(json.dumps(cached, indent=2), encoding="utf-8")

        return cached

    def _fetch_company_metadata(self, universe: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        metadata: dict[str, dict[str, Any]] = {}
        workers = min(4, max(1, len(universe) // 250 or 1))
        chunk_size = max(1, (len(universe) + workers - 1) // workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._fetch_company_metadata_chunk, universe[index : index + chunk_size]): index
                for index in range(0, len(universe), chunk_size)
            }
            for future in as_completed(futures):
                try:
                    metadata.update(future.result())
                except Exception:
                    continue
        return metadata

    def _fetch_company_metadata_chunk(self, universe: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        if not universe:
            return {}

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nseindia.com/",
        }
        metadata: dict[str, dict[str, Any]] = {}
        with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
            if any(str(item.get("exchange") or "").upper() == "NSE" for item in universe):
                client.get("https://www.nseindia.com/")
            for item in universe:
                symbol = item["symbol"]
                exchange = str(item.get("exchange") or "").upper()
                try:
                    if exchange == "BSE":
                        metadata[symbol] = self._fetch_bse_company_profile(item)
                    else:
                        metadata[symbol] = self._fetch_company_profile(client, symbol)
                except Exception:
                    metadata[symbol] = {
                        "sector": "Unclassified",
                        "sub_sector": "Unclassified",
                        "listing_date": None,
                        "circuit_band_label": None,
                        "upper_circuit_limit": None,
                        "lower_circuit_limit": None,
                    }

        return metadata

    def _fetch_bse_company_profile(self, item: dict[str, Any]) -> dict[str, Any]:
        symbol = str(item.get("symbol") or "").upper()
        ticker = str(item.get("ticker") or f"{symbol}.BO").upper()
        fallback_sub_sector = item.get("sub_sector") or item.get("INDUSTRY")
        try:
            info = yf.Ticker(ticker).get_info() or {}
        except Exception:
            info = {}

        sector = self._normalize_sector_label(info.get("sectorDisp") or info.get("sector") or item.get("sector"))
        sub_sector = self._normalize_classification_label(
            info.get("industryDisp") or info.get("industry") or fallback_sub_sector
        )

        return {
            "sector": sector,
            "sub_sector": sub_sector,
            "listing_date": item.get("listing_date"),
            "market_cap_crore": item.get("market_cap_crore"),
            "circuit_band_label": item.get("circuit_band_label"),
            "upper_circuit_limit": item.get("upper_circuit_limit"),
            "lower_circuit_limit": item.get("lower_circuit_limit"),
        }

    def _fetch_company_profile(self, client: httpx.Client, symbol: str) -> dict[str, Any]:
        response = client.get(NSE_QUOTE_EQUITY_URL, params={"symbol": symbol})
        response.raise_for_status()
        return self._company_profile_from_quote_payload(symbol, response.json())

    def _company_profile_from_quote_payload(self, symbol: str, payload: dict[str, Any]) -> dict[str, Any]:
        industry_info = payload.get("industryInfo") or {}
        metadata = payload.get("metadata") or {}
        info = payload.get("info") or {}
        price_info = payload.get("priceInfo") or {}
        security_info = payload.get("securityInfo") or {}

        sector = self._normalize_sector_label(
            industry_info.get("sector") or industry_info.get("macro") or metadata.get("pdSectorInd")
        )
        sub_sector = (
            str(industry_info.get("basicIndustry") or industry_info.get("industry") or metadata.get("industry") or info.get("industry") or "").strip()
            or "Unclassified"
        )
        listing_date = self._parse_nse_listing_date(info.get("listingDate") or metadata.get("listingDate"))
        issued_size = self._to_float(security_info.get("issuedSize"))
        last_price = self._to_float(price_info.get("lastPrice"))
        upper_circuit_limit = self._to_float(
            price_info.get("upperCP") or price_info.get("upperCp") or price_info.get("upperCircuit")
        )
        lower_circuit_limit = self._to_float(
            price_info.get("lowerCP") or price_info.get("lowerCp") or price_info.get("lowerCircuit")
        )
        circuit_band_label = str(price_info.get("pPriceBand") or security_info.get("priceBand") or "").strip() or None
        market_cap_crore = None
        if issued_size not in (None, 0) and last_price not in (None, 0):
            market_cap_crore = round((float(issued_size) * float(last_price)) / 10_000_000, 2)
        if sector == "Unclassified" or sub_sector == "Unclassified":
            override = MANUAL_SECTOR_OVERRIDES.get(symbol)
            if override:
                return {
                    **override,
                    "listing_date": listing_date,
                    "market_cap_crore": market_cap_crore,
                    "circuit_band_label": circuit_band_label,
                    "upper_circuit_limit": upper_circuit_limit,
                    "lower_circuit_limit": lower_circuit_limit,
                    "circuit_updated_on": self._current_or_previous_trading_day_ist().isoformat() if circuit_band_label or upper_circuit_limit is not None or lower_circuit_limit is not None else None,
                }
        return {
            "sector": sector,
            "sub_sector": sub_sector,
            "listing_date": listing_date,
            "market_cap_crore": market_cap_crore,
            "circuit_band_label": circuit_band_label,
            "upper_circuit_limit": upper_circuit_limit,
            "lower_circuit_limit": lower_circuit_limit,
            "circuit_updated_on": self._current_or_previous_trading_day_ist().isoformat() if circuit_band_label or upper_circuit_limit is not None or lower_circuit_limit is not None else None,
        }

    def _parse_nse_listing_date(self, value: Any) -> str | None:
        raw_value = str(value or "").strip()
        if not raw_value:
            return None

        for date_format in ("%d-%b-%Y", "%d-%B-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw_value, date_format).date().isoformat()
            except ValueError:
                continue
        return None

    def _apply_sector_benchmarks(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        aggregates: dict[str, dict[str, float]] = {}
        for row in rows:
            sector = str(row.get("sector") or "Unclassified")
            weight = max(float(row.get("market_cap_crore", 0)) or 0.0, 1.0)
            bucket = aggregates.setdefault(sector, {"weighted_return_20d": 0.0, "weight": 0.0})
            bucket["weighted_return_20d"] += float(row.get("stock_return_20d", 0) or 0) * weight
            bucket["weight"] += weight

        sector_returns = {
            sector: round(values["weighted_return_20d"] / values["weight"], 2) if values["weight"] else 0.0
            for sector, values in aggregates.items()
        }

        return [
            {
                **row,
                "sector_return_20d": sector_returns.get(str(row.get("sector") or "Unclassified"), float(row.get("sector_return_20d", 0) or 0)),
            }
            for row in rows
        ]

    async def get_chart(self, symbol: str, timeframe: str, bars: int = 520) -> list[ChartBar]:
        cached_bars = await asyncio.to_thread(self._read_chart_cache, symbol, timeframe, bars)
        min_required_bars = min(120, max(2, bars)) if timeframe == "1D" else 2
        cache_has_enough_data = len(cached_bars) >= min_required_bars
        cache_covers_snapshot_session = self._chart_cache_covers_snapshot_session(symbol, timeframe, cached_bars)
        if cached_bars and cache_has_enough_data and cache_covers_snapshot_session and self._is_chart_cache_fresh(symbol, timeframe):
            return cached_bars
        if timeframe == "1D" and cached_bars and cache_has_enough_data:
            refreshed_bars = await asyncio.to_thread(self._refresh_cached_daily_chart_from_quote, symbol, bars)
            if refreshed_bars:
                return refreshed_bars
        if timeframe == "1W" and cached_bars:
            refreshed_weekly = await asyncio.to_thread(self._refresh_cached_weekly_chart_from_daily_cache, symbol, bars)
            if refreshed_weekly:
                return refreshed_weekly
        try:
            chart_bars = await asyncio.to_thread(self._fetch_chart_bars, symbol, timeframe, bars)
            if len(chart_bars) < min_required_bars:
                legacy_bars = await asyncio.to_thread(self._read_chart_cache, symbol, timeframe, bars, allow_legacy=True)
                if len(cached_bars) >= min_required_bars:
                    return cached_bars
                if len(legacy_bars) >= min_required_bars:
                    return legacy_bars
            await asyncio.to_thread(self._write_chart_cache, symbol, timeframe, chart_bars)
            return chart_bars
        except Exception:
            if cached_bars:
                return cached_bars
            raise

    def _is_chart_cache_fresh(self, symbol: str, timeframe: str) -> bool:
        path = self._chart_cache_path(symbol, timeframe)
        if not path.exists():
            return False
        age_seconds = max(time.time() - path.stat().st_mtime, 0.0)
        normalized_symbol = str(symbol or "").strip().upper()
        if timeframe == "1D" and normalized_symbol in MACRO_CHART_SYMBOLS:
            return age_seconds < (6 * 60 * 60)
        if timeframe == "1W" and normalized_symbol in MACRO_CHART_SYMBOLS:
            return age_seconds < (24 * 60 * 60)
        if timeframe in ("15m", "30m"):
            return age_seconds < 5 * 60
        if timeframe == "1h":
            return age_seconds < 15 * 60
        if timeframe == "1D":
            # 5 mins if open, but only 2 hours if closed (ensures we get the 'official' close data quickly)
            return age_seconds < (5 * 60 if self._is_market_open_ist() else 2 * 60 * 60)
        if timeframe == "1W":
            return age_seconds < (10 * 60 if self._is_market_open_ist() else 2 * 60 * 60)
        return age_seconds < 2 * 60 * 60

    def _refresh_cached_daily_chart_from_quote(self, symbol: str, bars: int) -> list[ChartBar]:
        cached_daily = self._read_chart_cache(symbol, "1D", max(bars, 520))
        if not cached_daily:
            return []

        self._patch_daily_chart_cache_with_snapshot(symbol)

        cached_daily = self._read_chart_cache(symbol, "1D", max(bars, 520))

        return cached_daily[-bars:]

    def _refresh_cached_weekly_chart_from_daily_cache(self, symbol: str, bars: int) -> list[ChartBar]:
        daily_bars = self._refresh_cached_daily_chart_from_quote(symbol, 520)
        if not daily_bars:
            return []
        weekly_bars = self._aggregate_weekly_chart_bars(daily_bars)
        if weekly_bars:
            self._write_chart_cache(symbol, "1W", weekly_bars)
        return weekly_bars[-bars:]

    @staticmethod
    def _aggregate_weekly_chart_bars(daily_bars: list[ChartBar]) -> list[ChartBar]:
        if not daily_bars:
            return []

        frame = pd.DataFrame(
            [
                {
                    "Open": bar.open,
                    "High": bar.high,
                    "Low": bar.low,
                    "Close": bar.close,
                    "Volume": bar.volume,
                }
                for bar in daily_bars
            ],
            index=pd.to_datetime([int(bar.time) for bar in daily_bars], unit="s", utc=True),
        )
        weekly = pd.DataFrame(
            {
                "Open": frame["Open"].resample("W-FRI").first(),
                "High": frame["High"].resample("W-FRI").max(),
                "Low": frame["Low"].resample("W-FRI").min(),
                "Close": frame["Close"].resample("W-FRI").last(),
                "Volume": frame["Volume"].resample("W-FRI").sum(),
            }
        ).dropna(subset=["Close"])
        return [
            ChartBar(
                time=int(timestamp.timestamp()),
                open=round(float(row["Open"]), 2),
                high=round(float(row["High"]), 2),
                low=round(float(row["Low"]), 2),
                close=round(float(row["Close"]), 2),
                volume=int(float(row["Volume"]) or 0),
            )
            for timestamp, row in weekly.iterrows()
        ]

    async def get_fundamentals(self, symbol: str, snapshot: StockSnapshot | None = None) -> CompanyFundamentals:
        normalized_symbol = symbol.upper()
        cached = self._fundamentals_memory_cache.get(normalized_symbol)
        if cached and self._fundamentals_payload_fresh(cached.model_dump(mode="json"), max_age_hours=6):
            return cached

        try:
            payload = await asyncio.to_thread(self._load_or_refresh_fundamentals, normalized_symbol, snapshot)
            self._fundamentals_memory_cache[normalized_symbol] = payload
            return payload
        except Exception as exc:
            import logging, traceback
            logging.getLogger(__name__).error("Fundamentals failed for %s: %s\n%s", normalized_symbol, exc, traceback.format_exc())
            fallback = await self.demo.get_fundamentals(normalized_symbol, snapshot)
            fallback.data_warnings.append("Live fundamentals could not be refreshed, so fallback content is being shown.")
            self._fundamentals_memory_cache[normalized_symbol] = fallback
            return fallback

    async def get_fundamentals_cached(
        self,
        symbol: str,
        snapshot: StockSnapshot | None = None,
        max_age_hours: float | None = None,
    ) -> CompanyFundamentals | None:
        normalized_symbol = symbol.upper()
        cached = self._fundamentals_memory_cache.get(normalized_symbol)
        if cached is not None:
            if max_age_hours is None or self._fundamentals_payload_fresh(
                cached.model_dump(mode="json"),
                max_age_hours=max_age_hours,
            ):
                return cached

        cache = await asyncio.to_thread(self._load_json_file, self.fundamentals_cache_path)
        cached_payload = cache.get(normalized_symbol)
        if not isinstance(cached_payload, dict):
            return None
        if max_age_hours is not None and not self._fundamentals_payload_fresh(
            cached_payload,
            max_age_hours=max_age_hours,
        ):
            return None

        try:
            fundamentals = CompanyFundamentals.model_validate(cached_payload)
        except Exception:
            return None

        self._fundamentals_memory_cache[normalized_symbol] = fundamentals
        return fundamentals

    def _load_or_refresh_fundamentals(self, symbol: str, snapshot: StockSnapshot | None) -> CompanyFundamentals:
        cache = self._load_json_file(self.fundamentals_cache_path)
        cached_payload = cache.get(symbol)
        if isinstance(cached_payload, dict) and self._fundamentals_payload_fresh(cached_payload, max_age_hours=6):
            try:
                return CompanyFundamentals.model_validate(cached_payload)
            except Exception:
                pass

        fundamentals = self._build_company_fundamentals(symbol, snapshot)
        cache[symbol] = {
            **fundamentals.model_dump(mode="json"),
            "cache_version": FUNDAMENTALS_CACHE_VERSION,
        }
        self.fundamentals_cache_path.write_text(json.dumps(cache, indent=2), encoding="utf-8")
        return fundamentals

    def _build_company_fundamentals(self, symbol: str, snapshot: StockSnapshot | None) -> CompanyFundamentals:
        snapshot = snapshot or self._load_snapshot_from_cache(symbol)
        warnings: list[str] = []
        screener_payload: dict[str, Any] = {}
        yahoo_payload: dict[str, Any] = {}

        try:
            screener_html = self._fetch_screener_company_page(symbol)
            screener_payload = self._parse_screener_company_page(symbol, screener_html)
        except Exception:
            warnings.append("Quarterly tables and shareholding pattern could not be refreshed from the company page right now.")

        try:
            yahoo_payload = self._fetch_yfinance_fundamentals(symbol)
        except Exception:
            warnings.append("Recent headline/news fallback is unavailable right now.")

        quarterly_results = self._merge_quarterly_results(
            screener_payload.get("quarterly_results") or [],
            yahoo_payload.get("quarterly_results") or [],
        )
        profit_loss = self._merge_profit_loss_items(
            screener_payload.get("profit_loss") or [],
            yahoo_payload.get("profit_loss") or [],
        )
        shareholding_pattern = screener_payload.get("shareholding_pattern") or yahoo_payload.get("shareholding_pattern") or []
        growth = self._build_growth_snapshot(quarterly_results)
        shareholding_delta = self._build_shareholding_delta(shareholding_pattern)
        valuation = self._build_valuation_snapshot(
            snapshot=snapshot,
            screener_payload=screener_payload,
            yahoo_payload=yahoo_payload,
            growth=growth,
            profit_loss=profit_loss,
        )
        recent_updates = self._merge_updates(
            screener_payload.get("recent_updates") or [],
            yahoo_payload.get("recent_updates") or [],
        )
        recent_updates, management_guidance, detailed_news = self._enrich_updates_from_linked_sources(
            recent_updates,
            yahoo_payload.get("company_website"),
        )
        growth_drivers = self._build_growth_drivers(
            growth=growth,
            shareholding_delta=shareholding_delta,
            recent_updates=recent_updates,
            key_points=screener_payload.get("key_points") or [],
            pros=screener_payload.get("pros") or [],
            cons=screener_payload.get("cons") or [],
        )

        if not quarterly_results:
            warnings.append("Quarterly results are currently unavailable for this symbol.")
        if not recent_updates:
            warnings.append("Recent company updates/news are currently unavailable for this symbol.")
        if not shareholding_pattern:
            warnings.append("Latest promoter/FII/DII changes are currently unavailable for this symbol.")

        # Parse yfinance financial data
        balance_sheet = [
            BalanceSheetItem(**item)
            for item in (yahoo_payload.get("balance_sheet") or [])
        ]
        cash_flow = [
            CashFlowItem(**item)
            for item in (yahoo_payload.get("cash_flow") or [])
        ]
        financial_ratios = [
            FinancialRatios(**item)
            for item in (yahoo_payload.get("financial_ratios") or [])
        ]
        insider_transactions = [
            InsiderTransaction(**item)
            for item in (yahoo_payload.get("insider_transactions") or [])
        ]

        fundamentals = CompanyFundamentals(
            symbol=symbol,
            name=(snapshot.name if snapshot else None) or screener_payload.get("name") or yahoo_payload.get("name") or symbol,
            exchange=snapshot.exchange if snapshot else self._default_exchange(),
            sector=(snapshot.sector if snapshot else None) or screener_payload.get("sector"),
            sub_sector=(snapshot.sub_sector if snapshot else None) or screener_payload.get("sub_sector"),
            about=self._merge_about_text(
                screener_payload.get("about"),
                yahoo_payload.get("about"),
                screener_payload.get("key_points") or [],
            ),
            company_website=yahoo_payload.get("company_website"),
            headquarters=yahoo_payload.get("headquarters"),
            quarterly_results=quarterly_results[:8],
            profit_loss=profit_loss[-8:] if len(profit_loss) > 8 else profit_loss,
            balance_sheet=balance_sheet,
            cash_flow=cash_flow,
            financial_ratios=financial_ratios,
            growth=growth,
            valuation=valuation,
            growth_drivers=growth_drivers[:5],
            management_guidance=management_guidance[:6],
            recent_updates=recent_updates[:10],
            detailed_news=detailed_news[:6],
            shareholding_pattern=shareholding_pattern[:6],
            shareholding_delta=shareholding_delta,
            insider_transactions=insider_transactions,
            last_news_update=datetime.now(timezone.utc) if detailed_news else None,
            data_warnings=warnings,
        )

        # Enrich with AI analysis if available
        if self.ai_service.available:
            try:
                ai_data = self.ai_service.analyze_company(fundamentals)
                fundamentals = enrich_fundamentals_with_ai(fundamentals, ai_data)
            except Exception as exc:
                import logging
                logging.getLogger(__name__).warning("AI enrichment failed for %s: %s", symbol, exc)

        return fundamentals

    def _load_snapshot_from_cache(self, symbol: str) -> StockSnapshot | None:
        if not self.snapshot_cache_path.exists():
            return None

        try:
            rows = json.loads(self.snapshot_cache_path.read_text(encoding="utf-8"))
        except Exception:
            return None

        row = next((item for item in rows if str(item.get("symbol", "")).upper() == symbol.upper()), None)
        if row is None:
            return None

        metadata_by_symbol: dict[str, dict[str, str]] = {}
        if self.company_metadata_path.exists():
            try:
                metadata_by_symbol = json.loads(self.company_metadata_path.read_text(encoding="utf-8"))
            except Exception:
                metadata_by_symbol = {}

        metadata = metadata_by_symbol.get(symbol.upper(), {})
        enriched = self._with_snapshot_fallbacks(
            {
                **row,
                "sector": metadata.get("sector") or row.get("sector") or "Unclassified",
                "sub_sector": metadata.get("sub_sector") or row.get("sub_sector") or "Unclassified",
                "circuit_band_label": metadata.get("circuit_band_label") or row.get("circuit_band_label"),
                "upper_circuit_limit": metadata.get("upper_circuit_limit", row.get("upper_circuit_limit")),
                "lower_circuit_limit": metadata.get("lower_circuit_limit", row.get("lower_circuit_limit")),
            }
        )
        try:
            return StockSnapshot.model_validate(enriched)
        except Exception:
            return None

    def _fundamentals_payload_fresh(self, payload: dict[str, Any], max_age_hours: int) -> bool:
        if int(payload.get("cache_version", 0) or 0) != FUNDAMENTALS_CACHE_VERSION:
            return False
        warnings = [str(item).lower() for item in (payload.get("data_warnings") or [])]
        if any("could not be refreshed" in warning or "currently unavailable" in warning for warning in warnings):
            return False
        timestamp = payload.get("fetched_at")
        if not timestamp:
            return False
        try:
            fetched_at = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        except ValueError:
            return False
        return datetime.now(timezone.utc) - fetched_at <= timedelta(hours=max_age_hours)

    def _load_json_file(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def _load_json_rows(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, list) else []
        except Exception:
            return []

    def _fetch_screener_company_page(self, symbol: str) -> str:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.screener.in/",
        }
        candidates = [
            f"{SCREENER_URL}/{symbol}/consolidated/",
            f"{SCREENER_URL}/{symbol}/",
        ]
        with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
            for url in candidates:
                response = client.get(url)
                if response.status_code >= 400:
                    continue
                if "Quarterly Results" in response.text and "Profit & Loss" in response.text:
                    return response.text
        raise RuntimeError(f"Could not load Screener page for {symbol}")

    def _parse_screener_company_page(self, symbol: str, html: str) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        lines = self._extract_page_lines(soup)
        tables = self._read_html_tables(html)
        quarterly_frame = self._select_table(tables, table_type="quarterly")
        profit_loss_frame = self._select_table(tables, table_type="profit_loss")
        shareholding_frame = self._select_table(tables, table_type="shareholding")
        announcement_updates = self._extract_announcements(lines)
        link_updates = self._extract_screener_link_updates(soup)

        return {
            "name": self._extract_heading_name(lines) or symbol,
            "about": self._extract_about(lines),
            "key_points": self._extract_key_points(lines),
            "pros": self._extract_bullet_section(lines, start_markers=["Pros"], stop_markers=["Cons", "Quarterly Results"]),
            "cons": self._extract_bullet_section(lines, start_markers=["Cons"], stop_markers=["Quarterly Results", "Profit & Loss"]),
            "valuation": self._extract_screener_valuation(lines),
            "quarterly_results": self._parse_quarterly_results_table(quarterly_frame),
            "profit_loss": self._parse_profit_loss_table(profit_loss_frame),
            "shareholding_pattern": self._parse_shareholding_table(shareholding_frame),
            "recent_updates": self._merge_updates(announcement_updates, link_updates),
        }

    def _fetch_yfinance_fundamentals(self, symbol: str) -> dict[str, Any]:
        ticker = yf.Ticker(self._resolve_ticker(symbol))
        info: dict[str, Any] = {}
        try:
            info = ticker.get_info()
        except Exception:
            try:
                info = ticker.info
            except Exception:
                info = {}

        try:
            quarterly_frame = ticker.quarterly_income_stmt
        except Exception:
            quarterly_frame = pd.DataFrame()

        try:
            annual_frame = ticker.income_stmt
        except Exception:
            annual_frame = pd.DataFrame()

        recent_updates: list[CompanyUpdateItem] = []
        try:
            for item in getattr(ticker, "news", [])[:6]:
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
                published = item.get("providerPublishTime")
                published_at = (
                    datetime.fromtimestamp(int(published), tz=timezone.utc).isoformat()
                    if isinstance(published, (int, float))
                    else None
                )
                recent_updates.append(
                    CompanyUpdateItem(
                        title=title,
                        source=str(item.get("publisher") or "Yahoo Finance"),
                        published_at=published_at,
                        summary=str(item.get("summary") or "").strip() or None,
                        link=str(item.get("link") or "").strip() or None,
                        kind="news",
                    )
                )
        except Exception:
            recent_updates = []

        # --- Balance Sheet ---
        balance_sheet_items: list[dict[str, Any]] = []
        try:
            bs_frame = ticker.balance_sheet
            if bs_frame is not None and not bs_frame.empty:
                for col in bs_frame.columns[:4]:
                    period_label = col.strftime("%b %Y") if hasattr(col, "strftime") else str(col)
                    row_get = lambda key: self._safe_float(bs_frame.at[key, col]) / 10_000_000 if key in bs_frame.index and pd.notna(bs_frame.at[key, col]) else None
                    balance_sheet_items.append({
                        "period": period_label,
                        "total_assets_crore": row_get("Total Assets"),
                        "total_liabilities_crore": row_get("Total Liabilities Net Minority Interest"),
                        "shareholders_equity_crore": row_get("Stockholders Equity"),
                        "debt_crore": row_get("Total Debt"),
                        "cash_and_equivalents_crore": row_get("Cash And Cash Equivalents"),
                    })
        except Exception:
            pass

        # --- Cash Flow ---
        cash_flow_items: list[dict[str, Any]] = []
        try:
            cf_frame = ticker.cashflow
            if cf_frame is not None and not cf_frame.empty:
                for col in cf_frame.columns[:4]:
                    period_label = col.strftime("%b %Y") if hasattr(col, "strftime") else str(col)
                    row_get = lambda key: self._safe_float(cf_frame.at[key, col]) / 10_000_000 if key in cf_frame.index and pd.notna(cf_frame.at[key, col]) else None
                    operating_cf = row_get("Operating Cash Flow")
                    capex = row_get("Capital Expenditure")
                    free_cf = (operating_cf + capex) if operating_cf is not None and capex is not None else row_get("Free Cash Flow")
                    cash_flow_items.append({
                        "period": period_label,
                        "operating_cash_flow_crore": operating_cf,
                        "free_cash_flow_crore": free_cf,
                        "capital_expenditure_crore": abs(capex) if capex is not None else None,
                        "dividends_paid_crore": row_get("Common Stock Dividend Paid"),
                    })
        except Exception:
            pass

        # --- Financial Ratios (from info) ---
        financial_ratios: list[dict[str, Any]] = []
        try:
            ratios: dict[str, Any] = {"period": "TTM"}
            roe_val = info.get("returnOnEquity")
            if roe_val is not None:
                ratios["roe_pct"] = round(self._safe_float(roe_val) * 100, 2)
            roa_val = info.get("returnOnAssets")
            if roa_val is not None:
                ratios["roa_pct"] = round(self._safe_float(roa_val) * 100, 2)
            cr_val = info.get("currentRatio")
            if cr_val is not None:
                ratios["current_ratio"] = round(self._safe_float(cr_val), 2)
            de_val = info.get("debtToEquity")
            if de_val is not None:
                ratios["debt_to_equity_ratio"] = round(self._safe_float(de_val) / 100, 2)
            if len(ratios) > 1:
                financial_ratios.append(ratios)
        except Exception:
            pass

        # --- Insider Transactions ---
        insider_transactions: list[dict[str, Any]] = []
        try:
            insider_df = ticker.insider_transactions
            if insider_df is not None and not insider_df.empty:
                for _, row in insider_df.head(10).iterrows():
                    txn_text = str(row.get("Text", "") or "").lower()
                    if "purchase" in txn_text or "buy" in txn_text:
                        txn_type = "buy"
                    elif "sale" in txn_text or "sell" in txn_text:
                        txn_type = "sell"
                    else:
                        continue
                    shares = int(row.get("Shares", 0) or 0)
                    raw_value = self._safe_float(row.get("Value", 0)) or 0.0
                    value = raw_value if pd.notna(raw_value) else 0.0
                    if shares == 0:
                        continue
                    price = value / shares if shares else 0
                    if not pd.notna(price):
                        price = 0.0
                    start_date = row.get("Start Date")
                    date_str = start_date.strftime("%Y-%m-%d") if hasattr(start_date, "strftime") else str(start_date or "")
                    insider_transactions.append({
                        "person_name": str(row.get("Insider", "Unknown")),
                        "position": str(row.get("Position", "Insider")),
                        "transaction_type": txn_type,
                        "quantity": abs(shares),
                        "price_per_share": round(abs(price), 2),
                        "total_value_crore": round(abs(value) / 10_000_000, 4),
                        "date": date_str,
                    })
        except Exception:
            pass

        return {
            "name": info.get("shortName") or info.get("longName") or symbol,
            "about": info.get("longBusinessSummary"),
            "company_website": info.get("website"),
            "headquarters": f"{info.get('city', '')}, {info.get('country', '')}".strip(", ") or None,
            "valuation": {
                "market_cap_crore": ((float(info["marketCap"]) / 10_000_000) if info.get("marketCap") else None),
                "pe_ratio": self._safe_float(info.get("trailingPE")),
                "peg_ratio": self._safe_float(info.get("pegRatio")),
                "operating_margin_pct": (self._safe_float(info.get("operatingMargins")) * 100 if info.get("operatingMargins") is not None else None),
                "net_margin_pct": (self._safe_float(info.get("profitMargins")) * 100 if info.get("profitMargins") is not None else None),
                "roe_pct": (self._safe_float(info.get("returnOnEquity")) * 100 if info.get("returnOnEquity") is not None else None),
                "dividend_yield_pct": (self._safe_float(info.get("dividendYield")) * 100 if info.get("dividendYield") is not None else None),
            },
            "quarterly_results": self._parse_yfinance_statement(quarterly_frame, quarterly=True),
            "profit_loss": self._parse_yfinance_statement(annual_frame, quarterly=False),
            "recent_updates": recent_updates,
            "balance_sheet": balance_sheet_items,
            "cash_flow": cash_flow_items,
            "financial_ratios": financial_ratios,
            "insider_transactions": insider_transactions,
        }

    def _extract_page_lines(self, soup: BeautifulSoup) -> list[str]:
        lines: list[str] = []
        for raw_line in soup.get_text("\n").splitlines():
            line = self._clean_text(raw_line)
            if not line:
                continue
            if lines and lines[-1] == line:
                continue
            lines.append(line)
        return lines

    def _clean_text(self, value: Any) -> str:
        return re.sub(r"\s+", " ", unescape(str(value)).replace("\xa0", " ")).strip()

    def _extract_heading_name(self, lines: list[str]) -> str | None:
        for index, line in enumerate(lines):
            if line in {"Notebook AI", "Tools", "Home", "Screens"}:
                continue
            if index + 1 < len(lines) and lines[index + 1].startswith("₹"):
                return line
        return None

    def _extract_about(self, lines: list[str]) -> str | None:
        section = self._extract_section_lines(lines, start_markers=["About"], stop_markers=["Key Points", "Pros", "Cons", "Quarterly Results"])
        if not section:
            return None
        return " ".join(section[:3]).strip() or None

    def _extract_key_points(self, lines: list[str]) -> list[tuple[str, str]]:
        section = self._extract_section_lines(lines, start_markers=["Key Points"], stop_markers=["Read More", "Website", "Pros", "Cons"])
        items: list[tuple[str, str]] = []
        index = 0
        while index < len(section):
            title = section[index]
            detail = section[index + 1] if index + 1 < len(section) else ""
            if len(title) <= 60 and detail and len(detail) > 20:
                items.append((title, detail))
                index += 2
                continue
            index += 1
        return items[:3]

    def _extract_announcements(self, lines: list[str]) -> list[CompanyUpdateItem]:
        section = self._extract_section_lines(
            lines,
            start_markers=["Announcements", "Latest announcements"],
            stop_markers=["Annual reports", "Credit ratings", "Concalls", "Concall", "Shareholding Pattern", "Peers", "Quarterly Results"],
        )
        updates: list[CompanyUpdateItem] = []
        seen_titles: set[str] = set()
        for line in section:
            lower = line.lower()
            if len(line) < 18:
                continue
            if any(token in lower for token in ("button", "premium", "login", "show more", "show less", "upgrade")):
                continue
            title = re.sub(
                r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b",
                "",
                line,
            ).strip(" -")
            title = title or line
            if title.lower() in seen_titles:
                continue
            seen_titles.add(title.lower())
            updates.append(
                CompanyUpdateItem(
                    title=title,
                    source="Screener filing feed",
                    published_at=self._extract_date_from_text(line),
                    summary=line if title != line else None,
                    kind=self._classify_update_kind(title),
                )
            )
        return updates[:6]

    def _extract_bullet_section(
        self,
        lines: list[str],
        start_markers: list[str],
        stop_markers: list[str],
    ) -> list[str]:
        section = self._extract_section_lines(lines, start_markers=start_markers, stop_markers=stop_markers)
        items: list[str] = []
        seen: set[str] = set()
        for line in section:
            if len(line) < 12:
                continue
            lowered = line.lower()
            if any(token in lowered for token in ("read more", "show more", "show less", "edit ratios", "add ratio")):
                continue
            normalized = line.strip("•- ").strip()
            if not normalized or normalized.lower() in seen:
                continue
            seen.add(normalized.lower())
            items.append(normalized)
        return items[:4]

    def _extract_screener_link_updates(self, soup: BeautifulSoup) -> list[CompanyUpdateItem]:
        updates: list[CompanyUpdateItem] = []
        seen: set[str] = set()
        keywords = (
            "result",
            "quarter",
            "earnings",
            "conference",
            "concall",
            "transcript",
            "presentation",
            "investor",
            "shareholding",
            "promoter",
            "fii",
            "dii",
            "order",
            "contract",
            "approval",
            "acquisition",
            "launch",
            "annual report",
            "credit rating",
        )

        for anchor in soup.find_all("a", href=True):
            title = self._clean_text(anchor.get_text(" ", strip=True))
            href = str(anchor.get("href") or "").strip()
            lowered_title = title.lower()
            if ((len(title) < 16 and lowered_title not in {"transcript", "ppt", "rec"}) or not href):
                continue
            if href.startswith("#") or href.lower().startswith("javascript"):
                continue
            if not any(keyword in lowered_title for keyword in keywords):
                continue
            context_node = anchor.find_parent(["li", "tr", "div", "section"]) or anchor.parent
            context_text = self._clean_text(context_node.get_text(" ", strip=True)) if context_node else title
            display_title = self._derive_link_update_title(title, context_text)
            key = f"{display_title.lower()}::{href.lower()}"
            if key in seen:
                continue
            seen.add(key)
            updates.append(
                CompanyUpdateItem(
                    title=display_title,
                    source="Screener company page",
                    published_at=self._extract_date_from_text(context_text)
                    or self._extract_month_year_date_from_text(context_text)
                    or self._extract_month_year_date_from_text(display_title),
                    summary=context_text if context_text != title else None,
                    link=urljoin(SCREENER_BASE_URL, href),
                    kind=self._classify_update_kind(title),
                )
            )
        return updates[:10]

    def _derive_link_update_title(self, title: str, context_text: str) -> str:
        cleaned_title = self._clean_text(title)
        if cleaned_title.lower() not in {"transcript", "ppt", "rec"}:
            return cleaned_title
        context = re.sub(r"\b(Transcript|PPT|REC|Notes|AI Summary)\b", " ", context_text, flags=re.IGNORECASE)
        context = self._clean_text(context)
        if context:
            if len(context) > 120:
                context = context[:117].rstrip() + "..."
            return f"{cleaned_title} - {context}"
        return cleaned_title

    def _normalize_update_date(self, value: str | None) -> str | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date().isoformat()
        except ValueError:
            return self._extract_month_year_date_from_text(str(value))

    def _extract_month_year_date_from_text(self, text: str | None) -> str | None:
        cleaned = self._clean_text(text)
        if not cleaned:
            return None
        match = re.search(r"\b([A-Za-z]{3,9})\s+(20\d{2})\b", cleaned)
        if not match:
            return None
        raw = f"1 {match.group(1)} {match.group(2)}"
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    def _infer_fiscal_year_from_date(self, value: str | None) -> str:
        normalized = self._normalize_update_date(value)
        base_date = date.fromisoformat(normalized) if normalized else self._current_ist_date()
        fiscal_year = base_date.year + 1 if base_date.month >= 4 else base_date.year
        return f"FY{str(fiscal_year)[-2:]}"

    def _is_material_financial_update(self, text: str) -> bool:
        normalized = self._clean_text(text).lower()
        if not normalized:
            return False
        return any(keyword in normalized for keyword in MATERIAL_UPDATE_KEYWORDS)

    def _is_supported_source_link(self, url: str, company_website: str | None) -> bool:
        host = urlparse(url).netloc.lower().removeprefix("www.")
        if not host:
            return False
        if host.endswith("bseindia.com") or host.endswith("screener.in") or host.endswith("nseindia.com"):
            return True
        if company_website:
            company_host = urlparse(company_website).netloc.lower().removeprefix("www.")
            if company_host and host.endswith(company_host):
                return True
        return False

    def _fetch_linked_document_text(self, url: str) -> tuple[str | None, str]:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/pdf,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": SCREENER_BASE_URL,
        }
        with httpx.Client(timeout=25, headers=headers, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            final_url = str(response.url)
            content_type = (response.headers.get("content-type") or "").lower()
            if "application/pdf" in content_type or final_url.lower().endswith(".pdf") or response.content[:4] == b"%PDF":
                return self._extract_pdf_text(response.content), final_url
            soup = BeautifulSoup(response.text, "html.parser")
            for tag in soup(["script", "style", "noscript", "svg"]):
                tag.decompose()
            return self._clean_text(soup.get_text("\n", strip=True)) or None, final_url

    def _extract_pdf_text(self, payload: bytes) -> str | None:
        if not payload:
            return None
        try:
            reader = PdfReader(io.BytesIO(payload))
        except Exception:
            return None
        pages: list[str] = []
        for page in reader.pages[:12]:
            try:
                text = page.extract_text() or ""
            except Exception:
                text = ""
            if text:
                pages.append(text)
        return self._clean_text("\n".join(pages)) or None

    def _extract_source_highlights(self, text: str, *, prefer_management: bool) -> list[str]:
        if not text:
            return []
        candidates = re.split(r"(?<=[.!?])\s+|\n+", text[:120000])
        scored: list[tuple[int, int, str]] = []
        seen: set[str] = set()
        minimum_score = 3 if prefer_management else 2
        for index, raw in enumerate(candidates):
            line = self._clean_material_update_text(raw)
            lowered = line.lower()
            if len(line) < 40 or len(line) > 420:
                continue
            if any(skip in lowered for skip in SOURCE_HIGHLIGHT_SKIP_PHRASES):
                continue
            material_hits = sum(1 for keyword in MATERIAL_UPDATE_KEYWORDS if keyword in lowered)
            management_hits = sum(1 for keyword in MANAGEMENT_HIGHLIGHT_KEYWORDS if keyword in lowered)
            score = material_hits * 2
            score += management_hits * (3 if prefer_management else 1)
            if any(token in lowered for token in ("crore", "%", "percent", "basis points", "bps", "mw", "gw", "tpd", "mtpa", "order book")):
                score += 2
            fingerprint = re.sub(r"[^a-z0-9]+", "", lowered)[:140]
            if not fingerprint or fingerprint in seen or score < minimum_score:
                continue
            seen.add(fingerprint)
            scored.append((score, index, line))
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [item[2] for item in scored[:4]]

    def _source_sentiment(self, text: str) -> str:
        lowered = self._clean_text(text).lower()
        if any(token in lowered for token in ("decline", "drop", "pressure", "weak", "delay", "cut", "slowdown", "loss", "shutdown", "warning")):
            return "negative"
        if any(token in lowered for token in ("growth", "improve", "strong", "expand", "order win", "higher", "increase", "ramp-up", "approval")):
            return "positive"
        return "neutral"

    def _source_impact_category(self, update: CompanyUpdateItem, text: str) -> str:
        lowered = self._clean_text(text).lower()
        if update.kind in {"results", "concall"}:
            return "earnings"
        if any(token in lowered for token in ("acquisition", "capacity", "plant", "capex", "order", "contract", "launch")):
            return "strategic"
        if any(token in lowered for token in ("warning", "approval", "regulatory", "compliance")):
            return "regulatory"
        return "market"

    def _source_label(self, update: CompanyUpdateItem, final_url: str) -> str:
        host = urlparse(final_url).netloc.lower().removeprefix("www.")
        if host.endswith("bseindia.com"):
            return "BSE filing / transcript"
        if host.endswith("screener.in"):
            return update.source or "Screener"
        return host or update.source or "Company source"

    def _enrich_updates_from_linked_sources(
        self,
        recent_updates: list[CompanyUpdateItem],
        company_website: str | None,
    ) -> tuple[list[CompanyUpdateItem], list[ManagementGuidance], list[DetailedNews]]:
        if not recent_updates:
            return recent_updates, [], []

        enriched_updates: dict[str, CompanyUpdateItem] = {}
        management_guidance: list[ManagementGuidance] = []
        detailed_news: list[DetailedNews] = []
        seen_links: set[str] = set()

        candidates: list[CompanyUpdateItem] = []
        for update in sorted(recent_updates, key=self._update_sort_key, reverse=True):
            link = (update.link or "").strip()
            if not link or link in seen_links:
                continue
            if not self._is_supported_source_link(link, company_website):
                continue
            combined = self._clean_text(f"{update.title}. {update.summary or ''}")
            transcript_markers = ("transcript", "concall", "conference call", "earnings call", "q1", "q2", "q3", "q4")
            if update.kind == "concall" and any(marker in combined.lower() for marker in transcript_markers):
                candidates.append(update)
                seen_links.add(link)
                continue
            if update.title.lower().startswith("transcript"):
                candidates.append(update)
                seen_links.add(link)
                continue
            if update.kind in {"results", "news"} or self._is_material_financial_update(combined):
                candidates.append(update)
                seen_links.add(link)
            if len(candidates) >= 5:
                break

        for update in candidates:
            try:
                source_text, final_url = self._fetch_linked_document_text(update.link or "")
            except Exception:
                continue
            if not source_text:
                continue
            highlights = self._extract_source_highlights(source_text, prefer_management=update.kind == "concall")
            if not highlights:
                continue

            summary_parts = highlights[:2]
            summary = " ".join(summary_parts).strip()
            if summary:
                enriched_updates[update.title.strip().lower()] = update.model_copy(
                    update={
                        "summary": summary,
                        "link": final_url,
                    }
                )

            published_date = self._normalize_update_date(update.published_at) or self._current_ist_date().isoformat()
            source_label = self._source_label(update, final_url)

            if update.kind == "concall" and highlights:
                guidance_key = (published_date, "|".join(highlights[:2]).lower())
                if not any(
                    existing.guidance_date == guidance_key[0]
                    and "|".join(existing.key_guidance_points[:2]).lower() == guidance_key[1]
                    for existing in management_guidance
                ):
                    management_guidance.append(
                        ManagementGuidance(
                            fiscal_year=self._infer_fiscal_year_from_date(update.published_at),
                            guidance_date=published_date,
                            guidance_source=source_label,
                            key_guidance_points=highlights[:3],
                        )
                    )

            if summary:
                title = self._clean_text(update.title)
                news_key = f"{title.lower()}::{published_date}"
                if not any(existing.title.lower() == title.lower() and existing.published_date == published_date for existing in detailed_news):
                    detailed_news.append(
                        DetailedNews(
                            title=title,
                            summary=summary,
                            impact_category=self._source_impact_category(update, summary),
                            sentiment=self._source_sentiment(summary),
                            source=source_label,
                            published_date=published_date,
                            detailed_points=highlights[:3],
                            relevance_score=0.92 if update.kind == "concall" else 0.84,
                        )
                    )

        merged_updates = [
            enriched_updates.get(update.title.strip().lower(), update)
            for update in recent_updates
        ]
        merged_updates.sort(key=self._update_sort_key, reverse=True)
        management_guidance.sort(key=lambda item: item.guidance_date or "", reverse=True)
        detailed_news.sort(key=lambda item: item.published_date, reverse=True)
        return merged_updates, management_guidance, detailed_news

    def _merge_about_text(
        self,
        screener_about: str | None,
        yahoo_about: str | None,
        key_points: list[tuple[str, str]],
    ) -> str | None:
        if screener_about:
            return screener_about
        if yahoo_about:
            return yahoo_about
        if key_points:
            return " ".join(detail for _, detail in key_points[:2]).strip() or None
        return None

    def _extract_section_lines(
        self,
        lines: list[str],
        start_markers: list[str],
        stop_markers: list[str],
    ) -> list[str]:
        start_index = None
        lowered_start_markers = [marker.lower() for marker in start_markers]
        lowered_stop_markers = [marker.lower() for marker in stop_markers]
        for index, line in enumerate(lines):
            normalized = line.lower()
            if any(normalized == marker or normalized.startswith(marker) for marker in lowered_start_markers):
                start_index = index + 1
                break
        if start_index is None:
            return []

        section: list[str] = []
        for line in lines[start_index:]:
            normalized = line.lower()
            if any(normalized == marker or normalized.startswith(marker) for marker in lowered_stop_markers):
                break
            section.append(line)
        return section

    def _extract_screener_valuation(self, lines: list[str]) -> dict[str, float | None]:
        joined = "\n".join(lines)
        return {
            "market_cap_crore": self._search_number(joined, r"Market Cap\s+₹\s*([0-9,]+(?:\.\d+)?)\s*Cr\.?"),
            "pe_ratio": self._search_number(joined, r"Stock P/E\s+([0-9,]+(?:\.\d+)?)"),
            "roe_pct": self._search_number(joined, r"ROE\s+([0-9,]+(?:\.\d+)?)\s*%"),
            "roce_pct": self._search_number(joined, r"ROCE\s+([0-9,]+(?:\.\d+)?)\s*%"),
            "dividend_yield_pct": self._search_number(joined, r"Dividend Yield\s+([0-9,]+(?:\.\d+)?)\s*%"),
        }

    def _search_number(self, text: str, pattern: str) -> float | None:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            return None
        return self._safe_float(match.group(1))

    def _read_html_tables(self, html: str) -> list[pd.DataFrame]:
        try:
            return [table for table in pd.read_html(io.StringIO(html)) if not table.empty]
        except ValueError:
            return []

    def _select_table(self, tables: list[pd.DataFrame], table_type: str) -> pd.DataFrame | None:
        best_frame: pd.DataFrame | None = None
        best_score = -1
        for frame in tables:
            if frame.empty or frame.shape[1] < 3:
                continue
            first_column = frame.iloc[:, 0].astype(str).map(self._normalize_label)
            headers = " ".join(self._clean_text(column).lower() for column in frame.columns[1:])
            score = 0
            if table_type == "quarterly":
                if {"sales", "netprofit"}.issubset(set(first_column)):
                    score += 5
                if any(month in headers for month in ("jun", "sep", "dec")):
                    score += 4
                if "ttm" in headers:
                    score -= 1
            elif table_type == "profit_loss":
                if {"sales", "netprofit"}.issubset(set(first_column)):
                    score += 5
                if "ttm" in headers:
                    score += 4
                if "mar" in headers:
                    score += 2
            elif table_type == "shareholding":
                labels = set(first_column)
                if {"promoters", "fiis", "diis"}.issubset(labels):
                    score += 6
                if "shareholders" in " ".join(first_column):
                    score += 2
            if score > best_score:
                best_frame = frame
                best_score = score
        return best_frame

    def _normalize_label(self, value: Any) -> str:
        text = self._clean_text(value).lower()
        text = text.replace("revenue", "sales").replace("opm", "operating margin")
        text = re.sub(r"[^a-z0-9]+", "", text)
        replacements = {
            "sales": "sales",
            "salesplus": "sales",
            "operatingprofit": "operatingprofit",
            "operatingmargin": "operatingmargin",
            "opm": "operatingmargin",
            "netprofit": "netprofit",
            "netprofitplus": "netprofit",
            "profitbeforetax": "profitbeforetax",
            "epsinrs": "eps",
            "eps": "eps",
            "promoters": "promoters",
            "promotersplus": "promoters",
            "fiis": "fiis",
            "fiisplus": "fiis",
            "diis": "diis",
            "diisplus": "diis",
            "public": "public",
            "publicplus": "public",
            "noofshareholders": "shareholders",
            "numberofshareholders": "shareholders",
            "dividendpayout": "dividendpayout",
            "dividendpayoutpercent": "dividendpayout",
            "expenses": "expenses",
            "expensesplus": "expenses",
        }
        return replacements.get(text, text)

    def _frame_to_row_map(self, frame: pd.DataFrame) -> tuple[list[str], dict[str, list[Any]]]:
        columns = [self._clean_text(column) for column in frame.columns]
        row_map: dict[str, list[Any]] = {}
        for _, row in frame.iterrows():
            key = self._normalize_label(row.iloc[0])
            row_map[key] = list(row.iloc[1:])
        return columns[1:], row_map

    def _parse_quarterly_results_table(self, frame: pd.DataFrame | None) -> list[QuarterlyResultItem]:
        if frame is None:
            return []
        periods, row_map = self._frame_to_row_map(frame)
        results = [
            QuarterlyResultItem(
                period=period,
                sales_crore=self._metric_at(row_map.get("sales"), index),
                expenses_crore=self._metric_at(row_map.get("expenses"), index),
                operating_profit_crore=self._metric_at(row_map.get("operatingprofit"), index),
                operating_margin_pct=self._metric_at(row_map.get("operatingmargin"), index),
                profit_before_tax_crore=self._metric_at(row_map.get("profitbeforetax"), index),
                net_profit_crore=self._metric_at(row_map.get("netprofit"), index),
                eps=self._metric_at(row_map.get("eps"), index),
            )
            for index, period in enumerate(periods)
            if period
        ]
        return results[-8:]

    def _parse_profit_loss_table(self, frame: pd.DataFrame | None) -> list[ProfitLossItem]:
        if frame is None:
            return []
        periods, row_map = self._frame_to_row_map(frame)
        return [
            ProfitLossItem(
                period=period,
                sales_crore=self._metric_at(row_map.get("sales"), index),
                operating_profit_crore=self._metric_at(row_map.get("operatingprofit"), index),
                operating_margin_pct=self._metric_at(row_map.get("operatingmargin"), index),
                net_profit_crore=self._metric_at(row_map.get("netprofit"), index),
                eps=self._metric_at(row_map.get("eps"), index),
                dividend_payout_pct=self._metric_at(row_map.get("dividendpayout"), index),
            )
            for index, period in enumerate(periods)
            if period
        ]

    def _parse_shareholding_table(self, frame: pd.DataFrame | None) -> list[ShareholdingPatternItem]:
        if frame is None:
            return []
        periods, row_map = self._frame_to_row_map(frame)
        results: list[ShareholdingPatternItem] = []
        for index, period in enumerate(periods):
            results.append(
                ShareholdingPatternItem(
                    period=period,
                    promoter_pct=self._metric_at(row_map.get("promoters"), index),
                    fii_pct=self._metric_at(row_map.get("fiis"), index),
                    dii_pct=self._metric_at(row_map.get("diis"), index),
                    public_pct=self._metric_at(row_map.get("public"), index),
                    shareholder_count=self._metric_int_at(row_map.get("shareholders"), index),
                )
            )
        return results[-6:]

    def _merge_quarterly_results(
        self,
        primary: list[QuarterlyResultItem],
        secondary: list[QuarterlyResultItem],
    ) -> list[QuarterlyResultItem]:
        if not primary:
            return secondary
        if not secondary:
            return primary

        merged: dict[str, QuarterlyResultItem] = {item.period: item for item in secondary}
        for item in primary:
            fallback = merged.get(item.period)
            merged[item.period] = QuarterlyResultItem(
                period=item.period,
                sales_crore=item.sales_crore if item.sales_crore is not None else fallback.sales_crore if fallback else None,
                expenses_crore=item.expenses_crore if item.expenses_crore is not None else fallback.expenses_crore if fallback else None,
                operating_profit_crore=item.operating_profit_crore if item.operating_profit_crore is not None else fallback.operating_profit_crore if fallback else None,
                operating_margin_pct=item.operating_margin_pct if item.operating_margin_pct is not None else fallback.operating_margin_pct if fallback else None,
                profit_before_tax_crore=item.profit_before_tax_crore if item.profit_before_tax_crore is not None else fallback.profit_before_tax_crore if fallback else None,
                net_profit_crore=item.net_profit_crore if item.net_profit_crore is not None else fallback.net_profit_crore if fallback else None,
                eps=item.eps if item.eps is not None else fallback.eps if fallback else None,
                result_document_url=item.result_document_url or (fallback.result_document_url if fallback else None),
            )
        return sorted(merged.values(), key=lambda item: self._period_sort_key(item.period))

    def _merge_profit_loss_items(
        self,
        primary: list[ProfitLossItem],
        secondary: list[ProfitLossItem],
    ) -> list[ProfitLossItem]:
        if not primary:
            return secondary
        if not secondary:
            return primary

        merged: dict[str, ProfitLossItem] = {item.period: item for item in secondary}
        for item in primary:
            fallback = merged.get(item.period)
            merged[item.period] = ProfitLossItem(
                period=item.period,
                sales_crore=item.sales_crore if item.sales_crore is not None else fallback.sales_crore if fallback else None,
                operating_profit_crore=item.operating_profit_crore if item.operating_profit_crore is not None else fallback.operating_profit_crore if fallback else None,
                operating_margin_pct=item.operating_margin_pct if item.operating_margin_pct is not None else fallback.operating_margin_pct if fallback else None,
                net_profit_crore=item.net_profit_crore if item.net_profit_crore is not None else fallback.net_profit_crore if fallback else None,
                eps=item.eps if item.eps is not None else fallback.eps if fallback else None,
                dividend_payout_pct=item.dividend_payout_pct if item.dividend_payout_pct is not None else fallback.dividend_payout_pct if fallback else None,
            )
        return sorted(merged.values(), key=lambda item: self._period_sort_key(item.period))

    def _metric_at(self, values: list[Any] | None, index: int) -> float | None:
        if not values or index >= len(values):
            return None
        return self._safe_float(values[index])

    def _metric_int_at(self, values: list[Any] | None, index: int) -> int | None:
        metric = self._metric_at(values, index)
        return int(metric) if metric is not None else None

    def _safe_float(self, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            if pd.isna(value):
                return None
            return float(value)
        text = self._clean_text(value)
        if not text or text in {"-", "—", "--", "nan", "None"}:
            return None
        text = text.replace(",", "").replace("%", "").replace("₹", "").replace("Cr.", "").replace("Rs.", "")
        try:
            return float(text)
        except ValueError:
            return None

    def _parse_yfinance_statement(self, frame: pd.DataFrame, *, quarterly: bool) -> list[QuarterlyResultItem] | list[ProfitLossItem]:
        if frame is None or frame.empty:
            return []

        normalized_index = {self._normalize_label(label): label for label in frame.index}
        sales_row = normalized_index.get("totalrevenue") or normalized_index.get("sales")
        operating_row = normalized_index.get("operatingincome") or normalized_index.get("ebit")
        pbt_row = normalized_index.get("pretaxincome")
        profit_row = normalized_index.get("netincome")
        eps_row = normalized_index.get("dilutedeps") or normalized_index.get("basiceps")

        results: list[QuarterlyResultItem] | list[ProfitLossItem] = []
        for column in reversed(list(frame.columns)):
            period = column.strftime("%b %Y") if hasattr(column, "strftime") else self._clean_text(column)
            sales = self._safe_float(frame.at[sales_row, column]) if sales_row else None
            operating_profit = self._safe_float(frame.at[operating_row, column]) if operating_row else None
            profit = self._safe_float(frame.at[profit_row, column]) if profit_row else None
            eps = self._safe_float(frame.at[eps_row, column]) if eps_row else None
            operating_margin = ((operating_profit / sales) * 100) if sales and operating_profit is not None else None
            if quarterly:
                results.append(
                    QuarterlyResultItem(
                        period=period,
                        sales_crore=self._raw_value_to_crore(sales),
                        operating_profit_crore=self._raw_value_to_crore(operating_profit),
                        operating_margin_pct=round(operating_margin, 2) if operating_margin is not None else None,
                        profit_before_tax_crore=self._raw_value_to_crore(self._safe_float(frame.at[pbt_row, column]) if pbt_row else None),
                        net_profit_crore=self._raw_value_to_crore(profit),
                        eps=eps,
                    )
                )
            else:
                results.append(
                    ProfitLossItem(
                        period=period,
                        sales_crore=self._raw_value_to_crore(sales),
                        operating_profit_crore=self._raw_value_to_crore(operating_profit),
                        operating_margin_pct=round(operating_margin, 2) if operating_margin is not None else None,
                        net_profit_crore=self._raw_value_to_crore(profit),
                        eps=eps,
                    )
                )
        return results

    def _raw_value_to_crore(self, value: float | None) -> float | None:
        if value is None:
            return None
        return round(value / 10_000_000, 2)

    def _build_growth_snapshot(self, quarterly_results: list[QuarterlyResultItem]) -> GrowthSnapshot | None:
        if not quarterly_results:
            return None
        latest = quarterly_results[-1]
        previous = quarterly_results[-2] if len(quarterly_results) >= 2 else None
        year_ago = quarterly_results[-5] if len(quarterly_results) >= 5 else None
        latest_net_margin = self._margin_pct(latest.net_profit_crore, latest.sales_crore)
        previous_net_margin = self._margin_pct(previous.net_profit_crore, previous.sales_crore) if previous else None
        return GrowthSnapshot(
            latest_period=latest.period,
            sales_qoq_pct=self._growth_pct(latest.sales_crore, previous.sales_crore if previous else None),
            sales_yoy_pct=self._growth_pct(latest.sales_crore, year_ago.sales_crore if year_ago else None),
            profit_qoq_pct=self._growth_pct(latest.net_profit_crore, previous.net_profit_crore if previous else None),
            profit_yoy_pct=self._growth_pct(latest.net_profit_crore, year_ago.net_profit_crore if year_ago else None),
            operating_margin_latest_pct=latest.operating_margin_pct,
            operating_margin_previous_pct=previous.operating_margin_pct if previous else None,
            net_margin_latest_pct=latest_net_margin,
            net_margin_previous_pct=previous_net_margin,
        )

    def _build_shareholding_delta(self, items: list[ShareholdingPatternItem]) -> ShareholdingDelta | None:
        if len(items) < 2:
            return None
        latest = items[-1]
        previous = items[-2]
        return ShareholdingDelta(
            latest_period=latest.period,
            previous_period=previous.period,
            promoter_change_pct=self._diff_pct(latest.promoter_pct, previous.promoter_pct),
            fii_change_pct=self._diff_pct(latest.fii_pct, previous.fii_pct),
            dii_change_pct=self._diff_pct(latest.dii_pct, previous.dii_pct),
            public_change_pct=self._diff_pct(latest.public_pct, previous.public_pct),
        )

    def _build_valuation_snapshot(
        self,
        snapshot: StockSnapshot | None,
        screener_payload: dict[str, Any],
        yahoo_payload: dict[str, Any],
        growth: GrowthSnapshot | None,
        profit_loss: list[ProfitLossItem],
    ) -> ValuationSnapshot | None:
        screener_valuation = screener_payload.get("valuation") or {}
        yahoo_valuation = yahoo_payload.get("valuation") or {}
        pe_ratio = screener_valuation.get("pe_ratio") or yahoo_valuation.get("pe_ratio")
        profit_cagr = self._profit_cagr_pct(profit_loss)
        peg_ratio = yahoo_valuation.get("peg_ratio")
        if peg_ratio is None and pe_ratio and profit_cagr and profit_cagr > 0:
            peg_ratio = round(pe_ratio / profit_cagr, 2)

        return ValuationSnapshot(
            market_cap_crore=(snapshot.market_cap_crore if snapshot else None) or screener_valuation.get("market_cap_crore") or yahoo_valuation.get("market_cap_crore"),
            pe_ratio=pe_ratio,
            peg_ratio=peg_ratio,
            operating_margin_pct=growth.operating_margin_latest_pct if growth else yahoo_valuation.get("operating_margin_pct"),
            net_margin_pct=growth.net_margin_latest_pct if growth else yahoo_valuation.get("net_margin_pct"),
            roce_pct=screener_valuation.get("roce_pct") or yahoo_valuation.get("roce_pct"),
            roe_pct=screener_valuation.get("roe_pct") or yahoo_valuation.get("roe_pct"),
            dividend_yield_pct=screener_valuation.get("dividend_yield_pct") or yahoo_valuation.get("dividend_yield_pct"),
        )

    def _is_generic_compliance_update(self, text: str) -> bool:
        normalized = self._clean_text(text).lower()
        if not normalized:
            return False
        material_keywords = (
            "revenue",
            "sales",
            "profit",
            "margin",
            "ebitda",
            "guidance",
            "order",
            "contract",
            "approval",
            "capacity",
            "expansion",
            "acquisition",
            "buyback",
            "results",
            "earnings",
            "demand",
            "pricing",
            "capex",
        )
        if any(keyword in normalized for keyword in material_keywords):
            return False
        generic_keywords = (
            "regulation 30",
            "sebi (lodr)",
            "lodr regulations",
            "analyst / investor meet",
            "analyst/institutional investor meet",
            "investor meet",
            "investor meeting",
            "analyst meeting",
            "one-on-one meeting",
            "plant visit",
            "institutional investor",
            "investment community",
            "engaging with investors",
            "newspaper publication",
            "share certificate",
            "loss of share certificate",
            "trading window",
            "voting results",
            "scrutinizer",
            "certificate under regulation",
            "compliance certificate",
            "intimation",
            "audio recording",
            "transcript",
        )
        return any(keyword in normalized for keyword in generic_keywords)

    def _clean_material_update_text(self, text: str) -> str:
        cleaned = self._clean_text(text)
        cleaned = re.sub(r"^(announcement|disclosure|intimation)\s*[:\-]\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\bAnnouncement Under Regulation 30\b(?:\s*of\s*SEBI\s*\(LODR\)\s*Regulations,?\s*2015)?\s*[:\-]?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^\(?lodr\)?\s*[-:–]?\s*[a-z_ /]+\s*", "", cleaned, flags=re.IGNORECASE)
        return self._clean_text(cleaned)

    def _merge_updates(
        self,
        screener_updates: list[CompanyUpdateItem],
        yahoo_updates: list[CompanyUpdateItem],
    ) -> list[CompanyUpdateItem]:
        merged = screener_updates + yahoo_updates
        seen: set[str] = set()
        items: list[CompanyUpdateItem] = []
        for item in merged:
            key = item.title.strip().lower()
            if not key or key in seen:
                continue
            combined_text = self._clean_text(f"{item.title}. {item.summary or ''}")
            lowered_combined = combined_text.lower()
            if item.kind == "concall" and not any(
                marker in lowered_combined
                for marker in ("transcript", "concall", "conference call", "earnings call", "q1", "q2", "q3", "q4")
            ):
                continue
            if item.kind != "concall" and self._is_generic_compliance_update(combined_text):
                continue
            seen.add(key)
            items.append(item)
        items.sort(key=self._update_sort_key, reverse=True)
        return items

    def _build_growth_drivers(
        self,
        growth: GrowthSnapshot | None,
        shareholding_delta: ShareholdingDelta | None,
        recent_updates: list[CompanyUpdateItem],
        key_points: list[tuple[str, str]],
        pros: list[str],
        cons: list[str],
    ) -> list[GrowthDriver]:
        drivers: list[GrowthDriver] = []
        if growth:
            if growth.sales_yoy_pct is not None:
                tone = "positive" if growth.sales_yoy_pct > 8 else "watch" if growth.sales_yoy_pct < 0 else "neutral"
                drivers.append(
                    GrowthDriver(
                        title="Recent sales trend",
                        detail=f"{growth.latest_period} sales changed {growth.sales_yoy_pct:.2f}% YoY and {growth.sales_qoq_pct or 0:.2f}% QoQ.",
                        tone=tone,
                    )
                )
            if growth.profit_yoy_pct is not None:
                tone = "positive" if growth.profit_yoy_pct > 10 else "watch" if growth.profit_yoy_pct < 0 else "neutral"
                drivers.append(
                    GrowthDriver(
                        title="Recent profit trend",
                        detail=f"Net profit changed {growth.profit_yoy_pct:.2f}% YoY and {growth.profit_qoq_pct or 0:.2f}% QoQ in the latest reported quarter.",
                        tone=tone,
                    )
                )
            if growth.operating_margin_latest_pct is not None and growth.operating_margin_previous_pct is not None:
                margin_delta = growth.operating_margin_latest_pct - growth.operating_margin_previous_pct
                tone = "positive" if margin_delta > 0.5 else "watch" if margin_delta < -0.5 else "neutral"
                drivers.append(
                    GrowthDriver(
                        title="Margin profile",
                        detail=f"Operating margin is {growth.operating_margin_latest_pct:.2f}% versus {growth.operating_margin_previous_pct:.2f}% in the prior quarter.",
                        tone=tone,
                    )
                )

        if shareholding_delta and (shareholding_delta.fii_change_pct is not None or shareholding_delta.dii_change_pct is not None):
            fii_change = shareholding_delta.fii_change_pct or 0.0
            dii_change = shareholding_delta.dii_change_pct or 0.0
            tone = "positive" if fii_change > 0 or dii_change > 0 else "watch" if fii_change < 0 and dii_change < 0 else "neutral"
            drivers.append(
                GrowthDriver(
                    title="Institutional positioning",
                    detail=(
                        f"From {shareholding_delta.previous_period} to {shareholding_delta.latest_period}, "
                        f"FII holding changed {fii_change:+.2f}% and DII holding changed {dii_change:+.2f}%."
                    ),
                    tone=tone,
                )
            )

        for update in recent_updates:
            positive_keywords = ("order", "contract", "expansion", "capacity", "launch", "partnership", "acquisition", "guidance", "approval", "margin", "demand")
            update_text = self._clean_material_update_text(update.summary or update.title)
            source_text = f"{update.title} {update.summary or ''}".lower()
            if any(keyword in source_text for keyword in positive_keywords) and update_text:
                drivers.append(
                    GrowthDriver(
                        title="Recent company update",
                        detail=update_text,
                        tone="positive" if update.kind != "holding" else "neutral",
                    )
                )
                break

        for point in pros[:2]:
            if any(existing.detail == point for existing in drivers):
                continue
            drivers.append(
                GrowthDriver(
                    title="Operational strength",
                    detail=point,
                    tone="positive",
                )
            )

        for point in cons[:1]:
            if any(existing.detail == point for existing in drivers):
                continue
            drivers.append(
                GrowthDriver(
                    title="Risk to monitor",
                    detail=point,
                    tone="watch",
                )
            )

        if not drivers:
            for title, detail in key_points[:2]:
                drivers.append(GrowthDriver(title=title, detail=detail, tone="neutral"))

        return drivers

    def _update_sort_key(self, item: CompanyUpdateItem) -> tuple[int, str]:
        if not item.published_at:
            return (0, item.title.lower())
        try:
            timestamp = datetime.fromisoformat(str(item.published_at).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return (0, item.title.lower())
        return (int(timestamp), item.title.lower())

    def _period_sort_key(self, period: str) -> tuple[int, int, str]:
        cleaned = self._clean_text(period)
        if cleaned.upper() == "TTM":
            return (9999, 12, cleaned)
        for fmt in ("%b %Y", "%B %Y"):
            try:
                parsed = datetime.strptime(cleaned, fmt)
                return (parsed.year, parsed.month, cleaned)
            except ValueError:
                continue
        year_match = re.search(r"(20\d{2})", cleaned)
        if year_match:
            return (int(year_match.group(1)), 12, cleaned)
        return (0, 0, cleaned)

    def _profit_cagr_pct(self, profit_loss: list[ProfitLossItem]) -> float | None:
        annual_items = [item for item in profit_loss if item.period.upper() != "TTM" and item.net_profit_crore and item.net_profit_crore > 0]
        if len(annual_items) < 4:
            return None
        latest = annual_items[-1]
        base = annual_items[-4]
        if not latest.net_profit_crore or not base.net_profit_crore or base.net_profit_crore <= 0:
            return None
        years = 3
        cagr = ((latest.net_profit_crore / base.net_profit_crore) ** (1 / years) - 1) * 100
        return round(cagr, 2)

    def _growth_pct(self, latest: float | None, base: float | None) -> float | None:
        if latest is None or base is None or base == 0:
            return None
        return round(((latest / base) - 1) * 100, 2)

    def _diff_pct(self, latest: float | None, previous: float | None) -> float | None:
        if latest is None or previous is None:
            return None
        return round(latest - previous, 2)

    def _margin_pct(self, profit: float | None, sales: float | None) -> float | None:
        if profit is None or sales in (None, 0):
            return None
        return round((profit / sales) * 100, 2)

    def _extract_date_from_text(self, text: str) -> str | None:
        match = re.search(r"\b(\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})\b", text)
        if not match:
            return None
        raw = match.group(1)
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                continue
        return None

    def _classify_update_kind(self, title: str) -> str:
        lowered = title.lower()
        if any(keyword in lowered for keyword in ("result", "quarter", "earnings", "financial")):
            return "results"
        if any(keyword in lowered for keyword in ("conference", "concall", "investor", "analyst", "transcript", "presentation")):
            return "concall"
        if any(keyword in lowered for keyword in ("promoter", "fii", "dii", "shareholding")):
            return "holding"
        if any(keyword in lowered for keyword in ("order", "contract", "acquisition", "approval", "launch")):
            return "news"
        return "filing"

    def _is_market_open_ist(self) -> bool:
        now = datetime.now(timezone.utc).astimezone(IST)
        if not self._is_trading_day_ist(now.date()):
            return False
        total_minutes = (now.hour * 60) + now.minute
        return MARKET_OPEN_MINUTES_IST <= total_minutes <= MARKET_CLOSE_MINUTES_IST

    def _market_timezone(self):
        return IST

    def _market_close_minutes(self) -> int:
        return MARKET_CLOSE_MINUTES_IST

    def _post_close_refresh_grace_minutes(self) -> int:
        return 30

    def _previous_trading_day(self, trading_date: date) -> date:
        previous = trading_date - timedelta(days=1)
        while not self._is_trading_day_ist(previous):
            previous -= timedelta(days=1)
        return previous

    def _latest_clock_closed_market_session_date(self) -> date:
        market_timezone = self._market_timezone()
        market_now = datetime.now(timezone.utc).astimezone(market_timezone)
        session_date = market_now.date()
        session_close = datetime.combine(session_date, datetime.min.time(), tzinfo=market_timezone) + timedelta(
            minutes=self._market_close_minutes()
        )
        if not self._is_trading_day_ist(session_date) or market_now < session_close:
            return self._previous_trading_day(session_date)
        return session_date

    def _latest_closed_session_close(self) -> datetime:
        market_timezone = self._market_timezone()
        market_now = datetime.now(timezone.utc).astimezone(market_timezone)
        session_date = market_now.date()
        session_close = datetime.combine(session_date, datetime.min.time(), tzinfo=market_timezone) + timedelta(
            minutes=self._market_close_minutes()
        )
        if not self._is_trading_day_ist(session_date) or market_now < session_close:
            session_date = self._previous_trading_day(session_date)
            session_close = datetime.combine(session_date, datetime.min.time(), tzinfo=market_timezone) + timedelta(
                minutes=self._market_close_minutes()
            )
        return session_close

    def _market_close_refresh_due(self) -> bool:
        cached_rows = self._load_valid_cached_snapshot_rows()
        if not cached_rows:
            return True
        snapshot_session_date = self._snapshot_rows_session_date(cached_rows)
        if snapshot_session_date is None:
            return True
        return snapshot_session_date < self._latest_completed_market_session_date()

    def _strict_closed_session_refresh_due(self) -> bool:
        if not self._market_close_refresh_due():
            return False
        latest_closed_session_close = self._latest_closed_session_close()
        strict_refresh_at = latest_closed_session_close + timedelta(minutes=self._post_close_refresh_grace_minutes())
        market_now = datetime.now(timezone.utc).astimezone(self._market_timezone())
        return market_now >= strict_refresh_at

    def preferred_refresh_strategy(self) -> str:
        if not self._load_valid_cached_snapshot_rows():
            return "historical"
        if not self._is_market_open_ist() and self._market_close_refresh_due():
            return "historical"
        return "cache"

    def _snapshot_age_seconds(self) -> float:
        if not self.snapshot_cache_path.exists():
            return float("inf")
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(self.snapshot_cache_path.stat().st_mtime, tz=timezone.utc)
        return max(age.total_seconds(), 0.0)

    def _write_snapshot_rows(self, rows: list[dict[str, Any]]) -> None:
        temp_path = self.snapshot_cache_path.with_suffix(f"{self.snapshot_cache_path.suffix}.tmp")
        temp_path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        os.replace(temp_path, self.snapshot_cache_path)

    def _should_rebuild_snapshot_history(self, rows: list[dict[str, Any]], force_refresh: bool) -> bool:
        if not rows:
            return True
        session_date = self._parse_row_date(rows[0].get("history_session_date"))
        if session_date is None:
            return True
        current_session = self._current_or_previous_trading_day_ist()
        stale_days = max((current_session - session_date).days, 0)
        if force_refresh:
            return stale_days >= 1
        if not self._is_trading_day_ist():
            return False
        return stale_days >= 3

    def _normalize_cached_snapshot_rows(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
        normalized_rows: list[dict[str, Any]] = []
        changed = False
        for row in rows:
            if not isinstance(row, dict):
                normalized_rows.append(row)
                continue
            normalized_row = dict(row)
            for field in ("history_session_date", "history_as_of_date"):
                stored_date = self._parse_row_date(normalized_row.get(field))
                if stored_date is None or self._is_trading_day_ist(stored_date):
                    continue
                normalized_row[field] = self._previous_trading_day(stored_date).isoformat()
                changed = True
            normalized_rows.append(normalized_row)
        return normalized_rows, changed

    def _should_live_refresh_snapshot_cache(self, force_refresh: bool) -> bool:
        if not self.snapshot_cache_path.exists():
            return False
        if force_refresh:
            return True
        if not self._is_market_open_ist():
            return False
        return self._snapshot_age_seconds() >= LIVE_SNAPSHOT_MAX_AGE_SECONDS

    def _fetch_nse_live_prices(self) -> dict[str, dict[str, Any]]:
        prices: dict[str, dict[str, Any]] = {}
        fetched_at = int(datetime.now(timezone.utc).timestamp())
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
        }
        with requests.Session() as session:
            try:
                session.get("https://www.nseindia.com", headers=headers, timeout=10)
            except Exception:
                pass
            
            for url in [
                "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050",
                "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20TOTAL%20MARKET",
                "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20MICROCAP%20250"
            ]:
                try:
                    res = session.get(url, headers=headers, timeout=15)
                    if res.status_code == 200:
                        data = res.json().get("data", [])
                        for item in data:
                            sym = item.get("symbol")
                            if sym:
                                parsed_last_update = self._parse_nse_last_update_time(item.get("lastUpdateTime"))
                                prices[sym.upper()] = {
                                    "regularMarketPrice": self._to_float(item.get("lastPrice")),
                                    "regularMarketPreviousClose": self._to_float(item.get("previousClose")),
                                    "regularMarketDayHigh": self._to_float(item.get("dayHigh")),
                                    "regularMarketDayLow": self._to_float(item.get("dayLow")),
                                    "regularMarketOpen": self._to_float(item.get("open")),
                                    "regularMarketVolume": self._to_float(item.get("totalTradedVolume")),
                                    "regularMarketTime": int(parsed_last_update.timestamp()) if parsed_last_update else (fetched_at if self._is_market_open_ist() else None),
                                    "lastUpdateTime": item.get("lastUpdateTime"),
                                }
                except Exception:
                    pass
        return prices

    def _live_quote_from_nse_quote_payload(self, symbol: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        price_info = payload.get("priceInfo") or {}
        metadata = payload.get("metadata") or {}
        trade_info = ((payload.get("marketDeptOrderBook") or {}).get("tradeInfo") or {})
        intra_day = price_info.get("intraDayHighLow") or {}
        last_update = (
            metadata.get("lastUpdateTime")
            or price_info.get("lastUpdateTime")
            or trade_info.get("lastUpdateTime")
        )
        parsed_last_update = self._parse_nse_last_update_time(last_update)
        fetched_at = int(datetime.now(timezone.utc).timestamp())
        quote = {
            "symbol": symbol,
            "regularMarketPrice": self._to_float(price_info.get("lastPrice") or metadata.get("lastPrice")),
            "regularMarketPreviousClose": self._to_float(
                price_info.get("previousClose") or metadata.get("previousClose")
            ),
            "regularMarketDayHigh": self._to_float(
                intra_day.get("max") or price_info.get("dayHigh") or metadata.get("dayHigh")
            ),
            "regularMarketDayLow": self._to_float(
                intra_day.get("min") or price_info.get("dayLow") or metadata.get("dayLow")
            ),
            "regularMarketOpen": self._to_float(price_info.get("open") or metadata.get("open")),
            "regularMarketVolume": self._to_float(
                trade_info.get("totalTradedVolume")
                or trade_info.get("totalTradedQty")
                or metadata.get("totalTradedVolume")
            ),
            "regularMarketTime": int(parsed_last_update.timestamp()) if parsed_last_update else fetched_at,
            "lastUpdateTime": last_update,
        }
        if self._quote_number(quote, "regularMarketPrice") in (None, 0):
            return None
        return quote

    def _fetch_nse_quote_equity_live(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        normalized_symbols = sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()})
        if not normalized_symbols:
            return {}

        quotes: dict[str, dict[str, Any]] = {}
        workers = min(8, max(2, len(normalized_symbols) // 10 or 1))
        chunk_size = max(1, (len(normalized_symbols) + workers - 1) // workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._fetch_nse_quote_equity_live_chunk, normalized_symbols[index : index + chunk_size]): index
                for index in range(0, len(normalized_symbols), chunk_size)
            }
            for future in as_completed(futures):
                try:
                    quotes.update(future.result())
                except Exception:
                    continue
        return quotes

    def _fetch_nse_quote_equity_live_chunk(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nseindia.com/",
        }
        quotes: dict[str, dict[str, Any]] = {}
        with httpx.Client(timeout=10, headers=headers, follow_redirects=True) as client:
            client.get("https://www.nseindia.com/")
            for symbol in symbols:
                try:
                    response = client.get(NSE_QUOTE_EQUITY_URL, params={"symbol": symbol})
                    response.raise_for_status()
                    quote = self._live_quote_from_nse_quote_payload(symbol, response.json())
                    if quote:
                        quotes[symbol] = quote
                except Exception:
                    continue
        return quotes

    def _fetch_yahoo_live_quotes(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        normalized_tickers = [str(ticker).strip() for ticker in tickers if str(ticker).strip()]
        if not normalized_tickers:
            return {}

        quotes: dict[str, dict[str, Any]] = {}
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://finance.yahoo.com/",
        }

        with httpx.Client(timeout=20, headers=headers, follow_redirects=True) as client:
            for start in range(0, len(normalized_tickers), 50):
                chunk = normalized_tickers[start : start + 50]
                response = client.get(
                    YAHOO_QUOTE_URL,
                    params={"symbols": ",".join(chunk)},
                )
                response.raise_for_status()
                payload = response.json()

                for item in (payload.get("quoteResponse") or {}).get("result") or []:
                    symbol = str(item.get("symbol") or "").strip().upper()
                    if not symbol:
                        continue
                    quotes[symbol] = {
                        "symbol": symbol,
                        "regularMarketPrice": self._to_float(item.get("regularMarketPrice")),
                        "regularMarketPreviousClose": self._to_float(item.get("regularMarketPreviousClose") or item.get("regularMarketPreviousClose")),
                        "regularMarketDayHigh": self._to_float(item.get("regularMarketDayHigh")),
                        "regularMarketDayLow": self._to_float(item.get("regularMarketDayLow")),
                        "regularMarketOpen": self._to_float(item.get("regularMarketOpen")),
                        "regularMarketVolume": self._to_float(item.get("regularMarketVolume")),
                        "regularMarketTime": item.get("regularMarketTime"),
                        "fiftyTwoWeekHigh": self._to_float(item.get("fiftyTwoWeekHigh")),
                        "fiftyTwoWeekLow": self._to_float(item.get("fiftyTwoWeekLow")),
                    }
        return quotes

    def _fetch_quote_batch(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        normalized_tickers = [str(ticker).strip() for ticker in tickers if str(ticker).strip()]
        if not normalized_tickers:
            return {}

        quotes: dict[str, dict[str, Any]] = {}
        try:
            quotes.update(self._fetch_yahoo_live_quotes(normalized_tickers))
        except Exception:
            pass

        missing_tickers = [ticker for ticker in normalized_tickers if ticker.upper() not in quotes]
        if missing_tickers:
            try:
                quotes.update(self._fetch_intraday_quote_batch(missing_tickers))
            except Exception:
                pass

        missing_tickers = [ticker for ticker in normalized_tickers if ticker.upper() not in quotes]
        if not missing_tickers:
            return quotes

        quotes.update(self._fetch_daily_quote_batch(missing_tickers))
        return quotes

    def _fetch_intraday_quote_batch(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        normalized_tickers = [str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()]
        if not normalized_tickers:
            return {}

        result: dict[str, dict[str, Any]] = {}
        for start in range(0, len(normalized_tickers), QUOTE_DOWNLOAD_BATCH_SIZE):
            batch = normalized_tickers[start : start + QUOTE_DOWNLOAD_BATCH_SIZE]
            try:
                df = yf.download(
                    tickers=batch,
                    period="2d",
                    interval="1m",
                    group_by="ticker" if len(batch) > 1 else "column",
                    auto_adjust=False,
                    progress=False,
                    threads=True,
                )
            except Exception:
                continue
            if df.empty:
                continue

            for ticker_str in batch:
                try:
                    upper_ticker = ticker_str.upper()
                    ticker_data = self._extract_history(df, upper_ticker)
                    if ticker_data is None or ticker_data.empty:
                        continue

                    ticker_data = ticker_data.dropna(how="all")
                    if ticker_data.empty:
                        continue

                    ticker_data = ticker_data.rename(columns=str.title)
                    latest = ticker_data.iloc[-1]
                    latest_close = self._to_float(latest.get("Close"))
                    if latest_close in (None, 0):
                        continue

                    timestamp_index = pd.DatetimeIndex(pd.to_datetime(ticker_data.index))
                    latest_timestamp = pd.Timestamp(timestamp_index[-1])
                    latest_session_date = latest_timestamp.date()
                    same_session_mask = timestamp_index.date == latest_session_date
                    session_rows = ticker_data.loc[same_session_mask]
                    previous_rows = ticker_data.loc[~same_session_mask]

                    previous_close = None
                    if not previous_rows.empty and "Close" in previous_rows:
                        previous_close_series = previous_rows["Close"].dropna()
                        if not previous_close_series.empty:
                            previous_close = float(previous_close_series.iloc[-1])
                    if previous_close in (None, 0) and len(ticker_data) >= 2:
                        previous_close = self._to_float(ticker_data.iloc[-2].get("Close"))

                    high_series = session_rows["High"].dropna() if "High" in session_rows else pd.Series(dtype=float)
                    low_series = session_rows["Low"].dropna() if "Low" in session_rows else pd.Series(dtype=float)
                    open_series = session_rows["Open"].dropna() if "Open" in session_rows else pd.Series(dtype=float)
                    volume_series = session_rows["Volume"].fillna(0) if "Volume" in session_rows else pd.Series(dtype=float)

                    market_time = latest_timestamp.to_pydatetime()
                    if market_time.tzinfo is None:
                        market_time = market_time.replace(tzinfo=timezone.utc)

                    result[upper_ticker] = {
                        "symbol": upper_ticker,
                        "regularMarketPrice": float(latest_close),
                        "regularMarketPreviousClose": float(previous_close or latest_close),
                        "regularMarketDayHigh": float(high_series.max() if not high_series.empty else latest_close),
                        "regularMarketDayLow": float(low_series.min() if not low_series.empty else latest_close),
                        "regularMarketOpen": float(open_series.iloc[0] if not open_series.empty else previous_close or latest_close),
                        "regularMarketVolume": int(float(volume_series.sum()) or 0),
                        "regularMarketTime": int(market_time.timestamp()),
                    }
                except Exception:
                    continue

        return result

    def _fetch_daily_quote_batch(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        normalized_tickers = [str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()]
        if not normalized_tickers:
            return {}

        result: dict[str, dict[str, Any]] = {}
        for start in range(0, len(normalized_tickers), QUOTE_DOWNLOAD_BATCH_SIZE):
            batch = normalized_tickers[start : start + QUOTE_DOWNLOAD_BATCH_SIZE]
            try:
                df = yf.download(
                    tickers=batch,
                    period="2d",
                    interval="1d",
                    group_by="ticker" if len(batch) > 1 else "column",
                    progress=False,
                    threads=True,
                )
            except Exception:
                continue
            if df.empty:
                continue

            for ticker_str in batch:
                try:
                    upper_ticker = ticker_str.upper()
                    ticker_data = self._extract_history(df, upper_ticker)
                    if ticker_data is None or ticker_data.empty:
                        continue

                    ticker_data = ticker_data.dropna(how="all")
                    if ticker_data.empty:
                        continue

                    latest = ticker_data.iloc[-1]
                    previous_close = float(ticker_data.iloc[-2]["Close"]) if len(ticker_data) >= 2 else 0.0

                    result[upper_ticker] = {
                        "symbol": upper_ticker,
                        "regularMarketPrice": float(latest["Close"]),
                        "regularMarketPreviousClose": previous_close,
                        "regularMarketDayHigh": float(latest["High"]),
                        "regularMarketDayLow": float(latest["Low"]),
                        "regularMarketOpen": float(latest["Open"]),
                        "regularMarketVolume": int(latest["Volume"]),
                        "regularMarketTime": int(latest.name.timestamp()) if hasattr(latest.name, "timestamp") else int(datetime.now(timezone.utc).timestamp()),
                    }
                except Exception:
                    continue

        return result

    def _fetch_live_quotes_for_rows(self, rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
        live_quotes: dict[str, dict[str, Any]] = {}
        sources: dict[str, str] = {}

        try:
            nse_quotes = self._fetch_nse_live_prices()
        except Exception:
            nse_quotes = {}

        for row in rows:
            symbol = str(row.get("symbol") or "").upper()
            quote = nse_quotes.get(symbol)
            if not quote:
                continue
            live_quotes[symbol] = quote
            sources[symbol] = "nse"

        missing_rows = [row for row in rows if str(row.get("symbol") or "").upper() not in live_quotes]
        missing_nse_rows = sorted(
            [row for row in missing_rows if str(row.get("exchange") or "").upper() == "NSE"],
            key=lambda row: (
                float(row.get("market_cap_crore", 0) or 0),
                float(row.get("avg_rupee_volume_30d_crore", 0) or 0),
            ),
            reverse=True,
        )
        direct_nse_symbols = [
            str(row.get("symbol") or "").upper()
            for row in missing_nse_rows[:NSE_DIRECT_QUOTE_FALLBACK_MAX_SYMBOLS]
        ]
        if direct_nse_symbols:
            try:
                direct_nse_quotes = self._fetch_nse_quote_equity_live(direct_nse_symbols)
            except Exception:
                direct_nse_quotes = {}

            for row in missing_rows:
                symbol = str(row.get("symbol") or "").upper()
                quote = direct_nse_quotes.get(symbol)
                if not quote:
                    continue
                live_quotes[symbol] = quote
                sources[symbol] = "nse-direct"

        missing_rows = [row for row in rows if str(row.get("symbol") or "").upper() not in live_quotes]
        if not missing_rows:
            return live_quotes, sources

        # During India live hours, keep refresh bounded to exchange-native data.
        # Bulk NSE feeds plus a small direct fallback keep visible surfaces fresh
        # without blocking on slow Yahoo backfills for the long tail.
        if self._default_exchange() == "NSE" and self._is_market_open_ist():
            return live_quotes, sources

        yahoo_tickers = [
            str(row.get("instrument_key") or f"{str(row.get('symbol') or '').upper()}.NS").upper()
            for row in missing_rows
        ]
        try:
            yahoo_quotes = self._fetch_quote_batch(yahoo_tickers)
        except Exception:
            yahoo_quotes = {}

        for row in missing_rows:
            symbol = str(row.get("symbol") or "").upper()
            ticker = str(row.get("instrument_key") or f"{symbol}.NS").upper()
            quote = yahoo_quotes.get(ticker) or yahoo_quotes.get(symbol)
            if not quote:
                continue
            live_quotes[symbol] = quote
            sources[symbol] = "yahoo"

        return live_quotes, sources

    def _load_or_refresh_snapshots(self, market_cap_min_crore: float, force_refresh: bool) -> list[dict[str, Any]]:
        self._last_refresh_metadata = self._default_refresh_metadata()
        cached_rows: list[dict[str, Any]] = []
        if self.snapshot_cache_path.exists():
            cached_rows = json.loads(self.snapshot_cache_path.read_text(encoding="utf-8"))
            if cached_rows and self._snapshot_schema_ok(cached_rows):
                working_rows = cached_rows
                if self._should_rebuild_snapshot_history(cached_rows, force_refresh):
                    universe = self._load_or_refresh_universe(market_cap_min_crore, force_refresh)
                    working_rows = self._build_snapshot_cache(universe)
                    self._write_snapshot_rows(working_rows)
                    self._last_refresh_metadata["historical_rebuild"] = True

                filtered = [row for row in working_rows if float(row.get("market_cap_crore", 0)) >= market_cap_min_crore]
                if filtered and (
                    force_refresh
                    or self._last_refresh_metadata["historical_rebuild"]
                    or self._is_fresh(self.snapshot_cache_path, max_age_hours=12)
                ):
                    return filtered

        universe = self._load_or_refresh_universe(market_cap_min_crore, force_refresh)
        snapshots = self._build_snapshot_cache(universe)
        self._write_snapshot_rows(snapshots)
        self._last_refresh_metadata["historical_rebuild"] = True

        return [row for row in snapshots if float(row.get("market_cap_crore", 0)) >= market_cap_min_crore]

    def _refresh_snapshot_rows_live(self, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if not rows:
            return rows, self._default_refresh_metadata()

        refreshed_rows = [dict(row) for row in rows]
        live_prices, quote_sources = self._fetch_live_quotes_for_rows(refreshed_rows)
        benchmark_quote = self._live_quote_for_symbol(self._benchmark_label(), self._benchmark_symbol())
        applied_quote_count = 0
        applied_sources: set[str] = set()

        for row in refreshed_rows:
            symbol = str(row.get("symbol") or "").upper()
            quote = live_prices.get(symbol)
            if not quote:
                continue
            quote_updated_at = self._quote_updated_at_from_quote(quote)
            if not self._is_live_quote_recent(quote_updated_at):
                continue
            if not self._quote_matches_snapshot_session(symbol, quote):
                continue
            patch = self._quote_to_live_snapshot_patch(row, quote, benchmark_quote)
            if not self._live_quote_matches_cached_scale(row.get("last_price"), patch.get("last_price")):
                continue
            if not self._row_has_sane_session_price_scale({**row, **patch}):
                continue
            applied_quote_count += 1
            applied_sources.add(quote_sources.get(symbol, "unknown"))
            row.update(patch)

        refreshed_rows = self._apply_sector_benchmarks(refreshed_rows)
        refreshed_rows = self._apply_rs_rating(refreshed_rows)
        return refreshed_rows, {
            "applied_quote_count": applied_quote_count,
            "historical_rebuild": False,
            "quote_source": self._merge_quote_sources(applied_sources),
        }

    @staticmethod
    def _quote_number(quote: dict[str, Any] | None, key: str) -> float | None:
        if not quote:
            return None
        value = quote.get(key)
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _quote_change_pct(self, quote: dict[str, Any] | None) -> float:
        current_price = self._quote_number(quote, "regularMarketPrice")
        previous_close = self._quote_number(quote, "regularMarketPreviousClose")
        if current_price in (None, 0) or previous_close in (None, 0):
            return 0.0
        return round(((current_price / previous_close) - 1) * 100, 2)

    @staticmethod
    def _parse_nse_last_update_time(value: Any) -> datetime | None:
        if not isinstance(value, str) or not value.strip():
            return None
        for pattern in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
            try:
                return datetime.strptime(value.strip(), pattern).replace(tzinfo=IST).astimezone(timezone.utc)
            except ValueError:
                continue
        return None

    @classmethod
    def _quote_updated_at_from_quote(cls, quote: dict[str, Any] | None) -> datetime | None:
        if not quote:
            return None
        market_time = quote.get("regularMarketTime")
        if market_time not in (None, ""):
            try:
                return datetime.fromtimestamp(int(float(market_time)), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                pass
        return cls._parse_nse_last_update_time(quote.get("lastUpdateTime"))

    def _is_live_quote_recent(self, updated_at: datetime | None) -> bool:
        if updated_at is None:
            return False
        now_utc = datetime.now(timezone.utc)
        quote_session_date = updated_at.astimezone(IST).date()
        current_session_date = self._current_or_previous_trading_day_ist()
        age_seconds = max((now_utc - updated_at).total_seconds(), 0.0)
        if self._is_market_open_ist():
            return quote_session_date == current_session_date and age_seconds <= 20 * 60
        return quote_session_date == current_session_date and age_seconds <= 72 * 60 * 60

    def _quote_to_live_snapshot_patch(
        self,
        row: dict[str, Any],
        quote: dict[str, Any],
        benchmark_quote: dict[str, Any] | None,
    ) -> dict[str, Any]:
        quote_updated_at = self._quote_updated_at_from_quote(quote)
        current_price = self._quote_number(quote, "regularMarketPrice") or float(row.get("last_price") or 0)
        previous_close = self._quote_number(quote, "regularMarketPreviousClose") or float(row.get("previous_close") or row.get("last_price") or current_price)
        session_high = self._quote_number(quote, "regularMarketDayHigh") or current_price
        session_low = self._quote_number(quote, "regularMarketDayLow") or current_price
        session_open = self._quote_number(quote, "regularMarketOpen") or previous_close or current_price
        session_volume = int(self._quote_number(quote, "regularMarketVolume") or row.get("volume") or 0)
        high_52w = self._quote_number(quote, "fiftyTwoWeekHigh") or float(row.get("high_52w") or session_high)
        low_52w = self._quote_number(quote, "fiftyTwoWeekLow") or float(row.get("low_52w") or session_low)
        benchmark_change_pct = self._quote_change_pct(benchmark_quote)
        benchmark_current_price = self._quote_number(benchmark_quote, "regularMarketPrice")

        recent_highs = [float(value) for value in row.get("recent_highs", []) if value is not None]
        recent_lows = [float(value) for value in row.get("recent_lows", []) if value is not None]
        recent_volumes = [int(value) for value in row.get("recent_volumes", []) if value is not None]
        rolling_high = max([*recent_highs, session_high, current_price]) if recent_highs else max(session_high, current_price)

        def safe_max(*values: Any) -> float:
            valid = [float(value) for value in values if value not in (None, "")]
            return max(valid) if valid else current_price

        def safe_min(*values: Any) -> float:
            valid = [float(value) for value in values if value not in (None, "")]
            return min(valid) if valid else current_price

        live_change_pct = ((current_price / previous_close) - 1) * 100 if previous_close else 0.0
        live_gap_pct = ((session_open / previous_close) - 1) * 100 if previous_close else 0.0
        pullback_depth = ((rolling_high - current_price) / rolling_high) * 100 if rolling_high else 0.0
        adr_pct_20 = float(row.get("adr_pct_20") or row.get("long_base_avg_range_pct") or 0.0)
        quote_session_date = (
            quote_updated_at.astimezone(IST).date().isoformat()
            if quote_updated_at is not None
            else self._current_or_previous_trading_day_ist().isoformat()
        )
        stock_return_5d = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_5d")), float(row.get("stock_return_5d", 0) or 0))
        stock_return_20d = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_20d")), float(row.get("stock_return_20d", 0) or 0))
        stock_return_40d = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_40d")), float(row.get("stock_return_40d", 0) or 0))
        stock_return_60d = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_60d")), float(row.get("stock_return_60d", 0) or 0))
        stock_return_126d = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_126d")), float(row.get("stock_return_126d", 0) or 0))
        stock_return_189d = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_189d")), float(row.get("stock_return_189d", 0) or 0))
        stock_return_12m = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_252d")), float(row.get("stock_return_12m", 0) or 0))
        stock_return_504d = self._return_from_baseline(current_price, self._to_float(row.get("baseline_close_504d")), float(row.get("stock_return_504d", 0) or 0))
        rs_weighted_score = self._weighted_rs_score_from_price(row, current_price)
        chart_grid_points = self._apply_live_quote_to_chart_grid_points(row.get("chart_grid_points"), quote_updated_at, current_price)
        rs_line_today = (
            ((current_price / benchmark_current_price) * 100)
            if benchmark_current_price not in (None, 0)
            else float(row.get("rs_line_today") or 0)
        )

        return {
            "snapshot_cache_version": SNAPSHOT_CACHE_VERSION,
            "last_price": round(current_price, 2),
            "previous_close": round(previous_close, 2),
            "change_pct": round(live_change_pct, 2),
            "benchmark_return_1d": benchmark_change_pct,
            "volume": session_volume,
            "day_high": round(session_high, 2),
            "day_low": round(session_low, 2),
            "week_high": round(safe_max(session_high, row.get("week_high_prev"), row.get("week_high")), 2),
            "week_low": round(safe_min(session_low, row.get("week_low_prev"), row.get("week_low")), 2),
            "month_high": round(safe_max(session_high, row.get("month_high_prev"), row.get("month_high")), 2),
            "month_low": round(safe_min(session_low, row.get("month_low_prev"), row.get("month_low")), 2),
            "ath": round(safe_max(session_high, row.get("ath_prev"), row.get("ath")), 2),
            "atl": round(safe_min(session_low, row.get("atl")), 2),
            "high_52w": round(safe_max(session_high, row.get("high_52w_prev"), high_52w), 2),
            "low_52w": round(safe_min(session_low, row.get("low_52w_prev"), low_52w), 2),
            "high_6m": round(safe_max(session_high, row.get("high_6m_prev"), row.get("high_6m")), 2),
            "low_6m": round(safe_min(session_low, row.get("low_6m_prev"), row.get("low_6m")), 2),
            "high_3m": round(safe_max(session_high, row.get("high_3m")), 2),
            "range_high_20d": round(safe_max(session_high, row.get("range_high_prev_20d"), row.get("range_high_20d")), 2),
            "stock_return_5d": round(stock_return_5d, 2),
            "stock_return_20d": round(stock_return_20d, 2),
            "stock_return_40d": round(stock_return_40d, 2),
            "stock_return_60d": round(stock_return_60d, 2),
            "stock_return_126d": round(stock_return_126d, 2),
            "stock_return_189d": round(stock_return_189d, 2),
            "stock_return_12m": round(stock_return_12m, 2),
            "stock_return_504d": round(stock_return_504d, 2),
            "rs_weighted_score": round(rs_weighted_score, 4),
            "rs_line_today": round(rs_line_today, 2),
            "gap_pct": round(live_gap_pct, 2),
            "adr_pct_20": round(adr_pct_20, 2),
            "pullback_depth_pct": round(float(pullback_depth), 2),
            "trend_strength": round(
                float(
                    self._trend_strength(
                        current_price,
                        self._to_float(row.get("ema20")),
                        self._to_float(row.get("ema50")),
                        self._to_float(row.get("ema200")),
                    )
                ),
                2,
            ),
            "recent_highs": [*recent_highs[:-1], round(max(recent_highs[-1], session_high), 2)] if recent_highs else [round(session_high, 2)],
            "recent_lows": [*recent_lows[:-1], round(min(recent_lows[-1], session_low), 2)] if recent_lows else [round(session_low, 2)],
            "recent_volumes": [*recent_volumes[:-1], max(recent_volumes[-1], session_volume)] if recent_volumes else [session_volume],
            "chart_grid_points": chart_grid_points,
            "history_session_date": quote_session_date,
            "history_as_of_date": quote_session_date,
        }

    def _fetch_index_quotes(self, symbols: list[str]) -> list[IndexQuoteItem]:
        items: list[IndexQuoteItem] = []

        for symbol in symbols:
            try:
                chart_item = self._index_quote_from_cached_bars(symbol)
            except Exception:
                chart_item = None
            if chart_item is not None:
                items.append(chart_item)

        return items

    def _index_quote_from_bars(self, symbol: str, bars: list[ChartBar]) -> IndexQuoteItem | None:
        if len(bars) < 2:
            return None

        last_bar = bars[-1]
        previous_bar = bars[-2]
        current_price = float(last_bar.close or 0)
        previous_close = float(previous_bar.close or last_bar.close or 0)
        if current_price <= 0 or previous_close <= 0:
            return None

        return IndexQuoteItem(
            symbol=symbol,
            price=round(current_price, 2),
            change_pct=round(((current_price / previous_close) - 1) * 100, 2),
            updated_at=datetime.fromtimestamp(int(last_bar.time), tz=timezone.utc),
        )

    def _index_quote_from_cached_bars(self, symbol: str) -> IndexQuoteItem | None:
        daily_item = self._index_quote_from_bars(symbol, self._read_chart_cache(symbol, "1D", 5))
        if daily_item is not None:
            return daily_item
        return self._index_quote_from_bars(symbol, self._read_chart_cache(symbol, "15m", 64))

    def _fetch_index_quote_from_history(self, symbol: str) -> IndexQuoteItem | None:
        try:
            bars = self._fetch_chart_bars(symbol, "1D", 5)
        except Exception:
            return None

        if len(bars) < 2:
            return None

        current_bar = bars[-1]
        previous_bar = bars[-2]
        current_price = float(current_bar.close or 0)
        previous_close = float(previous_bar.close or 0)
        if current_price <= 0 or previous_close <= 0:
            return None

        return IndexQuoteItem(
            symbol=symbol,
            price=round(current_price, 2),
            change_pct=round(((current_price / previous_close) - 1) * 100, 2),
            updated_at=datetime.fromtimestamp(int(current_bar.time), tz=timezone.utc),
        )

    def _fetch_nse_index_quote(self, symbol: str) -> IndexQuoteItem | None:
        index_name = INDEX_SYMBOL_TO_NSE_NAME.get(symbol.upper())
        if not index_name:
            return None

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nseindia.com/",
        }
        with httpx.Client(timeout=20, headers=headers, follow_redirects=True) as client:
            client.get("https://www.nseindia.com/")
            response = client.get(NSE_ALL_INDICES_URL)
            response.raise_for_status()
            payload = response.json()

        rows = payload.get("data") or []
        matched = next((row for row in rows if str(row.get("index", "")).strip().upper() == index_name.upper()), None)
        if not matched:
            return None

        last_price = matched.get("last")
        percent_change = matched.get("percentChange")
        updated_at = datetime.now(timezone.utc)
        last_update = matched.get("lastUpdateTime")
        if isinstance(last_update, str) and last_update.strip():
            for pattern in ("%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M"):
                try:
                    updated_at = datetime.strptime(last_update.strip(), pattern).replace(tzinfo=IST).astimezone(timezone.utc)
                    break
                except ValueError:
                    continue

        try:
            return IndexQuoteItem(
                symbol=symbol,
                price=round(float(last_price), 2),
                change_pct=round(float(percent_change), 2),
                updated_at=updated_at,
            )
        except (TypeError, ValueError):
            return None

    def get_market_overview(self) -> list[dict[str, Any]]:
        """Fetch crude oil price + NSE index levels with P/E for the market macro strip."""
        results: list[dict[str, Any]] = []
        snapshot_only_mode = self._is_market_open_ist()

        def history_close_summary(symbol: str) -> tuple[float | None, float | None]:
            try:
                bars = self._fetch_chart_bars(symbol, "1D", 5)
            except Exception:
                return None, None
            if len(bars) < 2:
                return None, None
            latest_price = float(bars[-1].close or 0)
            previous_close = float(bars[-2].close or 0)
            if latest_price <= 0 or previous_close <= 0:
                return None, None
            return round(latest_price, 2), round(((latest_price / previous_close) - 1) * 100, 2)

        # ── Crude Oil WTI ─────────────────────────────────────────────────────
        if snapshot_only_mode:
            price, change_pct = history_close_summary("CL=F")
            results.append({
                "symbol": "CL=F",
                "label": "Crude Oil WTI",
                "price": price,
                "change_pct": change_pct,
                "trailing_pe": None,
                "currency": "USD",
            })
        else:
            try:
                tkr = yf.Ticker("CL=F")
                info = tkr.fast_info
                price = float(info.last_price) if info.last_price else None
                prev_close = float(info.previous_close) if info.previous_close else None
                change_pct = round(((price / prev_close) - 1) * 100, 2) if price and prev_close else None
                results.append({
                    "symbol": "CL=F",
                    "label": "Crude Oil WTI",
                    "price": round(price, 2) if price else None,
                    "change_pct": change_pct,
                    "trailing_pe": None,
                    "currency": "USD",
                })
            except Exception:
                price, change_pct = history_close_summary("CL=F")
                results.append({"symbol": "CL=F", "label": "Crude Oil WTI", "price": price, "change_pct": change_pct, "trailing_pe": None, "currency": "USD"})

        # ── NSE Indices (price + P/E via allIndices) ───────────────────────────
        nse_targets = [
            ("^NSEI", "Nifty 50", "NIFTY 50"),
            ("^CNXSC", "Nifty SmallCap 250", "NIFTY SMALLCAP 250"),
            ("^NSEMDCP50", "Nifty Midcap 50", "NIFTY MIDCAP 50"),
        ]
        nse_rows: dict[str, dict[str, Any]] = {}
        try:
            headers = {
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.nseindia.com/",
            }
            with httpx.Client(timeout=20, headers=headers, follow_redirects=True) as client:
                client.get("https://www.nseindia.com/")
                resp = client.get(NSE_ALL_INDICES_URL)
                resp.raise_for_status()
                payload = resp.json()
            for row in payload.get("data") or []:
                nse_rows[str(row.get("index", "")).strip().upper()] = row
        except Exception:
            pass

        for symbol, label, nse_name in nse_targets:
            row = nse_rows.get(nse_name.upper()) if not snapshot_only_mode else None
            if row:
                try:
                    price_val = round(float(row["last"]), 2)
                    chg = round(float(row["percentChange"]), 2)
                    pe_raw = row.get("pe")
                    pe_val = round(float(pe_raw), 2) if pe_raw not in (None, "", "-") else None
                    results.append({"symbol": symbol, "label": label, "price": price_val, "change_pct": chg, "trailing_pe": pe_val, "currency": "INR"})
                    continue
                except (TypeError, ValueError, KeyError):
                    pass
            if snapshot_only_mode:
                price_val, chg = history_close_summary(symbol)
                results.append({"symbol": symbol, "label": label, "price": price_val, "change_pct": chg, "trailing_pe": None, "currency": "INR"})
                continue
            # fallback: yfinance
            try:
                tkr = yf.Ticker(symbol)
                info = tkr.fast_info
                price_val = round(float(info.last_price), 2) if info.last_price else None
                prev = float(info.previous_close) if info.previous_close else None
                chg = round(((price_val / prev) - 1) * 100, 2) if price_val and prev else None
                results.append({"symbol": symbol, "label": label, "price": price_val, "change_pct": chg, "trailing_pe": None, "currency": "INR"})
            except Exception:
                results.append({"symbol": symbol, "label": label, "price": None, "change_pct": None, "trailing_pe": None, "currency": "INR"})

        return results

    # symbol → NSE index name used in the PE-history API
    _PE_HISTORY_INDEX_MAP = {
        "^NSEI": "NIFTY 50",
        "^CNXSC": "NIFTY SMALLCAP 250",
        "^NSEMDCP50": "NIFTY MIDCAP 50",
    }

    def get_index_pe_history(self, symbol: str) -> list[dict[str, Any]]:
        """Return daily P/E history from NSE for the given index symbol.
        Returns list of {date: str (YYYY-MM-DD), pe: float}.
        Caches in chart_cache_dir and refreshes when stale (>6 h).
        """
        cache_path = self.chart_cache_dir / f"_pe_{symbol.replace('^', '')}_history.json"
        age: float = time.time() - cache_path.stat().st_mtime if cache_path.exists() else 9e9
        if cache_path.exists() and age < 6 * 3600:
            try:
                with cache_path.open() as fh:
                    return json.load(fh)
            except Exception:
                pass

        index_name = self._PE_HISTORY_INDEX_MAP.get(symbol.upper())
        if not index_name:
            return []

        try:
            from_date = (datetime.now(IST) - timedelta(days=5 * 365 + 30)).strftime("%d-%m-%Y")
            to_date = datetime.now(IST).strftime("%d-%m-%Y")
            session = requests.Session()
            session.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "application/json,text/plain,*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.nseindia.com/",
            })
            # Prime cookies — NSE requires a valid session before API calls
            session.get("https://www.nseindia.com/", timeout=15)
            session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15)
            params = {"indexName": index_name, "from": from_date, "to": to_date}
            resp = session.get(NSE_INDEX_PE_HISTORY_URL, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()

            rows = payload.get("data") or []
            result: list[dict[str, Any]] = []
            for row in rows:
                try:
                    date_str = str(row.get("HistoricalDate") or row.get("date") or "").strip()
                    # NSE returns dates like "01 Jan 2024" or "01-Jan-2024"
                    for fmt in ("%d %b %Y", "%d-%b-%Y", "%Y-%m-%d"):
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            date_str = dt.strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                    # NSE uses "P/E" as the field name in historical data
                    pe_raw = (
                        row.get("P/E") or row.get("pe") or row.get("peRatio")
                        or row.get("indexPE") or row.get("PE")
                    )
                    pe_val = round(float(pe_raw), 2) if pe_raw not in (None, "", "-") else None
                    if pe_val and date_str:
                        result.append({"date": date_str, "pe": pe_val})
                except Exception:
                    continue
            result.sort(key=lambda r: r["date"])
            if result:
                with cache_path.open("w") as fh:
                    json.dump(result, fh)
            return result
        except Exception as exc:
            import logging as _log
            _log.getLogger(__name__).warning("PE history fetch failed for %s: %s", symbol, exc)
            return []

    def _snapshot_schema_ok(self, rows: list[dict[str, Any]]) -> bool:
        if not rows:
            return False
        if int(rows[0].get("snapshot_cache_version", 0) or 0) != SNAPSHOT_CACHE_VERSION:
            return False
        if any(not self._row_has_sane_session_price_scale(row) for row in rows):
            return False
        expected_fields = {
            "snapshot_cache_version",
            "previous_close",
            "week_high_prev",
            "month_high_prev",
            "ath_prev",
            "high_52w_prev",
            "range_high_prev_20d",
            "avg_volume_30d",
            "ema10",
            "benchmark_return_1d",
            "benchmark_return_5d",
            "benchmark_return_60d",
            "benchmark_return_126d",
            "benchmark_return_252d",
            "stock_return_5d",
            "stock_return_126d",
            "stock_return_189d",
            "stock_return_12m",
            "stock_return_504d",
            "stock_return_12m_1d_ago",
            "stock_return_12m_1w_ago",
            "stock_return_12m_1m_ago",
            "rs_weighted_score",
            "rs_weighted_score_1d_ago",
            "rs_weighted_score_1w_ago",
            "rs_weighted_score_1m_ago",
            "gap_pct",
            "rs_eligible",
            "rs_eligible_1d_ago",
            "rs_eligible_1w_ago",
            "rs_eligible_1m_ago",
            "rs_rating",
            "rs_rating_1d_ago",
            "rs_rating_1w_ago",
            "rs_rating_1m_ago",
            "recent_highs",
            "recent_lows",
            "recent_volumes",
            "chart_grid_points",
            "sma20",
            "sma50",
            "sma150",
            "sma200",
            "sma200_1m_ago",
            "sma200_5m_ago",
            "avg_volume_50d",
            "weekly_ema20",
            "multi_year_high",
            "stock_return_40d",
            "atr14",
            "long_base_avg_range_pct",
            "long_base_span_pct",
            "long_base_window_days",
            "ma200_type",
            "history_as_of_date",
            "history_session_date",
            "baseline_close_5d",
            "baseline_close_20d",
            "baseline_close_40d",
            "baseline_close_60d",
            "baseline_close_63d",
            "baseline_close_126d",
            "baseline_close_189d",
            "baseline_close_252d",
            "baseline_close_504d",
        }
        return expected_fields.issubset(rows[0].keys())

    def _load_or_refresh_universe(self, market_cap_min_crore: float, force_refresh: bool) -> list[dict[str, Any]]:
        rows = self._load_json_rows(self.universe_cache_path)
        cache_usable = self._universe_cache_ok(rows, market_cap_min_crore)
        if cache_usable and (
            not force_refresh
            or self._is_fresh(self.universe_cache_path, max_age_hours=UNIVERSE_FORCE_REFRESH_MAX_AGE_HOURS)
        ):
            filtered = [row for row in rows if float(row.get("market_cap_crore", 0)) >= market_cap_min_crore]
            if filtered:
                return sorted(filtered, key=lambda item: (-float(item.get("market_cap_crore", 0) or 0), str(item.get("symbol") or "")))

        universe = self._fetch_market_cap_universe(market_cap_min_crore)
        self.universe_cache_path.write_text(json.dumps(universe, indent=2), encoding="utf-8")
        return universe

    def _is_fresh(self, path: Path, max_age_hours: int) -> bool:
        if not path.exists():
            return False
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return age <= timedelta(hours=max_age_hours)

    def _universe_cache_ok(self, rows: list[dict[str, Any]], market_cap_min_crore: float) -> bool:
        if not rows:
            return False
        version = int(rows[0].get("universe_version", 0) or 0)
        session_date = self._parse_row_date(rows[0].get("universe_session_date"))
        cached_floor = self._to_float(rows[0].get("universe_market_cap_floor_crore"))
        return (
            version == UNIVERSE_CACHE_VERSION
            and session_date == self._current_or_previous_trading_day_ist()
            and cached_floor is not None
            and cached_floor <= market_cap_min_crore
        )

    def _fetch_nse_listed_equities(self) -> dict[str, dict[str, Any]]:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,*/*",
            "Referer": "https://www.nseindia.com/",
        }
        with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
            response = client.get(NSE_LISTED_EQUITIES_URL)
            response.raise_for_status()
            text = response.text

        listings: dict[str, dict[str, Any]] = {}
        reader = csv.DictReader(io.StringIO(text))
        for raw_row in reader:
            row = {str(key).strip(): value for key, value in raw_row.items()}
            series = str(row.get("SERIES") or "").strip().upper()
            if series and series != "EQ":
                continue
            symbol = self._normalize_symbol(row.get("SYMBOL"))
            if not symbol:
                continue
            listings[symbol] = {
                "symbol": symbol,
                "name": str(row.get("NAME OF COMPANY") or symbol).strip(),
                "isin": str(row.get("ISIN NUMBER") or "").strip() or None,
                "listing_date": self._parse_nse_listing_date(row.get("DATE OF LISTING")),
            }
        return listings

    def _fetch_bse_active_equities(self) -> list[dict[str, Any]]:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": BSE_LIST_SCRIPS_PAGE_URL,
            "Origin": "https://www.bseindia.com",
        }
        with requests.Session() as session:
            session.get(BSE_LIST_SCRIPS_PAGE_URL, headers=headers, timeout=30)
            response = session.get(
                BSE_LIST_SCRIPS_API_URL,
                params={"segment": "Equity", "status": "Active", "Group": "", "Scripcode": ""},
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            rows = payload.get("Table") or payload.get("table") or payload.get("data") or []
            return [item for item in rows if isinstance(item, dict)]
        return []

    def _fetch_nse_quote_details(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        normalized_symbols = [self._normalize_symbol(symbol) for symbol in symbols if self._normalize_symbol(symbol)]
        if not normalized_symbols:
            return {}

        details: dict[str, dict[str, Any]] = {}
        workers = min(12, max(3, len(normalized_symbols) // 100 or 1))
        chunk_size = max(1, (len(normalized_symbols) + workers - 1) // workers)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._fetch_nse_quote_details_chunk, normalized_symbols[index : index + chunk_size]): index
                for index in range(0, len(normalized_symbols), chunk_size)
            }
            for future in as_completed(futures):
                try:
                    details.update(future.result())
                except Exception:
                    continue
        return details

    def _fetch_nse_quote_details_chunk(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        if not symbols:
            return {}

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://www.nseindia.com/",
        }
        details: dict[str, dict[str, Any]] = {}
        with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
            client.get("https://www.nseindia.com/")
            for symbol in symbols:
                try:
                    response = client.get(NSE_QUOTE_EQUITY_URL, params={"symbol": symbol})
                    response.raise_for_status()
                    payload = response.json()
                    detail = self._company_profile_from_quote_payload(symbol, payload)
                    detail["name"] = str((payload.get("info") or {}).get("companyName") or symbol).strip()
                    details[symbol] = detail
                except Exception:
                    continue
        return details

    def _fetch_market_cap_universe(self, market_cap_min_crore: float) -> list[dict[str, Any]]:
        nse_listings = self._fetch_nse_listed_equities()
        bse_rows = self._fetch_bse_active_equities()
        cached_universe_rows = self._load_json_rows(self.universe_cache_path)
        cached_by_symbol = {
            str(row.get("symbol") or "").upper(): row
            for row in cached_universe_rows
            if str(row.get("symbol") or "").strip()
        }
        cached_by_isin = {
            str(row.get("isin") or "").strip().upper(): row
            for row in cached_universe_rows
            if str(row.get("isin") or "").strip()
        }
        metadata_cache = self._load_json_file(self.company_metadata_path)
        nse_by_isin = {
            str(item.get("isin") or "").strip().upper(): item
            for item in nse_listings.values()
            if str(item.get("isin") or "").strip()
        }
        session_date = self._current_or_previous_trading_day_ist().isoformat()
        rows_by_key: dict[str, dict[str, Any]] = {}
        matched_nse_symbols: set[str] = set()

        def seed_metadata(symbol: str, isin: str | None, fallback: dict[str, Any]) -> dict[str, Any]:
            cached_row = cached_by_symbol.get(symbol) or (cached_by_isin.get(isin or "") if isin else None) or {}
            cached_meta = metadata_cache.get(symbol, {}) if isinstance(metadata_cache, dict) else {}
            sector = self._normalize_sector_label(cached_meta.get("sector") or cached_row.get("sector") or fallback.get("sector"))
            sub_sector = self._normalize_classification_label(cached_meta.get("sub_sector") or cached_row.get("sub_sector") or fallback.get("sub_sector"))
            listing_date = cached_meta.get("listing_date") or cached_row.get("listing_date") or fallback.get("listing_date")
            return {
                "sector": sector,
                "sub_sector": sub_sector,
                "listing_date": listing_date,
            }

        for bse_row in bse_rows:
            market_cap_crore = self._to_float(bse_row.get("Mktcap"))
            if market_cap_crore is None or market_cap_crore < market_cap_min_crore:
                continue

            isin = str(bse_row.get("ISIN_NUMBER") or "").strip().upper() or None
            matched_nse = nse_by_isin.get(isin or "")
            bse_symbol = self._normalize_symbol(bse_row.get("scrip_id"))
            bse_code = str(bse_row.get("SCRIP_CD") or "").strip()

            if matched_nse:
                symbol = str(matched_nse["symbol"]).upper()
                fallback = seed_metadata(symbol, isin, matched_nse)
                matched_nse_symbols.add(symbol)
                row = {
                    "symbol": symbol,
                    "name": str(matched_nse.get("name") or symbol).strip(),
                    "exchange": "NSE",
                    "market_cap_crore": round(float(market_cap_crore), 2),
                    "ticker": f"{symbol}.NS",
                    "listing_date": fallback["listing_date"] or matched_nse.get("listing_date"),
                    "sector": fallback["sector"],
                    "sub_sector": fallback["sub_sector"],
                    "isin": isin,
                    "bse_code": bse_code or None,
                    "universe_version": UNIVERSE_CACHE_VERSION,
                    "universe_session_date": session_date,
                    "universe_market_cap_floor_crore": market_cap_min_crore,
                }
            else:
                symbol = bse_symbol or self._normalize_symbol(bse_code)
                if not symbol:
                    continue
                fallback = seed_metadata(symbol, isin, {})
                ticker = f"{bse_symbol}.BO" if bse_symbol else f"{bse_code}.BO"
                row = {
                    "symbol": symbol,
                    "name": str(bse_row.get("Issuer_Name") or bse_row.get("Scrip_Name") or symbol).strip(),
                    "exchange": "BSE",
                    "market_cap_crore": round(float(market_cap_crore), 2),
                    "ticker": ticker,
                    "listing_date": fallback["listing_date"],
                    "sector": fallback["sector"],
                    "sub_sector": fallback["sub_sector"],
                    "isin": isin,
                    "bse_code": bse_code or None,
                    "universe_version": UNIVERSE_CACHE_VERSION,
                    "universe_session_date": session_date,
                    "universe_market_cap_floor_crore": market_cap_min_crore,
                }

            row_key = isin or f"{row['exchange']}:{row['symbol']}"
            existing = rows_by_key.get(row_key)
            if existing is None or float(row["market_cap_crore"]) >= float(existing.get("market_cap_crore", 0) or 0):
                rows_by_key[row_key] = row

        nse_only_symbols = sorted(symbol for symbol in nse_listings if symbol not in matched_nse_symbols)
        nse_only_details = self._fetch_nse_quote_details(nse_only_symbols)
        for symbol in nse_only_symbols:
            listing = nse_listings[symbol]
            isin = str(listing.get("isin") or "").strip().upper() or None
            detail = nse_only_details.get(symbol, {})
            market_cap_crore = self._to_float(detail.get("market_cap_crore"))
            if market_cap_crore is None:
                cached_row = cached_by_symbol.get(symbol) or (cached_by_isin.get(isin or "") if isin else None) or {}
                market_cap_crore = self._to_float(cached_row.get("market_cap_crore"))
            if market_cap_crore is None or market_cap_crore < market_cap_min_crore:
                continue

            fallback = seed_metadata(symbol, isin, {**listing, **detail})
            rows_by_key[isin or f"NSE:{symbol}"] = {
                "symbol": symbol,
                "name": str(detail.get("name") or listing.get("name") or symbol).strip(),
                "exchange": "NSE",
                "market_cap_crore": round(float(market_cap_crore), 2),
                "ticker": f"{symbol}.NS",
                "listing_date": fallback["listing_date"] or listing.get("listing_date"),
                "sector": fallback["sector"],
                "sub_sector": fallback["sub_sector"],
                "isin": isin,
                "bse_code": None,
                "universe_version": UNIVERSE_CACHE_VERSION,
                "universe_session_date": session_date,
                "universe_market_cap_floor_crore": market_cap_min_crore,
            }

        rows = list(rows_by_key.values())
        metadata_missing = [
            row
            for row in rows
            if (
                (
                    row["exchange"] == "NSE"
                    and (
                        not row.get("listing_date")
                        or str(row.get("sector") or "").strip() in ("", "Unclassified")
                        or str(row.get("sub_sector") or "").strip() in ("", "Unclassified")
                    )
                )
                or (
                    row["exchange"] == "BSE"
                    and (
                        str(row.get("sector") or "").strip() in ("", "Unclassified")
                        or str(row.get("sub_sector") or "").strip() in ("", "Unclassified")
                    )
                )
            )
        ]
        if metadata_missing:
            fetched_metadata = self._fetch_company_metadata(metadata_missing)
            for row in rows:
                metadata = fetched_metadata.get(str(row.get("symbol") or "").upper())
                if not metadata:
                    continue
                if metadata.get("listing_date"):
                    row["listing_date"] = metadata["listing_date"]
                if str(metadata.get("sector") or "").strip() and str(metadata.get("sector")) != "Unclassified":
                    row["sector"] = self._normalize_sector_label(metadata["sector"])
                if str(metadata.get("sub_sector") or "").strip() and str(metadata.get("sub_sector")) != "Unclassified":
                    row["sub_sector"] = str(metadata["sub_sector"]).strip()

        rows.sort(key=lambda item: (-float(item.get("market_cap_crore", 0) or 0), str(item.get("symbol") or "")))
        return rows

    def _build_snapshot_cache(self, universe: list[dict[str, Any]]) -> list[dict[str, Any]]:
        history_universe = universe
        tickers = [item["ticker"] for item in history_universe]
        if not tickers:
            return self._build_quote_only_snapshots(universe, pd.Series(dtype=float))

        cached_rows = [
            row
            for row in self._load_json_rows(self.snapshot_cache_path)
            if int(row.get("snapshot_cache_version", 0) or 0) == SNAPSHOT_CACHE_VERSION
        ]
        cached_by_symbol = {
            str(row.get("symbol") or "").upper(): row
            for row in cached_rows
            if str(row.get("symbol") or "").strip()
        }

        try:
            benchmark = yf.download(
                tickers=self._benchmark_symbol(),
                period="2y",
                interval="1d",
                auto_adjust=False,
                actions=True,
                progress=False,
                threads=False,
            )
            benchmark_close = self._extract_close_series(benchmark, self._benchmark_symbol())
        except Exception:
            benchmark_close = pd.Series(dtype=float)

        results: list[dict[str, Any]] = []
        breadth_dfs: dict[str, pd.DataFrame] = {}
        cached_results, cached_breadth, remaining_universe = self._build_snapshot_rows_from_fresh_cached_bars(
            history_universe,
            benchmark_close,
        )
        if cached_results:
            results.extend(cached_results)
            breadth_dfs.update(cached_breadth)
        batch_size = 20
        universe_batches = [remaining_universe[index : index + batch_size] for index in range(0, len(remaining_universe), batch_size)]
        max_workers = min(5, max(1, len(universe_batches)))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._build_snapshot_batch, batch, benchmark_close)
                for batch in universe_batches
            ]
            for future in as_completed(futures):
                try:
                    batch_results, batch_breadth = future.result()
                except Exception:
                    continue
                results.extend(batch_results)
                breadth_dfs.update(batch_breadth)

        built_symbols = {str(row.get("symbol") or "").upper() for row in results if str(row.get("symbol") or "").strip()}
        unresolved_instruments = [
            item
            for item in universe
            if str(item.get("symbol") or "").upper() not in built_symbols
        ]
        if unresolved_instruments:
            recovered_results, recovered_breadth = self._recover_missing_snapshot_rows(unresolved_instruments, benchmark_close)
            if recovered_results:
                results.extend(recovered_results)
                breadth_dfs.update(recovered_breadth)
                built_symbols.update(
                    str(row.get("symbol") or "").upper()
                    for row in recovered_results
                    if str(row.get("symbol") or "").strip()
                )

        missing_rows: list[dict[str, Any]] = []
        reused_symbols: set[str] = set()
        quote_only_rows = self._build_quote_only_snapshots(
            [
                item
                for item in universe
                if str(item.get("symbol") or "").upper() not in built_symbols
                and str(item.get("symbol") or "").upper() not in reused_symbols
            ],
            benchmark_close,
        )
        merged_results = [*results, *[row for row in missing_rows if row is not None], *quote_only_rows]
        merged_results = self._apply_sector_benchmarks(merged_results)
        final_snapshots = self._apply_rs_rating(merged_results)
        try:
            self._aggregate_and_save_historical_breadth(final_snapshots, breadth_dfs)
        except Exception as e:
            print(f"Error accumulating historical breadth: {e}")
            
        return final_snapshots

    @staticmethod
    def _history_has_minimum_bars(history: pd.DataFrame, minimum_bars: int = 30) -> bool:
        if history.empty or "Close" not in history.columns:
            return False
        close_history = history.dropna(subset=["Close"])
        return not close_history.empty and len(close_history) >= minimum_bars

    def _cached_history_is_complete_enough(
        self,
        instrument: dict[str, Any],
        history: pd.DataFrame,
    ) -> bool:
        if not self._history_has_minimum_bars(history):
            return False
        if not self._history_has_sane_price_scale(history):
            return False

        close_history = history.dropna(subset=["Close"])
        bar_count = len(close_history)
        listing_date = self._parse_row_date(instrument.get("listing_date"))
        if listing_date is None:
            return bar_count > RETURN_1Y_BARS

        listing_age_days = max((self._current_or_previous_trading_day_ist() - listing_date).days, 0)
        if listing_age_days >= 365:
            return bar_count > RETURN_1Y_BARS
        return True

    def _recover_missing_snapshot_rows(
        self,
        instruments: list[dict[str, Any]],
        benchmark_close: pd.Series,
    ) -> tuple[list[dict[str, Any]], dict[str, pd.DataFrame]]:
        if not instruments:
            return [], {}

        recovered_results: list[dict[str, Any]] = []
        recovered_breadth: dict[str, pd.DataFrame] = {}
        recovery_batch_size = 5
        recovery_batches = [
            instruments[index : index + recovery_batch_size]
            for index in range(0, len(instruments), recovery_batch_size)
        ]
        max_workers = min(6, max(1, len(recovery_batches) // 4 or 1))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._build_snapshot_batch, batch, benchmark_close)
                for batch in recovery_batches
            ]
            for future in as_completed(futures):
                try:
                    batch_results, batch_breadth = future.result()
                except Exception:
                    continue
                recovered_results.extend(batch_results)
                recovered_breadth.update(batch_breadth)

        recovered_symbols = {
            str(row.get("symbol") or "").upper()
            for row in recovered_results
            if str(row.get("symbol") or "").strip()
        }
        remaining = [
            instrument
            for instrument in instruments
            if str(instrument.get("symbol") or "").upper() not in recovered_symbols
        ]
        if not remaining:
            return recovered_results, recovered_breadth

        with ThreadPoolExecutor(max_workers=min(6, max(1, len(remaining) // 12 or 1))) as executor:
            futures = [
                executor.submit(self._recover_snapshot_row, instrument, benchmark_close)
                for instrument in remaining
            ]
            for future in as_completed(futures):
                try:
                    snapshot, breadth_df = future.result()
                except Exception:
                    continue
                if snapshot is None:
                    continue
                recovered_results.append(snapshot)
                if breadth_df is not None:
                    recovered_breadth[str(snapshot.get("instrument_key") or snapshot.get("symbol") or "")] = breadth_df

        return recovered_results, recovered_breadth

    def _build_snapshot_rows_from_fresh_cached_bars(
        self,
        instruments: list[dict[str, Any]],
        benchmark_close: pd.Series,
    ) -> tuple[list[dict[str, Any]], dict[str, pd.DataFrame], list[dict[str, Any]]]:
        if not instruments:
            return [], {}, []

        cached_results: list[dict[str, Any]] = []
        cached_breadth: dict[str, pd.DataFrame] = {}
        remaining: list[dict[str, Any]] = []

        for instrument in instruments:
            symbol = str(instrument.get("symbol") or "").upper()
            if not symbol or not self._is_chart_cache_fresh(symbol, "1D"):
                remaining.append(instrument)
                continue

            history = self._history_frame_from_cached_bars(symbol)
            if not self._cached_history_is_complete_enough(instrument, history):
                remaining.append(instrument)
                continue

            snapshot, breadth_df = self._snapshot_from_cached_bars(instrument, benchmark_close)
            if snapshot is None:
                remaining.append(instrument)
                continue

            cached_results.append(snapshot)
            if breadth_df is not None:
                cached_breadth[str(snapshot.get("instrument_key") or snapshot.get("symbol") or "")] = breadth_df

        return cached_results, cached_breadth, remaining

    def _recover_snapshot_row(
        self,
        instrument: dict[str, Any],
        benchmark_close: pd.Series,
    ) -> tuple[dict[str, Any] | None, pd.DataFrame | None]:
        cached_snapshot, cached_breadth = self._snapshot_from_cached_bars(instrument, benchmark_close)
        cached_history_complete = False
        if cached_snapshot is not None:
            cached_history = self._history_frame_from_cached_bars(str(instrument.get("symbol") or ""))
            cached_history_complete = self._cached_history_is_complete_enough(instrument, cached_history)
        if cached_snapshot is not None and cached_history_complete:
            return cached_snapshot, cached_breadth

        history = self._download_history_frame(
            str(instrument["ticker"]),
            period=SNAPSHOT_HISTORY_PERIOD,
            interval="1d",
        )
        if not self._history_has_minimum_bars(history):
            legacy_history = self._history_frame_from_cached_bars(
                str(instrument.get("symbol") or ""),
                allow_legacy=True,
            )
            if self._cached_history_is_complete_enough(instrument, legacy_history):
                legacy_snapshot, legacy_breadth = self._snapshot_from_cached_bars(
                    instrument,
                    benchmark_close,
                    allow_legacy=True,
                )
                if legacy_snapshot is not None:
                    return legacy_snapshot, legacy_breadth

            quote_only_snapshot = self._quote_only_snapshot_row(
                instrument,
                self._live_quote_for_symbol(
                    str(instrument.get("symbol") or ""),
                    str(instrument.get("ticker") or instrument.get("symbol") or ""),
                ),
            )
            if quote_only_snapshot is not None:
                return quote_only_snapshot, None

            if cached_snapshot is not None and cached_history_complete:
                return cached_snapshot, cached_breadth
            return None, None

        adjusted_history = self._split_adjusted_history(history)
        snapshot = self._history_to_snapshot(instrument, history, benchmark_close)
        if snapshot is None:
            return None, None
        self._seed_daily_chart_cache(str(instrument["symbol"]), adjusted_history)
        breadth_df = self._compute_historical_breadth_for_stock(adjusted_history)
        return snapshot, breadth_df

    def _build_snapshot_batch(
        self,
        batch_universe: list[dict[str, Any]],
        benchmark_close: pd.Series,
    ) -> tuple[list[dict[str, Any]], dict[str, pd.DataFrame]]:
        if not batch_universe:
            return [], {}

        batch_results: list[dict[str, Any]] = []
        batch_breadth: dict[str, pd.DataFrame] = {}

        def build_snapshot_item(item: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, pd.DataFrame | None]:
            try:
                history = self._download_history_frame(
                    item["ticker"],
                    period=SNAPSHOT_HISTORY_PERIOD,
                    interval="1d",
                )
                if not self._history_has_minimum_bars(history):
                    return None, None, None
                adjusted_history = self._split_adjusted_history(history)
                snapshot = self._history_to_snapshot(item, history, benchmark_close)
                if snapshot is None:
                    return None, None, None
                self._seed_daily_chart_cache(item["symbol"], adjusted_history)
                breadth_df = self._compute_historical_breadth_for_stock(adjusted_history)
                return snapshot, item["ticker"], breadth_df
            except Exception:
                return None, None, None

        max_workers = min(4, max(1, len(batch_universe)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(build_snapshot_item, item) for item in batch_universe]
            for future in as_completed(futures):
                snapshot, ticker, breadth_df = future.result()
                if snapshot is None:
                    continue
                batch_results.append(snapshot)
                if ticker and breadth_df is not None:
                    batch_breadth[ticker] = breadth_df

        return batch_results, batch_breadth

    def _compute_historical_breadth_for_stock(self, history: pd.DataFrame) -> pd.DataFrame | None:
        history = history.dropna(subset=["Close"]).copy()
        if history.empty:
            return None
        if isinstance(history.index, pd.DatetimeIndex) and history.index.tz is not None:
            history.index = history.index.tz_convert(timezone.utc).tz_localize(None)
        close = history["Close"]
        high = history["High"]
        low = history["Low"]
        sma20 = close.rolling(window=20, min_periods=20).mean()
        sma50 = close.rolling(window=50, min_periods=50).mean()
        sma200 = close.rolling(window=200, min_periods=200).mean()
        high_52w = high.rolling(window=252, min_periods=252).max()
        low_52w = low.rolling(window=252, min_periods=252).min()

        def available_signal(signal: pd.Series, availability: pd.Series) -> pd.Series:
            return signal.astype(float).where(availability.notna())

        df = pd.DataFrame({
            "above_ma20": available_signal(close > sma20, sma20),
            "above_ma50": available_signal(close > sma50, sma50),
            "above_sma200": available_signal(close > sma200, sma200),
            "new_high_52w": available_signal(high >= high_52w * 0.98, high_52w),
            "new_low_52w": available_signal(low <= low_52w * 1.02, low_52w),
        }, index=history.index)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()].copy()
        if df.empty:
            return None
        if df.index.tz is not None:
            df.index = df.index.tz_convert(timezone.utc).tz_localize(None)
        df.index = df.index.normalize()
        return df.groupby(level=0).last().sort_index()

    def _historical_breadth_universes(self, snapshots: list[dict[str, Any]]) -> list[tuple[str, set[str]]]:
        sorted_snaps = sorted(snapshots, key=lambda row: row.get("market_cap_crore", 0), reverse=True)

        def snapshot_key(row: dict[str, Any]) -> str:
            return str(row.get("instrument_key") or row.get("ticker") or row.get("symbol") or "").strip().upper()

        nifty_50_tickers = {snapshot_key(snapshot) for snapshot in sorted_snaps if snapshot.get("market_cap_crore", 0) >= 50000 and snapshot_key(snapshot)}
        nifty_500_tickers = {snapshot_key(snapshot) for snapshot in sorted_snaps if snapshot.get("market_cap_crore", 0) >= 5000 and snapshot_key(snapshot)}

        if len(nifty_50_tickers) > 50:
            nifty_50_tickers = {snapshot_key(snapshot) for snapshot in sorted_snaps[:50] if snapshot_key(snapshot)}
        if len(nifty_500_tickers) > 500:
            nifty_500_tickers = {snapshot_key(snapshot) for snapshot in sorted_snaps[:500] if snapshot_key(snapshot)}

        return [("Nifty 500", nifty_500_tickers), ("Nifty 50", nifty_50_tickers)]

    def _aggregate_and_save_historical_breadth(self, snapshots: list[dict[str, Any]], breadth_dfs: dict[str, pd.DataFrame]) -> None:
        def build_universe_history(name: str, tickers_set: set[str]) -> dict[str, Any]:
            universe_dfs = [df for ticker, df in breadth_dfs.items() if ticker in tickers_set and not df.empty]
            if not universe_dfs:
                return {"universe": name, "history": []}
            
            # Align all DataFrames on a common index and sum them up
            combined = pd.concat(universe_dfs, axis=1, keys=range(len(universe_dfs)))
            # combined is a multi-index column DataFrame. We group by level=1.
            # E.g level=1 will be 'above_ma20', 'above_ma50', etc.
            combined_t = combined.T
            summed = combined_t.groupby(level=1).sum(min_count=1).T
            counts = combined_t.groupby(level=1).count().T
            
            # Convert sums to percentages
            pct_df = summed.div(counts.where(counts > 0)) * 100
            pct_df = pct_df.round(2).fillna(0)
            if not isinstance(pct_df.index, pd.DatetimeIndex):
                pct_df.index = pd.to_datetime(pct_df.index, errors="coerce")
            pct_df = pct_df[~pct_df.index.isna()].copy()
            if pct_df.empty:
                return {"universe": name, "history": []}
            if pct_df.index.tz is not None:
                pct_df.index = pct_df.index.tz_convert(timezone.utc).tz_localize(None)
            pct_df.index = pct_df.index.normalize()
            pct_df = pct_df.groupby(level=0).last().sort_index()
            
            # Convert to list of dicts
            history_list = []
            for date, row in pct_df.iterrows():
                try:
                    date_str = date.strftime("%Y-%m-%d")
                    history_list.append({
                        "date": date_str,
                        "above_ma20_pct": float(row.get("above_ma20", 0)),
                        "above_ma50_pct": float(row.get("above_ma50", 0)),
                        "above_sma200_pct": float(row.get("above_sma200", 0)),
                        "new_high_52w_pct": float(row.get("new_high_52w", 0)),
                        "new_low_52w_pct": float(row.get("new_low_52w", 0)),
                    })
                except Exception:
                    continue
            return {"universe": name, "history": history_list}

        data = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "universes": [build_universe_history(name, tickers_set) for name, tickers_set in self._historical_breadth_universes(snapshots)],
        }
        self.historical_breadth_cache_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _split_adjustment_factors(self, history: pd.DataFrame) -> pd.Series:
        if history.empty:
            return pd.Series(dtype=float)

        split_series = history.get("Stock Splits")
        if split_series is None:
            return pd.Series(1.0, index=history.index, dtype=float)

        normalized_splits = split_series.fillna(0).astype(float)
        factors_by_index: dict[Any, float] = {}
        cumulative_factor = 1.0
        for index in reversed(history.index):
            factors_by_index[index] = cumulative_factor
            split_ratio = float(normalized_splits.loc[index] or 0.0)
            if split_ratio > 0 and split_ratio != 1:
                cumulative_factor *= 1 / split_ratio

        return pd.Series([factors_by_index[index] for index in history.index], index=history.index, dtype=float)

    def _price_adjustment_factors(self, history: pd.DataFrame) -> pd.Series:
        if history.empty:
            return pd.Series(dtype=float)

        adjusted_close = history.get("Adj Close")
        close = history.get("Close")
        if adjusted_close is not None and close is not None:
            close_series = pd.to_numeric(close, errors="coerce").replace(0, pd.NA)
            adjusted_close_series = pd.to_numeric(adjusted_close, errors="coerce")
            factors = (adjusted_close_series / close_series).replace([pd.NA, pd.NaT], pd.NA)
            factors = factors.replace([float("inf"), float("-inf")], pd.NA)
            if factors.notna().any():
                return factors.ffill().bfill().fillna(1.0).astype(float)

        return self._split_adjustment_factors(history).fillna(1.0)

    def _split_adjusted_history(self, history: pd.DataFrame) -> pd.DataFrame:
        if history.empty:
            return history

        adjusted = history.copy()
        factors = self._price_adjustment_factors(adjusted)
        adjusted["Adjustment Factor"] = factors

        for column in ("Open", "High", "Low", "Close"):
            if column in adjusted.columns:
                adjusted[column] = adjusted[column].astype(float) * factors

        if "Adj Close" in adjusted.columns:
            adjusted["Adj Close"] = pd.to_numeric(adjusted["Adj Close"], errors="coerce").fillna(adjusted["Close"])

        return adjusted

    def _build_chart_grid_points(
        self,
        close: pd.Series,
        *,
        lookback_bars: int = 520,
        max_points: int = 240,
    ) -> list[dict[str, float]]:
        series = pd.to_numeric(close, errors="coerce").dropna().tail(lookback_bars)
        if series.empty:
            return []

        if len(series) <= max_points:
            sampled = series
        else:
            step = max(len(series) / max_points, 1)
            sampled_indices = [int(index * step) for index in range(max_points)]
            sampled = series.iloc[sampled_indices]
            if sampled.index[-1] != series.index[-1]:
                sampled = pd.concat([sampled.iloc[:-1], series.iloc[[-1]]])

        points: list[dict[str, float]] = []
        for index_value, value in sampled.items():
            timestamp = pd.Timestamp(index_value)
            if timestamp.tzinfo is None:
                timestamp = timestamp.tz_localize(timezone.utc)
            else:
                timestamp = timestamp.tz_convert(timezone.utc)
            points.append(
                {
                    "time": int(timestamp.timestamp()),
                    "value": round(float(value), 4),
                }
            )
        return points

    def _apply_live_quote_to_chart_grid_points(
        self,
        points: list[dict[str, Any]] | None,
        quote_updated_at: datetime | None,
        current_price: float,
    ) -> list[dict[str, float]]:
        normalized_points = [
            {
                "time": int(point.get("time") or 0),
                "value": round(float(point.get("value") or current_price), 4),
            }
            for point in (points or [])
            if point and point.get("time") not in (None, "")
        ]
        if not normalized_points:
            base_time = int((quote_updated_at or datetime.now(timezone.utc)).timestamp())
            return [{"time": base_time, "value": round(float(current_price), 4)}]

        quote_time = int((quote_updated_at or datetime.now(timezone.utc)).timestamp())
        quote_date = (quote_updated_at or datetime.now(timezone.utc)).astimezone(IST).date()
        last_point_time = int(normalized_points[-1]["time"])
        last_point_date = datetime.fromtimestamp(last_point_time, tz=timezone.utc).astimezone(IST).date()

        if last_point_date == quote_date:
            normalized_points[-1] = {
                "time": max(last_point_time, quote_time),
                "value": round(float(current_price), 4),
            }
        else:
            normalized_points.append(
                {
                    "time": quote_time,
                    "value": round(float(current_price), 4),
                }
            )

        return normalized_points[-240:]

    def _reuse_cached_snapshot_row(
        self,
        cached_row: dict[str, Any],
        instrument: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not cached_row or int(cached_row.get("snapshot_cache_version", 0) or 0) != SNAPSHOT_CACHE_VERSION:
            return None
        if str(cached_row.get("history_source") or "").lower() not in {"history", "chart_cache"}:
            return None

        row = dict(cached_row)
        row.update(
            {
                "snapshot_cache_version": SNAPSHOT_CACHE_VERSION,
                "symbol": instrument["symbol"],
                "name": instrument["name"],
                "exchange": instrument["exchange"],
                "listing_date": instrument.get("listing_date"),
                "sector": instrument.get("sector") or row.get("sector") or "Unclassified",
                "sub_sector": instrument.get("sub_sector") or row.get("sub_sector") or "Unclassified",
                "market_cap_crore": instrument["market_cap_crore"],
                "instrument_key": instrument["ticker"],
            }
        )
        if not isinstance(row.get("chart_grid_points"), list) or not row.get("chart_grid_points"):
            last_price = float(row.get("last_price") or 0.0)
            now = int(datetime.now(timezone.utc).timestamp())
            row["chart_grid_points"] = [
                {"time": now - 86400, "value": round(last_price, 4)},
                {"time": now, "value": round(last_price, 4)},
            ]
        return row

    def _chart_grid_points_from_cached_bars(self, symbol: str, *, allow_legacy: bool = False) -> list[dict[str, float]]:
        cached_bars = self._read_chart_cache(symbol, "1D", 240, allow_legacy=allow_legacy)
        if not cached_bars:
            return []
        return [
            {
                "time": int(bar.time),
                "value": round(float(bar.close), 4),
            }
            for bar in cached_bars
            if bar.close not in (None, 0)
        ]

    def _history_frame_from_cached_bars(
        self,
        symbol: str,
        bars: int = 520,
        *,
        allow_legacy: bool = False,
    ) -> pd.DataFrame:
        cached_bars = self._read_chart_cache(symbol, "1D", bars, allow_legacy=allow_legacy)
        if not cached_bars:
            return pd.DataFrame()

        records: list[dict[str, Any]] = []
        index_values: list[pd.Timestamp] = []
        for bar in cached_bars:
            close = self._to_float(getattr(bar, "close", None))
            if close in (None, 0):
                continue
            open_value = self._to_float(getattr(bar, "open", None)) or close
            high_value = self._to_float(getattr(bar, "high", None)) or max(open_value, close)
            low_value = self._to_float(getattr(bar, "low", None)) or min(open_value, close)
            volume_value = int(self._to_float(getattr(bar, "volume", None)) or 0)
            records.append(
                {
                    "Open": float(open_value),
                    "High": float(high_value),
                    "Low": float(low_value),
                    "Close": float(close),
                    "Adj Close": float(close),
                    "Volume": volume_value,
                    "Stock Splits": 0.0,
                }
            )
            index_values.append(pd.Timestamp(datetime.fromtimestamp(int(bar.time), tz=timezone.utc)))

        if not records:
            return pd.DataFrame()

        history = pd.DataFrame.from_records(records, index=pd.DatetimeIndex(index_values))
        return history[~history.index.duplicated(keep="last")].sort_index()

    def _snapshot_from_cached_bars(
        self,
        instrument: dict[str, Any],
        benchmark_close: pd.Series,
        *,
        allow_legacy: bool = False,
    ) -> tuple[dict[str, Any] | None, pd.DataFrame | None]:
        history = self._history_frame_from_cached_bars(
            str(instrument.get("symbol") or ""),
            allow_legacy=allow_legacy,
        )
        if not self._history_has_minimum_bars(history):
            return None, None

        snapshot = self._history_to_snapshot(instrument, history, benchmark_close)
        if snapshot is None:
            return None, None
        snapshot["history_source"] = "legacy_chart_cache" if allow_legacy else "chart_cache"
        breadth_df = self._compute_historical_breadth_for_stock(history)
        return snapshot, breadth_df

    def _quote_only_snapshot_row(self, instrument: dict[str, Any], quote: dict[str, Any] | None) -> dict[str, Any] | None:
        current_price = self._quote_number(quote, "regularMarketPrice")
        previous_close = self._quote_number(quote, "regularMarketPreviousClose")
        cached_history = self._history_frame_from_cached_bars(str(instrument.get("symbol") or ""), allow_legacy=True)
        cached_history_sane = not cached_history.empty and self._history_has_sane_price_scale(cached_history)
        if current_price in (None, 0):
            cached_points = (
                self._chart_grid_points_from_cached_bars(str(instrument.get("symbol") or ""), allow_legacy=True)
                if cached_history_sane
                else []
            )
            if cached_points:
                current_price = cached_points[-1]["value"]
                previous_close = cached_points[-2]["value"] if len(cached_points) >= 2 else current_price
            else:
                return None

        current_price = float(current_price)
        previous_close = float(previous_close or current_price)
        session_high = float(self._quote_number(quote, "regularMarketDayHigh") or max(current_price, previous_close))
        session_low = float(self._quote_number(quote, "regularMarketDayLow") or min(current_price, previous_close))
        session_open = float(self._quote_number(quote, "regularMarketOpen") or previous_close or current_price)
        session_volume = int(self._quote_number(quote, "regularMarketVolume") or 0)
        high_52w = float(self._quote_number(quote, "fiftyTwoWeekHigh") or session_high or current_price)
        low_52w = float(self._quote_number(quote, "fiftyTwoWeekLow") or session_low or current_price)
        quote_updated_at = self._quote_updated_at_from_quote(quote) or datetime.now(timezone.utc)
        session_date = quote_updated_at.astimezone(IST).date().isoformat()
        change_pct = ((current_price / previous_close) - 1) * 100 if previous_close else 0.0
        gap_pct = ((session_open / previous_close) - 1) * 100 if previous_close else 0.0

        chart_grid_points = (
            self._chart_grid_points_from_cached_bars(str(instrument.get("symbol") or ""))
            if cached_history_sane
            else []
        )
        if not chart_grid_points:
            quote_time = int(quote_updated_at.timestamp())
            chart_grid_points = [
                {"time": quote_time - 86400, "value": round(previous_close, 4)},
                {"time": quote_time, "value": round(current_price, 4)},
            ]

        return {
            "snapshot_cache_version": SNAPSHOT_CACHE_VERSION,
            "symbol": instrument["symbol"],
            "name": instrument["name"],
            "exchange": instrument["exchange"],
            "listing_date": instrument.get("listing_date"),
            "sector": instrument.get("sector") or "Unclassified",
            "sub_sector": instrument.get("sub_sector") or "Unclassified",
            "market_cap_crore": instrument["market_cap_crore"],
            "last_price": round(current_price, 2),
            "previous_close": round(previous_close, 2),
            "change_pct": round(change_pct, 2),
            "volume": session_volume,
            "avg_volume_20d": 0,
            "avg_volume_30d": 0,
            "avg_volume_50d": 0,
            "day_high": round(session_high, 2),
            "day_low": round(session_low, 2),
            "previous_day_high": round(previous_close, 2),
            "previous_day_low": round(previous_close, 2),
            "week_high": round(max(session_high, current_price), 2),
            "week_low": round(min(session_low, current_price), 2),
            "week_high_prev": round(max(session_high, current_price), 2),
            "week_low_prev": round(min(session_low, current_price), 2),
            "month_high": round(max(session_high, current_price), 2),
            "month_low": round(min(session_low, current_price), 2),
            "month_high_prev": round(max(session_high, current_price), 2),
            "month_low_prev": round(min(session_low, current_price), 2),
            "ath": round(max(high_52w, session_high, current_price), 2),
            "ath_prev": round(max(high_52w, session_high, current_price), 2),
            "atl": round(min(low_52w, session_low, current_price), 2),
            "multi_year_high": round(max(high_52w, session_high, current_price), 2),
            "high_52w": round(max(high_52w, session_high, current_price), 2),
            "low_52w": round(min(low_52w, session_low, current_price), 2),
            "high_52w_prev": round(max(high_52w, session_high, current_price), 2),
            "low_52w_prev": round(min(low_52w, session_low, current_price), 2),
            "high_6m": round(max(high_52w, session_high, current_price), 2),
            "low_6m": round(min(low_52w, session_low, current_price), 2),
            "high_6m_prev": round(max(high_52w, session_high, current_price), 2),
            "low_6m_prev": round(min(low_52w, session_low, current_price), 2),
            "high_3m": round(max(session_high, current_price), 2),
            "range_high_20d": round(max(session_high, current_price), 2),
            "range_high_prev_20d": round(max(session_high, current_price), 2),
            "sma20": None,
            "sma50": None,
            "sma150": None,
            "sma200": None,
            "sma200_1m_ago": None,
            "sma200_5m_ago": None,
            "ema10": None,
            "ema20": None,
            "ema50": None,
            "ema200": None,
            "weekly_ema20": None,
            "ma200_type": "ema",
            "benchmark_return_1d": 0.0,
            "benchmark_return_5d": 0.0,
            "benchmark_return_20d": 0.0,
            "benchmark_return_60d": 0.0,
            "benchmark_return_126d": 0.0,
            "benchmark_return_252d": 0.0,
            "sector_return_20d": 0.0,
            "stock_return_5d": 0.0,
            "stock_return_20d": 0.0,
            "stock_return_40d": 0.0,
            "stock_return_60d": 0.0,
            "stock_return_126d": 0.0,
            "stock_return_189d": 0.0,
            "stock_return_12m": 0.0,
            "stock_return_504d": 0.0,
            "stock_return_12m_1d_ago": 0.0,
            "stock_return_12m_1w_ago": 0.0,
            "stock_return_12m_1m_ago": 0.0,
            "baseline_close_5d": round(current_price, 4),
            "baseline_close_20d": round(current_price, 4),
            "baseline_close_40d": round(current_price, 4),
            "baseline_close_60d": round(current_price, 4),
            "baseline_close_63d": round(current_price, 4),
            "baseline_close_126d": round(current_price, 4),
            "baseline_close_189d": round(current_price, 4),
            "baseline_close_252d": round(current_price, 4),
            "baseline_close_504d": round(current_price, 4),
            "rs_weighted_score": 0.0,
            "rs_weighted_score_1d_ago": 0.0,
            "rs_weighted_score_1w_ago": 0.0,
            "rs_weighted_score_1m_ago": 0.0,
            "rs_eligible": False,
            "rs_eligible_1d_ago": False,
            "rs_eligible_1w_ago": False,
            "rs_eligible_1m_ago": False,
            "rs_line_today": 0.0,
            "rs_line_1m": 0.0,
            "rsi_14": 50.0,
            "gap_pct": round(gap_pct, 2),
            "pivot_high": round(max(session_high, current_price), 2),
            "darvas_high": round(max(session_high, current_price), 2),
            "darvas_low": round(min(session_low, current_price), 2),
            "atr14": round(max(session_high - session_low, 0.0), 2),
            "adr_pct_20": 0.0,
            "pullback_depth_pct": 0.0,
            "trend_strength": 0.0,
            "long_base_avg_range_pct": 0.0,
            "long_base_span_pct": 0.0,
            "long_base_window_days": 60,
            "recent_highs": [round(session_high, 2)],
            "recent_lows": [round(session_low, 2)],
            "recent_volumes": [],
            "chart_grid_points": chart_grid_points,
            "instrument_key": instrument["ticker"],
            "history_as_of_date": session_date,
            "history_session_date": session_date,
            "history_bars": len(chart_grid_points),
            "history_source": "quote",
        }

    def _build_quote_only_snapshots(
        self,
        instruments: list[dict[str, Any]],
        benchmark_close: pd.Series | None = None,
    ) -> list[dict[str, Any]]:
        if not instruments:
            return []

        snapshots: list[dict[str, Any]] = []
        unresolved: list[dict[str, Any]] = []
        benchmark = benchmark_close if benchmark_close is not None else pd.Series(dtype=float)

        for instrument in instruments:
            cached_history = self._history_frame_from_cached_bars(str(instrument.get("symbol") or ""))
            if self._cached_history_is_complete_enough(instrument, cached_history):
                snapshot, _ = self._snapshot_from_cached_bars(instrument, benchmark)
                if snapshot is not None:
                    snapshots.append(snapshot)
                    continue

            legacy_history = self._history_frame_from_cached_bars(
                str(instrument.get("symbol") or ""),
                allow_legacy=True,
            )
            if self._cached_history_is_complete_enough(instrument, legacy_history):
                legacy_snapshot, _ = self._snapshot_from_cached_bars(
                    instrument,
                    benchmark,
                    allow_legacy=True,
                )
                if legacy_snapshot is not None:
                    snapshots.append(legacy_snapshot)
                    continue

            unresolved.append(instrument)

        for start in range(0, len(unresolved), 50):
            chunk = unresolved[start : start + 50]
            tickers = [str(item.get("ticker") or "").upper() for item in chunk if str(item.get("ticker") or "").strip()]
            try:
                quotes = self._fetch_quote_batch(tickers)
            except Exception:
                quotes = {}

            for instrument in chunk:
                symbol = str(instrument.get("symbol") or "").upper()
                ticker = str(instrument.get("ticker") or f"{symbol}.NS").upper()
                row = self._quote_only_snapshot_row(instrument, quotes.get(ticker) or quotes.get(symbol))
                if row is not None:
                    snapshots.append(row)

        return snapshots

    def _history_to_snapshot(
        self,
        instrument: dict[str, Any],
        history: pd.DataFrame,
        benchmark_close: pd.Series,
    ) -> dict[str, Any] | None:
        history = history.dropna(subset=["Close"]).copy()
        if history.empty or len(history) < 30:
            return None

        adjusted_history = self._split_adjusted_history(history)
        latest = adjusted_history.iloc[-1]
        previous = adjusted_history.iloc[-2] if len(adjusted_history) >= 2 else latest
        close = adjusted_history["Close"]
        adjusted_close = close
        high = adjusted_history["High"]
        low = adjusted_history["Low"]
        volume = history["Volume"].fillna(0)
        latest_history_date = adjusted_history.index[-1].date().isoformat() if isinstance(adjusted_history.index, pd.DatetimeIndex) else None

        ema20 = self._ema_with_min_history(close, 20)
        ema50 = self._ema_with_min_history(close, 50)
        ema10 = self._ema_with_min_history(close, 10)
        ema200 = self._ema_with_min_history(close, 200)
        sma200 = self._sma_with_min_history(close, 200)
        sma200_series = close.rolling(window=200, min_periods=200).mean()
        sma200_1m_ago = (
            float(sma200_series.iloc[-(RETURN_1M_BARS + 1)])
            if len(sma200_series.dropna()) > RETURN_1M_BARS
            else None
        )
        sma200_5m_ago = (
            float(sma200_series.iloc[-(RETURN_5M_BARS + 1)])
            if len(sma200_series.dropna()) > RETURN_5M_BARS
            else None
        )
        sma20 = self._sma_with_min_history(close, 20)
        sma50 = self._sma_with_min_history(close, 50)
        sma150 = self._sma_with_min_history(close, 150)
        weekly_close = close.resample("W-FRI").last().dropna() if isinstance(close.index, pd.DatetimeIndex) else close.dropna()
        weekly_ema20 = self._ema_with_min_history(weekly_close, 20)

        previous_close_series = close.shift(1).fillna(close.iloc[0])
        true_range = pd.concat(
            [
                (high - low).abs(),
                (high - previous_close_series).abs(),
                (low - previous_close_series).abs(),
            ],
            axis=1,
        ).max(axis=1)
        atr14 = true_range.rolling(window=14, min_periods=1).mean().iloc[-1]

        delta = close.diff()
        gain = delta.where(delta > 0, 0.0).ewm(alpha=1 / 14, adjust=False).mean()
        loss = -delta.where(delta < 0, 0.0).ewm(alpha=1 / 14, adjust=False).mean()
        rs_indicator = gain / loss
        rsi_14_series = 100 - (100 / (1 + rs_indicator))
        rsi_14 = float(rsi_14_series.iloc[-1]) if not rsi_14_series.empty and not pd.isna(rsi_14_series.iloc[-1]) else 50.0

        history_bars = int(len(adjusted_close.dropna()))
        stock_return_5d = self._return_pct(adjusted_close, RETURN_1W_BARS)
        stock_return_20d = self._return_pct(adjusted_close, RETURN_1M_BARS)
        stock_return_40d = self._return_pct(adjusted_close, 40)
        stock_return_60d = self._return_pct(adjusted_close, RETURN_3M_BARS)
        stock_return_126d = self._return_pct(adjusted_close, RETURN_6M_BARS)
        stock_return_189d = self._return_pct(adjusted_close, RETURN_9M_BARS)
        stock_return_12m = self._return_pct_as_of(adjusted_close, RETURN_1Y_BARS, 0)
        stock_return_504d = self._return_pct(adjusted_close, RETURN_2Y_BARS)
        stock_return_12m_1d_ago = self._return_pct_as_of(adjusted_close, RETURN_1Y_BARS, 1)
        stock_return_12m_1w_ago = self._return_pct_as_of(adjusted_close, RETURN_1Y_BARS, RETURN_1W_BARS)
        stock_return_12m_1m_ago = self._return_pct_as_of(adjusted_close, RETURN_1Y_BARS, RETURN_1M_BARS)
        rs_lookbacks = self._adaptive_rs_lookbacks(adjusted_close, 0)
        rs_lookbacks_1d_ago = self._adaptive_rs_lookbacks(adjusted_close, 1)
        rs_lookbacks_1w_ago = self._adaptive_rs_lookbacks(adjusted_close, RETURN_1W_BARS)
        rs_lookbacks_1m_ago = self._adaptive_rs_lookbacks(adjusted_close, RETURN_1M_BARS)
        rs_weighted_score = self._weighted_rs_score(adjusted_close, 0)
        rs_weighted_score_1d_ago = self._weighted_rs_score(adjusted_close, 1)
        rs_weighted_score_1w_ago = self._weighted_rs_score(adjusted_close, RETURN_1W_BARS)
        rs_weighted_score_1m_ago = self._weighted_rs_score(adjusted_close, RETURN_1M_BARS)
        rs_line_today = self._relative_strength_line_value(adjusted_close, benchmark_close, offset=0)
        rs_line_1m = self._relative_strength_line_value(adjusted_close, benchmark_close, offset=RETURN_1M_BARS)
        rs_eligible = bool(rs_lookbacks)
        rs_eligible_1d_ago = bool(rs_lookbacks_1d_ago)
        rs_eligible_1w_ago = bool(rs_lookbacks_1w_ago)
        rs_eligible_1m_ago = bool(rs_lookbacks_1m_ago)
        baseline_close_5d = self._baseline_at_lookback(adjusted_close, RETURN_1W_BARS)
        baseline_close_20d = self._baseline_at_lookback(adjusted_close, RETURN_1M_BARS)
        baseline_close_40d = self._baseline_at_lookback(adjusted_close, 40)
        baseline_close_60d = self._baseline_at_lookback(adjusted_close, RETURN_3M_BARS)
        baseline_close_63d = self._baseline_at_lookback(adjusted_close, RETURN_3M_BARS)
        baseline_close_126d = self._baseline_at_lookback(adjusted_close, RETURN_6M_BARS)
        baseline_close_189d = self._baseline_at_lookback(adjusted_close, RETURN_9M_BARS)
        baseline_close_252d = self._baseline_at_lookback(adjusted_close, RETURN_1Y_BARS)
        baseline_close_504d = self._baseline_at_lookback(adjusted_close, RETURN_2Y_BARS)
        rs_baseline_close_q1 = self._baseline_at_lookback(adjusted_close, rs_lookbacks[0][0], allow_partial=False) if rs_lookbacks else None
        rs_baseline_close_q2 = self._baseline_at_lookback(adjusted_close, rs_lookbacks[1][0], allow_partial=False) if len(rs_lookbacks) > 1 else None
        rs_baseline_close_q3 = self._baseline_at_lookback(adjusted_close, rs_lookbacks[2][0], allow_partial=False) if len(rs_lookbacks) > 2 else None
        rs_baseline_close_q4 = self._baseline_at_lookback(adjusted_close, rs_lookbacks[3][0], allow_partial=False) if len(rs_lookbacks) > 3 else None
        chart_grid_points = self._build_chart_grid_points(adjusted_close)
        benchmark_return_1d = self._return_pct(benchmark_close, 1)
        benchmark_return_5d = self._return_pct(benchmark_close, RETURN_1W_BARS)
        benchmark_return_20d = self._return_pct(benchmark_close, RETURN_1M_BARS)
        benchmark_return_60d = self._return_pct(benchmark_close, RETURN_3M_BARS)
        benchmark_return_126d = self._return_pct(benchmark_close, RETURN_6M_BARS)
        benchmark_return_252d = self._return_pct(benchmark_close, RETURN_1Y_BARS)
        sector_return_20d = benchmark_return_20d
        gap_pct = ((float(latest["Open"]) / float(previous["Close"])) - 1) * 100 if float(previous["Close"]) else 0.0

        recent_high = high.tail(15).max()
        pullback_depth = ((recent_high - float(latest["Close"])) / recent_high) * 100 if recent_high else 0.0
        trend_strength = self._trend_strength(float(latest["Close"]), ema20, ema50, ema200)
        previous_week_high = self._window_max(high, 6, exclude_last=True)
        previous_week_low = self._window_min(low, 6, exclude_last=True)
        previous_month_high = self._window_max(high, 22, exclude_last=True)
        previous_month_low = self._window_min(low, 22, exclude_last=True)
        previous_high_6m = self._window_max(high, 127, exclude_last=True)
        previous_low_6m = self._window_min(low, 127, exclude_last=True)
        previous_high_52w = self._window_max(high, 253, exclude_last=True)
        previous_low_52w = self._window_min(low, 253, exclude_last=True)
        previous_ath = float(high.iloc[:-1].max()) if len(high) > 1 else float(high.max())
        previous_range_high = self._window_max(high, 21, exclude_last=True)
        three_year_window = min(len(high), 750)
        high_3y = float(high.tail(three_year_window).max()) if three_year_window else float(high.max())
        multi_year_window = min(len(high), 1250)
        multi_year_high = float(high.tail(multi_year_window).max()) if multi_year_window else float(high.max())

        base_candidates: list[tuple[float, float, int]] = []
        latest_close = float(latest["Close"])
        for window in (60, 120, 180, 250):
            if len(close) < window:
                continue
            window_close = close.tail(window).replace(0, pd.NA)
            window_high = high.tail(window)
            window_low = low.tail(window)
            avg_range_pct = (((window_high - window_low) / window_close) * 100).dropna().mean()
            if pd.isna(avg_range_pct):
                continue
            span_pct = (((float(window_high.max()) - float(window_low.min())) / latest_close) * 100) if latest_close else 0.0
            base_candidates.append((float(avg_range_pct), float(span_pct), window))

        if base_candidates:
            best_base_avg_range, best_base_span, best_base_window = min(base_candidates, key=lambda item: (item[0], item[1], item[2]))
        else:
            best_base_avg_range, best_base_span, best_base_window = 0.0, 0.0, 60

        adr_window_ranges = (high.tail(20) - low.tail(20)).dropna()
        adr_window_closes = close.tail(20).replace(0, pd.NA).dropna()
        if not adr_window_ranges.empty and not adr_window_closes.empty:
            adr_value = float(adr_window_ranges.mean())
            adr_reference = float(adr_window_closes.mean())
            adr_pct_20 = ((adr_value / adr_reference) * 100) if adr_reference > 0 else 0.0
        else:
            adr_pct_20 = 0.0

        listing_date = self._history_listing_date(instrument, adjusted_history)

        return {
            "snapshot_cache_version": SNAPSHOT_CACHE_VERSION,
            "symbol": instrument["symbol"],
            "name": instrument["name"],
            "exchange": instrument["exchange"],
            "listing_date": listing_date,
            "sector": instrument.get("sector") or "Unclassified",
            "sub_sector": instrument.get("sub_sector") or "Unclassified",
            "market_cap_crore": instrument["market_cap_crore"],
            "last_price": round(float(latest["Close"]), 2),
            "previous_close": round(float(previous["Close"]), 2) if float(previous["Close"]) else round(float(latest["Close"]), 2),
            "change_pct": round(((float(latest["Close"]) / float(previous["Close"])) - 1) * 100, 2) if float(previous["Close"]) else 0.0,
            "volume": int(float(latest["Volume"]) or 0),
            "avg_volume_20d": int(volume.tail(20).mean() or 0),
            "avg_volume_30d": int(volume.tail(30).mean() or 0),
            "avg_volume_50d": int(volume.tail(50).mean() or 0),
            "day_high": round(float(latest["High"]), 2),
            "day_low": round(float(latest["Low"]), 2),
            "previous_day_high": round(float(previous["High"]), 2),
            "previous_day_low": round(float(previous["Low"]), 2),
            "week_high": round(float(high.tail(5).max()), 2),
            "week_low": round(float(low.tail(5).min()), 2),
            "week_high_prev": round(previous_week_high, 2),
            "week_low_prev": round(previous_week_low, 2),
            "month_high": round(float(high.tail(21).max()), 2),
            "month_low": round(float(low.tail(21).min()), 2),
            "month_high_prev": round(previous_month_high, 2),
            "month_low_prev": round(previous_month_low, 2),
            "ath": round(float(high.max()), 2),
            "ath_prev": round(previous_ath, 2),
            "atl": round(float(low.min()), 2),
            "multi_year_high": round(multi_year_high, 2),
            "high_3y": round(high_3y, 2),
            "high_52w": round(float(high.tail(252).max()), 2),
            "low_52w": round(float(low.tail(252).min()), 2),
            "high_52w_prev": round(previous_high_52w, 2),
            "low_52w_prev": round(previous_low_52w, 2),
            "high_6m": round(float(high.tail(126).max()), 2),
            "low_6m": round(float(low.tail(126).min()), 2),
            "high_6m_prev": round(previous_high_6m, 2),
            "low_6m_prev": round(previous_low_6m, 2),
            "high_3m": round(float(high.tail(63).max()), 2),
            "range_high_20d": round(float(high.tail(20).max()), 2),
            "range_high_prev_20d": round(previous_range_high, 2),
            "sma20": self._round_or_none(sma20),
            "sma50": self._round_or_none(sma50),
            "sma150": self._round_or_none(sma150),
            "sma200": self._round_or_none(sma200),
            "sma200_1m_ago": self._round_or_none(sma200_1m_ago),
            "sma200_5m_ago": self._round_or_none(sma200_5m_ago),
            "ema10": self._round_or_none(ema10),
            "ema20": self._round_or_none(ema20),
            "ema50": self._round_or_none(ema50),
            "ema200": self._round_or_none(ema200),
            "weekly_ema20": self._round_or_none(weekly_ema20),
            "ma200_type": "ema",
            "benchmark_return_1d": round(benchmark_return_1d, 2),
            "benchmark_return_5d": round(benchmark_return_5d, 2),
            "benchmark_return_20d": round(benchmark_return_20d, 2),
            "benchmark_return_60d": round(benchmark_return_60d, 2),
            "benchmark_return_126d": round(benchmark_return_126d, 2),
            "benchmark_return_252d": round(benchmark_return_252d, 2),
            "sector_return_20d": round(sector_return_20d, 2),
            "stock_return_5d": round(stock_return_5d, 2),
            "stock_return_20d": round(stock_return_20d, 2),
            "stock_return_40d": round(stock_return_40d, 2),
            "stock_return_60d": round(stock_return_60d, 2),
            "stock_return_126d": round(stock_return_126d, 2),
            "stock_return_189d": round(stock_return_189d, 2),
            "stock_return_12m": round(stock_return_12m, 2),
            "stock_return_504d": round(stock_return_504d, 2),
            "stock_return_12m_1d_ago": round(stock_return_12m_1d_ago, 2),
            "stock_return_12m_1w_ago": round(stock_return_12m_1w_ago, 2),
            "stock_return_12m_1m_ago": round(stock_return_12m_1m_ago, 2),
            "baseline_close_5d": round(float(baseline_close_5d), 4) if baseline_close_5d is not None else None,
            "baseline_close_20d": round(float(baseline_close_20d), 4) if baseline_close_20d is not None else None,
            "baseline_close_40d": round(float(baseline_close_40d), 4) if baseline_close_40d is not None else None,
            "baseline_close_60d": round(float(baseline_close_60d), 4) if baseline_close_60d is not None else None,
            "baseline_close_63d": round(float(baseline_close_63d), 4) if baseline_close_63d is not None else None,
            "baseline_close_126d": round(float(baseline_close_126d), 4) if baseline_close_126d is not None else None,
            "baseline_close_189d": round(float(baseline_close_189d), 4) if baseline_close_189d is not None else None,
            "baseline_close_252d": round(float(baseline_close_252d), 4) if baseline_close_252d is not None else None,
            "baseline_close_504d": round(float(baseline_close_504d), 4) if baseline_close_504d is not None else None,
            "rs_baseline_close_q1": round(float(rs_baseline_close_q1), 4) if rs_baseline_close_q1 is not None else None,
            "rs_baseline_close_q2": round(float(rs_baseline_close_q2), 4) if rs_baseline_close_q2 is not None else None,
            "rs_baseline_close_q3": round(float(rs_baseline_close_q3), 4) if rs_baseline_close_q3 is not None else None,
            "rs_baseline_close_q4": round(float(rs_baseline_close_q4), 4) if rs_baseline_close_q4 is not None else None,
            "rs_weighted_score": round(rs_weighted_score, 4),
            "rs_weighted_score_1d_ago": round(rs_weighted_score_1d_ago, 4),
            "rs_weighted_score_1w_ago": round(rs_weighted_score_1w_ago, 4),
            "rs_weighted_score_1m_ago": round(rs_weighted_score_1m_ago, 4),
            "rs_eligible": rs_eligible,
            "rs_eligible_1d_ago": rs_eligible_1d_ago,
            "rs_eligible_1w_ago": rs_eligible_1w_ago,
            "rs_eligible_1m_ago": rs_eligible_1m_ago,
            "rs_line_today": round(rs_line_today, 2),
            "rs_line_1m": round(rs_line_1m, 2),
            "rsi_14": round(rsi_14, 2),
            "gap_pct": round(gap_pct, 2),
            "pivot_high": round(float(high.tail(10).iloc[:-1].max() if len(high.tail(10)) > 1 else high.tail(10).max()), 2),
            "darvas_high": round(float(high.tail(15).iloc[:-1].max() if len(high.tail(15)) > 1 else high.tail(15).max()), 2),
            "darvas_low": round(float(low.tail(15).iloc[:-1].min() if len(low.tail(15)) > 1 else low.tail(15).min()), 2),
            "atr14": round(float(atr14), 2),
            "adr_pct_20": round(float(adr_pct_20), 2),
            "pullback_depth_pct": round(float(pullback_depth), 2),
            "trend_strength": round(float(trend_strength), 2),
            "long_base_avg_range_pct": round(float(best_base_avg_range), 2),
            "long_base_span_pct": round(float(best_base_span), 2),
            "long_base_window_days": int(best_base_window),
            "recent_highs": [round(float(value), 2) for value in high.tail(20).tolist()],
            "recent_lows": [round(float(value), 2) for value in low.tail(20).tolist()],
            "recent_volumes": [int(float(value) or 0) for value in volume.tail(20).tolist()],
            "chart_grid_points": chart_grid_points,
            "instrument_key": instrument["ticker"],
            "history_as_of_date": latest_history_date,
            "history_session_date": latest_history_date,
            "history_bars": history_bars,
            "history_source": "history",
        }

    def _history_listing_date(self, instrument: dict[str, Any], history: pd.DataFrame) -> str | None:
        del history
        return instrument.get("listing_date")

    def _fetch_chart_bars(self, symbol: str, timeframe: str, bars: int) -> list[ChartBar]:
        ticker = self._resolve_ticker(symbol)
        interval_map = {
            "15m": "15m",
            "30m": "30m",
            "1h": "60m",
            "1D": "1d",
        }
        period_map = {
            "15m": "60d",
            "30m": "60d",
            "1h": "730d",
            "1D": "max" if bars > 1300 else "5y",
        }
        if timeframe == "1W":
            history = self._history_frame_from_cached_bars(symbol)
            if not self._history_has_minimum_bars(history):
                history = self._history_frame_from_cached_bars(symbol, allow_legacy=True)
            if self._history_has_minimum_bars(history):
                history = history.copy()
            else:
                history = self._download_history_frame(ticker=ticker, period="10y", interval="1d")
                history = self._split_adjusted_history(history)
            history = self._apply_live_quote_to_daily_history(symbol, ticker, history)
            history = self._aggregate_weekly_history(history)
        else:
            interval = interval_map.get(timeframe, "1d")
            period = period_map.get(timeframe, "max")
            history = self._download_history_frame(ticker=ticker, period=period, interval=interval)
            if timeframe == "1D":
                history = self._split_adjusted_history(history)
                history = self._apply_live_quote_to_daily_history(symbol, ticker, history)

                # Yahoo can intermittently return sparse/empty index or commodity series.
                # Fill gaps from alternate public sources before giving up.
                if symbol.startswith("^") and (history.empty or len(history) < min(120, bars)):
                    index_fallback = self._download_nse_index_chart_history(symbol)
                    if not index_fallback.empty and len(index_fallback) > len(history):
                        history = index_fallback
                if symbol.upper() in {"CL=F", "BZ=F"} and (history.empty or len(history) < min(120, bars)):
                    commodity_fallback = self._download_fred_wti_history()
                    if not commodity_fallback.empty and len(commodity_fallback) > len(history):
                        history = commodity_fallback

        if history.empty:
            raise RuntimeError(f"No chart history for {symbol}")

        history = history.tail(bars)
        chart_bars = self._history_to_chart_bars(history)
        if not self._chart_bars_match_symbol_scale(symbol, chart_bars):
            raise RuntimeError(f"Chart history scale mismatch for {symbol}")
        return chart_bars

    def _download_nse_index_chart_history(self, symbol: str) -> pd.DataFrame:
        index_name = INDEX_SYMBOL_TO_NSE_NAME.get(symbol.upper())
        if not index_name:
            return pd.DataFrame()

        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
        }
        try:
            session = requests.Session()
            session.headers.update(headers)
            # Prime session for NSE cookies.
            session.get("https://www.nseindia.com/", timeout=15)
            session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=15)
            response = session.get(
                NSE_INDEX_CHART_URL,
                params={"index": index_name, "indices": "true"},
                timeout=20,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception:
            return pd.DataFrame()

        raw_points = payload.get("grapthData") or payload.get("graphData") or []
        records: list[dict[str, Any]] = []
        index_values: list[datetime] = []
        for point in raw_points:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            ts_raw, close_raw = point[0], point[1]
            try:
                ts_int = int(float(ts_raw))
                if ts_int > 10_000_000_000:
                    ts_int //= 1000
                close_val = float(close_raw)
            except (TypeError, ValueError):
                continue
            dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
            records.append(
                {
                    "Open": close_val,
                    "High": close_val,
                    "Low": close_val,
                    "Close": close_val,
                    "Adj Close": close_val,
                    "Volume": 0,
                    "Stock Splits": 0.0,
                }
            )
            index_values.append(dt)

        if not records:
            return pd.DataFrame()
        frame = pd.DataFrame.from_records(records, index=pd.DatetimeIndex(index_values))
        return frame[~frame.index.duplicated(keep="last")].sort_index()

    def _download_fred_wti_history(self) -> pd.DataFrame:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/csv,*/*",
            "Referer": "https://fred.stlouisfed.org/",
        }
        try:
            response = requests.get(FRED_WTI_HISTORY_CSV_URL, headers=headers, timeout=45)
            response.raise_for_status()
            csv_text = response.text
        except Exception:
            try:
                proc = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "25", FRED_WTI_HISTORY_CSV_URL],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                csv_text = proc.stdout if proc.returncode == 0 else ""
            except Exception:
                csv_text = ""
            if not csv_text:
                return pd.DataFrame()

        records: list[dict[str, Any]] = []
        index_values: list[datetime] = []
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            try:
                date_str = str(row.get("observation_date") or "").strip()
                value_str = str(row.get("DCOILWTICO") or "").strip()
                if not date_str or value_str in {"", "."}:
                    continue
                close_val = float(value_str)
                dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                continue
            records.append(
                {
                    "Open": close_val,
                    "High": close_val,
                    "Low": close_val,
                    "Close": close_val,
                    "Adj Close": close_val,
                    "Volume": 0,
                    "Stock Splits": 0.0,
                }
            )
            index_values.append(dt)

        if not records:
            return pd.DataFrame()
        frame = pd.DataFrame.from_records(records, index=pd.DatetimeIndex(index_values))
        return frame[~frame.index.duplicated(keep="last")].sort_index()

    def _live_quote_for_symbol(self, symbol: str, ticker: str) -> dict[str, Any] | None:
        normalized_ticker = str(ticker).upper()
        if self._default_exchange() == "NSE" and normalized_ticker in INDEX_SYMBOL_TO_NSE_NAME:
            try:
                index_item = self._fetch_nse_index_quote(normalized_ticker)
            except Exception:
                index_item = None
            if index_item is not None and self._is_live_quote_recent(index_item.updated_at):
                previous_close = index_item.price
                if index_item.change_pct not in (None, -100):
                    previous_close = index_item.price / (1 + (float(index_item.change_pct) / 100))
                return {
                    "symbol": normalized_ticker,
                    "regularMarketPrice": float(index_item.price),
                    "regularMarketPreviousClose": float(previous_close),
                    "regularMarketDayHigh": float(index_item.price),
                    "regularMarketDayLow": float(index_item.price),
                    "regularMarketOpen": float(previous_close),
                    "regularMarketVolume": 0,
                    "regularMarketTime": int(index_item.updated_at.timestamp()),
                }
        try:
            quotes = self._fetch_quote_batch([ticker])
        except Exception:
            return None
        return quotes.get(normalized_ticker) or quotes.get(str(symbol).upper())

    def _apply_live_quote_to_daily_history(self, symbol: str, ticker: str, history: pd.DataFrame) -> pd.DataFrame:
        del ticker
        if history.empty:
            return history
        return self._apply_snapshot_row_to_daily_history(symbol, history)

    def _download_history_frame(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        try:
            history = self._download_history_frame_http(ticker=ticker, period=period, interval=interval)
        except Exception:
            history = pd.DataFrame()
        if history.empty:
            try:
                frame = yf.download(
                    tickers=ticker,
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    actions=True,
                    progress=False,
                    threads=False,
                )
                history = self._extract_single_frame(frame).dropna(subset=["Close"])
            except Exception:
                history = pd.DataFrame()
        if history.empty:
            try:
                history = yf.Ticker(ticker).history(
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    actions=True,
                )
                history = history.rename(columns=str.title).dropna(subset=["Close"])
            except Exception:
                history = pd.DataFrame()
        if history.empty:
            return history

        normalized = history.copy()
        normalized.index = pd.to_datetime(normalized.index)
        normalized = normalized[~normalized.index.duplicated(keep="last")].sort_index()
        return normalized

    def _download_history_frame_http(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://finance.yahoo.com/",
        }
        with httpx.Client(timeout=20, headers=headers, follow_redirects=True) as client:
            response = client.get(
                f"{YAHOO_CHART_URL}/{ticker}",
                params={
                    "range": period,
                    "interval": interval,
                    "includePrePost": "false",
                    "events": "div,splits",
                },
            )
            response.raise_for_status()
            payload = response.json()

        result = ((payload.get("chart") or {}).get("result") or [None])[0]
        if not result:
            return pd.DataFrame()

        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote")) or [None])[0] or {}
        adjusted_quote = (((result.get("indicators") or {}).get("adjclose")) or [None])[0] or {}
        split_events = ((result.get("events") or {}).get("splits")) or {}
        if not timestamps:
            return pd.DataFrame()

        records: list[dict[str, Any]] = []
        index_values: list[datetime] = []
        opens = quote.get("open") or []
        highs = quote.get("high") or []
        lows = quote.get("low") or []
        closes = quote.get("close") or []
        adjusted_closes = adjusted_quote.get("adjclose") or []
        volumes = quote.get("volume") or []

        for index, timestamp_value in enumerate(timestamps):
            close = closes[index] if index < len(closes) else None
            if close in (None, ""):
                continue
            split_ratio = 0.0
            split_payload = split_events.get(str(timestamp_value)) or split_events.get(timestamp_value)
            if isinstance(split_payload, dict):
                numerator = self._to_float(split_payload.get("numerator"))
                denominator = self._to_float(split_payload.get("denominator"))
                if numerator not in (None, 0) and denominator not in (None, 0):
                    split_ratio = float(numerator) / float(denominator)
            records.append(
                {
                    "Open": opens[index] if index < len(opens) else close,
                    "High": highs[index] if index < len(highs) else close,
                    "Low": lows[index] if index < len(lows) else close,
                    "Close": close,
                    "Adj Close": adjusted_closes[index] if index < len(adjusted_closes) and adjusted_closes[index] is not None else close,
                    "Volume": volumes[index] if index < len(volumes) and volumes[index] is not None else 0,
                    "Stock Splits": split_ratio,
                }
            )
            index_values.append(datetime.fromtimestamp(int(timestamp_value), tz=timezone.utc))

        if not records:
            return pd.DataFrame()

        return pd.DataFrame.from_records(records, index=pd.DatetimeIndex(index_values))

    def _aggregate_weekly_history(self, history: pd.DataFrame) -> pd.DataFrame:
        if history.empty:
            return history

        source = history.copy()
        if isinstance(source.index, pd.DatetimeIndex) and source.index.tz is not None:
            week_periods = source.index.tz_localize(None).to_period("W-FRI")
        else:
            week_periods = source.index.to_period("W-FRI")
        grouped = source.groupby(week_periods)
        weekly = grouped.agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        last_dates = source.index.to_series(index=source.index).groupby(week_periods).last()
        weekly.index = pd.DatetimeIndex(last_dates.to_list())
        weekly.index.name = history.index.name
        return weekly.dropna(subset=["Close"]).sort_index()

    def _history_to_chart_bars(self, history: pd.DataFrame) -> list[ChartBar]:
        bars: list[ChartBar] = []
        prev_close = None
        for index, row in history.iterrows():
            o = round(float(row["Open"]), 2)
            h = round(float(row["High"]), 2)
            l = round(float(row["Low"]), 2)
            c = round(float(row["Close"]), 2)
            v = int(float(row["Volume"]) or 0)
            # Enforce OHLC consistency: high must be >= max(open, close), low must be <= min(open, close)
            h = max(h, o, c)
            l = min(l, o, c)
            if h <= 0 or c <= 0:
                continue

            # Filtering for holidays and weekends to prevent "flat" holiday candles
            bar_timestamp = index.to_pydatetime()
            bar_date = bar_timestamp.date()
            if not self._is_trading_day_ist(bar_date):
                # If it's a non-trading day and price is flat relative to prev close, skip it.
                if v == 0 and (prev_close is None or abs(c - prev_close) < 0.01):
                    continue

            bars.append(
                ChartBar(
                    time=int(bar_timestamp.replace(tzinfo=timezone.utc).timestamp()),
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=v,
                )
            )
            prev_close = c
        return bars

    def _window_max(self, series: pd.Series, window: int, *, exclude_last: bool) -> float:
        subset = series.tail(window)
        if exclude_last and len(subset) > 1:
            subset = subset.iloc[:-1]
        if subset.empty:
            subset = series
        return float(subset.max())

    def _window_min(self, series: pd.Series, window: int, *, exclude_last: bool) -> float:
        subset = series.tail(window)
        if exclude_last and len(subset) > 1:
            subset = subset.iloc[:-1]
        if subset.empty:
            subset = series
        return float(subset.min())

    def _resolve_ticker(self, symbol: str) -> str:
        if symbol.startswith("^") or symbol.endswith(".NS") or symbol.endswith(".BO"):
            return symbol
        if self.snapshot_cache_path.exists():
            rows = json.loads(self.snapshot_cache_path.read_text(encoding="utf-8"))
            row = next((item for item in rows if item["symbol"] == symbol), None)
            if row and row.get("instrument_key"):
                return str(row["instrument_key"])
        return f"{symbol}.NS"

    def _extract_history(self, frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame()
        if isinstance(frame.columns, pd.MultiIndex):
            level_zero = frame.columns.get_level_values(0)
            level_one = frame.columns.get_level_values(1)
            if ticker in level_zero:
                subset = frame.xs(ticker, axis=1, level=0)
            elif ticker in level_one:
                subset = frame.xs(ticker, axis=1, level=1)
            else:
                subset = self._extract_single_frame(frame)
        else:
            subset = frame
        return subset.rename(columns=str.title)

    def _extract_single_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame()
        if isinstance(frame.columns, pd.MultiIndex):
            level_zero = frame.columns.get_level_values(0)
            level_one = frame.columns.get_level_values(1)
            unique_level_zero = list(dict.fromkeys(level_zero))
            unique_level_one = list(dict.fromkeys(level_one))
            price_fields = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}

            if price_fields.intersection(set(unique_level_zero)) and len(unique_level_one) == 1:
                return frame.xs(unique_level_one[0], axis=1, level=1).rename(columns=str.title)
            if price_fields.intersection(set(unique_level_one)) and len(unique_level_zero) == 1:
                return frame.xs(unique_level_zero[0], axis=1, level=0).rename(columns=str.title)
            if len(unique_level_zero) == 1:
                return frame.xs(unique_level_zero[0], axis=1, level=0).rename(columns=str.title)
            if len(unique_level_one) == 1:
                return frame.xs(unique_level_one[0], axis=1, level=1).rename(columns=str.title)
        return frame.rename(columns=str.title)

    def _extract_close_series(self, frame: pd.DataFrame, ticker: str | None = None) -> pd.Series:
        history = self._extract_history(frame, ticker) if ticker else self._extract_single_frame(frame)
        if history.empty or "Close" not in history.columns:
            return pd.Series(dtype=float)

        close = history["Close"]
        if isinstance(close, pd.DataFrame):
            if close.empty:
                return pd.Series(dtype=float)
            close = close.iloc[:, 0]

        return close.dropna()

    def _effective_lookback(
        self,
        series: pd.Series,
        lookback: int,
        offset: int = 0,
        *,
        allow_partial: bool = True,
    ) -> int | None:
        series = series.dropna()
        available = len(series) - offset - 1
        if available < 1:
            return None
        if available >= lookback:
            return lookback
        if not allow_partial:
            return None
        return available

    def _return_pct(self, series: pd.Series, lookback: int, *, allow_partial: bool = True) -> float:
        series = series.dropna()
        effective_lookback = self._effective_lookback(series, lookback, 0, allow_partial=allow_partial)
        if effective_lookback is None:
            return 0.0
        baseline = float(series.iloc[-effective_lookback - 1])
        current = float(series.iloc[-1])
        if baseline == 0:
            return 0.0
        return ((current / baseline) - 1) * 100

    def _return_pct_as_of(
        self,
        series: pd.Series,
        lookback: int,
        offset: int,
        *,
        allow_partial: bool = True,
    ) -> float:
        series = series.dropna()
        effective_lookback = self._effective_lookback(series, lookback, offset, allow_partial=allow_partial)
        if effective_lookback is None:
            return 0.0
        current = float(series.iloc[-offset - 1])
        baseline = float(series.iloc[-offset - effective_lookback - 1])
        if baseline == 0:
            return 0.0
        return ((current / baseline) - 1) * 100

    def _baseline_at_lookback(
        self,
        series: pd.Series,
        lookback: int,
        offset: int = 0,
        *,
        allow_partial: bool = True,
    ) -> float | None:
        series = series.dropna()
        effective_lookback = self._effective_lookback(series, lookback, offset, allow_partial=allow_partial)
        if effective_lookback is None:
            return None
        baseline = float(series.iloc[-offset - effective_lookback - 1])
        if baseline == 0:
            return None
        return baseline

    @staticmethod
    def _return_from_baseline(current_price: float, baseline: float | None, fallback: float) -> float:
        if baseline in (None, 0):
            return fallback
        return ((current_price / float(baseline)) - 1) * 100

    def _has_full_history(self, series: pd.Series, lookback: int, offset: int) -> bool:
        return len(series.dropna()) > lookback + offset

    def _adaptive_rs_lookbacks(self, series: pd.Series, offset: int) -> list[tuple[int, float]]:
        available = self._effective_lookback(series, RETURN_1Y_BARS, offset, allow_partial=True)
        if available is None or available < MIN_ADAPTIVE_RS_HISTORY_BARS:
            return []
        if available >= RETURN_1Y_BARS:
            return list(RS_LOOKBACKS)

        weighted_fractions = zip((0.25, 0.5, 0.75, 1.0), (0.4, 0.2, 0.2, 0.2), strict=True)
        adaptive_lookbacks: list[tuple[int, float]] = []
        previous_lookback = 0
        for fraction, weight in weighted_fractions:
            candidate = max(1, int(round(available * fraction)))
            candidate = max(candidate, previous_lookback + 1)
            candidate = min(candidate, available)
            adaptive_lookbacks.append((candidate, weight))
            previous_lookback = candidate
        return adaptive_lookbacks

    def _weighted_rs_score(self, series: pd.Series, offset: int) -> float:
        lookbacks = self._adaptive_rs_lookbacks(series, offset)
        if not lookbacks:
            return 0.0

        score = 0.0
        for lookback, weight in lookbacks:
            score += self._return_pct_as_of(series, lookback, offset, allow_partial=False) * weight
        return score

    def _weighted_rs_score_from_price(self, row: dict[str, Any], current_price: float) -> float:
        weighted_baselines = [
            (self._to_float(row.get("rs_baseline_close_q1")), 0.4),
            (self._to_float(row.get("rs_baseline_close_q2")), 0.2),
            (self._to_float(row.get("rs_baseline_close_q3")), 0.2),
            (self._to_float(row.get("rs_baseline_close_q4")), 0.2),
        ]
        if any(baseline in (None, 0) for baseline, _ in weighted_baselines):
            return float(row.get("rs_weighted_score", 0) or 0)

        score = 0.0
        for baseline, weight in weighted_baselines:
            assert baseline is not None
            score += (((current_price / baseline) - 1) * 100) * weight
        return score

    def _relative_strength_line_value(self, stock_close: pd.Series, benchmark_close: pd.Series, *, offset: int) -> float:
        aligned = pd.concat(
            [
                stock_close.rename("stock"),
                benchmark_close.rename("benchmark"),
            ],
            axis=1,
            join="inner",
        ).dropna()
        if aligned.empty or len(aligned) <= offset:
            return 0.0

        row = aligned.iloc[-offset - 1]
        benchmark_value = float(row["benchmark"])
        if benchmark_value == 0:
            return 0.0
        return (float(row["stock"]) / benchmark_value) * 100

    @staticmethod
    def _ema_with_min_history(series: pd.Series, span: int) -> float | None:
        cleaned = series.dropna()
        if len(cleaned) < span:
            return None
        value = cleaned.ewm(span=span, adjust=False).mean().iloc[-1]
        if pd.isna(value):
            return None
        return float(value)

    @staticmethod
    def _sma_with_min_history(series: pd.Series, window: int) -> float | None:
        cleaned = series.dropna()
        if len(cleaned) < window:
            return None
        value = cleaned.rolling(window=window, min_periods=window).mean().iloc[-1]
        if pd.isna(value):
            return None
        return float(value)

    @staticmethod
    def _round_or_none(value: float | None, digits: int = 2) -> float | None:
        if value is None:
            return None
        return round(float(value), digits)

    def _trend_strength(self, price: float, ema20: float | None, ema50: float | None, ema200: float | None) -> float:
        if ema20 is None or ema50 is None or ema200 is None:
            return 0.0
        score = 0.0
        if price >= ema20:
            score += 0.35
        if ema20 >= ema50:
            score += 0.35
        if ema50 >= ema200:
            score += 0.30
        return min(score, 0.99)

    def _full_period_return_pct(self, series: pd.Series) -> float:
        series = series.dropna()
        if len(series) < 2:
            return 0.0
        baseline = float(series.iloc[0])
        current = float(series.iloc[-1])
        if baseline == 0:
            return 0.0
        return ((current / baseline) - 1) * 100

    def _apply_rs_rating(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows

        self._assign_percentile_rating(rows, "rs_weighted_score", "rs_rating", "rs_eligible")
        self._assign_percentile_rating(rows, "rs_weighted_score_1d_ago", "rs_rating_1d_ago", "rs_eligible_1d_ago")
        self._assign_percentile_rating(rows, "rs_weighted_score_1w_ago", "rs_rating_1w_ago", "rs_eligible_1w_ago")
        self._assign_percentile_rating(rows, "rs_weighted_score_1m_ago", "rs_rating_1m_ago", "rs_eligible_1m_ago")
        return rows

    def _assign_percentile_rating(
        self,
        rows: list[dict[str, Any]],
        return_field: str,
        rating_field: str,
        eligibility_field: str,
    ) -> None:
        eligible = [
            (float(row.get(return_field, 0) or 0), index)
            for index, row in enumerate(rows)
            if bool(row.get(eligibility_field, False))
        ]

        for row in rows:
            row[rating_field] = 0

        if not eligible:
            return
        if len(eligible) == 1:
            rows[eligible[0][1]][rating_field] = 99
            return

        ordered = sorted(eligible)
        cursor = 0
        total = len(ordered)

        while cursor < total:
            end = cursor
            current_return = ordered[cursor][0]
            while end + 1 < total and ordered[end + 1][0] == current_return:
                end += 1

            average_rank = (cursor + end) / 2
            percentile = round((average_rank / (total - 1)) * 99)
            rating = max(1, min(99, percentile))
            for position in range(cursor, end + 1):
                rows[ordered[position][1]][rating_field] = rating

            cursor = end + 1

    def _fetch_text(self, url: str) -> str:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        with httpx.Client(timeout=30, headers=headers, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.text

    def _fetch_bytes(self, url: str) -> bytes:
        headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
        with httpx.Client(timeout=60, headers=headers, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.content

    def _find_market_cap_download_url(self, html: str) -> str | None:
        soup = BeautifulSoup(html, "html.parser")
        candidates: list[tuple[int, str]] = []

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if href.lower().startswith("javascript"):
                continue

            context_node = anchor.find_parent("tr") or anchor.find_parent("li") or anchor.parent
            context = " ".join(context_node.stripped_strings) if context_node else ""
            lowered = context.lower()
            if "all companies" not in lowered:
                continue
            if "top 500" in lowered or "top 1000" in lowered:
                continue

            years = [int(year) for year in re.findall(r"20\d{2}", context)]
            score = max(years) if years else 0
            candidates.append((score, urljoin(NSE_MARKET_CAP_URL, href)))

        if candidates:
            candidates.sort(key=lambda item: item[0], reverse=True)
            return candidates[0][1]

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if any(ext in href.lower() for ext in (".xls", ".xlsx", ".csv")):
                return urljoin(NSE_MARKET_CAP_URL, href)
        return None

    def _parse_market_cap_sheet(self, content: bytes) -> pd.DataFrame:
        try:
            return pd.read_excel(io.BytesIO(content), engine="calamine")
        except Exception:
            text = content.decode("utf-8", errors="ignore")
            soup = BeautifulSoup(text, "html.parser")
            table = soup.find("table")
            if table is None:
                raise
            headers = [cell.get_text(" ", strip=True) for cell in table.find_all("tr")[0].find_all(["th", "td"])]
            rows: list[list[str]] = []
            for tr in table.find_all("tr")[1:]:
                cells = [cell.get_text(" ", strip=True) for cell in tr.find_all(["th", "td"])]
                if cells:
                    rows.append(cells)
            return pd.DataFrame(rows, columns=headers)

    def _find_column(self, columns: Any, aliases: list[str]) -> str:
        normalized = {str(column): self._normalize_column(column) for column in columns}
        for alias in aliases:
            alias_key = self._normalize_column(alias)
            for column, key in normalized.items():
                if alias_key in key or key in alias_key:
                    return column
        raise KeyError(f"Could not find any of {aliases} in {list(columns)}")

    def _normalize_column(self, value: Any) -> str:
        return "".join(char.lower() for char in str(value) if char.isalnum())

    def _normalize_symbol(self, value: Any) -> str:
        if value is None:
            return ""
        symbol = str(value).strip().upper()
        return "".join(char for char in symbol if char.isalnum() or char in "&-.")

    def _to_float(self, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).replace(",", "").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
