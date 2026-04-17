import asyncio
import gc
import json
import logging
import resource
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")
ET = ZoneInfo("America/New_York")


@dataclass
class RetryStrategy:
    max_retries: int = 3
    base_delay: float = 2.0
    timeout_seconds: float = 30.0


@dataclass
class DataQuality:
    row_count: int
    expected_min_rows: int
    has_null_prices: bool
    has_future_dates: bool
    suspicious_values: list[str] = field(default_factory=list)


@dataclass
class HealthStatus:
    healthy: bool
    component_id: str
    checked_at: datetime
    staleness_seconds: float | None
    detail: str
    action_needed: str | None
    severity: str
    data_quality: DataQuality | None = None


@dataclass
class HealthContract:
    component_id: str
    display_name: str
    market: str
    depends_on: list[str]
    check_fn: Callable[[], Awaitable[HealthStatus]]
    heal_fn: Callable[[], Awaitable[dict[str, Any] | None]]
    check_interval_open: int
    check_interval_closed: int
    max_staleness_seconds: float
    priority: int
    retry_strategy: RetryStrategy = field(default_factory=RetryStrategy)


@dataclass
class HealResult:
    success: bool
    deferred: bool = False
    retry_after: float | None = None
    detail: str | None = None


@dataclass
class AuditEntry:
    timestamp: datetime
    component: str
    action: str
    detail: str
    severity: str = "info"


class AuditTrail:
    """Ring-buffer + jsonl audit log for watchdog decisions."""

    def __init__(self, disk_path: Path, max_entries: int = 500):
        self._entries: deque[AuditEntry] = deque(maxlen=max_entries)
        self._disk_path = disk_path
        self._disk_path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, component: str, action: str, detail: str, severity: str = "info") -> None:
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc),
            component=component,
            action=action,
            detail=detail,
            severity=severity,
        )
        self._entries.append(entry)
        try:
            payload = {
                "timestamp": entry.timestamp.isoformat(),
                "component": entry.component,
                "action": entry.action,
                "detail": entry.detail,
                "severity": entry.severity,
            }
            with self._disk_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload) + "\n")
        except Exception as exc:
            logger.warning("Watchdog audit write failed: %s", exc)

    def recent(self, component: str | None = None, limit: int = 50) -> list[AuditEntry]:
        entries = list(self._entries)
        if component:
            entries = [item for item in entries if item.component == component]
        return entries[-max(1, min(limit, 500)) :]

    def failure_count(self, component: str, window_seconds: int = 600) -> int:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        return sum(
            1
            for item in self._entries
            if item.component == component and item.action in {"heal_failed", "heal_exhausted"} and item.timestamp >= cutoff
        )


class SignalBus:
    """SSE signal hub for frontend invalidation events."""

    def __init__(self):
        self._subscribers: dict[asyncio.Queue[str], str | None] = {}
        self._lock = asyncio.Lock()

    async def subscribe(self, market: str | None = None) -> asyncio.Queue[str]:
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=100)
        normalized = str(market).strip().lower() if market else None
        async with self._lock:
            self._subscribers[queue] = normalized
        return queue

    async def unsubscribe(self, queue: asyncio.Queue[str]) -> None:
        async with self._lock:
            self._subscribers.pop(queue, None)

    async def emit(self, event: str, data: dict[str, Any], market: str | None = None) -> None:
        payload = {
            "event": event,
            "data": data,
            "market": (str(market).strip().lower() if market else "global"),
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        message = json.dumps(payload)
        async with self._lock:
            subscribers = list(self._subscribers.items())

        dead: list[asyncio.Queue[str]] = []
        for queue, subscriber_market in subscribers:
            target_market = payload["market"]
            if subscriber_market and target_market not in {subscriber_market, "global"}:
                continue
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                dead.append(queue)

        if dead:
            async with self._lock:
                for queue in dead:
                    self._subscribers.pop(queue, None)


class DependencyGraph:
    _edges: dict[str, list[str]] = {
        "india.snapshot": [
            "india.dashboard",
            "india.sector_tab",
            "india.scan_catalog",
            "india.industry_groups",
            "india.market_health",
            "india.improving_rs",
            "india.sector_rotation",
        ],
        "us.snapshot": [
            "us.dashboard",
            "us.sector_tab",
            "us.scan_catalog",
            "us.industry_groups",
            "us.market_health",
            "us.improving_rs",
            "us.sector_rotation",
        ],
        "india.fundamentals": ["india.ai_analysis"],
        "us.fundamentals": ["us.ai_analysis"],
    }

    def cascade_invalidate(self, refreshed_component: str) -> list[str]:
        invalidated: list[str] = []
        queue: list[str] = [refreshed_component]
        while queue:
            current = queue.pop(0)
            for child in self._edges.get(current, []):
                if child in invalidated:
                    continue
                invalidated.append(child)
                queue.append(child)
        return invalidated


class AdaptiveScheduler:
    def get_market_state(self, market: str) -> str:
        normalized = str(market or "global").strip().lower()
        if normalized not in {"india", "us"}:
            return "closed"
        now_local = datetime.now(IST if normalized == "india" else ET)
        weekday = now_local.weekday()
        total_minutes = now_local.hour * 60 + now_local.minute
        open_min = 9 * 60 + (15 if normalized == "india" else 30)
        close_min = (15 * 60 + 30) if normalized == "india" else (16 * 60)
        if weekday >= 5 or total_minutes < open_min or total_minutes > close_min:
            return "closed"
        if total_minutes - open_min < 10:
            return "just_opened"
        if close_min - total_minutes < 5:
            return "near_close"
        return "open"

    def get_check_interval(self, contract: HealthContract, market_state: str, recent_failures: int) -> int:
        base = contract.check_interval_open if market_state in {"open", "just_opened", "near_close"} else contract.check_interval_closed
        if recent_failures > 0:
            urgency_multiplier = max(0.3, 1.0 - (recent_failures * 0.2))
            return max(15, int(base * urgency_multiplier))
        if market_state == "just_opened" and contract.priority == 1:
            return 30
        if market_state == "near_close" and contract.priority == 1:
            return 45
        return max(15, int(base))


class RateLimitTracker:
    def __init__(self):
        self._failure_windows: dict[str, list[float]] = {}
        self._backoff_until: dict[str, float] = {}

    def record_failure(self, provider: str, error: Exception) -> str | None:
        error_text = str(error).lower()
        is_rate_limited = "429" in error_text or "rate" in error_text or "too many" in error_text
        now = time.time()
        window = self._failure_windows.setdefault(provider, [])
        window.append(now)
        window[:] = [item for item in window if now - item < 600]
        if is_rate_limited or len(window) >= 3:
            backoff_seconds = min(300, 30 * (2 ** min(len(window), 4)))
            self._backoff_until[provider] = now + backoff_seconds
            return f"backoff {provider} for {backoff_seconds}s"
        return None

    def can_call(self, provider: str) -> bool:
        return time.time() >= self._backoff_until.get(provider, 0)

    def time_until_available(self, provider: str) -> float:
        return max(0.0, self._backoff_until.get(provider, 0) - time.time())


class DataQualityValidator:
    def validate_snapshots(self, rows: list[dict[str, Any]], market: str) -> list[str]:
        normalized = str(market).strip().lower()
        expected_min = 1200 if normalized == "india" else 3000
        issues: list[str] = []
        if len(rows) < expected_min:
            issues.append(f"Only {len(rows)} rows; expected >= {expected_min}")

        today = date.today()
        for row in rows[:80]:
            symbol = str(row.get("symbol") or "?")
            try:
                price = float(row.get("last_price") or 0)
            except Exception:
                price = 0
            try:
                change_pct = float(row.get("change_pct") or 0)
            except Exception:
                change_pct = 0
            try:
                market_cap = float(row.get("market_cap_crore") or 0)
            except Exception:
                market_cap = 0

            if price <= 0:
                issues.append(f"{symbol}: price={price}")
            if abs(change_pct) > 50:
                issues.append(f"{symbol}: change_pct={change_pct}")
            if market_cap <= 0:
                issues.append(f"{symbol}: market_cap={market_cap}")

            session_date = row.get("history_session_date")
            if session_date:
                try:
                    parsed = date.fromisoformat(str(session_date))
                    age_days = (today - parsed).days
                    if age_days > 5:
                        issues.append(f"{symbol}: session_date stale ({age_days}d)")
                    if parsed > today:
                        issues.append(f"{symbol}: session_date in future ({parsed.isoformat()})")
                except Exception:
                    issues.append(f"{symbol}: bad session_date={session_date}")

            if len(issues) >= 24:
                break
        return issues

    def validate_dashboard(self, dashboard_payload: Any) -> list[str]:
        issues: list[str] = []
        gainers = list(getattr(dashboard_payload, "top_gainers", []) or [])
        losers = list(getattr(dashboard_payload, "top_losers", []) or [])
        spikes = list(getattr(dashboard_payload, "top_volume_spikes", []) or [])

        if not gainers:
            issues.append("top_gainers empty")
        if not losers:
            issues.append("top_losers empty")
        if not spikes:
            issues.append("top_volume_spikes empty")

        for item in gainers[:3]:
            if float(getattr(item, "change_pct", 0) or 0) <= 0:
                issues.append(f"gainer {getattr(item, 'symbol', '?')} has non-positive change")
        for item in losers[:3]:
            if float(getattr(item, "change_pct", 0) or 0) >= 0:
                issues.append(f"loser {getattr(item, 'symbol', '?')} has non-negative change")
        return issues


class ActionEngine:
    def __init__(
        self,
        rate_limiter: RateLimitTracker,
        signal_bus: SignalBus,
        audit: AuditTrail,
    ):
        self.rate_limiter = rate_limiter
        self.signal_bus = signal_bus
        self.audit = audit

    async def execute_heal(
        self,
        contract: HealthContract,
        status: HealthStatus,
        dep_graph: DependencyGraph,
        registry: dict[str, HealthContract],
    ) -> HealResult:
        component = contract.component_id
        provider = "yfinance" if any(token in component for token in ("snapshot", "fundamentals", "universe", "bhavcopy")) else "internal"

        self.audit.log(component, "heal_started", status.detail)
        if provider != "internal" and not self.rate_limiter.can_call(provider):
            wait_seconds = self.rate_limiter.time_until_available(provider)
            self.audit.log(component, "heal_deferred", f"rate-limited; retry in {wait_seconds:.0f}s", severity="warning")
            return HealResult(success=False, deferred=True, retry_after=wait_seconds, detail="rate-limited")

        for attempt in range(contract.retry_strategy.max_retries):
            try:
                payload = await asyncio.wait_for(contract.heal_fn(), timeout=contract.retry_strategy.timeout_seconds)
                self.audit.log(component, "heal_success", f"attempt {attempt + 1}")

                downstream = dep_graph.cascade_invalidate(component)
                for child_id in downstream:
                    child_contract = registry.get(child_id)
                    if child_contract is None:
                        continue
                    try:
                        await asyncio.wait_for(child_contract.heal_fn(), timeout=min(20.0, child_contract.retry_strategy.timeout_seconds))
                        self.audit.log(child_id, "cascade_healed", f"rewarmed after {component}")
                    except Exception as exc:
                        self.audit.log(child_id, "cascade_failed", f"{type(exc).__name__}: {exc}", severity="warning")

                await self.signal_bus.emit(
                    "cache_invalidated",
                    {
                        "component": component,
                        "downstream": downstream,
                        "heal_payload": payload or {},
                    },
                    market=contract.market,
                )
                if component.endswith("snapshot"):
                    await self.signal_bus.emit(
                        "snapshot_refreshed",
                        {"component": component, "downstream": downstream},
                        market=contract.market,
                    )
                if component.endswith("fundamentals") and isinstance(payload, dict) and payload.get("symbols"):
                    first_symbol = str((payload.get("symbols") or [""])[0]).upper()
                    if first_symbol:
                        await self.signal_bus.emit(
                            "fundamentals_updated",
                            {"component": component, "symbol": first_symbol},
                            market=contract.market,
                        )
                return HealResult(success=True)
            except Exception as exc:
                backoff_note = self.rate_limiter.record_failure(provider, exc)
                delay_seconds = contract.retry_strategy.base_delay * (2 ** attempt)
                detail = f"attempt {attempt + 1} failed: {type(exc).__name__}: {exc}"
                if backoff_note:
                    detail = f"{detail}; {backoff_note}"
                self.audit.log(component, "heal_failed", detail, severity="warning")
                if attempt < contract.retry_strategy.max_retries - 1:
                    await asyncio.sleep(delay_seconds)

        self.audit.log(component, "heal_exhausted", f"all retries failed for {component}", severity="critical")
        return HealResult(success=False, detail="retries exhausted")


class WatchdogAgent:
    """Autonomous data guardian for backend caches, files, and refresh pipelines."""

    def __init__(
        self,
        services: dict[str, Any],
        settings_by_market: dict[str, Any],
        data_dir: Path,
        tick_seconds: int = 30,
    ):
        self.services = services
        self.settings_by_market = settings_by_market
        self.tick_seconds = tick_seconds
        self.registry: dict[str, HealthContract] = {}
        self.dep_graph = DependencyGraph()
        self.scheduler = AdaptiveScheduler()
        self.rate_limiter = RateLimitTracker()
        self.signal_bus = SignalBus()
        self.audit = AuditTrail(data_dir / "watchdog_audit.jsonl")
        self.action_engine = ActionEngine(self.rate_limiter, self.signal_bus, self.audit)
        self.validator = DataQualityValidator()
        self._last_check: dict[str, float] = {}
        self._last_status: dict[str, HealthStatus] = {}
        self._boot_time = time.time()

        for market in ("india", "us"):
            service_obj = services.get(market)
            settings_obj = settings_by_market.get(market)
            if service_obj is not None and settings_obj is not None:
                self._register_market_contracts(market, service_obj, settings_obj)
        self._register_global_contracts(data_dir)

    def register(self, contract: HealthContract) -> None:
        self.registry[contract.component_id] = contract

    def recent_audit(self, component: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in self.audit.recent(component=component, limit=limit):
            payload = asdict(item)
            payload["timestamp"] = item.timestamp.isoformat()
            rows.append(payload)
        return rows

    def failure_count(self, component: str, window_seconds: int = 600) -> int:
        return self.audit.failure_count(component, window_seconds)

    async def run_cycle(self) -> None:
        import os
        is_hf = os.getenv("SPACE_ID") is not None
        now = time.time()
        
        # Stagger the first cycle on HF to avoid competing with web server startup memory spikes
        if is_hf and (now - self._boot_time) < 60:
            logger.info("WATCHDOG: staggering initial cycle (waiting for web server to settle)...")
            return

        for component_id, contract in self.registry.items():
            market_state = self.scheduler.get_market_state(contract.market)
            recent_failures = self.audit.failure_count(component_id)
            interval = self.scheduler.get_check_interval(contract, market_state, recent_failures)
            last_run = self._last_check.get(component_id, 0)
            if now - last_run < interval:
                continue

            if not self._dependencies_healthy(contract):
                self._last_check[component_id] = now
                self.audit.log(component_id, "check_skipped", "parent dependency unhealthy", severity="warning")
                continue

            self._last_check[component_id] = now
            try:
                status = await asyncio.wait_for(contract.check_fn(), timeout=10)
            except Exception as exc:
                status = HealthStatus(
                    healthy=False,
                    component_id=component_id,
                    checked_at=datetime.now(timezone.utc),
                    staleness_seconds=None,
                    detail=f"health check failed: {type(exc).__name__}: {exc}",
                    action_needed="retry_check",
                    severity="warning",
                    data_quality=None,
                )
            self._last_status[component_id] = status

            if status.healthy:
                continue

            self.audit.log(component_id, "unhealthy_detected", f"{status.severity}: {status.detail}", severity=status.severity)
            heal_result = await self.action_engine.execute_heal(contract, status, self.dep_graph, self.registry)
            if not heal_result.success and not heal_result.deferred:
                self.audit.log(component_id, "heal_not_fixed", heal_result.detail or "heal failed", severity="critical")

    def _dependencies_healthy(self, contract: HealthContract) -> bool:
        for dep_id in contract.depends_on:
            dep_status = self._last_status.get(dep_id)
            if dep_status is not None and not dep_status.healthy:
                return False
            dep_contract = self.registry.get(dep_id)
            if dep_contract is None:
                continue
            dep_last_check = self._last_check.get(dep_id, 0)
            if dep_last_check == 0:
                return False
            if time.time() - dep_last_check > dep_contract.max_staleness_seconds * 2:
                return False
        return True

    @staticmethod
    def _generated_at_from_cache(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, tuple):
            for item in value:
                if isinstance(item, datetime):
                    return item
            return None
        for attr in ("generated_at", "updated_at"):
            candidate = getattr(value, attr, None)
            if isinstance(candidate, datetime):
                return candidate
        return None

    async def _warm_dashboard_children(self, service_obj: Any) -> None:
        await asyncio.gather(
            service_obj.build_dashboard(),
            service_obj.get_sector_tab("1D", "desc"),
            service_obj.get_industry_groups(),
            return_exceptions=True,
        )

    async def _sample_symbols(self, service_obj: Any, limit: int = 6) -> list[str]:
        dashboard_cache = getattr(service_obj, "_dashboard_cache", None)
        symbols: list[str] = []
        if dashboard_cache is not None:
            for collection_name in ("top_gainers", "top_losers", "top_volume_spikes"):
                for item in list(getattr(dashboard_cache, collection_name, []) or [])[:3]:
                    symbol = str(getattr(item, "symbol", "")).upper().strip()
                    if symbol and symbol not in symbols:
                        symbols.append(symbol)
                    if len(symbols) >= limit:
                        return symbols
        snapshots = await service_obj._snapshots()
        for snapshot in snapshots[:limit]:
            symbol = str(getattr(snapshot, "symbol", "")).upper().strip()
            if symbol and symbol not in symbols:
                symbols.append(symbol)
        return symbols[:limit]

    def _register_market_contracts(self, market: str, service_obj: Any, settings_obj: Any) -> None:
        market_name = str(market).strip().lower()
        provider_obj = service_obj.provider

        async def check_snapshot() -> HealthStatus:
            age_seconds = float(getattr(provider_obj, "_snapshot_age_seconds")())
            is_market_open = bool(getattr(provider_obj, "_is_market_open_ist")())
            load_rows_fn = getattr(provider_obj, "_load_valid_cached_snapshot_rows", None)
            rows = await asyncio.to_thread(load_rows_fn) if callable(load_rows_fn) else []
            quality_issues = self.validator.validate_snapshots(rows, market_name)
            quality = DataQuality(
                row_count=len(rows),
                expected_min_rows=1200 if market_name == "india" else 3000,
                has_null_prices=any(float(row.get("last_price", 0) or 0) <= 0 for row in rows[:100]),
                has_future_dates=any(str(row.get("history_session_date") or "") > date.today().isoformat() for row in rows[:100]),
                suspicious_values=quality_issues[:12],
            )
            stale = bool((is_market_open and age_seconds > 180) or quality_issues)
            detail = (
                f"snapshot age={age_seconds:.0f}s open={is_market_open} rows={len(rows)}"
                if not quality_issues
                else f"snapshot quality issues: {', '.join(quality_issues[:5])}"
            )
            return HealthStatus(
                healthy=not stale,
                component_id=f"{market_name}.snapshot",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=age_seconds,
                detail=detail,
                action_needed=None if not stale else "refresh_live_snapshots",
                severity="ok" if not stale else "critical",
                data_quality=quality,
            )

        async def heal_snapshot() -> dict[str, Any]:
            service_obj._clear_runtime_caches()
            refresh_live = getattr(provider_obj, "refresh_live_snapshots", None)
            get_snapshots = getattr(provider_obj, "get_snapshots", None)
            if callable(refresh_live):
                snapshots = await refresh_live(settings_obj.market_cap_min_crore)
            elif callable(get_snapshots):
                snapshots = await get_snapshots(settings_obj.market_cap_min_crore)
            else:
                snapshots = []
            prewarm = await service_obj.prewarm_watchdog_sections(snapshots)
            return {
                "snapshot_count": len(snapshots),
                "section_count": prewarm.get("section_count", 0),
            }

        async def check_dashboard() -> HealthStatus:
            snapshot_updated_at = service_obj._snapshot_updated_at()
            cache_generated_at = self._generated_at_from_cache(getattr(service_obj, "_dashboard_cache", None))
            healthy = cache_generated_at is not None and cache_generated_at >= snapshot_updated_at
            detail = (
                f"dashboard cache @ {cache_generated_at.isoformat()}"
                if cache_generated_at
                else "dashboard cache missing"
            )
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.dashboard",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=(datetime.now(timezone.utc) - cache_generated_at).total_seconds() if cache_generated_at else None,
                detail=detail,
                action_needed=None if healthy else "build_dashboard",
                severity="ok" if healthy else "critical",
            )

        async def heal_dashboard() -> dict[str, Any]:
            service_obj._dashboard_cache = None
            payload = await service_obj.build_dashboard()
            issues = self.validator.validate_dashboard(payload)
            if issues:
                self.audit.log(f"{market_name}.dashboard", "quality_warning", ", ".join(issues[:5]), severity="warning")
            return {"issues": issues[:5]}

        async def check_sector_tab() -> HealthStatus:
            snapshot_updated_at = service_obj._snapshot_updated_at()
            cache_payload = (getattr(service_obj, "_sector_tab_cache", {}) or {}).get(("1D", "desc"))
            generated_at = self._generated_at_from_cache(cache_payload)
            healthy = generated_at is not None and generated_at >= snapshot_updated_at
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.sector_tab",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=(datetime.now(timezone.utc) - generated_at).total_seconds() if generated_at else None,
                detail="sector tab cache warm" if healthy else "sector tab cache missing/stale",
                action_needed=None if healthy else "get_sector_tab",
                severity="ok" if healthy else "critical",
            )

        async def heal_sector_tab() -> dict[str, Any]:
            cache_obj = getattr(service_obj, "_sector_tab_cache", None)
            if isinstance(cache_obj, dict):
                cache_obj.pop(("1D", "desc"), None)
            await service_obj.get_sector_tab("1D", "desc")
            return {"ok": True}

        async def check_market_overview() -> HealthStatus:
            cache_entry = getattr(service_obj, "_market_overview_cache", None)
            is_open_fn = getattr(provider_obj, "_is_market_open_ist", None)
            is_open = bool(is_open_fn()) if callable(is_open_fn) else False
            ttl = 90 if is_open else 300
            now = time.time()
            if not cache_entry:
                return HealthStatus(
                    healthy=False,
                    component_id=f"{market_name}.market_overview",
                    checked_at=datetime.now(timezone.utc),
                    staleness_seconds=None,
                    detail="market overview cache missing",
                    action_needed="get_market_overview",
                    severity="critical",
                )
            age_seconds = now - float(cache_entry[0])
            healthy = age_seconds < ttl
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.market_overview",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=age_seconds,
                detail=f"market overview age={age_seconds:.0f}s ttl={ttl}s",
                action_needed=None if healthy else "refresh_market_overview",
                severity="ok" if healthy else "warning",
            )

        async def heal_market_overview() -> dict[str, Any]:
            service_obj._market_overview_cache = None
            await service_obj.get_market_overview()
            return {"ok": True}

        async def check_industry_groups() -> HealthStatus:
            snapshot_updated_at = service_obj._snapshot_updated_at()
            generated_at = self._generated_at_from_cache(getattr(service_obj, "_industry_groups_cache", None))
            healthy = generated_at is not None and generated_at >= snapshot_updated_at
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.industry_groups",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=(datetime.now(timezone.utc) - generated_at).total_seconds() if generated_at else None,
                detail="industry groups cache warm" if healthy else "industry groups cache missing/stale",
                action_needed=None if healthy else "get_industry_groups",
                severity="ok" if healthy else "warning",
            )

        async def heal_industry_groups() -> dict[str, Any]:
            service_obj._industry_groups_cache = None
            await service_obj.get_industry_groups()
            return {"ok": True}

        async def check_market_health() -> HealthStatus:
            snapshot_updated_at = service_obj._snapshot_updated_at()
            generated_at = self._generated_at_from_cache(getattr(service_obj, "_market_health_cache", None))
            healthy = generated_at is not None and generated_at >= snapshot_updated_at
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.market_health",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=(datetime.now(timezone.utc) - generated_at).total_seconds() if generated_at else None,
                detail="market health cache warm" if healthy else "market health cache missing/stale",
                action_needed=None if healthy else "get_market_health",
                severity="ok" if healthy else "warning",
            )

        async def heal_market_health() -> dict[str, Any]:
            service_obj._market_health_cache = None
            await service_obj.get_market_health()
            return {"ok": True}

        async def check_sector_rotation() -> HealthStatus:
            snapshot_updated_at = service_obj._snapshot_updated_at()
            generated_at = self._generated_at_from_cache(getattr(service_obj, "_sector_rotation_cache", None))
            healthy = generated_at is not None and generated_at >= snapshot_updated_at
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.sector_rotation",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=(datetime.now(timezone.utc) - generated_at).total_seconds() if generated_at else None,
                detail="sector rotation cache warm" if healthy else "sector rotation cache missing/stale",
                action_needed=None if healthy else "get_sector_rotation",
                severity="ok" if healthy else "warning",
            )

        async def heal_sector_rotation() -> dict[str, Any]:
            service_obj._sector_rotation_cache = None
            get_sector_rotation = getattr(service_obj, "get_sector_rotation", None)
            if callable(get_sector_rotation):
                await get_sector_rotation()
            return {"ok": True}

        async def check_scan_catalog() -> HealthStatus:
            snapshot_updated_at = service_obj._snapshot_updated_at()
            cache = getattr(service_obj, "_scan_catalog_cache", None)
            generated_at = cache[0] if isinstance(cache, tuple) and cache else None
            healthy = isinstance(generated_at, datetime) and generated_at >= snapshot_updated_at
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.scan_catalog",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=(datetime.now(timezone.utc) - generated_at).total_seconds() if isinstance(generated_at, datetime) else None,
                detail="scan catalog warm" if healthy else "scan catalog missing/stale",
                action_needed=None if healthy else "get_scan_counts",
                severity="ok" if healthy else "warning",
            )

        async def heal_scan_catalog() -> dict[str, Any]:
            service_obj._scan_catalog_cache = None
            await service_obj.get_scan_counts()
            return {"ok": True}

        async def check_improving_rs() -> HealthStatus:
            cache = getattr(service_obj, "_improving_rs_cache", {}) or {}
            one_day = cache.get("1D") if isinstance(cache, dict) else None
            healthy = one_day is not None
            generated_at = self._generated_at_from_cache(one_day)
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.improving_rs",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=(datetime.now(timezone.utc) - generated_at).total_seconds() if generated_at else None,
                detail="improving RS 1D warm" if healthy else "improving RS 1D cache missing",
                action_needed=None if healthy else "get_improving_rs",
                severity="ok" if healthy else "warning",
            )

        async def heal_improving_rs() -> dict[str, Any]:
            service_obj._improving_rs_cache.clear()
            await service_obj.get_improving_rs("1D")
            return {"ok": True}

        async def check_fundamentals() -> HealthStatus:
            get_fundamentals_cached = getattr(provider_obj, "get_fundamentals_cached", None)
            if not callable(get_fundamentals_cached):
                return HealthStatus(
                    healthy=True,
                    component_id=f"{market_name}.fundamentals",
                    checked_at=datetime.now(timezone.utc),
                    staleness_seconds=None,
                    detail="provider has no fundamentals cache API",
                    action_needed=None,
                    severity="ok",
                )
            symbols = await self._sample_symbols(service_obj, limit=4)
            stale_symbols: list[str] = []
            for symbol in symbols:
                item = await get_fundamentals_cached(symbol, max_age_hours=6)
                if item is None:
                    stale_symbols.append(symbol)
            healthy = len(stale_symbols) == 0
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.fundamentals",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail="all sampled fundamentals fresh" if healthy else f"stale fundamentals: {', '.join(stale_symbols[:3])}",
                action_needed=None if healthy else "get_fundamentals",
                severity="ok" if healthy else "warning",
            )

        async def heal_fundamentals() -> dict[str, Any]:
            get_fundamentals = getattr(provider_obj, "get_fundamentals", None)
            if not callable(get_fundamentals):
                return {"symbols": []}
            symbols = await self._sample_symbols(service_obj, limit=3)
            refreshed: list[str] = []
            for symbol in symbols:
                try:
                    await get_fundamentals(symbol)
                    refreshed.append(symbol)
                except Exception:
                    continue
            return {"symbols": refreshed}

        async def check_ai_analysis() -> HealthStatus:
            ai_service = getattr(provider_obj, "ai_service", None)
            if ai_service is None:
                return HealthStatus(
                    healthy=True,
                    component_id=f"{market_name}.ai_analysis",
                    checked_at=datetime.now(timezone.utc),
                    staleness_seconds=None,
                    detail="AI service unavailable",
                    action_needed=None,
                    severity="ok",
                )
            disk_cache = ai_service._load_disk_cache()
            stale_symbols: list[str] = []
            for symbol, entry in list(disk_cache.items())[:25]:
                try:
                    fundamentals = await provider_obj.get_fundamentals_cached(symbol, max_age_hours=None)
                    fetched_at = fundamentals.fetched_at.isoformat() if fundamentals is not None else None
                    if not ai_service._is_cache_fresh(entry, fundamentals_fetched_at=fetched_at):
                        stale_symbols.append(symbol)
                except Exception:
                    stale_symbols.append(symbol)
            healthy = len(stale_symbols) == 0
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.ai_analysis",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail="AI cache fresh" if healthy else f"stale AI cache symbols: {', '.join(stale_symbols[:3])}",
                action_needed=None if healthy else "analyze_company",
                severity="ok" if healthy else "warning",
            )

        async def heal_ai_analysis() -> dict[str, Any]:
            ai_service = getattr(provider_obj, "ai_service", None)
            if ai_service is None:
                return {"symbols": []}
            disk_cache = ai_service._load_disk_cache()
            refreshed: list[str] = []
            for symbol in list(disk_cache.keys())[:2]:
                fundamentals = await provider_obj.get_fundamentals(symbol)
                ai_service.clear_cache(symbol)
                ai_service.analyze_company(fundamentals)
                refreshed.append(symbol)
            return {"symbols": refreshed}

        async def check_money_flow_stocks() -> HealthStatus:
            now_local = datetime.now(IST if market_name == "india" else ET)
            cutoff = dt_time(hour=18, minute=0) if market_name == "india" else dt_time(hour=16, minute=30)
            if now_local.weekday() >= 5 or now_local.time() < cutoff:
                return HealthStatus(
                    healthy=True,
                    component_id=f"{market_name}.money_flow_stocks",
                    checked_at=datetime.now(timezone.utc),
                    staleness_seconds=None,
                    detail="before cutoff or non-trading day",
                    action_needed=None,
                    severity="ok",
                )
            payload = await service_obj.get_money_flow_stock_ideas()
            recommendation_date = str(getattr(payload, "recommendation_date", ""))
            today_key = now_local.date().isoformat()
            healthy = recommendation_date == today_key
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.money_flow_stocks",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail=f"recommendation_date={recommendation_date or 'missing'}",
                action_needed=None if healthy else "ensure_money_flow_stock_ideas_current",
                severity="ok" if healthy else "warning",
            )

        async def heal_money_flow_stocks() -> dict[str, Any]:
            payload = await service_obj.ensure_money_flow_stock_ideas_current()
            return {"updated": payload is not None}

        async def check_money_flow_report() -> HealthStatus:
            now_local = datetime.now(IST if market_name == "india" else ET)
            current_week_key = f"{now_local.date().isocalendar().year}-W{now_local.date().isocalendar().week:02d}"
            latest = await service_obj.get_money_flow_latest()
            week_key = str(getattr(latest, "week_key", "")) if latest is not None else ""
            healthy = week_key == current_week_key
            return HealthStatus(
                healthy=healthy,
                component_id=f"{market_name}.money_flow_report",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail=f"week_key={week_key or 'missing'} current={current_week_key}",
                action_needed=None if healthy else "ensure_money_flow_report_current",
                severity="ok" if healthy else "warning",
            )

        async def heal_money_flow_report() -> dict[str, Any]:
            payload = await service_obj.ensure_money_flow_report_current()
            return {"updated": payload is not None}

        async def check_bhavcopy() -> HealthStatus:
            if market_name != "india":
                return HealthStatus(
                    healthy=True,
                    component_id="india.bhavcopy",
                    checked_at=datetime.now(timezone.utc),
                    staleness_seconds=None,
                    detail="not applicable",
                    action_needed=None,
                    severity="ok",
                )
            now_local = datetime.now(IST)
            if now_local.weekday() >= 5 or now_local.time() < dt_time(hour=16, minute=30):
                return HealthStatus(
                    healthy=True,
                    component_id="india.bhavcopy",
                    checked_at=datetime.now(timezone.utc),
                    staleness_seconds=None,
                    detail="before bhavcopy window",
                    action_needed=None,
                    severity="ok",
                )
            last_patch_fn = getattr(provider_obj, "_last_applied_bhavcopy_date", None)
            last_patch_date = last_patch_fn() if callable(last_patch_fn) else None
            healthy = last_patch_date == now_local.date()
            return HealthStatus(
                healthy=healthy,
                component_id="india.bhavcopy",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail=f"last_applied={last_patch_date}",
                action_needed=None if healthy else "apply_bhavcopy_eod",
                severity="ok" if healthy else "warning",
            )

        async def heal_bhavcopy() -> dict[str, Any]:
            live_patch_fn = getattr(provider_obj, "apply_bhavcopy_eod", None)
            fallback_patch_fn = getattr(provider_obj, "apply_committed_bhavcopy_patch", None)
            service_obj._clear_runtime_caches()
            result: dict[str, Any] = {}
            if callable(live_patch_fn):
                result = await asyncio.to_thread(live_patch_fn)
            if result.get("status") != "ok" and callable(fallback_patch_fn):
                fallback = await asyncio.to_thread(fallback_patch_fn)
                if fallback.get("status") == "ok" or fallback.get("snapshots_updated", 0) > 0:
                    result = fallback
            if int(result.get("snapshots_updated", 0) or 0) > 0:
                await service_obj.prewarm_watchdog_sections()
            return result

        async def check_universe() -> HealthStatus:
            universe_path = getattr(provider_obj, "universe_cache_path", None)
            is_fresh_fn = getattr(provider_obj, "_is_fresh", None)
            fresh = bool(callable(is_fresh_fn) and universe_path and is_fresh_fn(universe_path, max_age_hours=48))
            return HealthStatus(
                healthy=fresh,
                component_id=f"{market_name}.universe",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail="universe cache fresh" if fresh else "universe cache stale",
                action_needed=None if fresh else "refresh_universe",
                severity="ok" if fresh else "warning",
            )

        async def heal_universe() -> dict[str, Any]:
            load_refresh_universe = getattr(provider_obj, "_load_or_refresh_universe", None)
            if callable(load_refresh_universe):
                await asyncio.to_thread(load_refresh_universe, settings_obj.market_cap_min_crore, True)
            await provider_obj.get_snapshots(settings_obj.market_cap_min_crore)
            return {"ok": True}

        contracts = [
            HealthContract(
                component_id=f"{market_name}.snapshot",
                display_name=f"{market_name.upper()} Snapshot",
                market=market_name,
                depends_on=[],
                check_fn=check_snapshot,
                heal_fn=heal_snapshot,
                check_interval_open=60,
                check_interval_closed=90,
                max_staleness_seconds=180,
                priority=1,
                retry_strategy=RetryStrategy(max_retries=3, base_delay=2.0, timeout_seconds=45.0),
            ),
            HealthContract(
                component_id=f"{market_name}.dashboard",
                display_name=f"{market_name.upper()} Dashboard",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_dashboard,
                heal_fn=heal_dashboard,
                check_interval_open=60,
                check_interval_closed=120,
                max_staleness_seconds=180,
                priority=1,
            ),
            HealthContract(
                component_id=f"{market_name}.sector_tab",
                display_name=f"{market_name.upper()} Sector Tab",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_sector_tab,
                heal_fn=heal_sector_tab,
                check_interval_open=60,
                check_interval_closed=120,
                max_staleness_seconds=180,
                priority=1,
            ),
            HealthContract(
                component_id=f"{market_name}.market_overview",
                display_name=f"{market_name.upper()} Market Overview",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_market_overview,
                heal_fn=heal_market_overview,
                check_interval_open=90,
                check_interval_closed=180,
                max_staleness_seconds=300,
                priority=1,
            ),
            HealthContract(
                component_id=f"{market_name}.industry_groups",
                display_name=f"{market_name.upper()} Industry Groups",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_industry_groups,
                heal_fn=heal_industry_groups,
                check_interval_open=300,
                check_interval_closed=300,
                max_staleness_seconds=600,
                priority=2,
            ),
            HealthContract(
                component_id=f"{market_name}.market_health",
                display_name=f"{market_name.upper()} Market Health",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_market_health,
                heal_fn=heal_market_health,
                check_interval_open=300,
                check_interval_closed=300,
                max_staleness_seconds=600,
                priority=2,
            ),
            HealthContract(
                component_id=f"{market_name}.sector_rotation",
                display_name=f"{market_name.upper()} Sector Rotation",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_sector_rotation,
                heal_fn=heal_sector_rotation,
                check_interval_open=300,
                check_interval_closed=300,
                max_staleness_seconds=600,
                priority=2,
            ),
            HealthContract(
                component_id=f"{market_name}.scan_catalog",
                display_name=f"{market_name.upper()} Scan Catalog",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_scan_catalog,
                heal_fn=heal_scan_catalog,
                check_interval_open=300,
                check_interval_closed=300,
                max_staleness_seconds=600,
                priority=2,
            ),
            HealthContract(
                component_id=f"{market_name}.improving_rs",
                display_name=f"{market_name.upper()} Improving RS",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_improving_rs,
                heal_fn=heal_improving_rs,
                check_interval_open=300,
                check_interval_closed=300,
                max_staleness_seconds=600,
                priority=2,
            ),
            HealthContract(
                component_id=f"{market_name}.fundamentals",
                display_name=f"{market_name.upper()} Fundamentals",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_fundamentals,
                heal_fn=heal_fundamentals,
                check_interval_open=1800,
                check_interval_closed=1800,
                max_staleness_seconds=6 * 3600,
                priority=3,
            ),
            HealthContract(
                component_id=f"{market_name}.ai_analysis",
                display_name=f"{market_name.upper()} AI Analysis",
                market=market_name,
                depends_on=[f"{market_name}.fundamentals"],
                check_fn=check_ai_analysis,
                heal_fn=heal_ai_analysis,
                check_interval_open=1800,
                check_interval_closed=1800,
                max_staleness_seconds=24 * 3600,
                priority=3,
            ),
            HealthContract(
                component_id=f"{market_name}.money_flow_stocks",
                display_name=f"{market_name.upper()} Money Flow Stocks",
                market=market_name,
                depends_on=[f"{market_name}.snapshot", f"{market_name}.fundamentals"],
                check_fn=check_money_flow_stocks,
                heal_fn=heal_money_flow_stocks,
                check_interval_open=900,
                check_interval_closed=900,
                max_staleness_seconds=24 * 3600,
                priority=3,
            ),
            HealthContract(
                component_id=f"{market_name}.money_flow_report",
                display_name=f"{market_name.upper()} Money Flow Report",
                market=market_name,
                depends_on=[f"{market_name}.snapshot"],
                check_fn=check_money_flow_report,
                heal_fn=heal_money_flow_report,
                check_interval_open=1800,
                check_interval_closed=1800,
                max_staleness_seconds=7 * 24 * 3600,
                priority=3,
            ),
            HealthContract(
                component_id=f"{market_name}.universe",
                display_name=f"{market_name.upper()} Universe",
                market=market_name,
                depends_on=[],
                check_fn=check_universe,
                heal_fn=heal_universe,
                check_interval_open=1800,
                check_interval_closed=1800,
                max_staleness_seconds=48 * 3600,
                priority=3,
            ),
        ]

        if market_name == "india":
            contracts.append(
                HealthContract(
                    component_id="india.bhavcopy",
                    display_name="India Bhavcopy",
                    market="india",
                    depends_on=["india.snapshot"],
                    check_fn=check_bhavcopy,
                    heal_fn=heal_bhavcopy,
                    check_interval_open=900,
                    check_interval_closed=900,
                    max_staleness_seconds=6 * 3600,
                    priority=2,
                )
            )

        for contract in contracts:
            self.register(contract)

    def _register_global_contracts(self, data_dir: Path) -> None:
        async def check_disk_space() -> HealthStatus:
            total_bytes = 0
            largest_file_bytes = 0
            for file_path in data_dir.rglob("*"):
                if not file_path.is_file():
                    continue
                try:
                    size = file_path.stat().st_size
                except Exception:
                    continue
                total_bytes += size
                largest_file_bytes = max(largest_file_bytes, size)
            healthy = total_bytes < 2 * 1024 * 1024 * 1024 and largest_file_bytes < 500 * 1024 * 1024
            detail = f"data_size={total_bytes}B largest_file={largest_file_bytes}B"
            return HealthStatus(
                healthy=healthy,
                component_id="global.disk_space",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail=detail,
                action_needed=None if healthy else "prune_cache_files",
                severity="ok" if healthy else "warning",
            )

        async def heal_disk_space() -> dict[str, Any]:
            removed = 0
            for market in ("", "_us"):
                chart_dir = data_dir / f"chart_cache{market}"
                if not chart_dir.exists() or not chart_dir.is_dir():
                    continue
                files = sorted(
                    [item for item in chart_dir.glob("*.json") if item.is_file()],
                    key=lambda item: item.stat().st_mtime,
                )
                if len(files) <= 2000:
                    continue
                for item in files[: len(files) - 2000]:
                    try:
                        item.unlink(missing_ok=True)
                        removed += 1
                    except Exception:
                        continue
            return {"removed_files": removed}

        async def check_memory() -> HealthStatus:
            rss_raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            rss_bytes = int(rss_raw if rss_raw > 10_000_000 else rss_raw * 1024)
            healthy = rss_bytes < int(1.5 * 1024 * 1024 * 1024)
            return HealthStatus(
                healthy=healthy,
                component_id="global.memory",
                checked_at=datetime.now(timezone.utc),
                staleness_seconds=None,
                detail=f"rss={rss_bytes}B",
                action_needed=None if healthy else "clear_cold_caches",
                severity="ok" if healthy else "warning",
            )

        async def heal_memory() -> dict[str, Any]:
            for service_obj in self.services.values():
                clear_fn = getattr(service_obj, "_clear_runtime_caches", None)
                if callable(clear_fn):
                    clear_fn()
            gc.collect()
            return {"gc": True}

        self.register(
            HealthContract(
                component_id="global.disk_space",
                display_name="Global Disk Space",
                market="global",
                depends_on=[],
                check_fn=check_disk_space,
                heal_fn=heal_disk_space,
                check_interval_open=300,
                check_interval_closed=300,
                max_staleness_seconds=600,
                priority=3,
            )
        )
        self.register(
            HealthContract(
                component_id="global.memory",
                display_name="Global Memory",
                market="global",
                depends_on=[],
                check_fn=check_memory,
                heal_fn=heal_memory,
                check_interval_open=300,
                check_interval_closed=300,
                max_staleness_seconds=600,
                priority=3,
            )
        )


_ACTIVE_WATCHDOG_AGENT: WatchdogAgent | None = None


def set_active_watchdog_agent(agent: WatchdogAgent | None) -> None:
    global _ACTIVE_WATCHDOG_AGENT
    _ACTIVE_WATCHDOG_AGENT = agent


def get_active_watchdog_agent() -> WatchdogAgent | None:
    return _ACTIVE_WATCHDOG_AGENT
