import { useEffect, useMemo, useRef, useState } from "react";

import { getLiveBaselines, type LiveBaseline } from "../lib/api";
import { LiveFeed, type LiveFeedStatus, type LiveTick } from "../lib/liveFeed";
import { Panel } from "./Panel";
import type { LocalWatchlist } from "./WatchlistsPanel";

import "./LivePanel.css";

type LivePanelProps = {
  watchlists: LocalWatchlist[];
  onOpenSymbolChart: (symbol: string) => void;
};

type Criteria = {
  minChgPct: number;
  minRvol: number;
  nearHighOn: boolean;
  nearHighPct: number;
  soundOn: boolean;
};

type LiveSettings = {
  watchlistIds: string[];
  manualSymbols: string[];
  criteria: Criteria;
};

const STORAGE_KEY = "stockScanner.livePage.v1";
const MAX_SYMBOLS = 180;
const DEFAULT_CRITERIA: Criteria = {
  minChgPct: 4,
  minRvol: 2,
  nearHighOn: true,
  nearHighPct: 1.5,
  soundOn: false,
};

function readSettings(): LiveSettings {
  const fallback: LiveSettings = { watchlistIds: [], manualSymbols: [], criteria: DEFAULT_CRITERIA };
  if (typeof window === "undefined") return fallback;
  try {
    const raw = JSON.parse(window.localStorage.getItem(STORAGE_KEY) ?? "null");
    if (!raw || typeof raw !== "object") return fallback;
    return {
      watchlistIds: Array.isArray(raw.watchlistIds) ? raw.watchlistIds.map(String) : [],
      manualSymbols: Array.isArray(raw.manualSymbols) ? raw.manualSymbols.map(String) : [],
      criteria: { ...DEFAULT_CRITERIA, ...(raw.criteria && typeof raw.criteria === "object" ? raw.criteria : {}) },
    };
  } catch {
    return fallback;
  }
}

function istNow(): { minutes: number; weekday: number } {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    hour: "2-digit",
    minute: "2-digit",
    weekday: "short",
    hour12: false,
  }).formatToParts(new Date());
  const get = (type: string) => parts.find((p) => p.type === type)?.value ?? "";
  const minutes = Number(get("hour")) * 60 + Number(get("minute"));
  const weekday = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(get("weekday"));
  return { minutes, weekday };
}

const MARKET_OPEN_MIN = 9 * 60 + 15;
const MARKET_CLOSE_MIN = 15 * 60 + 30;

function marketOpenNow(): boolean {
  const { minutes, weekday } = istNow();
  return weekday >= 1 && weekday <= 5 && minutes >= MARKET_OPEN_MIN && minutes < MARKET_CLOSE_MIN;
}

function elapsedSessionFraction(): number {
  const { minutes } = istNow();
  const f = (minutes - MARKET_OPEN_MIN) / (MARKET_CLOSE_MIN - MARKET_OPEN_MIN);
  return Math.min(1, Math.max(0.08, f));
}

function beep(): void {
  try {
    const Ctx = window.AudioContext ?? (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!Ctx) return;
    const ctx = new Ctx();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.frequency.value = 880;
    gain.gain.value = 0.06;
    osc.connect(gain).connect(ctx.destination);
    osc.start();
    osc.stop(ctx.currentTime + 0.18);
    osc.onended = () => void ctx.close();
  } catch {
    // audio unavailable — silent
  }
}

function fmtNum(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return value.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function fmtVol(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value) || value <= 0) return "—";
  if (value >= 1e7) return (value / 1e7).toFixed(2) + " Cr";
  if (value >= 1e5) return (value / 1e5).toFixed(2) + " L";
  if (value >= 1e3) return (value / 1e3).toFixed(1) + " K";
  return String(Math.round(value));
}

export function LivePanel({ watchlists, onOpenSymbolChart }: LivePanelProps) {
  const initial = useMemo(readSettings, []);
  const [watchlistIds, setWatchlistIds] = useState<string[]>(initial.watchlistIds);
  const [manualSymbols, setManualSymbols] = useState<string[]>(initial.manualSymbols);
  const [criteria, setCriteria] = useState<Criteria>(initial.criteria);
  const [manualInput, setManualInput] = useState("");
  const [feedStatus, setFeedStatus] = useState<LiveFeedStatus>("closed");
  const [baselines, setBaselines] = useState<Record<string, LiveBaseline>>({});
  const [baselineError, setBaselineError] = useState<string | null>(null);
  const [, setRenderTick] = useState(0);
  const [marketOpen, setMarketOpen] = useState<boolean>(() => marketOpenNow());
  const [lastTickAt, setLastTickAt] = useState<number | null>(null);

  const ticksRef = useRef<Map<string, LiveTick>>(new Map());
  const feedRef = useRef<LiveFeed | null>(null);
  const dirtyRef = useRef(false);
  const triggeredRef = useRef<Set<string>>(new Set());
  const criteriaRef = useRef(criteria);
  criteriaRef.current = criteria;

  const symbols = useMemo(() => {
    const set = new Set<string>();
    for (const wl of watchlists) {
      if (watchlistIds.includes(wl.id)) {
        for (const s of wl.symbols) set.add(s.toUpperCase());
      }
    }
    for (const s of manualSymbols) set.add(s.toUpperCase());
    return [...set].slice(0, MAX_SYMBOLS);
  }, [watchlists, watchlistIds, manualSymbols]);
  const symbolsKey = symbols.join(",");

  // Persist settings.
  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify({ watchlistIds, manualSymbols, criteria }));
    } catch {
      // best effort
    }
  }, [watchlistIds, manualSymbols, criteria]);

  // Live feed lifecycle — browser connects directly to the stream. The
  // backend is NOT involved in this loop.
  useEffect(() => {
    if (!symbols.length) {
      feedRef.current?.stop();
      feedRef.current = null;
      return;
    }
    if (!feedRef.current) {
      const feed = new LiveFeed(
        (tick) => {
          ticksRef.current.set(tick.symbol, tick);
          dirtyRef.current = true;
          setLastTickAt(Date.now());
        },
        (status) => setFeedStatus(status),
      );
      feedRef.current = feed;
      feed.start(symbols);
    } else {
      feedRef.current.setSymbols(symbols);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbolsKey]);

  useEffect(() => () => {
    feedRef.current?.stop();
    feedRef.current = null;
  }, []);

  // Render at most once a second no matter the tick rate.
  useEffect(() => {
    const id = window.setInterval(() => {
      if (dirtyRef.current) {
        dirtyRef.current = false;
        setRenderTick((v) => v + 1);
      }
      setMarketOpen(marketOpenNow());
    }, 1000);
    return () => window.clearInterval(id);
  }, []);

  // ONE baselines request per symbol-set from the existing backend (EOD data:
  // prev close, 20D avg volume). Debounced; failures degrade gracefully.
  useEffect(() => {
    if (!symbols.length) return;
    const id = window.setTimeout(() => {
      getLiveBaselines(symbols)
        .then((resp) => {
          setBaselines((prev) => ({ ...prev, ...(resp.baselines || {}) }));
          setBaselineError(null);
        })
        .catch(() => setBaselineError("Baselines unavailable — %chg uses stream data; RVOL hidden."));
    }, 400);
    return () => window.clearTimeout(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbolsKey]);

  type Row = {
    symbol: string;
    name: string | null;
    ltp: number | null;
    chgPct: number | null;
    projRvol: number | null;
    vol: number | null;
    dayHigh: number | null;
    dayLow: number | null;
    prevClose: number | null;
    live: boolean;
    triggered: boolean;
  };

  const rows: Row[] = useMemo(() => {
    const open = marketOpen;
    const elapsed = open ? elapsedSessionFraction() : 1;
    const out: Row[] = [];
    for (const symbol of symbols) {
      const tick = ticksRef.current.get(symbol);
      const base = baselines[symbol];
      const ltp = tick?.price ?? base?.close ?? null;
      const prevClose = tick?.previousClose ?? base?.prev_close ?? null;
      let chgPct = tick?.changePercent ?? null;
      if (chgPct === null && ltp !== null && prevClose) chgPct = (ltp / prevClose - 1) * 100;
      const liveVol = tick?.dayVolume ?? null;
      const vol = liveVol ?? base?.volume ?? null;
      const avgVol = base?.avg_volume_20d ?? null;
      // Time-adjust only when the volume figure is a live intraday number;
      // an EOD fallback is a full session's volume already.
      const rvolElapsed = liveVol !== null && open ? elapsed : 1;
      const projRvol = vol !== null && avgVol ? vol / (avgVol * rvolElapsed) : null;
      const dayHigh = tick?.dayHigh ?? null;
      const dayLow = tick?.dayLow ?? null;
      const crit = criteriaRef.current;
      const nearHighOk = !crit.nearHighOn
        || (ltp !== null && dayHigh !== null && dayHigh > 0 && ltp >= dayHigh * (1 - crit.nearHighPct / 100));
      const triggered = Boolean(
        tick
        && chgPct !== null && chgPct >= crit.minChgPct
        && (projRvol === null ? false : projRvol >= crit.minRvol)
        && nearHighOk,
      );
      out.push({
        symbol,
        name: base?.name ?? null,
        ltp,
        chgPct,
        projRvol,
        vol,
        dayHigh,
        dayLow,
        prevClose,
        live: Boolean(tick),
        triggered,
      });
    }
    out.sort((a, b) => (b.chgPct ?? -999) - (a.chgPct ?? -999));
    return out;
    // renderTick drives recomputes; deps intentionally coarse.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbolsKey, baselines, marketOpen, criteria, lastTickAt]);

  // New-trigger alerts (flash handled by CSS class; optional beep).
  useEffect(() => {
    const current = new Set(rows.filter((r) => r.triggered).map((r) => r.symbol));
    const previous = triggeredRef.current;
    const fresh = [...current].filter((s) => !previous.has(s));
    if (fresh.length && criteriaRef.current.soundOn) beep();
    triggeredRef.current = current;
  }, [rows]);

  const addManual = () => {
    const parts = manualInput
      .split(/[,\s]+/)
      .map((s) => s.trim().toUpperCase())
      .filter((s) => /^[A-Z0-9&.\-]{1,20}$/.test(s));
    if (parts.length) setManualSymbols((prev) => [...new Set([...prev, ...parts])]);
    setManualInput("");
  };

  const triggeredRows = rows.filter((r) => r.triggered);
  const statusLabel =
    feedStatus === "open" ? "stream connected" : feedStatus === "connecting" ? "connecting…" : feedStatus;

  return (
    <Panel
      title="Live"
      subtitle="Streaming watch + intraday criteria — prices flow straight to your browser; the backend stays untouched"
      className="live-panel"
    >
      <div className="live-toolbar">
        <div className="live-sources">
          <span className="live-label">Watchlists</span>
          {watchlists.length === 0 ? <span className="live-muted">none yet</span> : null}
          {watchlists.map((wl) => (
            <button
              key={wl.id}
              type="button"
              className={watchlistIds.includes(wl.id) ? "live-chip active" : "live-chip"}
              style={watchlistIds.includes(wl.id) ? { borderColor: wl.color } : undefined}
              onClick={() =>
                setWatchlistIds((prev) =>
                  prev.includes(wl.id) ? prev.filter((id) => id !== wl.id) : [...prev, wl.id],
                )
              }
            >
              {wl.name} <small>{wl.symbols.length}</small>
            </button>
          ))}
        </div>
        <div className="live-manual">
          <input
            value={manualInput}
            onChange={(e) => setManualInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                addManual();
              }
            }}
            placeholder="Add symbols (RELIANCE, TCS…)"
          />
          <button type="button" onClick={addManual}>Add</button>
        </div>
      </div>

      {manualSymbols.length ? (
        <div className="live-manual-chips">
          {manualSymbols.map((s) => (
            <span key={s} className="live-chip small">
              {s}
              <button
                type="button"
                aria-label={`Remove ${s}`}
                onClick={() => setManualSymbols((prev) => prev.filter((x) => x !== s))}
              >
                ×
              </button>
            </span>
          ))}
        </div>
      ) : null}

      <div className="live-criteria">
        <span className="live-label">Alert when</span>
        <label>
          %chg ≥
          <input
            type="number" step="0.5" min="0"
            value={criteria.minChgPct}
            onChange={(e) => setCriteria((c) => ({ ...c, minChgPct: Number(e.target.value) || 0 }))}
          />
        </label>
        <label>
          RVOL ≥
          <input
            type="number" step="0.5" min="0"
            value={criteria.minRvol}
            onChange={(e) => setCriteria((c) => ({ ...c, minRvol: Number(e.target.value) || 0 }))}
          />
        </label>
        <label className="live-check">
          <input
            type="checkbox"
            checked={criteria.nearHighOn}
            onChange={(e) => setCriteria((c) => ({ ...c, nearHighOn: e.target.checked }))}
          />
          within {criteria.nearHighPct}% of day high
        </label>
        <label className="live-check">
          <input
            type="checkbox"
            checked={criteria.soundOn}
            onChange={(e) => setCriteria((c) => ({ ...c, soundOn: e.target.checked }))}
          />
          sound
        </label>
        <span className={`live-status ${feedStatus}`}>
          {symbols.length} symbols · {statusLabel}
          {lastTickAt ? ` · last tick ${new Date(lastTickAt).toLocaleTimeString("en-IN", { hour12: false })}` : ""}
        </span>
      </div>

      {!marketOpen ? (
        <div className="live-banner">
          Market closed — showing last traded / EOD values. The stream reconnects and flows automatically when the
          session opens (Mon–Fri 9:15–15:30 IST).
        </div>
      ) : null}
      {baselineError ? <div className="live-banner warn">{baselineError}</div> : null}

      {triggeredRows.length ? (
        <div className="live-triggered">
          <div className="live-triggered-head">Triggered ({triggeredRows.length})</div>
          <div className="live-triggered-row">
            {triggeredRows.map((r) => (
              <button key={r.symbol} type="button" className="live-trigger-card" onClick={() => onOpenSymbolChart(r.symbol)}>
                <strong>{r.symbol}</strong>
                <span className="pos">+{fmtNum(r.chgPct, 1)}%</span>
                <small>RVOL {fmtNum(r.projRvol, 1)}x</small>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {symbols.length === 0 ? (
        <div className="live-empty">
          Pick a watchlist above or add symbols to start tracking. Up to {MAX_SYMBOLS} symbols stream at once.
        </div>
      ) : (
        <div className="live-table-wrap">
          <table className="live-table">
            <thead>
              <tr>
                <th>Symbol</th>
                <th className="num">LTP</th>
                <th className="num">%Chg</th>
                <th className="num" title="Day volume vs 20-day average, adjusted for time elapsed in the session">RVOL</th>
                <th className="num">Volume</th>
                <th className="num">Day Range</th>
                <th className="num">Prev Close</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => {
                const rangePos =
                  r.ltp !== null && r.dayHigh !== null && r.dayLow !== null && r.dayHigh > r.dayLow
                    ? (r.ltp - r.dayLow) / (r.dayHigh - r.dayLow)
                    : null;
                return (
                  <tr key={r.symbol} className={r.triggered ? "triggered" : undefined}>
                    <td>
                      <button type="button" className="live-symbol" onClick={() => onOpenSymbolChart(r.symbol)}>
                        {r.symbol}
                      </button>
                      {!r.live ? <small className="live-eod-tag">EOD</small> : null}
                    </td>
                    <td className="num">{fmtNum(r.ltp)}</td>
                    <td className={`num ${r.chgPct !== null && r.chgPct >= 0 ? "pos" : "neg"}`}>
                      {r.chgPct !== null && r.chgPct >= 0 ? "+" : ""}
                      {fmtNum(r.chgPct)}%
                    </td>
                    <td className="num">{r.projRvol !== null ? fmtNum(r.projRvol, 1) + "x" : "—"}</td>
                    <td className="num">{fmtVol(r.vol)}</td>
                    <td className="num">
                      {rangePos !== null ? (
                        <span className="live-range" title={`${fmtNum(r.dayLow)} – ${fmtNum(r.dayHigh)}`}>
                          <span className="live-range-track">
                            <span className="live-range-dot" style={{ left: `${Math.round(rangePos * 100)}%` }} />
                          </span>
                        </span>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="num">{fmtNum(r.prevClose)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}
