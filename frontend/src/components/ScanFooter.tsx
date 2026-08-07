import { useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  Clock,
  Database,
  Gauge,
  TrendingUp,
  TrendingDown,
} from "lucide-react";

import type { ScanMatch } from "../lib/api";

import "./ScanFooter.css";

type ScanFooterProps = {
  /** Whether a scan is currently running (used to measure execution time). */
  loading: boolean;
  /** Visible items for top gainer/loser widgets and result count. */
  items: ScanMatch[];
  /** ISO string from ScanResultsResponse.generated_at. */
  generatedAt: string | null;
  /** Open a symbol's chart. These rows looked identical to the clickable
      rows in the results table above but did nothing. */
  onPickSymbol?: (symbol: string) => void;
};

/* ---------- Logo helpers (mirrors GroupsPanel / ScanTable) ---------- */
const LOGO_MAP: Record<string, string> = {
  RELIANCE: "reliance-industries",
  TCS: "tata-consultancy-services",
  HDFCBANK: "hdfc-bank",
  INFY: "infosys",
  ICICIBANK: "icici-bank",
  SBIN: "state-bank-of-india",
  BHARTIARTL: "bharti-airtel",
  LICI: "lic-of-india",
  ITC: "itc",
  HINDUNILVR: "hindustan-unilever",
  LT: "larsen-and-toubro",
  BAJFINANCE: "bajaj-finance",
  MARUTI: "maruti-suzuki",
  ASIANPAINT: "asian-paints",
  AXISBANK: "axis-bank",
  ADANIENT: "adani-enterprises",
  SUNPHARMA: "sun-pharma",
  TITAN: "titan",
  ULTRACEMCO: "ultratech-cement",
  WIPRO: "wipro",
  NTPC: "ntpc",
  ONGC: "ongc",
  JSWSTEEL: "jsw-steel",
  "M&M": "mahindra-and-mahindra",
  POWERGRID: "power-grid",
  HCLTECH: "hcl-technologies",
  KOTAKBANK: "kotak-mahindra-bank",
  COALINDIA: "coal-india",
  ADANIPORTS: "adani-ports",
  TATASTEEL: "tata-steel",
};

function getLogoUrl(symbol: string): string | null {
  const id = LOGO_MAP[symbol.replace("^", "").toUpperCase()];
  return id ? `https://s3-symbol-logo.tradingview.com/${id}.svg` : null;
}

function initials(symbol: string): string {
  return symbol.slice(0, 2).toUpperCase();
}

/* ---------- Helpers ---------- */
function formatRelativeTime(iso: string | null): string {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return iso;
  const diffMs = Date.now() - t;
  const sec = Math.max(0, Math.floor(diffMs / 1000));
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} min ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr} hr ago`;
  const day = Math.floor(hr / 24);
  return `${day}d ago`;
}

function formatExecutionTime(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

/* ---------- Mini horizontal bar (relative-to-max magnitude) ---------- */
function MiniBar({
  pct,
  maxAbs,
  positive,
}: {
  pct: number;
  maxAbs: number;
  positive: boolean;
}) {
  const ratio = maxAbs > 0 ? Math.min(1, Math.abs(pct) / maxAbs) : 0;
  return (
    <span className="sf-bar-track" aria-hidden>
      <span
        className={`sf-bar-fill ${positive ? "sf-bar-pos" : "sf-bar-neg"}`}
        style={{ width: `${ratio * 100}%` }}
      />
    </span>
  );
}

/* ---------- Widget row ---------- */
function MoverRow({
  item,
  maxAbs,
  positive,
  onPick,
}: {
  item: ScanMatch;
  maxAbs: number;
  positive: boolean;
  onPick?: (symbol: string) => void;
}) {
  const logo = getLogoUrl(item.symbol);
  return (
    <li className={onPick ? "sf-mover is-clickable" : "sf-mover"}>
      {onPick ? (
        <button
          type="button"
          className="sf-mover-hit"
          onClick={() => onPick(item.symbol)}
          title={`Open ${item.symbol} chart`}
        />
      ) : null}
      <span className="sf-mover-logo">
        {logo ? (
          <img
            src={logo}
            alt=""
            onError={(e) => {
              e.currentTarget.style.display = "none";
            }}
          />
        ) : (
          <span className="sf-mover-fallback">{initials(item.symbol)}</span>
        )}
      </span>
      <span className="sf-mover-text">
        <strong>{item.symbol}</strong>
        <small>{item.name}</small>
      </span>
      <span className="sf-mover-trend">
        <MiniBar pct={item.change_pct} maxAbs={maxAbs} positive={positive} />
      </span>
      <span className={`sf-mover-pct ${positive ? "sf-pos" : "sf-neg"}`}>
        {item.change_pct >= 0 ? "+" : ""}
        {item.change_pct.toFixed(2)}%
      </span>
    </li>
  );
}

/* ---------- Main component ---------- */
export function ScanFooter({ loading, items, generatedAt, onPickSymbol }: ScanFooterProps) {
  /* Execution time measurement — flips on transition true → false */
  const startRef = useRef<number | null>(null);
  const [executionMs, setExecutionMs] = useState<number | null>(null);

  useEffect(() => {
    if (loading) {
      startRef.current = performance.now();
    } else if (startRef.current !== null) {
      const elapsed = Math.round(performance.now() - startRef.current);
      setExecutionMs(elapsed);
      startRef.current = null;
    }
  }, [loading]);

  /* Re-render every 30s so "Last Refreshed" stays fresh */
  const [, forceTick] = useState(0);
  useEffect(() => {
    const id = window.setInterval(() => forceTick((c) => c + 1), 30_000);
    return () => window.clearInterval(id);
  }, []);

  /* Confidence proxy — % of results with valid RS rating */
  const confidence = useMemo(() => {
    if (items.length === 0) return null;
    const withRs = items.filter(
      (m) => m.rs_rating !== null && m.rs_rating !== undefined && Number.isFinite(m.rs_rating),
    ).length;
    return (withRs / items.length) * 100;
  }, [items]);

  /* Top gainers / losers (top 5 each) */
  const { gainers, losers, gainersMax, losersMax } = useMemo(() => {
    if (items.length === 0) {
      return { gainers: [], losers: [], gainersMax: 0, losersMax: 0 };
    }
    const sortedDesc = [...items].sort((a, b) => b.change_pct - a.change_pct);
    const sortedAsc = [...items].sort((a, b) => a.change_pct - b.change_pct);
    const g = sortedDesc.slice(0, 5).filter((m) => Number.isFinite(m.change_pct));
    const l = sortedAsc.slice(0, 5).filter((m) => Number.isFinite(m.change_pct));
    return {
      gainers: g,
      losers: l,
      gainersMax: g.reduce((m, item) => Math.max(m, Math.abs(item.change_pct)), 0),
      losersMax: l.reduce((m, item) => Math.max(m, Math.abs(item.change_pct)), 0),
    };
  }, [items]);

  return (
    <div className="sf-root">
      {/* ===== Scan Performance ===== */}
      <section className="sf-card sf-card-perf">
        <header className="sf-card-head">
          <span className="sf-card-icon sf-icon-indigo">
            <Gauge size={14} strokeWidth={2.4} />
          </span>
          <strong>Scan Performance</strong>
          <small className="sf-card-sub">Live diagnostics</small>
        </header>

        <div className="sf-stats">
          <div className="sf-stat">
            <span className="sf-stat-icon sf-icon-indigo">
              <Clock size={13} strokeWidth={2.2} />
            </span>
            <span className="sf-stat-text">
              <small>Execution Time</small>
              <strong>{loading ? "Running…" : formatExecutionTime(executionMs)}</strong>
            </span>
          </div>

          <div className="sf-stat">
            <span className="sf-stat-icon sf-icon-violet">
              <Activity size={13} strokeWidth={2.2} />
            </span>
            <span className="sf-stat-text">
              <small>Last Refreshed</small>
              <strong>{formatRelativeTime(generatedAt)}</strong>
            </span>
          </div>

          <div className="sf-stat">
            <span className="sf-stat-icon sf-icon-pink">
              <Database size={13} strokeWidth={2.2} />
            </span>
            <span className="sf-stat-text">
              <small>Results Count</small>
              <strong>{items.length.toLocaleString("en-IN")}</strong>
            </span>
          </div>

          <div className="sf-stat">
            <span className="sf-stat-icon sf-icon-emerald">
              <Gauge size={13} strokeWidth={2.2} />
            </span>
            <span
              className="sf-stat-text"
              title="% of results with a verified RS rating — proxy for the scanner's signal accuracy."
            >
              <small>Accuracy</small>
              <strong>
                {confidence === null ? "—" : `${confidence.toFixed(0)}%`}
              </strong>
            </span>
          </div>
        </div>
      </section>

      {/* ===== Top Gainers ===== */}
      <section className="sf-card sf-card-mover">
        <header className="sf-card-head">
          <span className="sf-card-icon sf-icon-emerald">
            <TrendingUp size={14} strokeWidth={2.4} />
          </span>
          <strong>Top Gainers</strong>
          <small className="sf-card-sub">in this scan</small>
        </header>
        {gainers.length === 0 ? (
          <div className="sf-empty">No gainers yet — run a scan to populate.</div>
        ) : (
          <ul className="sf-mover-list">
            {gainers.map((item) => (
              <MoverRow
                key={`g-${item.scan_id}-${item.symbol}`}
                item={item}
                maxAbs={gainersMax}
                positive
                onPick={onPickSymbol}
              />
            ))}
          </ul>
        )}
      </section>

      {/* ===== Top Losers ===== */}
      <section className="sf-card sf-card-mover">
        <header className="sf-card-head">
          <span className="sf-card-icon sf-icon-red">
            <TrendingDown size={14} strokeWidth={2.4} />
          </span>
          <strong>Top Losers</strong>
          <small className="sf-card-sub">in this scan</small>
        </header>
        {losers.length === 0 ? (
          <div className="sf-empty">No losers yet — run a scan to populate.</div>
        ) : (
          <ul className="sf-mover-list">
            {losers.map((item) => (
              <MoverRow
                key={`l-${item.scan_id}-${item.symbol}`}
                item={item}
                maxAbs={losersMax}
                positive={false}
                onPick={onPickSymbol}
              />
            ))}
          </ul>
        )}
      </section>
    </div>
  );
}
