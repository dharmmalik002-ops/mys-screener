import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { ExternalLink, X } from "lucide-react";

import { getChart, getLookalikeSimilar, type ChartBar, type LookalikeRefRow, type LookalikeSimilar } from "../lib/api";
import { fullChartUrl } from "../lib/chartLink";
import { LookalikeChart, type LookalikeSeries } from "./LookalikeChart";

// The modals open from the big chart as well as from the Look-alikes page, and
// that page's stylesheet only loads with it.
import "./LookalikesPanel.css";

/* Pop-up views for the look-alikes.

   Both portal to <body>: main.workspace carries a transform, which makes a
   position:fixed backdrop inside it size to the whole page (gotcha 17). */

function fmtDate(iso?: string | null) {
  if (!iso) return "—";
  const d = new Date(`${iso}T00:00:00`);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
}

const WINDOW = 120;
const SMA_LEN = 50;

function isoFromTime(t: number) {
  // Chart bars carry epoch seconds (lightweight-charts' convention).
  return new Date(t * 1000).toISOString().slice(0, 10);
}

/** The last 120 daily bars as a look-alike series — real candles, the 50-day
    average, volume — normalised the way the model sees a chart. */
export function seriesFromBars(bars: ChartBar[] | null | undefined): LookalikeSeries | null {
  const all = (bars ?? []).filter((b) => Number.isFinite(b?.close) && b.close > 0);
  if (all.length < 30) return null;
  const startIdx = Math.max(0, all.length - WINDOW);
  const win = all.slice(startIdx);
  const sma = win.map((_, k) => {
    const end = startIdx + k;
    const from = Math.max(0, end - SMA_LEN + 1);
    let sum = 0;
    for (let i = from; i <= end; i += 1) sum += all[i].close;
    return sum / (end - from + 1);
  });
  const lo = Math.min(...win.map((b) => b.low), ...sma);
  const hi = Math.max(...win.map((b) => b.high), ...sma);
  const span = hi - lo || 1;
  const vmax = Math.max(...win.map((b) => b.volume || 0)) || 1;
  const n = (x: number) => (x - lo) / span;
  return {
    o: win.map((b) => n(b.open)),
    h: win.map((b) => n(b.high)),
    l: win.map((b) => n(b.low)),
    c: win.map((b) => n(b.close)),
    sma: sma.map(n),
    v: win.map((b) => (b.volume || 0) / vmax),
    setup_index: win.length - 1,
    lo,
    hi,
    dates: { start: isoFromTime(win[0].time), setup: isoFromTime(win[win.length - 1].time), end: isoFromTime(win[win.length - 1].time) },
  };
}

/** A stock's current chart as real candles, fetched from the app's own chart API. */
export function useIndianSeries(symbol: string | null, enabled = true) {
  const [series, setSeries] = useState<LookalikeSeries | null>(null);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    if (!symbol || !enabled) return;
    let cancelled = false;
    setSeries(null);
    setFailed(false);
    getChart(symbol, "1D", "india")
      .then((res) => {
        if (cancelled) return;
        const s = seriesFromBars(res?.bars);
        setSeries(s);
        setFailed(!s);
      })
      .catch(() => !cancelled && setFailed(true));
    return () => {
      cancelled = true;
    };
  }, [symbol, enabled]);
  return { series, failed };
}

function useViewportHeight() {
  const [h, setH] = useState(() => (typeof window === "undefined" ? 800 : window.innerHeight));
  useEffect(() => {
    const on = () => setH(window.innerHeight);
    window.addEventListener("resize", on);
    return () => window.removeEventListener("resize", on);
  }, []);
  return h;
}

export function RefOutcome({ ref: r }: { ref: Pick<LookalikeRefRow, "label" | "max_gain_pct" | "max_loss_pct" | "days_to_result"> }) {
  const text =
    r.label === "worked"
      ? `Worked · +${r.max_gain_pct.toFixed(0)}%${r.days_to_result ? ` in ${r.days_to_result} session${r.days_to_result === 1 ? "" : "s"}` : ""}`
      : r.label === "failed"
        ? `Failed · ${r.max_loss_pct.toFixed(0)}%${r.days_to_result ? ` in ${r.days_to_result} session${r.days_to_result === 1 ? "" : "s"}` : ""}`
        : "Still running";
  return <span className={`lookalike-chip is-${r.label}`}>{text}</span>;
}

function Modal({ title, onClose, children, wide = true }: { title: string; onClose: () => void; children: React.ReactNode; wide?: boolean }) {
  const closeRef = useRef<HTMLButtonElement | null>(null);
  const backdropRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    closeRef.current?.focus();
    // Capture phase, and the event stops here: the big chart behind these
    // dialogs closes on Esc too, and one Esc must close one dialog — the
    // topmost (Compare opens on top of Similar) — not everything at once.
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      e.stopPropagation();
      const all = document.querySelectorAll(".lookalike-modal-backdrop");
      if (all[all.length - 1] === backdropRef.current) onClose();
    };
    window.addEventListener("keydown", onKey, true);
    return () => window.removeEventListener("keydown", onKey, true);
  }, [onClose]);
  return createPortal(
    <div ref={backdropRef} className="lookalike-modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className={`lookalike-modal${wide ? " is-wide" : ""}`} role="dialog" aria-modal="true" aria-label={title}>
        <header className="lookalike-modal-head">
          <h3>{title}</h3>
          <button ref={closeRef} type="button" className="lookalike-refresh" onClick={onClose} aria-label="Close">
            <X size={14} />
          </button>
        </header>
        <div className="lookalike-modal-body">{children}</div>
      </div>
    </div>,
    document.body,
  );
}

/** Indian chart and the reference it resembles, each at its own date, large. */
export function CompareModal({
  symbol,
  chart,
  session,
  reference,
  onClose,
}: {
  symbol: string;
  chart: LookalikeSeries | null | undefined;
  session?: string | null;
  reference: LookalikeRefRow;
  onClose: () => void;
}) {
  // A chart passed in may be closes only (from the daily index); then fetch
  // the stock's real bars so the left side is candles, not dots.
  const hasCandles = !!chart?.o?.length && chart.o.some((v, i) => v !== chart.c[i]);
  const live = useIndianSeries(symbol, !hasCandles);
  const left = hasCandles ? chart : live.series;
  const vh = useViewportHeight();
  const height = Math.max(320, Math.round(vh * 0.72));
  return (
    <Modal title={`${symbol} vs ${reference.name}`} onClose={onClose}>
      <div className="lookalike-compare">
        <figure>
          <figcaption>
            <strong>{symbol}</strong> on {fmtDate(left?.dates?.setup ?? session)}
          </figcaption>
          {left ? (
            <LookalikeChart data={left} height={height} labels ariaLabel={`${symbol} at ${session}`} />
          ) : (
            <div className="lookalike-canvas lookalike-missing" style={{ height }}>
              {live.failed ? "Could not load this chart" : "Loading chart…"}
            </div>
          )}
          <a className="lookalike-link" href={fullChartUrl(symbol)} target="_blank" rel="noreferrer noopener">
            Open {symbol} on my site <ExternalLink size={12} />
          </a>
        </figure>
        <figure>
          <figcaption>
            <strong>{reference.ticker}</strong> on {fmtDate(reference.date)} · {reference.style} example ·{" "}
            <RefOutcome ref={reference} />
          </figcaption>
          <LookalikeChart data={reference.chart} height={height} labels ariaLabel={`${reference.ticker} at ${reference.date}`} />
          <a className="lookalike-link" href={reference.link} target="_blank" rel="noreferrer noopener" title="TradingView cannot open a past date from a link">
            {reference.ticker} on TradingView (opens its latest chart) <ExternalLink size={12} />
          </a>
        </figure>
      </div>
      <p className="lookalike-stat-sub">
        Each chart is shown at the date it looked like this, over the 120 sessions before it. The shaded part after the
        dashed line is what happened next, up to 40 sessions.
      </p>
    </Modal>
  );
}

const COLS_KEY = "mr-malik-lookalike-similar-cols:v1";

function readCols(): number {
  try {
    const v = Number(window.localStorage.getItem(COLS_KEY));
    return v >= 1 && v <= 6 ? v : 1;
  } catch {
    return 1;
  }
}

function chartHeightFor(cols: number, vh: number) {
  if (cols <= 1) return Math.max(320, Math.round(vh * 0.62));
  if (cols === 2) return Math.max(260, Math.round(vh * 0.42));
  if (cols === 3) return 240;
  if (cols === 4) return 190;
  return 160;
}

function PeerCard({ symbol, similarity, height, labels }: { symbol: string; similarity: number; height: number; labels: boolean }) {
  const { series, failed } = useIndianSeries(symbol);
  return (
    <a className="lookalike-similar-card" href={fullChartUrl(symbol)} target="_blank" rel="noreferrer noopener" title="Open on my site in a new tab">
      {series ? (
        <LookalikeChart data={series} height={height} labels={labels} ariaLabel={`${symbol} last 120 sessions`} />
      ) : (
        <div className="lookalike-canvas lookalike-missing" style={{ height }}>
          {failed ? "Chart unavailable" : "Loading…"}
        </div>
      )}
      <span className="lookalike-similar-meta">
        <strong>{symbol}</strong> · {(similarity * 100).toFixed(0)}% alike <ExternalLink size={11} />
      </span>
    </a>
  );
}

/** The big chart's Similar button: which reference setups and which Indian
    stocks look most like this stock's latest chart. */
export function SimilarChartsModal({ symbol, onClose }: { symbol: string; onClose: () => void }) {
  const [data, setData] = useState<LookalikeSimilar | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [compare, setCompare] = useState<LookalikeRefRow | null>(null);
  const [cols, setColsState] = useState<number>(readCols);
  const setCols = (n: number) => {
    setColsState(n);
    try {
      window.localStorage.setItem(COLS_KEY, String(n));
    } catch {
      /* the choice just won't be remembered */
    }
  };
  const vh = useViewportHeight();
  const chartH = chartHeightFor(cols, vh);
  const gridStyle = { gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` };

  useEffect(() => {
    let cancelled = false;
    setData(null);
    setError(null);
    getLookalikeSimilar(symbol)
      .then((res) => !cancelled && setData(res))
      .catch((err) => !cancelled && setError(err instanceof Error ? err.message : "Could not load similar charts."));
    return () => {
      cancelled = true;
    };
  }, [symbol]);

  const ok = data && data.available ? data : null;

  return (
    <Modal title={`Charts similar to ${symbol}`} onClose={onClose}>
      {error ? <p className="lookalike-empty">{error}</p> : null}
      {!data && !error ? <p className="lookalike-empty">Finding similar charts…</p> : null}
      {data && !data.available ? <p className="lookalike-empty">{data.reason}</p> : null}
      {ok ? (
        <>
          <div className="lookalike-toolbar">
            <span className="lookalike-stat-sub" style={{ marginRight: "auto" }}>
              Based on {symbol}'s last 120 sessions up to {fmtDate(ok.session)} · Trend Template {ok.template ?? "—"}/8 ·
              updated each weekday evening.
            </span>
            <span>Charts per row:</span>
            {[1, 2, 3, 4, 5, 6].map((n) => (
              <button
                key={n}
                type="button"
                className={`lookalike-filter${cols === n ? " is-active" : ""}`}
                onClick={() => setCols(n)}
                aria-pressed={cols === n}
              >
                {n}
              </button>
            ))}
          </div>
          {Object.entries(ok.styles ?? {}).map(([style, st]) => (
            <section key={style} className="lookalike-curve-wrap">
              <h3>
                {style.charAt(0).toUpperCase() + style.slice(1)} setups it resembles · looks more like his setups than{" "}
                {st.percentile.toFixed(0)}% of ordinary charts
              </h3>
              <div className="lookalike-similar-grid" style={gridStyle}>
                {st.near.map(([key, sim]) => {
                  const r = ok.refs?.[key];
                  if (!r) return null;
                  return (
                    <button key={key} type="button" className="lookalike-similar-card" onClick={() => setCompare(r)}>
                      <LookalikeChart data={r.chart} height={chartH} labels={cols <= 2} ariaLabel={`${r.ticker} at ${r.date}`} />
                      <span className="lookalike-similar-meta">
                        <strong>{r.ticker}</strong> · {fmtDate(r.date)} · {(sim * 100).toFixed(0)}% alike
                      </span>
                      <RefOutcome ref={r} />
                    </button>
                  );
                })}
              </div>
            </section>
          ))}
          {ok.peers?.length ? (
            <section className="lookalike-curve-wrap">
              <h3>Indian stocks with the most similar charts today</h3>
              <div className="lookalike-similar-grid" style={gridStyle}>
                {ok.peers.map((p) => (
                  <PeerCard key={p.symbol} symbol={p.symbol} similarity={p.similarity} height={chartH} labels={cols <= 2} />
                ))}
              </div>
            </section>
          ) : null}
        </>
      ) : null}
      {compare && ok ? (
        <CompareModal symbol={symbol} chart={null} session={ok.session} reference={compare} onClose={() => setCompare(null)} />
      ) : null}
    </Modal>
  );
}
