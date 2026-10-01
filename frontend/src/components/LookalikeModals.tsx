import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { ExternalLink, X } from "lucide-react";

import { getLookalikeSimilar, type LookalikeRefRow, type LookalikeSimilar } from "../lib/api";
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

export function RefOutcome({ ref: r }: { ref: Pick<LookalikeRefRow, "label" | "max_gain_pct" | "max_loss_pct" | "days_to_result"> }) {
  const text =
    r.label === "worked"
      ? `Worked · +${r.max_gain_pct.toFixed(0)}%${r.days_to_result ? ` in ${r.days_to_result} session${r.days_to_result === 1 ? "" : "s"}` : ""}`
      : r.label === "failed"
        ? `Failed · ${r.max_loss_pct.toFixed(0)}%${r.days_to_result ? ` in ${r.days_to_result} session${r.days_to_result === 1 ? "" : "s"}` : ""}`
        : "Still running";
  return <span className={`lookalike-chip is-${r.label}`}>{text}</span>;
}

function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: React.ReactNode }) {
  const closeRef = useRef<HTMLButtonElement | null>(null);
  useEffect(() => {
    closeRef.current?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  return createPortal(
    <div className="lookalike-modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="lookalike-modal" role="dialog" aria-modal="true" aria-label={title}>
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
  return (
    <Modal title={`${symbol} vs ${reference.name}`} onClose={onClose}>
      <div className="lookalike-compare">
        <figure>
          <figcaption>
            <strong>{symbol}</strong> on {fmtDate(chart?.dates?.setup ?? session)}
          </figcaption>
          <LookalikeChart data={chart} height={320} labels ariaLabel={`${symbol} at ${session}`} />
          <a className="lookalike-link" href={fullChartUrl(symbol)} target="_blank" rel="noreferrer noopener">
            Open {symbol} on my site <ExternalLink size={12} />
          </a>
        </figure>
        <figure>
          <figcaption>
            <strong>{reference.ticker}</strong> on {fmtDate(reference.date)} · {reference.style} example ·{" "}
            <RefOutcome ref={reference} />
          </figcaption>
          <LookalikeChart data={reference.chart} height={320} labels ariaLabel={`${reference.ticker} at ${reference.date}`} />
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

function Sparkline({ closes, ariaLabel }: { closes: number[] | null | undefined; ariaLabel: string }) {
  const ref = useRef<HTMLCanvasElement | null>(null);
  useEffect(() => {
    const canvas = ref.current;
    if (!canvas || !closes?.length) return;
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth || 200;
    const h = canvas.clientHeight || 70;
    canvas.width = Math.round(w * dpr);
    canvas.height = Math.round(h * dpr);
    const g = canvas.getContext("2d");
    if (!g) return;
    g.setTransform(dpr, 0, 0, dpr, 0, 0);
    g.clearRect(0, 0, w, h);
    const lo = Math.min(...closes);
    const hi = Math.max(...closes);
    const span = hi - lo || 1;
    g.strokeStyle = "#3d6fd6";
    g.lineWidth = 1.5;
    g.beginPath();
    closes.forEach((c, i) => {
      const x = (i / (closes.length - 1)) * (w - 2) + 1;
      const y = 3 + (1 - (c - lo) / span) * (h - 6);
      if (i === 0) g.moveTo(x, y);
      else g.lineTo(x, y);
    });
    g.stroke();
  }, [closes]);
  return <canvas ref={ref} className="lookalike-spark" role="img" aria-label={ariaLabel} />;
}

/** The big chart's Similar button: which reference setups and which Indian
    stocks look most like this stock's latest chart. */
export function SimilarChartsModal({ symbol, onClose }: { symbol: string; onClose: () => void }) {
  const [data, setData] = useState<LookalikeSimilar | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [compare, setCompare] = useState<LookalikeRefRow | null>(null);

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
  const ownSeries: LookalikeSeries | null = ok?.closes
    ? { o: ok.closes.map((c) => c / 999), h: ok.closes.map((c) => c / 999), l: ok.closes.map((c) => c / 999), c: ok.closes.map((c) => c / 999) }
    : null;

  return (
    <Modal title={`Charts similar to ${symbol}`} onClose={onClose}>
      {error ? <p className="lookalike-empty">{error}</p> : null}
      {!data && !error ? <p className="lookalike-empty">Finding similar charts…</p> : null}
      {data && !data.available ? <p className="lookalike-empty">{data.reason}</p> : null}
      {ok ? (
        <>
          <p className="lookalike-stat-sub">
            Based on {symbol}'s last 120 sessions up to {fmtDate(ok.session)} · Trend Template {ok.template ?? "—"}/8 · updated
            each weekday evening.
          </p>
          {Object.entries(ok.styles ?? {}).map(([style, st]) => (
            <section key={style} className="lookalike-curve-wrap">
              <h3>
                {style.charAt(0).toUpperCase() + style.slice(1)} setups it resembles · looks more like his setups than{" "}
                {st.percentile.toFixed(0)}% of ordinary charts
              </h3>
              <div className="lookalike-similar-grid">
                {st.near.map(([key, sim]) => {
                  const r = ok.refs?.[key];
                  if (!r) return null;
                  return (
                    <button key={key} type="button" className="lookalike-similar-card" onClick={() => setCompare(r)}>
                      <LookalikeChart data={r.chart} height={130} ariaLabel={`${r.ticker} at ${r.date}`} />
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
              <div className="lookalike-similar-grid">
                {ok.peers.map((p) => (
                  <a
                    key={p.symbol}
                    className="lookalike-similar-card"
                    href={fullChartUrl(p.symbol)}
                    target="_blank"
                    rel="noreferrer noopener"
                  >
                    <Sparkline closes={p.closes} ariaLabel={`${p.symbol} last 120 sessions`} />
                    <span className="lookalike-similar-meta">
                      <strong>{p.symbol}</strong> · {(p.similarity * 100).toFixed(0)}% alike <ExternalLink size={11} />
                    </span>
                  </a>
                ))}
              </div>
            </section>
          ) : null}
        </>
      ) : null}
      {compare && ok ? (
        <CompareModal symbol={symbol} chart={ownSeries} session={ok.session} reference={compare} onClose={() => setCompare(null)} />
      ) : null}
    </Modal>
  );
}
