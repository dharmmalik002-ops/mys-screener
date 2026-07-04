import { useEffect, useMemo, useState } from "react";
import { Area, AreaChart, ResponsiveContainer, Tooltip, YAxis } from "recharts";

import { getChartGridSeries, type ChartBar, type MarketKey } from "../lib/api";

import "./SymbolGridModal.css";

export type SymbolGridItem = {
  symbol: string;
  name?: string;
  note?: string;
  badge?: string;
  badgeTone?: "pos" | "neg" | "muted";
};

type Props = {
  title: string;
  subtitle?: string;
  items: SymbolGridItem[];
  market: MarketKey;
  onOpenSymbolChart?: (symbol: string) => void;
  onClose: () => void;
};

const TIMEFRAMES = ["3M", "6M", "1Y"] as const;
type TF = (typeof TIMEFRAMES)[number];

export function SymbolGridModal({ title, subtitle, items, market, onOpenSymbolChart, onClose }: Props) {
  const [timeframe, setTimeframe] = useState<TF>("6M");
  const [series, setSeries] = useState<Record<string, ChartBar[]>>({});
  const [loading, setLoading] = useState(false);

  const symbols = useMemo(() => items.map((i) => i.symbol), [items]);
  const symbolsKey = symbols.join(",");

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handler);
    document.body.style.overflow = "hidden";
    return () => {
      window.removeEventListener("keydown", handler);
      document.body.style.overflow = "";
    };
  }, [onClose]);

  useEffect(() => {
    if (!symbols.length) return;
    let active = true;
    setSeries({});
    setLoading(true);
    // Small chunks so each request stays well under the client timeout on the
    // free-tier backend, and charts fill in PROGRESSIVELY as each chunk lands
    // (a big leader list would otherwise risk one slow request aborting).
    const chunks: string[][] = [];
    for (let i = 0; i < symbols.length; i += 10) chunks.push(symbols.slice(i, i + 10));
    let done = 0;
    Promise.all(
      chunks.map((c) =>
        getChartGridSeries(c, timeframe, market)
          .then((r) => {
            if (!active) return;
            setSeries((prev) => {
              const next = { ...prev };
              for (const item of (r as { items?: Array<{ symbol: string; bars: ChartBar[] }> }).items ?? []) {
                next[item.symbol] = item.bars;
              }
              return next;
            });
          })
          .catch(() => { /* leave this chunk blank; others still render */ })
          .finally(() => {
            done += 1;
            if (active && done >= chunks.length) setLoading(false);
          }),
      ),
    );
    return () => {
      active = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [symbolsKey, timeframe, market]);

  return (
    <div className="sgm-overlay" onClick={onClose}>
      <div className="sgm-modal" onClick={(e) => e.stopPropagation()}>
        <div className="sgm-head">
          <div>
            <h2>{title}</h2>
            {subtitle ? <p>{subtitle}</p> : null}
          </div>
          <div className="sgm-controls">
            <div className="sgm-tf">
              {TIMEFRAMES.map((tf) => (
                <button key={tf} type="button" className={tf === timeframe ? "active" : ""} onClick={() => setTimeframe(tf)}>
                  {tf}
                </button>
              ))}
            </div>
            <button type="button" className="sgm-close" onClick={onClose} aria-label="Close">×</button>
          </div>
        </div>
        {loading && Object.keys(series).length === 0 ? <div className="sgm-loading">Loading {symbols.length} charts…</div> : null}
        <div className="sgm-grid">
          {items.map((item) => {
            const bars = series[item.symbol] ?? [];
            const data = bars.map((b) => ({ t: b.time, c: b.close }));
            const first = data[0]?.c;
            const last = data[data.length - 1]?.c;
            const up = first !== undefined && last !== undefined ? last >= first : true;
            const color = up ? "#089981" : "#f23645";
            return (
              <button
                key={item.symbol}
                type="button"
                className="sgm-cell"
                onClick={() => onOpenSymbolChart?.(item.symbol)}
              >
                <div className="sgm-cell-head">
                  <strong>{item.symbol}</strong>
                  {item.badge ? <span className={`sgm-badge ${item.badgeTone ?? "muted"}`}>{item.badge}</span> : null}
                </div>
                <div className="sgm-chart">
                  {data.length > 1 ? (
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart data={data} margin={{ top: 2, right: 0, bottom: 0, left: 0 }}>
                        <defs>
                          <linearGradient id={`g-${item.symbol}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor={color} stopOpacity={0.28} />
                            <stop offset="100%" stopColor={color} stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <YAxis domain={["dataMin", "dataMax"]} hide />
                        <Tooltip
                          contentStyle={{ fontSize: 11, padding: "2px 6px" }}
                          labelFormatter={() => ""}
                          formatter={(v) => [Number(v).toFixed(2), "Close"]}
                        />
                        <Area type="monotone" dataKey="c" stroke={color} strokeWidth={1.6} fill={`url(#g-${item.symbol})`} />
                      </AreaChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="sgm-nochart">{loading ? "…" : "no data"}</div>
                  )}
                </div>
                {item.note ? <div className="sgm-note">{item.note}</div> : null}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
