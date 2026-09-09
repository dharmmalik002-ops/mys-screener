import { useMemo, useState } from "react";
import { createPortal } from "react-dom";

import type { IndustryGroupStockItem, MarketKey } from "../lib/api";
import { useModalShell } from "../lib/useModalShell";

import "./GroupStocksModal.css";

export type GroupStocksMember = IndustryGroupStockItem & { group_member_rank: number };

export type GroupStocksContext = {
  /** Group id, or `__sector__<Sector>` when a whole sector was opened. */
  id: string;
  kind: "group" | "sector";
  title: string;
  subtitle: string;
  description: string;
  rankLabel: string | null;
  strengthBucket: string | null;
  trendLabel: string | null;
  symbols: string[];
  members: GroupStocksMember[];
};

type SortKey = "rank" | "symbol" | "change_pct" | "return_1m" | "return_3m" | "return_6m" | "rs_rating";

type Props = {
  market: MarketKey;
  context: GroupStocksContext;
  selectedSymbol: string | null;
  onClose: () => void;
  onSelectSymbol: (symbol: string) => void;
  onAddToWatchlist: (symbol: string) => void;
  /** The full chart panel, rendered in the right pane. */
  chart: React.ReactNode;
};

function pct(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

function tone(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "";
  return value >= 0 ? " up" : " down";
}

function price(value: number | null | undefined) {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  return `₹${value.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

export function GroupStocksModal({
  market,
  context,
  selectedSymbol,
  onClose,
  onSelectSymbol,
  onAddToWatchlist,
  chart,
}: Props) {
  void market;
  const shellRef = useModalShell(true, onClose);
  const [sort, setSort] = useState<{ key: SortKey; desc: boolean }>({ key: "rank", desc: false });
  const [query, setQuery] = useState("");

  const rows = useMemo(() => {
    const needle = query.trim().toUpperCase();
    const filtered = needle
      ? context.members.filter(
          (m) =>
            m.symbol.toUpperCase().includes(needle) ||
            (m.company_name ?? "").toUpperCase().includes(needle),
        )
      : context.members;
    const dir = sort.desc ? -1 : 1;
    const value = (m: GroupStocksMember): string | number => {
      switch (sort.key) {
        case "rank": return m.group_member_rank;
        case "symbol": return m.symbol;
        case "rs_rating": return m.rs_rating ?? -1;
        default: return (m[sort.key] as number | null) ?? Number.NEGATIVE_INFINITY;
      }
    };
    return [...filtered].sort((a, b) => {
      const va = value(a);
      const vb = value(b);
      if (typeof va === "string" || typeof vb === "string") return String(va).localeCompare(String(vb)) * dir;
      return (va - vb) * dir;
    });
  }, [context.members, sort, query]);

  const breadth = useMemo(() => {
    let up = 0;
    let counted = 0;
    for (const m of context.members) {
      if (typeof m.change_pct !== "number" || !Number.isFinite(m.change_pct)) continue;
      counted += 1;
      if (m.change_pct > 0) up += 1;
    }
    return counted ? Math.round((up / counted) * 100) : null;
  }, [context.members]);

  const header = (key: SortKey, label: string, numeric = true) => (
    <th
      className={`${numeric ? "num" : ""}${sort.key === key ? " sorted" : ""}`}
      onClick={() => setSort((prev) => (prev.key === key ? { key, desc: !prev.desc } : { key, desc: key !== "rank" && key !== "symbol" }))}
      aria-sort={sort.key === key ? (sort.desc ? "descending" : "ascending") : "none"}
    >
      {label}
      {sort.key === key ? <span className="gsm-sort">{sort.desc ? "▾" : "▴"}</span> : null}
    </th>
  );

  /**
   * Portalled to <body> on purpose: `main.workspace` carries a transform, which
   * makes it the containing block for `position: fixed`, so a backdrop rendered
   * in the tree sized itself to the whole scrollable page (1445px tall in a
   * 1200px window) and the dialog hung off the bottom of the screen.
   */
  return createPortal(
    <div className="gsm-backdrop" onClick={onClose}>
      <div
        ref={shellRef as React.RefObject<HTMLDivElement>}
        className="gsm-shell"
        role="dialog"
        aria-modal="true"
        aria-label={`${context.title} constituents`}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="gsm-head">
          <div className="gsm-head-text">
            <p className="gsm-eyebrow">
              {context.subtitle}
              {context.kind === "sector" ? <span className="gsm-chip">Sector</span> : null}
            </p>
            <h3>{context.title}</h3>
            {context.description ? <p className="gsm-desc">{context.description}</p> : null}
          </div>
          <div className="gsm-head-meta">
            {context.rankLabel ? (
              <span className="gsm-stat"><small>Rank</small><strong>{context.rankLabel}</strong></span>
            ) : null}
            <span className="gsm-stat"><small>Stocks</small><strong>{context.members.length}</strong></span>
            {breadth !== null ? (
              <span className="gsm-stat"><small>Up today</small><strong>{breadth}%</strong></span>
            ) : null}
            {context.strengthBucket ? (
              <span className="gsm-stat"><small>Strength</small><strong>{context.strengthBucket}</strong></span>
            ) : null}
            {context.trendLabel ? (
              <span className="gsm-stat"><small>Trend</small><strong>{context.trendLabel}</strong></span>
            ) : null}
          </div>
          <button type="button" className="gsm-close" onClick={onClose} aria-label="Close">
            ✕
          </button>
        </header>

        <div className="gsm-body">
          <div className="gsm-list">
            <div className="gsm-list-head">
              <input
                type="search"
                className="gsm-search"
                placeholder="Filter constituents…"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                aria-label="Filter constituents"
              />
              <span className="gsm-count">{rows.length}</span>
            </div>
            <div className="gsm-table-wrap">
              <table className="gsm-table">
                <thead>
                  <tr>
                    {header("rank", "#")}
                    {header("symbol", "Stock", false)}
                    <th className="num">Price</th>
                    {header("change_pct", "1D")}
                    {header("return_1m", "1M")}
                    {header("return_3m", "3M")}
                    {header("return_6m", "6M")}
                    {header("rs_rating", "RS")}
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {rows.map((m) => {
                    const active = selectedSymbol?.toUpperCase() === m.symbol.toUpperCase();
                    return (
                      <tr
                        key={m.symbol}
                        className={active ? "is-active" : ""}
                        onClick={() => onSelectSymbol(m.symbol)}
                      >
                        <td className="num gsm-rank">{m.group_member_rank}</td>
                        <td>
                          <strong>{m.symbol}</strong>
                          <small>{m.company_name}</small>
                        </td>
                        <td className="num">{price(m.last_price)}</td>
                        <td className={`num${tone(m.change_pct)}`}>{pct(m.change_pct)}</td>
                        <td className={`num${tone(m.return_1m)}`}>{pct(m.return_1m)}</td>
                        <td className={`num${tone(m.return_3m)}`}>{pct(m.return_3m)}</td>
                        <td className={`num${tone(m.return_6m)}`}>{pct(m.return_6m)}</td>
                        <td className="num">{m.rs_rating ?? "--"}</td>
                        <td className="gsm-actions">
                          <button
                            type="button"
                            className="gsm-add"
                            onClick={(e) => { e.stopPropagation(); onAddToWatchlist(m.symbol); }}
                            title={`Add ${m.symbol} to a watchlist`}
                          >
                            +
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                  {!rows.length ? (
                    <tr><td colSpan={9} className="gsm-empty">No constituent matches “{query.trim()}”.</td></tr>
                  ) : null}
                </tbody>
              </table>
            </div>
          </div>

          <div className="gsm-chart">{chart}</div>
        </div>
      </div>
    </div>,
    document.body,
  );
}

export default GroupStocksModal;
