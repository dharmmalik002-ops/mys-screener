import { useMemo, useState } from "react";
import type { MfOverlap, MfOverlapPair } from "../lib/api";

import "./PortfolioOverlap.css";

/**
 * How much each pair of funds you hold is the same fund in different packaging.
 *
 * This answers a different question from the concentration table below it. That
 * one asks "is any single fund making a big bet"; this one asks "if I hold four
 * funds, how many portfolios do I actually own". A fund can be impeccably
 * diversified on its own and still be a near-copy of the fund beside it, and
 * nothing on either factsheet would ever tell you — only the holder can see
 * both books at once.
 *
 * The number shown is the sum of min(weight in A, weight in B) across shared
 * stocks: the share of a rupee that would end up in identical positions
 * whichever fund it went into. Identical portfolios read 100%, disjoint ones 0%.
 *
 * Nothing here says to drop a fund. Which one to keep depends on exit loads,
 * capital gains and goals this app knows nothing about.
 */

const BAND_LABEL: Record<string, string> = {
  very_high: "near-duplicate",
  high: "heavily overlapping",
  substantial: "substantial overlap",
  modest: "mostly distinct",
  unknown: "not measurable",
};

const rupees = (value: number): string => {
  if (!Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 1e7) return `₹${(value / 1e7).toFixed(2)} cr`;
  if (Math.abs(value) >= 1e5) return `₹${(value / 1e5).toFixed(2)} L`;
  if (Math.abs(value) >= 1000) return `₹${(value / 1000).toFixed(1)}k`;
  return `₹${Math.round(value)}`;
};

/** The overlap bar: a filled proportion, tinted by band. */
function OverlapBar({ pct, band }: { pct: number; band: string }) {
  return (
    <span className="pov-bar" title={`${pct.toFixed(1)}% overlap`}>
      <i className={`pov-bar-fill is-${band}`} style={{ width: `${Math.min(100, Math.max(0, pct))}%` }} />
    </span>
  );
}

function PairDetail({ pair }: { pair: MfOverlapPair }) {
  return (
    <div className="pov-detail">
      <div className="pov-detail-head">
        <span>
          Shared stocks, largest first. <b>Common</b> is the part of each name that is genuinely
          duplicated — the smaller of the two weights.
        </span>
        <span className="pov-detail-counts">
          {pair.left_holdings} and {pair.right_holdings} disclosed holdings · {pair.shared_count} in common
        </span>
      </div>
      <table className="pov-shared">
        <thead>
          <tr>
            <th className="is-left">Stock</th>
            <th>{pair.left_name}</th>
            <th>{pair.right_name}</th>
            <th>Common</th>
          </tr>
        </thead>
        <tbody>
          {pair.shared_top.map((row) => (
            <tr key={row.name}>
              <td className="is-left">{row.name}</td>
              <td>{row.left_pct.toFixed(2)}%</td>
              <td>{row.right_pct.toFixed(2)}%</td>
              <td className="pov-strong">{row.common_pct.toFixed(2)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
      {pair.shared_count > pair.shared_top.length ? (
        <p className="pov-more">
          and {pair.shared_count - pair.shared_top.length} more shared names below these.
        </p>
      ) : null}
      <p className="pov-detail-foot">
        {pair.overlap_pct.toFixed(0)}% of a rupee lands in the same stocks either way. That is{" "}
        {pair.share_of_left.toFixed(0)}% of {pair.left_name}'s disclosed book and{" "}
        {pair.share_of_right.toFixed(0)}% of {pair.right_name}'s
        {pair.same_amc ? " — and both are run by the same fund house." : "."}
      </p>
    </div>
  );
}

export function PortfolioOverlap({
  overlap,
  onOpenFund,
}: {
  overlap: MfOverlap | null;
  onOpenFund: (schemeCode: string) => void;
}) {
  const [expanded, setExpanded] = useState<string | null>(null);
  const [showAll, setShowAll] = useState(false);

  const threshold = overlap?.thresholds?.substantial_pct ?? 30;
  const visible = useMemo(() => {
    const pairs = overlap?.pairs ?? [];
    if (showAll) return pairs;
    // Default to the pairs that clear the bar, but never show an empty table
    // when there is something to look at.
    const notable = pairs.filter((pair) => pair.overlap_pct >= threshold);
    return notable.length ? notable : pairs.slice(0, 3);
  }, [overlap, showAll, threshold]);

  if (!overlap || overlap.funds_compared < 2) {
    return (
      <section className="pfd-panel">
        <header className="pfd-panel-head">
          <div>
            <h3>Fund overlap</h3>
            <p>
              How much each pair of your funds holds the same stocks. Needs at least two funds with
              disclosed portfolios.
            </p>
          </div>
        </header>
        <p className="pov-empty">
          {overlap?.funds_compared === 1
            ? "Only one of your funds has disclosed its holdings, so there is nothing to compare it against yet."
            : "No disclosed holdings to compare yet. Holdings arrive with the monthly portfolio disclosure."}
        </p>
      </section>
    );
  }

  return (
    <section className="pfd-panel">
      <header className="pfd-panel-head">
        <div>
          <h3>Fund overlap</h3>
          <p>
            How much of each pair holds the same stocks, as the share of a rupee that would land in
            identical positions either way. Two funds in the same category always share something —
            the Nifty 50 is only fifty stocks — so the question is whether the second fund is
            bringing its own book. These are measured weights, not a suggestion to drop either fund.
          </p>
        </div>
        <span className={overlap.notable_count ? "pfd-chip is-down" : "pfd-chip is-up"}>
          {overlap.notable_count} of {overlap.pair_count} pairs above {threshold.toFixed(0)}%
        </span>
      </header>

      {overlap.summary?.length ? (
        <ul className="pfd-findings">
          {overlap.summary.map((line) => <li key={line}>{line}</li>)}
        </ul>
      ) : null}

      <div className="pfd-scroll">
        <table className="pfd-table pov-table">
          <thead>
            <tr>
              <th className="is-left">Pair</th>
              <th className="is-left">Overlap</th>
              <th>Shared</th>
              <th>Duplicated</th>
              <th>Combined weight</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {visible.map((pair) => {
              const key = `${pair.left_code}-${pair.right_code}`;
              const open = expanded === key;
              return [
                <tr key={key} className={pair.overlap_pct >= threshold ? "is-flagged" : undefined}>
                  <td className="is-left">
                    <div className="pov-pair">
                      <button type="button" className="pfd-fund" onClick={() => onOpenFund(pair.left_code)}>
                        {pair.left_name}
                      </button>
                      <span className="pov-x">×</span>
                      <button type="button" className="pfd-fund" onClick={() => onOpenFund(pair.right_code)}>
                        {pair.right_name}
                      </button>
                    </div>
                    <span className="pfd-tags">
                      <i>{pair.left_category}</i>
                      {pair.right_category !== pair.left_category ? <i>{pair.right_category}</i> : null}
                      {pair.same_amc ? <i className="pov-tag-amc">same fund house</i> : null}
                    </span>
                  </td>
                  <td className="is-left pov-bar-cell">
                    <OverlapBar pct={pair.overlap_pct} band={pair.band} />
                    <b className={pair.overlap_pct >= threshold ? "is-down" : ""}>
                      {pair.overlap_pct.toFixed(1)}%
                    </b>
                    <em>{BAND_LABEL[pair.band] ?? pair.band}</em>
                  </td>
                  <td className="pfd-muted">{pair.shared_count}</td>
                  <td>{pair.duplicated_value ? rupees(pair.duplicated_value) : "—"}</td>
                  <td>{pair.combined_weight_pct.toFixed(1)}%</td>
                  <td className="pfd-actions">
                    <button type="button" onClick={() => setExpanded(open ? null : key)}>
                      {open ? "Hide" : "Stocks"}
                    </button>
                  </td>
                </tr>,
                open ? (
                  <tr key={`${key}-detail`} className="pov-detail-row">
                    <td colSpan={6}><PairDetail pair={pair} /></td>
                  </tr>
                ) : null,
              ];
            })}
          </tbody>
        </table>
      </div>

      <div className="pov-foot">
        {overlap.pair_count > visible.length || showAll ? (
          <button type="button" className="pov-more-btn" onClick={() => setShowAll((value) => !value)}>
            {showAll ? "Show only overlapping pairs" : `Show all ${overlap.pair_count} pairs`}
          </button>
        ) : <span />}
        <p className="pfd-footnote">
          From each fund's latest disclosed portfolio, matched on stock symbol so the same company
          written two ways still counts.
          {overlap.funds_without_holdings > 0
            ? ` ${overlap.funds_without_holdings} of your funds disclose no holdings and are left out.`
            : ""}
        </p>
      </div>
    </section>
  );
}
