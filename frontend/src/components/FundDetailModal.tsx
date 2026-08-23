import { useCallback, useEffect, useMemo, useState } from "react";
import { ArrowUpRight, X } from "lucide-react";
import {
  getMfFund,
  getMfFundAiReview,
  getMfFundReview,
  getMfFundSeries,
  type MfAiReview,
  type MfFundResponse,
  type MfReview,
  type MfSeriesResponse,
} from "../lib/api";
import { FundNavChart, type CandlePeriod, type FundNavChartMode } from "./FundNavChart";

import "./FundDetailModal.css";

/**
 * Everything known about one fund.
 *
 * Ordered by what actually decides whether to hold a fund: where it ranks in
 * its category, how it did against its benchmark, how consistent that record
 * is (rolling, not point-to-point), what it costs, what it holds, and who runs
 * it. Point-to-point returns come first only because that is the number people
 * look for — the rolling table underneath is the more honest one and says so.
 */

const RANGE_LABELS: Record<string, string> = {
  "1m": "1M", "3m": "3M", "6m": "6M", "1y": "1Y",
  "2y": "2Y", "3y": "3Y", "5y": "5Y", "10y": "10Y", max: "Max",
};

const RETURN_COLUMNS: { key: string; label: string }[] = [
  { key: "return_1m", label: "1M" },
  { key: "return_3m", label: "3M" },
  { key: "return_6m", label: "6M" },
  { key: "return_1y", label: "1Y" },
  { key: "return_3y", label: "3Y" },
  { key: "return_5y", label: "5Y" },
  { key: "return_10y", label: "10Y" },
];

const ASSET_LABELS: Record<string, string> = {
  equity: "Indian equity",
  international_equity: "International equity",
  debt: "Debt",
  cash: "Cash & equivalents",
  derivatives: "Derivatives",
  mutual_fund: "Other funds",
  reit: "REITs / InvITs",
  commodity: "Commodities",
  other: "Other",
};

const CAP_LABELS: Record<string, string> = { large: "Large cap", mid: "Mid cap", small: "Small cap" };

const num = (value: unknown, digits = 2): string =>
  typeof value === "number" && Number.isFinite(value)
    ? value.toLocaleString("en-IN", { minimumFractionDigits: digits, maximumFractionDigits: digits })
    : "—";

const pct = (value: unknown, digits = 2): string =>
  typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(digits)}%` : "—";

const signedPct = (value: unknown, digits = 2): string =>
  typeof value === "number" && Number.isFinite(value)
    ? `${value >= 0 ? "+" : ""}${value.toFixed(digits)}%`
    : "—";

const crore = (value: unknown): string => {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  if (value >= 100000) return `₹${(value / 100000).toFixed(2)} lakh cr`;
  return `₹${value.toLocaleString("en-IN", { maximumFractionDigits: 0 })} cr`;
};

const rupees = (value: unknown): string =>
  typeof value === "number" && Number.isFinite(value)
    ? `₹${value.toLocaleString("en-IN", { maximumFractionDigits: 0 })}`
    : "—";

const toneClass = (value: unknown): string =>
  typeof value === "number" && Number.isFinite(value) ? (value < 0 ? "neg" : "pos") : "";

/** "1 of 32" reads better than "1/32" when the denominator is the point. */
function RankBadge({ rank, count, quartile }: { rank?: unknown; count?: unknown; quartile?: unknown }) {
  if (typeof rank !== "number" || typeof count !== "number" || count < 2) {
    return <span className="fdm-rank fdm-rank-none">Not ranked</span>;
  }
  const q = typeof quartile === "number" ? quartile : Math.min(4, Math.floor(((rank - 1) / count) * 4) + 1);
  return (
    <span className={`fdm-rank fdm-rank-q${q}`}>
      <strong>#{rank}</strong> of {count}
      <em>Q{q}</em>
    </span>
  );
}

export function FundDetailModal({
  schemeCode,
  onClose,
  onOpenSymbolChart,
  onTogglePortfolio,
  inPortfolio,
}: {
  schemeCode: string;
  onClose: () => void;
  onOpenSymbolChart?: (symbol: string) => void;
  onTogglePortfolio?: (schemeCode: string) => void;
  inPortfolio?: boolean;
}) {
  const [data, setData] = useState<MfFundResponse | null>(null);
  const [series, setSeries] = useState<MfSeriesResponse | null>(null);
  const [range, setRange] = useState("3y");
  const [chartMode, setChartMode] = useState<FundNavChartMode>("growth");
  const [candlePeriod, setCandlePeriod] = useState<CandlePeriod>("weekly");
  const [showDrawdown, setShowDrawdown] = useState(false);
  const [tab, setTab] = useState<"performance" | "review" | "portfolio" | "peers" | "about">("performance");
  const [review, setReview] = useState<MfReview | null>(null);
  const [aiReview, setAiReview] = useState<MfAiReview | null>(null);
  const [aiLoading, setAiLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [loadingSeries, setLoadingSeries] = useState(false);
  const [holdingFilter, setHoldingFilter] = useState("");

  useEffect(() => {
    let cancelled = false;
    setData(null);
    setError(null);
    getMfFund(schemeCode)
      .then((payload) => { if (!cancelled) setData(payload); })
      .catch((err: unknown) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Could not load this fund.");
      });
    return () => { cancelled = true; };
  }, [schemeCode]);

  useEffect(() => {
    let cancelled = false;
    setLoadingSeries(true);
    getMfFundSeries(schemeCode, { range, drawdown: showDrawdown })
      .then((payload) => { if (!cancelled) setSeries(payload); })
      .catch(() => { if (!cancelled) setSeries(null); })
      .finally(() => { if (!cancelled) setLoadingSeries(false); });
    return () => { cancelled = true; };
  }, [schemeCode, range, showDrawdown]);

  // The measured review is cheap and deterministic — fetch it with the fund.
  // The AI prose is a separate, explicit action so a page open never waits on
  // Gemini and the numbers are never gated behind it.
  useEffect(() => {
    let cancelled = false;
    setReview(null);
    setAiReview(null);
    getMfFundReview(schemeCode)
      .then((payload) => { if (!cancelled) setReview(payload); })
      .catch(() => { if (!cancelled) setReview(null); });
    return () => { cancelled = true; };
  }, [schemeCode]);

  const loadAiReview = useCallback(() => {
    setAiLoading(true);
    getMfFundAiReview(schemeCode)
      .then(setAiReview)
      .catch((err: unknown) => setAiReview({
        available: false,
        reason: err instanceof Error ? err.message : "Could not generate the summary.",
      }))
      .finally(() => setAiLoading(false));
  }, [schemeCode]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => { if (event.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const fund = data?.fund;
  const detail = data?.detail;
  const categorySummary = data?.category_summary ?? null;
  const benchmarkLine = series?.benchmark ?? null;

  const holdings = useMemo(() => {
    const rows = detail?.holdings ?? [];
    const needle = holdingFilter.trim().toLowerCase();
    if (!needle) return rows;
    return rows.filter((row) =>
      `${row.name} ${row.sector ?? ""} ${row.symbol ?? ""}`.toLowerCase().includes(needle));
  }, [detail?.holdings, holdingFilter]);

  const peers = data?.category_peers ?? [];
  const selfIndex = peers.findIndex((peer) => peer.is_self);

  const handleBackdrop = useCallback((event: React.MouseEvent) => {
    if (event.target === event.currentTarget) onClose();
  }, [onClose]);

  return (
    <div className="fdm-backdrop" onClick={handleBackdrop} role="presentation">
      <div className="fdm-shell" role="dialog" aria-modal="true" aria-label={fund?.name ?? "Fund detail"}>
        <header className="fdm-head">
          <div className="fdm-head-main">
            <h2>{fund?.name ?? "Loading…"}</h2>
            <div className="fdm-head-meta">
              {fund?.amc ? <span>{fund.amc}</span> : null}
              {fund?.sub_category ? <span className="fdm-chip">{fund.sub_category}</span> : null}
              {fund?.plan && fund?.option ? <span>{fund.plan} · {fund.option}</span> : null}
              {fund?.risk_label ? <span className="fdm-chip fdm-chip-risk">{fund.risk_label} risk</span> : null}
            </div>
          </div>
          <div className="fdm-head-actions">
            {onTogglePortfolio && fund ? (
              <button
                type="button"
                className={inPortfolio ? "fdm-btn fdm-btn-active" : "fdm-btn"}
                onClick={() => onTogglePortfolio(fund.scheme_code)}
              >
                {inPortfolio ? "In my portfolio" : "Add to portfolio"}
              </button>
            ) : null}
            <button type="button" className="fdm-close" onClick={onClose} aria-label="Close">
              <X size={16} />
            </button>
          </div>
        </header>

        {error ? <div className="fdm-error">{error}</div> : null}

        {fund ? (
          <div className="fdm-keystrip">
            <div className="fdm-key">
              <span>NAV</span>
              <strong>₹{num(fund.nav_latest ?? fund.nav)}</strong>
              <small>{fund.nav_last_date ?? fund.nav_date ?? ""}</small>
            </div>
            <div className="fdm-key">
              <span>3Y CAGR</span>
              <strong className={toneClass(fund.return_3y)}>{pct(fund.return_3y)}</strong>
              <small>category avg {pct(categorySummary?.return_3y)}</small>
            </div>
            <div className="fdm-key">
              <span>Rank in {fund.sub_category ?? "category"}</span>
              <strong><RankBadge rank={fund.rank_3y} count={fund.rank_count_3y} quartile={fund.quartile_3y} /></strong>
              <small>on 3Y CAGR</small>
            </div>
            <div className="fdm-key">
              <span>Expense ratio</span>
              <strong>{pct(fund.expense_ratio)}</strong>
              <small>category avg {pct(categorySummary?.expense_ratio)}</small>
            </div>
            <div className="fdm-key">
              <span>AUM</span>
              <strong>{crore(fund.aum_crore)}</strong>
              <small>{fund.age_years ? `${fund.age_years} yr track record` : ""}</small>
            </div>
            <div className="fdm-key">
              <span>Worst fall</span>
              <strong className="neg">{pct(fund.max_drawdown)}</strong>
              <small>
                {data?.drawdown_profile?.avg_recovery_days
                  ? `avg recovery ${data.drawdown_profile.avg_recovery_days}d`
                  : "peak to trough"}
              </small>
            </div>
          </div>
        ) : null}

        <nav className="fdm-tabs">
          {([
            ["performance", "Performance"],
            ["review", "Vs peers"],
            ["portfolio", "Holdings"],
            ["peers", `Category (${peers.length})`],
            ["about", "About"],
          ] as const).map(([key, label]) => (
            <button
              key={key}
              type="button"
              className={tab === key ? "fdm-tab active" : "fdm-tab"}
              onClick={() => setTab(key)}
            >
              {label}
            </button>
          ))}
        </nav>

        <div className="fdm-body">
          {/* ------------------------------------------------ performance */}
          {tab === "performance" ? (
            <>
              <section className="fdm-section">
                <div className="fdm-chart-bar">
                  <div className="fdm-range">
                    {(series?.available_ranges ?? ["1y", "3y", "5y", "max"]).map((key) => (
                      <button
                        key={key}
                        type="button"
                        className={range === key ? "fdm-pill active" : "fdm-pill"}
                        onClick={() => setRange(key)}
                      >
                        {RANGE_LABELS[key] ?? key.toUpperCase()}
                      </button>
                    ))}
                  </div>
                  <div className="fdm-chart-toggles">
                    <button
                      type="button"
                      className={chartMode === "growth" ? "fdm-pill active" : "fdm-pill"}
                      onClick={() => setChartMode("growth")}
                    >
                      vs benchmark
                    </button>
                    <button
                      type="button"
                      className={chartMode === "nav" ? "fdm-pill active" : "fdm-pill"}
                      onClick={() => setChartMode("nav")}
                    >
                      NAV
                    </button>
                    <button
                      type="button"
                      className={chartMode === "candles" ? "fdm-pill active" : "fdm-pill"}
                      onClick={() => setChartMode("candles")}
                      title="Weekly or monthly OHLC candles built from daily NAV"
                    >
                      Candles
                    </button>
                    {chartMode === "candles" ? (
                      <>
                        <button
                          type="button"
                          className={candlePeriod === "weekly" ? "fdm-pill active" : "fdm-pill"}
                          onClick={() => setCandlePeriod("weekly")}
                        >
                          W
                        </button>
                        <button
                          type="button"
                          className={candlePeriod === "monthly" ? "fdm-pill active" : "fdm-pill"}
                          onClick={() => setCandlePeriod("monthly")}
                        >
                          M
                        </button>
                      </>
                    ) : null}
                    {/* A drawdown ribbon rides the fund's daily line; there is no
                        daily line under candles to hang it from. */}
                    {chartMode === "candles" ? null : (
                      <button
                        type="button"
                        className={showDrawdown ? "fdm-pill active" : "fdm-pill"}
                        onClick={() => setShowDrawdown((value) => !value)}
                      >
                        Drawdown
                      </button>
                    )}
                  </div>
                </div>

                {series ? (
                  <FundNavChart
                    series={series.series}
                    benchmark={chartMode === "growth" ? benchmarkLine : null}
                    drawdown={showDrawdown && chartMode !== "candles" ? series.drawdown : null}
                    mode={chartMode}
                    candlePeriod={candlePeriod}
                    height={330}
                  />
                ) : (
                  <div className="fdm-placeholder">{loadingSeries ? "Loading NAV history…" : "No NAV history."}</div>
                )}

                {benchmarkLine ? (
                  <div className="fdm-bench">
                    <div className="fdm-bench-row">
                      <span>
                        Over this range the fund returned{" "}
                        <b className={toneClass(benchmarkLine.window_fund_return_pct)}>
                          {signedPct(benchmarkLine.window_fund_return_pct)}
                        </b>{" "}
                        against {benchmarkLine.label}{" "}
                        <b className={toneClass(benchmarkLine.window_benchmark_return_pct)}>
                          {signedPct(benchmarkLine.window_benchmark_return_pct)}
                        </b>{" "}
                        — a gap of{" "}
                        <b className={toneClass(benchmarkLine.window_excess_pct)}>
                          {signedPct(benchmarkLine.window_excess_pct)}
                        </b>.
                      </span>
                    </div>
                    <div className="fdm-bench-stats">
                      <span>Beta <em>{num(benchmarkLine.beta)}</em></span>
                      <span>Correlation <em>{num(benchmarkLine.correlation)}</em></span>
                      <span>Up capture <em>{pct(benchmarkLine.up_capture, 0)}</em></span>
                      <span>Down capture <em>{pct(benchmarkLine.down_capture, 0)}</em></span>
                      <span>Tracking error <em>{pct(benchmarkLine.tracking_error, 1)}</em></span>
                    </div>
                    <p className="fdm-note">
                      Official benchmark: {benchmarkLine.official_benchmark ?? "—"}.{" "}
                      {benchmarkLine.notes}
                    </p>
                  </div>
                ) : null}
              </section>

              <section className="fdm-section">
                <h3>Returns, and where they rank</h3>
                <p className="fdm-sub">
                  Windows of a year or more are annualised. Rank is against every fund in{" "}
                  {fund?.sub_category ?? "the category"} that has a record over that window.
                </p>
                <div className="fdm-scroll">
                  <table className="fdm-table">
                    <thead>
                      <tr>
                        <th>Window</th>
                        {RETURN_COLUMNS.map((column) => <th key={column.key}>{column.label}</th>)}
                      </tr>
                    </thead>
                    <tbody>
                      <tr>
                        <th>This fund</th>
                        {RETURN_COLUMNS.map((column) => (
                          <td key={column.key} className={toneClass(fund?.[column.key])}>
                            {pct(fund?.[column.key])}
                          </td>
                        ))}
                      </tr>
                      <tr className="fdm-row-muted">
                        <th>Category average</th>
                        {RETURN_COLUMNS.map((column) => (
                          <td key={column.key}>{pct(categorySummary?.[column.key])}</td>
                        ))}
                      </tr>
                      <tr className="fdm-row-muted">
                        <th>vs category</th>
                        {RETURN_COLUMNS.map((column) => {
                          const mine = fund?.[column.key];
                          const avg = categorySummary?.[column.key];
                          const diff =
                            typeof mine === "number" && typeof avg === "number" ? mine - avg : null;
                          return <td key={column.key} className={toneClass(diff)}>{signedPct(diff)}</td>;
                        })}
                      </tr>
                      <tr>
                        <th>Rank</th>
                        {RETURN_COLUMNS.map((column) => {
                          const window = column.key.replace("return_", "");
                          const rank = fund?.[`rank_${window}`];
                          const count = fund?.[`rank_count_${window}`];
                          return (
                            <td key={column.key}>
                              {typeof rank === "number" && typeof count === "number"
                                ? `${rank} / ${count}`
                                : "—"}
                            </td>
                          );
                        })}
                      </tr>
                    </tbody>
                  </table>
                </div>
              </section>

              {Object.keys(data?.rolling_returns ?? {}).length ? (
                <section className="fdm-section">
                  <h3>Rolling returns — the consistency test</h3>
                  <p className="fdm-sub">
                    A single 5-year number is an accident of its start date. This is what <em>every</em>{" "}
                    holding period of that length in the fund's life actually returned, annualised.
                  </p>
                  <div className="fdm-scroll">
                    <table className="fdm-table">
                      <thead>
                        <tr>
                          <th>Holding period</th>
                          <th>Worst</th>
                          <th>25th pct</th>
                          <th>Median</th>
                          <th>Best</th>
                          <th>Periods that lost money</th>
                          <th>Samples</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(data?.rolling_returns ?? {}).map(([window, stats]) => (
                          <tr key={window}>
                            <th>{window.toUpperCase()}</th>
                            <td className={toneClass(stats.min)}>{pct(stats.min)}</td>
                            <td>{pct(stats.p25)}</td>
                            <td className={toneClass(stats.median)}><b>{pct(stats.median)}</b></td>
                            <td className="pos">{pct(stats.max)}</td>
                            <td className={(stats.pct_negative ?? 0) > 0 ? "neg" : ""}>
                              {pct(stats.pct_negative, 1)}
                            </td>
                            <td className="fdm-dim">{stats.count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </section>
              ) : null}

              <div className="fdm-split">
                {data?.calendar_year_returns?.length ? (
                  <section className="fdm-section">
                    <h3>Calendar years</h3>
                    <div className="fdm-years">
                      {data.calendar_year_returns.map((row) => (
                        <div className="fdm-year" key={row.year}>
                          <span>{row.year}{row.partial ? "*" : ""}</span>
                          <strong className={toneClass(row.return_pct)}>{signedPct(row.return_pct, 1)}</strong>
                          <i
                            className={row.return_pct < 0 ? "fdm-year-bar neg" : "fdm-year-bar pos"}
                            style={{ width: `${Math.min(100, Math.abs(row.return_pct) * 1.6)}%` }}
                          />
                        </div>
                      ))}
                    </div>
                    <p className="fdm-note">* year to date.</p>
                  </section>
                ) : null}

                <section className="fdm-section">
                  <h3>Risk</h3>
                  <dl className="fdm-dl">
                    <div><dt>Volatility (annualised)</dt><dd>{pct(fund?.volatility)}</dd></div>
                    <div><dt>Worst drawdown</dt><dd className="neg">{pct(fund?.max_drawdown)}</dd></div>
                    <div><dt>Currently below peak</dt><dd className={toneClass(fund?.current_drawdown)}>{pct(fund?.current_drawdown)}</dd></div>
                    <div><dt>Sharpe</dt><dd>{num(fund?.sharpe)}</dd></div>
                    <div><dt>Sortino</dt><dd>{num(fund?.sortino)}</dd></div>
                    <div>
                      <dt>Alpha vs benchmark</dt>
                      <dd className={toneClass(fund?.alpha)}>
                        {pct(fund?.alpha)}
                        {fund?.alpha_vs_price_index ? <span className="fdm-flag" title="Measured against a price index, which excludes dividends — this flatters alpha by roughly 1.2% a year.">†</span> : null}
                      </dd>
                    </div>
                    <div><dt>Beta</dt><dd>{num(fund?.beta)}</dd></div>
                    <div><dt>Information ratio</dt><dd>{num(fund?.information_ratio)}</dd></div>
                  </dl>
                  {fund?.alpha_vs_price_index ? (
                    <p className="fdm-note">
                      † Alpha here is measured against a price index, which excludes dividends and so
                      overstates the fund's edge by roughly 1.2% a year.
                    </p>
                  ) : null}
                </section>
              </div>

              {data?.drawdown_profile?.worst?.length ? (
                <section className="fdm-section">
                  <h3>Worst falls, and how long they took to recover</h3>
                  <div className="fdm-scroll">
                    <table className="fdm-table">
                      <thead>
                        <tr><th>Depth</th><th>Peak</th><th>Trough</th><th>Fell over</th><th>Recovered</th><th>Took</th></tr>
                      </thead>
                      <tbody>
                        {data.drawdown_profile.worst.map((episode) => (
                          <tr key={`${episode.peak_date}-${episode.trough_date}`}>
                            <td className="neg"><b>{pct(episode.depth_pct, 1)}</b></td>
                            <td>{episode.peak_date}</td>
                            <td>{episode.trough_date}</td>
                            <td className="fdm-dim">{episode.fall_days ?? "—"}d</td>
                            <td>{episode.recovery_date ?? <span className="neg">not yet</span>}</td>
                            <td className="fdm-dim">{episode.recovery_days != null ? `${episode.recovery_days}d` : "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </section>
              ) : null}
            </>
          ) : null}

          {/* ---------------------------------------------------- review */}
          {tab === "review" ? (
            !review ? (
              <div className="fdm-placeholder">Measuring this fund against its category…</div>
            ) : (
              <>
                <section className="fdm-section">
                  <div className="fdm-alloc-head">
                    <h3>Where it stands in {review.sub_category}</h3>
                    <span className="fdm-dim">{review.peer_count} funds in the category</span>
                  </div>
                  <div className="fdm-standing">
                    <div className="fdm-standing-score">
                      <span>Measured standing</span>
                      <strong className={
                        (review.measured_standing ?? 0) >= 60 ? "pos"
                          : (review.measured_standing ?? 0) <= 40 ? "neg" : ""
                      }>
                        {review.measured_standing != null ? `${review.measured_standing.toFixed(0)}` : "—"}
                        <i>/100</i>
                      </strong>
                      <small>
                        average percentile across every dimension below — a summary of the evidence,
                        not a rating
                      </small>
                    </div>
                    <div className="fdm-standing-traj">
                      <span>Category standing over time</span>
                      <strong className={
                        review.rank_trajectory.direction === "slipping" ? "neg"
                          : review.rank_trajectory.direction === "improving" ? "pos" : ""
                      }>
                        {review.rank_trajectory.direction}
                      </strong>
                      <small>
                        {review.rank_trajectory.points.map(([window, value]) =>
                          `${window.toUpperCase()} ${value.toFixed(0)}th`).join(" → ") || "not enough history"}
                      </small>
                    </div>
                  </div>
                </section>

                <section className="fdm-section">
                  <h3>Scorecard</h3>
                  <p className="fdm-sub">
                    Percentile is against the same category, 100 = best. Every figure is computed from
                    NAV history — nothing here is an opinion.
                  </p>
                  <div className="fdm-scorecard">
                    {review.scorecard.map((item) => (
                      <div className={`fdm-score fdm-score-${item.standing}`} key={item.key}>
                        <span className="fdm-score-label">{item.label}</span>
                        <span className="fdm-score-value">{num(item.value)}{item.unit}</span>
                        <i className="fdm-score-bar">
                          <i style={{ width: `${Math.max(2, Math.min(100, item.percentile ?? 0))}%` }} />
                        </i>
                        <span className="fdm-score-pct">
                          {item.percentile != null ? `${item.percentile.toFixed(0)}th` : "—"}
                        </span>
                        <span className="fdm-score-median">
                          median {item.category_median != null ? `${num(item.category_median)}${item.unit}` : "—"}
                        </span>
                      </div>
                    ))}
                  </div>
                </section>

                <section className="fdm-section">
                  <h3>What the record shows</h3>
                  <ul className="fdm-signals">
                    {review.signals.map((signal) => (
                      <li className={`fdm-signal fdm-signal-${signal.kind}`} key={signal.text}>
                        {signal.text}
                      </li>
                    ))}
                  </ul>
                </section>

                {review.peers_ahead.length ? (
                  <section className="fdm-section">
                    <h3>Funds in this category that measured better</h3>
                    <p className="fdm-sub">
                      Better on 3-year return <em>and</em> expense ratio <em>and</em> worst fall — all
                      three, not just a higher headline number. This is a filtered peer table for
                      comparison, not a shortlist to buy.
                    </p>
                    <div className="fdm-scroll">
                      <table className="fdm-table">
                        <thead>
                          <tr>
                            <th>Fund</th><th>3Y</th><th>vs this fund</th><th>5Y</th>
                            <th>TER</th><th>Worst fall</th><th>Sharpe</th>
                          </tr>
                        </thead>
                        <tbody>
                          {review.peers_ahead.map((peer) => (
                            <tr key={peer.scheme_code}>
                              <td>{peer.name}<br /><small className="fdm-dim">{peer.amc}</small></td>
                              <td className={toneClass(peer.return_3y)}><b>{pct(peer.return_3y)}</b></td>
                              <td className="pos">+{num(peer.return_gap)}%</td>
                              <td className={toneClass(peer.return_5y)}>{pct(peer.return_5y)}</td>
                              <td>{pct(peer.expense_ratio)}</td>
                              <td className="neg">{pct(peer.max_drawdown, 1)}</td>
                              <td>{num(peer.sharpe)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </section>
                ) : null}

                <section className="fdm-section">
                  <div className="fdm-alloc-head">
                    <h3>Plain-English summary</h3>
                    {!aiReview ? (
                      <button type="button" className="fdm-btn" disabled={aiLoading} onClick={loadAiReview}>
                        {aiLoading ? "Writing…" : "Generate"}
                      </button>
                    ) : null}
                  </div>
                  {!aiReview ? (
                    <p className="fdm-note">
                      Turns the scorecard above into a few paragraphs. It describes what the record
                      shows and what it leaves open — it will not tell you whether to buy, sell or
                      switch, which is a decision only you (or a licensed adviser) should make.
                    </p>
                  ) : aiReview.available && aiReview.note ? (
                    <div className="fdm-ai">
                      {aiReview.note.headline ? <p className="fdm-ai-headline">{aiReview.note.headline}</p> : null}
                      {(aiReview.note.assessment ?? []).map((paragraph) => (
                        <p className="fdm-prose" key={paragraph}>{paragraph}</p>
                      ))}
                      <div className="fdm-split">
                        {aiReview.note.working?.length ? (
                          <div>
                            <h4 className="fdm-h4">Measuring well</h4>
                            <ul className="fdm-list pos">
                              {aiReview.note.working.map((item) => <li key={item}>{item}</li>)}
                            </ul>
                          </div>
                        ) : null}
                        {aiReview.note.not_working?.length ? (
                          <div>
                            <h4 className="fdm-h4">Measuring poorly</h4>
                            <ul className="fdm-list neg">
                              {aiReview.note.not_working.map((item) => <li key={item}>{item}</li>)}
                            </ul>
                          </div>
                        ) : null}
                      </div>
                      {aiReview.note.what_to_watch ? (
                        <p className="fdm-note">
                          <b>Worth watching:</b> {aiReview.note.what_to_watch}
                          {aiReview.note.record_quality ? ` · Record length: ${aiReview.note.record_quality}.` : ""}
                        </p>
                      ) : null}
                      <p className="fdm-note">
                        Written from the measured figures above, which are the only inputs it was
                        given. Not advice, and not a recommendation to act.
                      </p>
                    </div>
                  ) : (
                    <p className="fdm-note">{aiReview.reason ?? "Summary unavailable."}</p>
                  )}
                </section>
              </>
            )
          ) : null}

          {/* -------------------------------------------------- holdings */}
          {tab === "portfolio" ? (
            <>
              {!data?.detail_available ? (
                <div className="fdm-placeholder">
                  Holdings for this fund could not be read from the source. Everything else on this
                  page is computed from AMFI NAV and is unaffected.
                </div>
              ) : (
                <>
                  <section className="fdm-section">
                    <div className="fdm-alloc-head">
                      <h3>Portfolio as disclosed {detail?.portfolio_date ? `on ${detail.portfolio_date}` : ""}</h3>
                      <span className="fdm-dim">
                        {detail?.holdings_count ?? 0} holdings · top 10 = {pct(detail?.top10_weight_pct, 1)}
                        {data?.detail_stale ? " · cached" : ""}
                      </span>
                    </div>

                    <div className="fdm-alloc-grid">
                      <div className="fdm-alloc">
                        <h4>Asset mix</h4>
                        {Object.entries(detail?.asset_allocation ?? {}).map(([key, weight]) => (
                          <div className="fdm-bar-row" key={key}>
                            <span>{ASSET_LABELS[key] ?? key}</span>
                            <i className="fdm-bar"><i style={{ width: `${Math.min(100, weight)}%` }} /></i>
                            <em>{pct(weight, 1)}</em>
                          </div>
                        ))}
                      </div>

                      <div className="fdm-alloc">
                        <h4>Market cap of the equity book</h4>
                        {Object.keys(detail?.cap_allocation ?? {}).length ? (
                          <>
                            {(["large", "mid", "small"] as const).map((key) => (
                              <div className="fdm-bar-row" key={key}>
                                <span>{CAP_LABELS[key]}</span>
                                <i className="fdm-bar"><i style={{ width: `${Math.min(100, detail?.cap_allocation?.[key] ?? 0)}%` }} /></i>
                                <em>{pct(detail?.cap_allocation?.[key], 1)}</em>
                              </div>
                            ))}
                            <p className="fdm-note">
                              Classified by SEBI rank (top 100 large, next 150 mid, rest small) using this
                              app's own market-cap data — covering {pct(detail?.cap_coverage_pct, 0)} of the
                              Indian equity book.
                            </p>
                          </>
                        ) : (
                          <p className="fdm-note">No classifiable Indian equity holdings.</p>
                        )}
                      </div>

                      <div className="fdm-alloc">
                        <h4>Sectors ({detail?.sector_count ?? 0})</h4>
                        {Object.entries(detail?.sector_allocation ?? {}).slice(0, 10).map(([key, weight]) => (
                          <div className="fdm-bar-row" key={key}>
                            <span>{key}</span>
                            <i className="fdm-bar"><i style={{ width: `${Math.min(100, weight * 2)}%` }} /></i>
                            <em>{pct(weight, 1)}</em>
                          </div>
                        ))}
                      </div>
                    </div>
                  </section>

                  <section className="fdm-section">
                    <div className="fdm-alloc-head">
                      <h3>All holdings</h3>
                      <input
                        className="fdm-input"
                        placeholder="Filter holdings…"
                        value={holdingFilter}
                        onChange={(event) => setHoldingFilter(event.target.value)}
                      />
                    </div>
                    <div className="fdm-scroll fdm-scroll-tall">
                      <table className="fdm-table fdm-table-holdings">
                        <thead>
                          <tr>
                            <th>#</th><th>Holding</th><th>Weight</th><th>Sector</th>
                            <th>Cap</th><th>Value</th><th>Type</th><th />
                          </tr>
                        </thead>
                        <tbody>
                          {holdings.map((row, index) => (
                            <tr key={`${row.name}-${index}`}>
                              <td className="fdm-dim">{index + 1}</td>
                              <td className="fdm-holding-name">
                                {row.name}
                                {row.is_foreign ? <span className="fdm-tag">intl</span> : null}
                                {row.is_derivative ? <span className="fdm-tag">deriv</span> : null}
                              </td>
                              <td><b>{pct(row.weight_pct, 2)}</b></td>
                              <td className="fdm-dim">{row.sector ?? "—"}</td>
                              <td>{row.cap_class ? CAP_LABELS[row.cap_class] : "—"}</td>
                              <td className="fdm-dim">{crore(row.market_value_crore)}</td>
                              <td className="fdm-dim">{ASSET_LABELS[row.asset_class] ?? row.asset_class}</td>
                              <td>
                                {row.symbol && onOpenSymbolChart ? (
                                  <button
                                    type="button"
                                    className="fdm-link"
                                    // Close on the way out: the chart opens
                                    // behind this modal otherwise.
                                    onClick={() => { onOpenSymbolChart(row.symbol as string); onClose(); }}
                                    title={`Open ${row.symbol} chart`}
                                  >
                                    {row.symbol} <ArrowUpRight size={11} />
                                  </button>
                                ) : null}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    {!holdings.length ? <p className="fdm-note">Nothing matches that filter.</p> : null}
                  </section>
                </>
              )}
            </>
          ) : null}

          {/* ----------------------------------------------------- peers */}
          {tab === "peers" ? (
            <section className="fdm-section">
              <h3>Every fund in {fund?.sub_category ?? "this category"}</h3>
              <p className="fdm-sub">
                Ranked on 3-year CAGR. This fund is highlighted
                {selfIndex >= 0 ? ` at position ${selfIndex + 1}` : ""}.
              </p>
              <div className="fdm-scroll fdm-scroll-tall">
                <table className="fdm-table">
                  <thead>
                    <tr>
                      <th>#</th><th>Fund</th><th>1Y</th><th>3Y</th><th>5Y</th>
                      <th>TER</th><th>Sharpe</th><th>Worst fall</th><th>AUM</th>
                    </tr>
                  </thead>
                  <tbody>
                    {peers.map((peer, index) => (
                      <tr key={peer.scheme_code} className={peer.is_self ? "fdm-row-self" : undefined}>
                        <td className="fdm-dim">{index + 1}</td>
                        <td>{peer.name}<br /><small className="fdm-dim">{peer.amc}</small></td>
                        <td className={toneClass(peer.return_1y)}>{pct(peer.return_1y)}</td>
                        <td className={toneClass(peer.return_3y)}><b>{pct(peer.return_3y)}</b></td>
                        <td className={toneClass(peer.return_5y)}>{pct(peer.return_5y)}</td>
                        <td>{pct(peer.expense_ratio)}</td>
                        <td>{num(peer.sharpe)}</td>
                        <td className="neg">{pct(peer.max_drawdown, 1)}</td>
                        <td className="fdm-dim">{crore(peer.aum_crore)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          ) : null}

          {/* ----------------------------------------------------- about */}
          {tab === "about" ? (
            <>
              {detail?.objective ? (
                <section className="fdm-section">
                  <h3>Objective</h3>
                  <p className="fdm-prose">{detail.objective}</p>
                </section>
              ) : null}

              {detail?.pros?.length || detail?.cons?.length ? (
                <section className="fdm-section fdm-split">
                  {detail?.pros?.length ? (
                    <div>
                      <h3>Noted strengths</h3>
                      <ul className="fdm-list pos">{detail.pros.map((item) => <li key={item}>{item}</li>)}</ul>
                    </div>
                  ) : null}
                  {detail?.cons?.length ? (
                    <div>
                      <h3>Noted weaknesses</h3>
                      <ul className="fdm-list neg">{detail.cons.map((item) => <li key={item}>{item}</li>)}</ul>
                    </div>
                  ) : null}
                </section>
              ) : null}

              <section className="fdm-section fdm-split">
                <div>
                  <h3>The fund</h3>
                  <dl className="fdm-dl">
                    <div><dt>Launched</dt><dd>{fund?.launch_date ?? "—"}</dd></div>
                    <div><dt>Track record</dt><dd>{fund?.age_years ? `${fund.age_years} years` : "—"}</dd></div>
                    <div><dt>Expense ratio</dt><dd>{pct(fund?.expense_ratio)}</dd></div>
                    <div><dt>Portfolio turnover</dt><dd>{pct(fund?.portfolio_turnover, 0)}</dd></div>
                    <div><dt>Minimum lumpsum</dt><dd>{rupees(fund?.min_lumpsum)}</dd></div>
                    <div><dt>Minimum SIP</dt><dd>{rupees(fund?.min_sip)}</dd></div>
                    <div><dt>Lock-in</dt><dd>{fund?.lock_in_years ? `${fund.lock_in_years} years` : "None"}</dd></div>
                    <div><dt>ISIN</dt><dd>{fund?.isin ?? "—"}</dd></div>
                    <div><dt>AMFI scheme code</dt><dd>{fund?.scheme_code}</dd></div>
                  </dl>
                  {fund?.exit_load ? (
                    <>
                      <h4 className="fdm-h4">Exit load</h4>
                      <p className="fdm-prose fdm-dim">{fund.exit_load}</p>
                    </>
                  ) : null}
                </div>

                <div>
                  <h3>Who runs it</h3>
                  {detail?.managers?.length ? (
                    <ul className="fdm-managers">
                      {detail.managers.map((manager) => (
                        <li key={`${manager.name}-${manager.since}`}>
                          <strong>{manager.name}</strong>
                          {manager.since ? <span className="fdm-dim"> since {manager.since}</span> : null}
                          {manager.education ? <p className="fdm-dim">{manager.education}</p> : null}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="fdm-dim">{fund?.fund_manager ?? "—"}</p>
                  )}
                  {detail?.amc?.name ? (
                    <p className="fdm-note">
                      {detail.amc.name}
                      {detail.amc.total_aum_crore ? ` · ${crore(detail.amc.total_aum_crore)} across all schemes` : ""}
                    </p>
                  ) : null}
                </div>
              </section>

              {detail?.expense_history?.length ? (
                <section className="fdm-section">
                  <h3>Expense ratio over time</h3>
                  <div className="fdm-spark">
                    {detail.expense_history.map((entry) => {
                      const values = detail.expense_history!
                        .map((item) => item.expense_ratio)
                        .filter((value): value is number => typeof value === "number");
                      const max = Math.max(...values, 0.01);
                      const height = entry.expense_ratio ? (entry.expense_ratio / max) * 100 : 0;
                      return (
                        <i
                          key={entry.date ?? Math.random()}
                          style={{ height: `${Math.max(4, height)}%` }}
                          title={`${entry.date}: ${pct(entry.expense_ratio)}`}
                        />
                      );
                    })}
                  </div>
                  <p className="fdm-note">
                    {detail.expense_history[0]?.date} → {detail.expense_history[detail.expense_history.length - 1]?.date}.
                    A rising expense ratio on a growing fund is worth a second look.
                  </p>
                </section>
              ) : null}
            </>
          ) : null}
        </div>

        <footer className="fdm-foot">
          <span>
            NAV and all returns from AMFI, as of {fund?.nav_last_date ?? fund?.nav_date ?? "—"}. Holdings,
            expense ratio and AUM are third-party reference data and may lag the AMC's own disclosure.
          </span>
          <span className="fdm-dim">Measured history, not a projection. Nothing here is advice.</span>
        </footer>
      </div>
    </div>
  );
}
