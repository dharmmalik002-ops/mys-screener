import { useEffect, useMemo, useState } from "react";
import { getMfAiPortfolioHealth, type MfAiHealthNote, type MfHealthPoint, type MfPortfolioHealth } from "../lib/api";

import "./PortfolioHealth.css";

/**
 * What is measurably true about the portfolio as a whole, at the foot of the page.
 *
 * The numbers a fund investor most needs are the ones no factsheet can give
 * them, because no factsheet knows what else they hold: what the whole book
 * costs to run against its own categories, how much of it is duplicated, which
 * holdings have trailed their peers long enough to be a record, and which parts
 * of the market it never reaches.
 *
 * **This panel reports; it does not advise.** No fund is recommended, nothing
 * is ranked as a replacement, no SIP is sized and no purchase is timed — that
 * is personalised investment advice and this app is not a licensed adviser. The
 * same line is drawn on the single-fund review, and it is enforced upstream by
 * `portfolio_health.py` and its tests. The findings state the mechanism (what a
 * cost gap compounds into, what overlap does to diversification) and leave the
 * decision where it belongs.
 */

const TONE_LABEL: Record<string, string> = {
  watch: "worth a look",
  neutral: "for information",
  good: "measures well",
};

/**
 * Where each fund you hold ranks inside its own category.
 *
 * This replaced a two-axis scatter (cost gap across, return gap up). The
 * scatter was accurate and unreadable: it asked the reader to hold two signed
 * gaps and a quadrant convention in their head before it said anything. One
 * bar per fund says the same thing in the order people actually ask it —
 * *how is this fund doing against funds like it?* — and the two gaps stay on
 * the row as plain badges for anyone who wants them.
 *
 * The rank is the fund's percentile within its own SEBI sub-category over
 * three years, which is why a large cap and a small cap can sit on one scale:
 * each is measured only against its own peers, never against the other.
 */
function CategoryStanding({ points }: { points: MfHealthPoint[] }) {
  const rows = useMemo(
    () =>
      points
        .filter((point) => typeof point.percentile_3y === "number")
        .sort((a, b) => (b.percentile_3y ?? 0) - (a.percentile_3y ?? 0)),
    [points],
  );

  if (!rows.length) {
    return (
      <p className="pfh-plot-empty">
        Not enough three-year category data to rank these funds yet.
      </p>
    );
  }

  return (
    <div className="pfh-standing">
      <div className="pfh-standing-scale" aria-hidden="true">
        <span>worst in category</span>
        <span className="pfh-standing-mid">average</span>
        <span>best</span>
      </div>

      <ul className="pfh-standing-list">
        {rows.map((row) => {
          const pct = row.percentile_3y ?? 0;
          const ahead = pct >= 50;
          // "Top 28%" reads better than "72nd percentile" and means the same.
          const label = ahead ? `top ${Math.max(1, Math.round(100 - pct))}%` : `bottom ${Math.max(1, Math.round(pct))}%`;
          return (
            <li key={row.scheme_code}>
              <div className="pfh-standing-head">
                <span className="pfh-standing-name">{row.name}</span>
                <em>{row.category}</em>
              </div>
              <div className="pfh-standing-row">
                <span className="pfh-standing-track">
                  <i className="pfh-standing-median" />
                  <i
                    className={ahead ? "pfh-standing-fill is-up" : "pfh-standing-fill is-down"}
                    style={
                      ahead
                        ? { left: "50%", width: `${(pct - 50)}%` }
                        : { right: "50%", width: `${(50 - pct)}%` }
                    }
                  />
                  <i
                    className={ahead ? "pfh-standing-dot is-up" : "pfh-standing-dot is-down"}
                    style={{ left: `${pct}%` }}
                  />
                </span>
                <b className={ahead ? "is-up" : "is-down"}>{label}</b>
              </div>
              <div className="pfh-standing-badges">
                <span className={row.return_gap >= 0 ? "is-up" : "is-down"}>
                  {row.return_gap >= 0 ? "+" : "−"}{Math.abs(row.return_gap).toFixed(1)}% return vs peers
                </span>
                <span className={row.cost_gap <= 0 ? "is-up" : "is-down"}>
                  {row.cost_gap > 0 ? "+" : row.cost_gap < 0 ? "−" : ""}{Math.abs(row.cost_gap).toFixed(2)}% cost vs peers
                </span>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

export function PortfolioHealth({
  health,
  onOpenFund,
}: {
  health: MfPortfolioHealth | null;
  onOpenFund: (schemeCode: string) => void;
}) {
  const [note, setNote] = useState<MfAiHealthNote | null>(null);
  const [loadingNote, setLoadingNote] = useState(false);
  const [askedFor, setAskedFor] = useState<string | null>(null);

  // The prose is generated on demand rather than on page load: it costs a model
  // call, and the measured findings below stand on their own without it.
  const fingerprint = health?.findings?.map((item) => `${item.key}:${item.tone}`).join("|") ?? "";
  useEffect(() => {
    if (askedFor && askedFor !== fingerprint) { setNote(null); setAskedFor(null); }
  }, [fingerprint, askedFor]);

  const requestNote = () => {
    setLoadingNote(true);
    setAskedFor(fingerprint);
    getMfAiPortfolioHealth()
      .then((payload) => setNote(payload))
      .catch(() => setNote({ available: false, reason: "Could not reach the AI service." }))
      .finally(() => setLoadingNote(false));
  };

  if (!health) return null;

  if (!health.available) {
    return (
      <section className="pfd-panel">
        <header className="pfd-panel-head">
          <div>
            <h3>What this portfolio measures</h3>
            <p>Cost, duplication, long-run standing and market reach, across everything you hold.</p>
          </div>
        </header>
        <p className="pfh-empty">{health.reason ?? "Nothing to measure yet."}</p>
      </section>
    );
  }

  return (
    <section className="pfd-panel pfh">
      <header className="pfd-panel-head">
        <div>
          <h3>What this portfolio measures</h3>
          <p>
            The numbers no single factsheet can give you, because none of them know what else you
            hold: what the whole book costs against its own categories, how much is duplicated,
            which holdings have trailed their peers over the long windows, and which parts of the
            market it reaches. Facts about {health.fund_count} funds you already own.
          </p>
        </div>
        <span className={health.watch_count ? "pfd-chip is-down" : "pfd-chip is-up"}>
          {health.watch_count} to look at
        </span>
      </header>

      <ul className="pfh-findings">
        {health.findings.map((finding) => (
          <li key={finding.key} className={`pfh-finding is-${finding.tone}`}>
            <div className="pfh-finding-head">
              <h4>{finding.headline}</h4>
              {finding.metric ? <span className="pfh-metric">{finding.metric}</span> : null}
            </div>
            <p>{finding.detail}</p>
            {finding.evidence?.length ? (
              <ul className="pfh-evidence">
                {finding.evidence.map((row, index) => (
                  <li key={`${row.name}-${index}`}>
                    {row.scheme_code ? (
                      <button type="button" className="pfd-fund" onClick={() => onOpenFund(row.scheme_code!)}>
                        {row.name}
                      </button>
                    ) : (
                      <span className="pfh-evidence-name">{row.name}</span>
                    )}
                    <b>
                      {typeof row.value === "number" ? row.value.toFixed(2).replace(/\.00$/, "") : "—"}
                      {row.label?.includes("%") ? "" : ""}
                    </b>
                    {typeof row.reference === "number" ? (
                      <em>vs {row.reference.toFixed(2)} median</em>
                    ) : row.label ? <em>{row.label}</em> : <em />}
                  </li>
                ))}
              </ul>
            ) : null}
            <span className="pfh-tone">{TONE_LABEL[finding.tone] ?? finding.tone}</span>
          </li>
        ))}
      </ul>

      {/* ------------------------------------------------- positioning plot */}
      <div className="pfh-plot-block">
        <div className="pfh-plot-head">
          <h4>How each fund ranks in its own category</h4>
          <p>
            Every fund is measured only against funds of its own SEBI sub-category over three
            years — so a small cap is judged against small caps, never against a large cap. The
            dot is where it sits from worst to best; the notch in the middle is the category
            average.
          </p>
        </div>
        <CategoryStanding points={health.chart?.points ?? []} />
      </div>

      {/* ------------------------------------------------------- AI reading */}
      <div className="pfh-ai">
        {note?.available && note.note ? (
          <div className="pfh-ai-note">
            {note.note.headline ? <h4>{note.note.headline}</h4> : null}
            {(note.note.assessment ?? []).map((paragraph) => (
              <p key={paragraph.slice(0, 40)}>{paragraph}</p>
            ))}
            <div className="pfh-ai-cols">
              {note.note.strengths?.length ? (
                <div>
                  <h5>Measures well</h5>
                  <ul>{note.note.strengths.map((item) => <li key={item}>{item}</li>)}</ul>
                </div>
              ) : null}
              {note.note.frictions?.length ? (
                <div>
                  <h5>Working against you</h5>
                  <ul>{note.note.frictions.map((item) => <li key={item}>{item}</li>)}</ul>
                </div>
              ) : null}
            </div>
            {note.note.watch ? (
              <p className="pfh-ai-watch"><b>Most worth watching:</b> {note.note.watch}</p>
            ) : null}
          </div>
        ) : note && !note.available ? (
          <p className="pfh-ai-unavailable">{note.reason}</p>
        ) : (
          <button type="button" className="pfh-ai-btn" disabled={loadingNote} onClick={requestNote}>
            {loadingNote ? "Reading the findings…" : "Summarise these findings in plain English"}
          </button>
        )}
      </div>

      <p className="pfd-footnote pfh-disclaimer">{health.disclaimer}</p>
    </section>
  );
}
