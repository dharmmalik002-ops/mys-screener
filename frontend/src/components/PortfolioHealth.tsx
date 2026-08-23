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
 * Each fund you hold, placed against the median of its own SEBI sub-category:
 * cost gap across, three-year return gap up. Expressing both as a gap from the
 * fund's own category is what lets a small cap and a large cap share one plot —
 * comparing their raw returns would just rank the categories.
 *
 * Up and to the left is "returned more than its peers while charging less".
 * That is a description of where a holding sits, not a shortlist to buy.
 */
function PositioningPlot({ points }: { points: MfHealthPoint[] }) {
  const [active, setActive] = useState<string | null>(null);

  const geometry = useMemo(() => {
    if (!points.length) return null;
    const costs = points.map((p) => p.cost_gap);
    const returns = points.map((p) => p.return_gap);
    // Pad the extent so a point never sits on the frame, and keep the axes
    // symmetric about zero so the quadrants read honestly.
    const costExtent = Math.max(Math.abs(Math.min(...costs)), Math.abs(Math.max(...costs)), 0.25) * 1.25;
    const returnExtent = Math.max(Math.abs(Math.min(...returns)), Math.abs(Math.max(...returns)), 1) * 1.25;
    const weights = points.map((p) => p.weight_pct ?? 0);
    const maxWeight = Math.max(...weights, 1);
    return { costExtent, returnExtent, maxWeight };
  }, [points]);

  if (!geometry || !points.length) {
    return <p className="pfh-plot-empty">Not enough category data to place these funds yet.</p>;
  }

  const W = 100;
  const H = 100;
  const x = (gap: number) => ((gap + geometry.costExtent) / (2 * geometry.costExtent)) * W;
  const y = (gap: number) => H - ((gap + geometry.returnExtent) / (2 * geometry.returnExtent)) * H;

  return (
    <div className="pfh-plot">
      <svg viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none" className="pfh-plot-svg" role="img"
           aria-label="Each fund held, plotted against its category median on cost and three-year return">
        {/* Quadrant tint: cheaper-and-ahead of peers, top left. */}
        <rect x="0" y="0" width={W / 2} height={H / 2} className="pfh-quad-good" />
        <rect x={W / 2} y={H / 2} width={W / 2} height={H / 2} className="pfh-quad-poor" />
        <line x1="0" y1={H / 2} x2={W} y2={H / 2} className="pfh-axis" vectorEffect="non-scaling-stroke" />
        <line x1={W / 2} y1="0" x2={W / 2} y2={H} className="pfh-axis" vectorEffect="non-scaling-stroke" />
        {points.map((point) => (
          <circle
            key={point.scheme_code}
            cx={x(point.cost_gap)}
            cy={y(point.return_gap)}
            // Area, not radius, tracks portfolio weight — a radius scale
            // exaggerates the big holdings by the square.
            r={Math.sqrt(((point.weight_pct ?? 0) / geometry.maxWeight) || 0.04) * 5 + 1.6}
            className={active === point.scheme_code ? "pfh-dot is-active" : "pfh-dot"}
            vectorEffect="non-scaling-stroke"
            onMouseEnter={() => setActive(point.scheme_code)}
            onMouseLeave={() => setActive(null)}
          />
        ))}
      </svg>

      <span className="pfh-axis-label pfh-axis-y">3y return vs category median</span>
      <span className="pfh-axis-label pfh-axis-x">Cost vs category median</span>
      <span className="pfh-quad-note pfh-quad-note-tl">cheaper, ahead of peers</span>
      <span className="pfh-quad-note pfh-quad-note-br">dearer, behind peers</span>

      <ul className="pfh-plot-legend">
        {points.map((point) => (
          <li
            key={point.scheme_code}
            className={active === point.scheme_code ? "is-active" : undefined}
            onMouseEnter={() => setActive(point.scheme_code)}
            onMouseLeave={() => setActive(null)}
          >
            <span className="pfh-plot-name">{point.name}</span>
            <em>{point.category}</em>
            <b className={point.return_gap < 0 ? "is-down" : "is-up"}>
              {point.return_gap > 0 ? "+" : ""}{point.return_gap.toFixed(1)}% return
            </b>
            <b className={point.cost_gap > 0 ? "is-down" : "is-up"}>
              {point.cost_gap > 0 ? "+" : ""}{point.cost_gap.toFixed(2)}% cost
            </b>
          </li>
        ))}
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
          <h4>Where your funds sit against their own categories</h4>
          <p>{health.chart?.note}</p>
        </div>
        <PositioningPlot points={health.chart?.points ?? []} />
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
