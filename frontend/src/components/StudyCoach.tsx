import { useCallback, useEffect, useState } from "react";
import { getStudyReview, type StudyReviewResponse, type StudySlice } from "../lib/api";

import "./StudyCoach.css";

const SLICE_TITLES: Record<string, string> = {
  by_setup: "By setup",
  by_wait: "By how long you waited",
  by_stop_width: "By your stop width",
  by_rs: "By RS rating",
  by_base_depth: "By base depth",
  by_volume_dryup: "By volume dry-up",
  by_group: "By industry group",
};

const signed = (v: number, digits = 2) => `${v >= 0 ? "+" : ""}${v.toFixed(digits)}`;

function SliceTable({ title, rows }: { title: string; rows: StudySlice[] }) {
  if (!rows?.length) return null;
  return (
    <div className="coach-slice">
      <h5>{title}</h5>
      <table>
        <tbody>
          {rows.map((row) => (
            <tr key={row.label}>
              <td>{row.label}</td>
              <td className={row.avg_r >= 0 ? "pos" : "neg"}>{signed(row.avg_r)}R</td>
              <td>{row.hit_rate_pct.toFixed(0)}%</td>
              {/* The sample size sits next to every number on purpose: a
                  six-trade bucket and a sixty-trade bucket look identical
                  otherwise, and only one of them means anything. */}
              <td className="coach-n">{row.trades}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/**
 * The Chart Gym coach.
 *
 * Everything numeric on this panel is computed server-side in `study_coach.py`
 * from the user's own graded cards; the written review is a model talking about
 * those numbers and nothing else. The two are shown separately so it is always
 * obvious which is which — and the numbers still render when the model is
 * unavailable.
 */
export function StudyCoach({ version }: { version: number }) {
  const [data, setData] = useState<StudyReviewResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    getStudyReview()
      .then(setData)
      .catch((err) => setError(err instanceof Error ? err.message : String(err)))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const stats = data?.stats;
  const review = data?.review;
  const overall = stats?.overall;

  return (
    <div className="study-coach">
      <div className="coach-head">
        <h3>Coach</h3>
        <button type="button" onClick={load} disabled={loading}>
          {loading ? "Reading your record…" : version > 0 ? "Refresh review" : "Refresh"}
        </button>
      </div>

      {error ? <p className="coach-error">{error}</p> : null}

      {overall ? (
        <div className="coach-numbers">
          <div><span>Graded</span><strong>{overall.graded}</strong></div>
          <div><span>Taken</span><strong>{overall.taken}</strong></div>
          <div>
            <span>Avg R</span>
            <strong className={(overall.avg_r ?? 0) >= 0 ? "pos" : "neg"}>
              {overall.avg_r != null ? signed(overall.avg_r) : "—"}
            </strong>
          </div>
          <div><span>Hit rate</span><strong>{overall.hit_rate_pct != null ? `${overall.hit_rate_pct.toFixed(0)}%` : "—"}</strong></div>
          <div>
            <span>Total R</span>
            <strong className={(overall.total_r ?? 0) >= 0 ? "pos" : "neg"}>
              {overall.total_r != null ? signed(overall.total_r, 1) : "—"}
            </strong>
          </div>
        </div>
      ) : null}

      {stats?.selection ? (
        <p className="coach-edge">
          Of the cards you took, <strong>{stats.selection.cards_that_were_winners_pct.toFixed(0)}%</strong> were
          winners. The deck deals a 50/50 mix, so that is{" "}
          <strong className={stats.selection.edge_pts >= 0 ? "pos" : "neg"}>
            {signed(stats.selection.edge_pts, 1)} points
          </strong>{" "}
          of selection edge over picking at random, across {stats.selection.trades} trades.
        </p>
      ) : null}

      {stats && !stats.ready ? (
        <p className="coach-note">
          Grade at least {stats.min_sample} cards you actually traded and the review will start saying something
          worth reading. Right now it would just be describing noise.
        </p>
      ) : null}

      {data?.review_error && stats?.ready ? <p className="coach-note">{data.review_error}</p> : null}

      {review ? (
        <div className="coach-review">
          {review.headline ? <p className="coach-headline">{review.headline}</p> : null}
          {review.overall ? <p>{review.overall}</p> : null}

          {review.doing_right?.length ? (
            <section>
              <h4 className="pos">What you're doing right</h4>
              <ul>
                {review.doing_right.map((item) => (
                  <li key={item.what}>
                    <strong>{item.what}</strong>
                    <span>{item.evidence}</span>
                  </li>
                ))}
              </ul>
            </section>
          ) : null}

          {review.doing_wrong?.length ? (
            <section>
              <h4 className="neg">What's costing you</h4>
              <ul>
                {review.doing_wrong.map((item) => (
                  <li key={item.what}>
                    <strong>{item.what}</strong>
                    <span>{item.evidence}{item.cost ? ` — ${item.cost}` : ""}</span>
                  </li>
                ))}
              </ul>
            </section>
          ) : null}

          {review.biggest_leak ? (
            <section className="coach-leak">
              <h4>Biggest leak</h4>
              <p><strong>{review.biggest_leak.leak}</strong></p>
              <p className="coach-evidence">{review.biggest_leak.evidence}</p>
              <p className="coach-rule">Rule for your next 20 cards: {review.biggest_leak.rule}</p>
            </section>
          ) : null}

          {review.reading_the_chart?.length ? (
            <section>
              <h4>What you may be misreading</h4>
              <ul>{review.reading_the_chart.map((line) => <li key={line}><span>{line}</span></li>)}</ul>
            </section>
          ) : null}

          {review.trajectory ? (
            <section><h4>Are you improving?</h4><p>{review.trajectory}</p></section>
          ) : null}

          {review.next_focus ? (
            <section><h4>Focus next</h4><p>{review.next_focus}</p></section>
          ) : null}

          {review.confidence ? <p className="coach-confidence">{review.confidence}</p> : null}
        </div>
      ) : null}

      {stats?.slices ? (
        <div className="coach-slices">
          <h4>The numbers behind it</h4>
          {Object.entries(stats.slices).map(([key, rows]) => (
            <SliceTable key={key} title={SLICE_TITLES[key] ?? key} rows={rows} />
          ))}
          <p className="coach-note">
            Last column is the number of trades in that row. Anything under {stats.min_sample} is not shown at all.
          </p>
        </div>
      ) : null}
    </div>
  );
}
