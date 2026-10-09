import type { MarketEnvironmentResponse, MarketsExposure, XpBreadthScore } from "../../lib/api";
import {
  breadthFacts,
  disagreement,
  freshness,
  longDate,
  readCondition,
  type IndexRead,
} from "../../lib/marketsBrief";
import "./MarketsBrief.css";

type Props = {
  exposure: MarketsExposure | null;
  xp: XpBreadthScore | null;
  env: MarketEnvironmentResponse | null;
  indices: IndexRead[];
  /** At most three, already ranked by impact. */
  reasons: string[];
  /** At most two. */
  flips: string[];
};

const DIRECTION_WORD = { improving: "improving", deteriorating: "deteriorating", stable: "steady", unknown: "" } as const;

function signed(value: number, digits = 2): string {
  return `${value >= 0 ? "+" : "−"}${Math.abs(value).toFixed(digits)}%`;
}

function trendWord(index: IndexRead): string {
  if (index.above50 && index.above200) return "above 50 & 200-day";
  if (index.above200) return "below 50-day, above 200-day";
  if (index.above50) return "above 50-day, below 200-day";
  return "below 50 & 200-day";
}

/**
 * The first screen of the Markets page: condition, size, the three indices, five
 * breadth facts, and why. The rules that decide what appears live in
 * lib/marketsBrief.ts; this file only lays the result out.
 */
export function MarketsBrief({ exposure, xp, env, indices, reasons, flips }: Props) {
  const condition = readCondition(exposure, xp);
  const mixed = disagreement(condition, indices);
  const facts = breadthFacts(env, xp);
  const { asOf, notes } = freshness(indices, exposure);
  const verdict = exposure?.available && exposure.verdict?.available ? exposure.verdict : null;
  const ruleToday = env?.ai?.one_rule_today?.trim() || null;
  const aiRead = env?.ai?.headline?.trim() || null;
  const dateLabel = longDate(asOf);

  return (
    <section className={`mkb mkb-${condition.tone}`} aria-label="Market condition">
      <div className="ol-kicker">Market condition{dateLabel ? ` · close of ${dateLabel}` : ""}</div>

      <h2 className="mkb-headline">
        The market is <span className="mkb-word">{condition.word}</span>.{" "}
        <span className="mkb-soft">{condition.guidance}</span>
      </h2>

      {verdict ? (
        <p className="mkb-size">
          <strong>{verdict.exposure_pct}%</strong> of full size
          {verdict.direction !== "unknown" ? <em> · {DIRECTION_WORD[verdict.direction]}</em> : null}
        </p>
      ) : null}

      {indices.length ? (
        <dl className="mkb-indices" aria-label="Index levels">
          {indices.map((index) => (
            <div key={index.label}>
              <dt>{index.label}</dt>
              <dd>
                {index.last.toLocaleString("en-IN", { maximumFractionDigits: 2 })}
                {index.dayPct !== null ? <small className={index.dayPct >= 0 ? "pos" : "neg"}> {signed(index.dayPct)}</small> : null}
              </dd>
              <span className="mkb-index-trend">{trendWord(index)}</span>
            </div>
          ))}
        </dl>
      ) : null}

      {facts.length ? (
        <dl className="mkb-facts" aria-label="Breadth">
          {facts.map((fact) => (
            <div key={fact.label}>
              <dt>{fact.label}</dt>
              <dd className={fact.tone ?? ""}>{fact.value}</dd>
            </div>
          ))}
        </dl>
      ) : null}

      {mixed ? <p className="mkb-mixed">{mixed}</p> : null}

      {ruleToday ? (
        <p className="mkb-rule">
          <span>Rule today</span> {ruleToday}
        </p>
      ) : null}

      {reasons.length || flips.length ? (
        <div className="mkb-why">
          {reasons.length ? (
            <div>
              <h3>Why</h3>
              <ul>{reasons.slice(0, 3).map((r) => <li key={r}>{r}</li>)}</ul>
            </div>
          ) : null}
          {flips.length ? (
            <div>
              <h3>What would change it</h3>
              <ul>{flips.slice(0, 2).map((f) => <li key={f}>{f}</li>)}</ul>
            </div>
          ) : null}
        </div>
      ) : null}

      {aiRead ? (
        <p className="mkb-ai">
          <span>AI read</span> {aiRead}
        </p>
      ) : null}

      {notes.length ? (
        <p className="mkb-stale" role="status">
          {notes.join(" ")}
        </p>
      ) : null}
    </section>
  );
}
