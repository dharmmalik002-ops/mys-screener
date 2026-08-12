import { AlertTriangle, TrendingDown, TrendingUp, Minus } from "lucide-react";
import type { MarketsExposure } from "../../lib/api";
import "./ExposureVerdict.css";

type Props = { data: MarketsExposure | null };

const DIRECTION_ICON = {
  improving: TrendingUp,
  deteriorating: TrendingDown,
  stable: Minus,
  unknown: Minus,
} as const;

const DIRECTION_WORD = {
  improving: "and improving",
  deteriorating: "and deteriorating",
  stable: "and steady",
  unknown: "",
} as const;

function pct(value: number | null | undefined, digits = 1): string {
  return value === null || value === undefined ? "—" : `${value.toFixed(digits)}%`;
}

/**
 * The page's single verdict.
 *
 * This replaced three competing top-level verdicts that used different
 * vocabularies and could disagree on screen. It states one thing — how much
 * capital should be at risk — and shows the arithmetic that produced it, so
 * the number is checkable rather than authoritative-looking.
 *
 * It is deliberately NOT a forecast. Every environment indicator tested
 * against this market's history failed to predict either index direction or
 * whether breakouts pay, so the verdict rests only on measured present
 * performance against the user's own break-even.
 */
export function ExposureVerdict({ data }: Props) {
  const verdict = data?.verdict;

  if (!data?.available || !verdict?.available) {
    return (
      <section className="mkx mkx-empty" aria-label="Exposure verdict">
        <AlertTriangle size={16} aria-hidden />
        <p>{verdict?.reason ?? data?.reason ?? "Exposure cannot be judged yet."}</p>
      </section>
    );
  }

  const DirIcon = DIRECTION_ICON[verdict.direction];
  const exposure = verdict.exposure_pct ?? 0;
  const tone = verdict.clears_breakeven ? "ok" : exposure <= 25 ? "danger" : "warn";

  return (
    <section className={`mkx mkx-${tone}`} aria-label="Exposure verdict">
      <div className="mkx-head">
        <div className="mkx-number">
          <span className="mkx-label">Exposure</span>
          <strong>{exposure}%</strong>
          <span className="mkx-band">
            {verdict.band}
            {verdict.direction !== "unknown" ? (
              <em>
                <DirIcon size={13} aria-hidden /> {DIRECTION_WORD[verdict.direction]}
              </em>
            ) : null}
          </span>
        </div>

        {/* The ladder makes the rule visible: you can see which step you are on
            and how far the win rate must move to change it. */}
        <div className="mkx-ladder" role="img" aria-label={`Exposure ${exposure} percent of full size`}>
          {[25, 50, 75, 100].map((step) => (
            <span
              key={step}
              className={`mkx-step${exposure >= step ? " on" : ""}`}
              data-step={step}
            >
              {step}%
            </span>
          ))}
        </div>
      </div>

      <p className="mkx-why">{verdict.why}</p>

      <dl className="mkx-arith">
        <div>
          <dt>Breakouts paying</dt>
          <dd>{pct(verdict.win_rate, 2)}</dd>
        </div>
        <div>
          <dt>Break-even needs</dt>
          <dd>{pct(verdict.breakeven_win_rate)}</dd>
        </div>
        <div>
          <dt>{verdict.clears_breakeven ? "Clearing by" : "Short by"}</dt>
          <dd className={verdict.clears_breakeven ? "pos" : "neg"}>
            {verdict.clears_breakeven
              ? pct((verdict.win_rate ?? 0) - (verdict.breakeven_win_rate ?? 0), 2)
              : pct(verdict.shortfall_pts, 2)}
          </dd>
        </div>
        <div>
          <dt>Expected per trade</dt>
          <dd className={(verdict.expected_pct_per_trade ?? 0) >= 0 ? "pos" : "neg"}>
            {pct(verdict.expected_pct_per_trade, 2)}
          </dd>
        </div>
      </dl>

      <p className="mkx-basis">
        Measured over {verdict.weeks_used.length} fully-resolved weeks
        {verdict.weeks_used.length ? ` (${verdict.weeks_used.join(", ")})` : ""}.
        {verdict.weeks_excluded_unresolved.length ? (
          <>
            {" "}
            {verdict.weeks_excluded_unresolved.join(" and ")} excluded — still open, and
            open weeks read high because winners resolve faster than losers.
          </>
        ) : null}{" "}
        This describes conditions now; it is not a forecast.
      </p>
    </section>
  );
}
