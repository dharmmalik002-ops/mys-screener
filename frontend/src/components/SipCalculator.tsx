import { useEffect, useMemo, useState } from "react";

import "./SipCalculator.css";

/**
 * SIP projection calculator.
 *
 * This is arithmetic on assumptions the user types, not a forecast. The return
 * rate is an input, not a prediction — so the output is shown across a range
 * of rates rather than as a single number, because a single number invites
 * being read as what *will* happen.
 *
 * Two things most SIP calculators get wrong and this one does not:
 *
 * 1. **Inflation.** A 15-year corpus quoted in nominal rupees badly overstates
 *    what it buys. Every figure here carries its inflation-adjusted twin.
 * 2. **Contribution timing.** Instalments are compounded from the date each
 *    one goes in (annuity-due), not as a lump sum at the start or end of the
 *    year — the difference over 20 years is not small.
 */

type Frequency = { key: string; label: string; perYear: number };

const FREQUENCIES: Frequency[] = [
  { key: "weekly", label: "Weekly", perYear: 52 },
  { key: "fortnightly", label: "Fortnightly", perYear: 26 },
  { key: "monthly", label: "Monthly", perYear: 12 },
  { key: "quarterly", label: "Quarterly", perYear: 4 },
];

// Shown alongside whatever rate the user picks, so the answer reads as a range
// of outcomes rather than a promise.
const SCENARIO_RATES = [8, 10, 12, 14];

const rupees = (value: number): string => {
  if (!Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 1e7) return `₹${(value / 1e7).toFixed(2)} cr`;
  if (Math.abs(value) >= 1e5) return `₹${(value / 1e5).toFixed(2)} L`;
  return `₹${Math.round(value).toLocaleString("en-IN")}`;
};

/**
 * Future value of a recurring contribution, compounded per instalment, with an
 * optional annual step-up applied on each anniversary.
 */
function project({
  contribution,
  perYear,
  years,
  annualReturnPct,
  stepUpPct,
  lumpSum = 0,
}: {
  contribution: number;
  perYear: number;
  years: number;
  annualReturnPct: number;
  stepUpPct: number;
  /** A starting balance that compounds alongside the instalments — normally
   *  what the portfolio is already worth, so the projection continues from
   *  where things actually stand rather than from zero. */
  lumpSum?: number;
}): { invested: number; value: number } {
  const periods = Math.round(perYear * years);
  // Per-period rate from the annual rate, compounded — not annual/12, which
  // quietly understates the result.
  const ratePerPeriod = (1 + annualReturnPct / 100) ** (1 / perYear) - 1;
  let value = lumpSum;
  let invested = lumpSum;
  let amount = contribution;
  for (let period = 0; period < periods; period += 1) {
    if (period > 0 && period % perYear === 0 && stepUpPct) {
      amount *= 1 + stepUpPct / 100;
    }
    invested += amount;
    // Contribution goes in at the start of the period, then the whole balance
    // earns for that period.
    value = (value + amount) * (1 + ratePerPeriod);
  }
  return { invested, value };
}

export function SipCalculator({
  currentValue,
  monthlySip,
}: {
  /** Today's portfolio value, used as the default starting balance. */
  currentValue?: number | null;
  /** Monthly SIP already recorded, used as the default instalment. */
  monthlySip?: number | null;
} = {}) {
  const [amount, setAmount] = useState(5000);
  const [lumpSum, setLumpSum] = useState<number>(Math.round(currentValue ?? 0));
  const [lumpTouched, setLumpTouched] = useState(false);

  // Track the live portfolio until the user overrides it, so the projection
  // starts from what they actually hold today rather than a stale number.
  useEffect(() => {
    if (!lumpTouched && typeof currentValue === "number") setLumpSum(Math.round(currentValue));
  }, [currentValue, lumpTouched]);
  const [frequency, setFrequency] = useState(FREQUENCIES[0]);
  const [amountTouched, setAmountTouched] = useState(false);

  useEffect(() => {
    // Seed the instalment from a recorded SIP so the default projection
    // reflects what is actually being invested.
    if (!amountTouched && typeof monthlySip === "number" && monthlySip > 0) {
      setAmount(Math.round(monthlySip / (FREQUENCIES[0].perYear / 12)));
    }
  }, [monthlySip, amountTouched]);
  const [years, setYears] = useState(15);
  const [returnPct, setReturnPct] = useState(12);
  const [inflationPct, setInflationPct] = useState(6);
  const [stepUpPct, setStepUpPct] = useState(0);

  const result = useMemo(() => {
    const base = project({
      contribution: amount,
      perYear: frequency.perYear,
      years,
      annualReturnPct: returnPct,
      stepUpPct,
      lumpSum,
    });
    const realDivisor = (1 + inflationPct / 100) ** years;
    return {
      ...base,
      gain: base.value - base.invested,
      // What the corpus buys in today's money — the number that actually
      // answers "will this be enough".
      realValue: base.value / realDivisor,
      realGain: base.value / realDivisor - base.invested,
      multiple: base.invested > 0 ? base.value / base.invested : 0,
    };
  }, [amount, frequency, years, returnPct, inflationPct, stepUpPct]);

  const scenarios = useMemo(
    () =>
      SCENARIO_RATES.map((rate) => {
        const projected = project({
          contribution: amount,
          perYear: frequency.perYear,
          years,
          annualReturnPct: rate,
          stepUpPct,
          lumpSum,
        });
        return {
          rate,
          value: projected.value,
          real: projected.value / (1 + inflationPct / 100) ** years,
        };
      }),
    [amount, frequency, years, inflationPct, stepUpPct, lumpSum],
  );

  const maxScenario = Math.max(...scenarios.map((s) => s.value), 1);

  return (
    <section className="sipc">
      <header className="sipc-head">
        <h3>SIP calculator</h3>
        <p>
          Arithmetic on the assumptions you set below — not a forecast. The return rate is
          your input, so the table shows a spread of rates rather than one answer.
        </p>
      </header>

      <div className="sipc-controls">
        <label className="sipc-field">
          <span>
            Starting balance
            {typeof currentValue === "number" && !lumpTouched ? " · your portfolio today" : ""}
          </span>
          <div className="sipc-input-row">
            <em>₹</em>
            <input
              type="number"
              min={0}
              step={10000}
              value={lumpSum}
              onChange={(event) => {
                setLumpTouched(true);
                setLumpSum(Math.max(0, Number(event.target.value) || 0));
              }}
            />
            {lumpTouched && typeof currentValue === "number" ? (
              <button
                type="button"
                className="sipc-reset"
                title="Reset to your current portfolio value"
                onClick={() => { setLumpTouched(false); setLumpSum(Math.round(currentValue)); }}
              >
                reset
              </button>
            ) : null}
          </div>
        </label>

        <label className="sipc-field">
          <span>Instalment</span>
          <div className="sipc-input-row">
            <em>₹</em>
            <input
              type="number"
              min={100}
              step={500}
              value={amount}
              onChange={(event) => {
                setAmountTouched(true);
                setAmount(Math.max(0, Number(event.target.value) || 0));
              }}
            />
          </div>
        </label>

        <label className="sipc-field">
          <span>Frequency</span>
          <div className="sipc-seg">
            {FREQUENCIES.map((option) => (
              <button
                key={option.key}
                type="button"
                className={frequency.key === option.key ? "active" : ""}
                onClick={() => setFrequency(option)}
              >
                {option.label}
              </button>
            ))}
          </div>
        </label>

        <label className="sipc-field">
          <span>For {years} years</span>
          <input
            type="range"
            min={1}
            max={40}
            value={years}
            onChange={(event) => setYears(Number(event.target.value))}
          />
        </label>

        <label className="sipc-field">
          <span>Assumed return {returnPct}% a year</span>
          <input
            type="range"
            min={1}
            max={25}
            step={0.5}
            value={returnPct}
            onChange={(event) => setReturnPct(Number(event.target.value))}
          />
        </label>

        <label className="sipc-field">
          <span>Inflation {inflationPct}% a year</span>
          <input
            type="range"
            min={0}
            max={12}
            step={0.5}
            value={inflationPct}
            onChange={(event) => setInflationPct(Number(event.target.value))}
          />
        </label>

        <label className="sipc-field">
          <span>Step up instalment {stepUpPct}% a year</span>
          <input
            type="range"
            min={0}
            max={25}
            value={stepUpPct}
            onChange={(event) => setStepUpPct(Number(event.target.value))}
          />
        </label>
      </div>

      <div className="sipc-results">
        <div className="sipc-result">
          <span>You put in</span>
          <strong>{rupees(result.invested)}</strong>
          <small>
            {lumpSum ? `${rupees(lumpSum)} to start plus ` : ""}
            {Math.round(frequency.perYear * years).toLocaleString("en-IN")} instalments
            {stepUpPct ? `, stepped up ${stepUpPct}% a year` : ""}
          </small>
        </div>
        <div className="sipc-result">
          <span>Nominal value</span>
          <strong className="pos">{rupees(result.value)}</strong>
          <small>{result.multiple.toFixed(2)}× what you put in</small>
        </div>
        <div className="sipc-result sipc-result-key">
          <span>Worth in today's money</span>
          <strong className="pos">{rupees(result.realValue)}</strong>
          <small>after {inflationPct}% inflation for {years} years</small>
        </div>
        <div className="sipc-result">
          <span>Real gain</span>
          <strong className={result.realGain < 0 ? "neg" : "pos"}>{rupees(result.realGain)}</strong>
          <small>above what you put in, in today's money</small>
        </div>
      </div>

      <div className="sipc-scenarios">
        <h4>If the return turns out different</h4>
        {scenarios.map((scenario) => (
          <div className="sipc-scenario" key={scenario.rate}>
            <span>{scenario.rate}%</span>
            <i className="sipc-bar">
              <i style={{ width: `${(scenario.value / maxScenario) * 100}%` }} />
            </i>
            <em>{rupees(scenario.value)}</em>
            <b>{rupees(scenario.real)}</b>
          </div>
        ))}
        <p className="sipc-foot">
          Nominal, then the same figure in today's money. Equity returns are not smooth —
          a real SIP arrives at its outcome through drawdowns of the kind shown on each
          fund's chart, not on a straight line.
        </p>
      </div>
    </section>
  );
}
