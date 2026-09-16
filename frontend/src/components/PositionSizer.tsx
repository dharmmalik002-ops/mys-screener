import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, Ban, Check } from "lucide-react";
import {
  computeSizing,
  stopPctFromPrice,
  stopPriceFromPct,
  type SizingResult,
} from "../lib/positionSizing";
import { type ChargesConfig, type Product } from "../lib/chargesCalculator";
import { type RegimeGate } from "../lib/journalInsights";
import "./PositionSizer.css";

/** Preferences are the trader's own rules, so they persist across sessions —
 *  a risk limit you have to retype every morning is a risk limit you will
 *  eventually skip typing. */
const LS_PREFS = "positionSizerPrefs";

type Prefs = {
  riskPctOfEquity: number;
  maxPositionPct: number;
  minRewardRisk: number;
  product: Product;
};

const DEFAULT_PREFS: Prefs = {
  // 1% of equity per trade is the conventional swing-trading ceiling; the
  // point of the field is that it is the trader's to set, not ours.
  riskPctOfEquity: 1,
  maxPositionPct: 25,
  minRewardRisk: 2,
  product: "delivery",
};

export type PositionSizerSeed = {
  symbol?: string;
  entry?: number;
  stop?: number;
  target?: number;
};

type Props = {
  equity: number;
  chargesConfig: ChargesConfig;
  /** Prefill from a scan row or an open position so nothing is retyped. */
  seed?: PositionSizerSeed | null;
  /** Defaults applied to a fresh entry price when no stop/target is supplied:
   *  the user's own 3% stop and 5% target. */
  defaultStopPct?: number;
  defaultTargetPct?: number;
  /** The market-regime haircut, derived from the trader's own record. Only
   *  ever reduces the suggested size. */
  regimeGate?: RegimeGate | null;
};

function money(value: number): string {
  return value.toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

function price(value: number): string {
  return value.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function num(text: string): number {
  const parsed = parseFloat(text);
  return Number.isFinite(parsed) ? parsed : 0;
}

function Verdict({ result }: { result: SizingResult }) {
  if (result.blockers.length) {
    return (
      <div className="psz-verdict psz-verdict--blocked">
        <Ban size={16} aria-hidden />
        <div>
          <strong>Do not take this trade as specified.</strong>
          <ul>{result.blockers.map((b) => <li key={b}>{b}</li>)}</ul>
        </div>
      </div>
    );
  }
  if (result.verdict === "reduced") {
    return (
      <div className="psz-verdict psz-verdict--reduced">
        <AlertTriangle size={16} aria-hidden />
        <div>
          <strong>Sizeable, with caveats.</strong>
          <ul>{result.warnings.map((w) => <li key={w}>{w}</li>)}</ul>
        </div>
      </div>
    );
  }
  return (
    <div className="psz-verdict psz-verdict--ok">
      <Check size={16} aria-hidden />
      <div>
        <strong>Within your rules.</strong>
        <p>
          This size risks {result.riskPctActual.toFixed(2)}% of the account and the reward justifies it.
          Nothing here says the setup is good — only that if you are wrong, the loss is one you planned for.
        </p>
      </div>
    </div>
  );
}

/**
 * Pre-trade position sizer.
 *
 * The stop is entered as a PRICE, not only as a percentage, because that is
 * how the decision is actually made: the stop belongs under the pivot or the
 * base low that the chart gives you, and the percentage is whatever that
 * happens to be. Entering a round 2% and deriving the stop from it inverts the
 * reasoning — it lets the position size choose the stop.
 */
export function PositionSizer({
  equity,
  chargesConfig,
  seed,
  defaultStopPct = 3,
  defaultTargetPct = 5,
  regimeGate = null,
}: Props) {
  const [prefs, setPrefs] = useState<Prefs>(() => {
    try {
      const raw = window.localStorage.getItem(LS_PREFS);
      return raw ? { ...DEFAULT_PREFS, ...(JSON.parse(raw) as Partial<Prefs>) } : DEFAULT_PREFS;
    } catch {
      return DEFAULT_PREFS;
    }
  });
  const savePrefs = useCallback((next: Partial<Prefs>) => {
    setPrefs((prev) => {
      const merged = { ...prev, ...next };
      try {
        window.localStorage.setItem(LS_PREFS, JSON.stringify(merged));
      } catch {
        /* private mode — the preference just does not persist */
      }
      return merged;
    });
  }, []);

  const [equityText, setEquityText] = useState(String(equity || ""));
  const [entryText, setEntryText] = useState("");
  const [stopText, setStopText] = useState("");
  const [targetText, setTargetText] = useState("");
  const [symbol, setSymbol] = useState("");

  useEffect(() => {
    setEquityText(String(equity || ""));
  }, [equity]);

  // A seed from a scan row fills the whole form, deriving the stop and target
  // from the user's own parameters when the row does not carry them.
  useEffect(() => {
    if (!seed) return;
    setSymbol(seed.symbol ?? "");
    if (seed.entry && seed.entry > 0) {
      setEntryText(seed.entry.toFixed(2));
      setStopText((seed.stop ?? stopPriceFromPct(seed.entry, defaultStopPct)).toFixed(2));
      setTargetText((seed.target ?? seed.entry * (1 + defaultTargetPct / 100)).toFixed(2));
    }
  }, [seed, defaultStopPct, defaultTargetPct]);

  const entry = num(entryText);
  const stop = num(stopText);

  // Typing an entry with no stop yet fills in the trader's default stop and
  // target rather than leaving the calculation dead.
  const applyDefaults = useCallback(() => {
    if (!(entry > 0)) return;
    if (!stopText) setStopText(stopPriceFromPct(entry, defaultStopPct).toFixed(2));
    if (!targetText) setTargetText((entry * (1 + defaultTargetPct / 100)).toFixed(2));
  }, [entry, stopText, targetText, defaultStopPct, defaultTargetPct]);

  const result = useMemo(
    () =>
      computeSizing({
        equity: num(equityText),
        entry,
        stop,
        target: num(targetText) || null,
        riskPctOfEquity: prefs.riskPctOfEquity,
        maxPositionPct: prefs.maxPositionPct,
        minRewardRisk: prefs.minRewardRisk,
        product: prefs.product,
        charges: chargesConfig,
        sizeMultiplier: regimeGate?.active ? regimeGate.multiplier : 1,
        sizeMultiplierReason: regimeGate?.active ? regimeGate.reason : undefined,
      }),
    [equityText, entry, stop, targetText, prefs, chargesConfig, regimeGate],
  );

  const stopPct = stopPctFromPrice(entry, stop);
  const ready = result.blockers.length === 0;

  return (
    <div className="psz">
      <div className="psz-grid">
        <label className="psz-field">
          <span>Account equity (₹)</span>
          <input type="number" value={equityText} onChange={(e) => setEquityText(e.target.value)} />
        </label>
        <label className="psz-field">
          <span>Risk per trade (% of equity)</span>
          <input
            type="number" step="0.05" value={prefs.riskPctOfEquity}
            onChange={(e) => savePrefs({ riskPctOfEquity: num(e.target.value) })}
          />
        </label>
        <label className="psz-field">
          <span>Max position (% of equity)</span>
          <input
            type="number" step="1" value={prefs.maxPositionPct}
            onChange={(e) => savePrefs({ maxPositionPct: num(e.target.value) })}
          />
        </label>
        <label className="psz-field">
          <span>Minimum reward:risk</span>
          <input
            type="number" step="0.1" value={prefs.minRewardRisk}
            onChange={(e) => savePrefs({ minRewardRisk: num(e.target.value) })}
          />
        </label>
      </div>

      <div className="psz-grid psz-grid--trade">
        <label className="psz-field">
          <span>Symbol <em>optional</em></span>
          <input value={symbol} onChange={(e) => setSymbol(e.target.value.toUpperCase())} placeholder="e.g. TITAN" />
        </label>
        <label className="psz-field">
          <span>Entry price (₹)</span>
          <input
            type="number" step="any" value={entryText}
            onChange={(e) => setEntryText(e.target.value)} onBlur={applyDefaults}
          />
        </label>
        <label className="psz-field">
          <span>
            Stop price (₹)
            {entry > 0 && stop > 0 && stop < entry ? <em>{stopPct.toFixed(2)}% below entry</em> : null}
          </span>
          <input type="number" step="any" value={stopText} onChange={(e) => setStopText(e.target.value)} />
        </label>
        <label className="psz-field">
          <span>
            Target price (₹)
            {result.targetDistancePct !== null ? <em>{result.targetDistancePct.toFixed(2)}% above entry</em> : null}
          </span>
          <input type="number" step="any" value={targetText} onChange={(e) => setTargetText(e.target.value)} />
        </label>
        <label className="psz-field">
          <span>Product</span>
          <select value={prefs.product} onChange={(e) => savePrefs({ product: e.target.value as Product })}>
            <option value="delivery">Delivery (CNC)</option>
            <option value="intraday">Intraday (MIS)</option>
          </select>
        </label>
      </div>

      {regimeGate ? (
        <p className={regimeGate.active ? "psz-gate psz-gate--on" : "psz-gate"}>
          <strong>Market regime{regimeGate.regime ? `: ${regimeGate.regime}` : ""}</strong> — {regimeGate.reason}
        </p>
      ) : null}

      <Verdict result={result} />

      {ready ? (
        <>
          <div className="psz-answer">
            <div className="psz-answer-main">
              <span className="psz-answer-label">Buy</span>
              <strong>{result.shares.toLocaleString("en-IN")}</strong>
              <span className="psz-answer-unit">shares{symbol ? ` of ${symbol}` : ""}</span>
            </div>
            <p className="psz-answer-sub">
              ₹{money(result.positionValue)} deployed · {result.positionPctOfEquity.toFixed(1)}% of the account ·
              risking ₹{money(result.riskAmount)} ({result.riskPctActual.toFixed(2)}%) if the stop is hit
              {result.limitedBy === "weight" ? " · held back by your position cap" : ""}
              {result.limitedBy === "gate"
                ? ` · cut from ${result.sharesBeforeMultiplier.toLocaleString("en-IN")} by the regime gate`
                : ""}
            </p>
          </div>

          <div className="psz-stats">
            <div><span>Risk per share</span><strong>₹{price(result.riskPerShare)}</strong></div>
            <div><span>Stop distance</span><strong>{result.stopDistancePct.toFixed(2)}%</strong></div>
            <div>
              <span>Reward : risk</span>
              <strong>{result.rewardRisk === null ? "—" : `${result.rewardRisk.toFixed(2)} : 1`}</strong>
            </div>
            <div>
              <span>After charges</span>
              <strong>{result.rewardRiskNet === null ? "—" : `${result.rewardRiskNet.toFixed(2)} : 1`}</strong>
            </div>
            <div><span>Round-trip cost</span><strong>₹{money(result.roundTripCharges)}</strong></div>
            <div><span>Breakeven move</span><strong>{result.breakevenMovePct.toFixed(2)}%</strong></div>
            <div className="psz-neg"><span>Loss at stop</span><strong>−₹{money(result.netLossAtStop)}</strong></div>
            <div className="psz-pos">
              <span>Gain at target</span>
              <strong>{result.netGainAtTarget === null ? "—" : `+₹${money(result.netGainAtTarget)}`}</strong>
            </div>
          </div>

          <p className="psz-note">
            Both figures above are net of brokerage and statutory charges, so the reward:risk is what you
            actually keep — not what the chart promises. Edit the rates under ⚙ Charges.
          </p>
        </>
      ) : null}
    </div>
  );
}
