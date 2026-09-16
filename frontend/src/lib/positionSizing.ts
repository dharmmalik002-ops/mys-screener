/**
 * Pre-trade position sizing.
 *
 * Why this is a module and not four lines in a component
 * ------------------------------------------------------
 * This is the one calculation in the app that decides how much money is at
 * risk, so it is kept pure, named, and documented: every rule below is a
 * deliberate choice about someone's capital, and each is commented with what
 * goes wrong if it is changed.
 *
 * The sizer answers one question — "given my account, my entry and where the
 * chart says my stop belongs, how many shares may I buy?" — and then tells the
 * trader when the honest answer is "fewer than you wanted" or "none".
 *
 * Worked example used to check the arithmetic by hand:
 *   equity 1,000,000 · risk 1% · entry 500 · stop 485 · target 545
 *   risk budget      = 10,000
 *   risk per share   = 15            (stop is 3.00% below entry)
 *   shares by risk   = floor(10,000 / 15)      = 666
 *   shares by weight = floor(250,000 / 500)    = 500   (25% cap binds)
 *   shares           = 500  → limited by weight
 *   position value   = 250,000 (25.0% of equity)
 *   actual risk      = 7,500 (0.75% of equity)
 *   reward/share     = 45 → gross R = 3.00
 */

import {
  breakevenPct,
  computeCharges,
  DEFAULT_CHARGES,
  type ChargesConfig,
  type Product,
} from "./chargesCalculator";

export type SizingInputs = {
  equity: number;
  entry: number;
  /** Stop as a price. The chart decides this, not a round percentage. */
  stop: number;
  /** Optional profit target; without it there is no reward side to judge. */
  target?: number | null;
  /** Share of the account the trader is willing to lose on this one trade. */
  riskPctOfEquity: number;
  /** Cap on what any single position may become, as a share of equity. */
  maxPositionPct: number;
  product: Product;
  charges?: ChargesConfig;
  /** Below this, the trade is flagged as not worth its own risk. */
  minRewardRisk?: number;
};

export type SizingVerdict = "take" | "reduced" | "blocked";

export type SizingResult = {
  verdict: SizingVerdict;
  /** Reasons the trade cannot be sized at all. */
  blockers: string[];
  /** Reasons to think twice, or explanations of why the size shrank. */
  warnings: string[];

  stopDistancePct: number;
  riskPerShare: number;
  riskBudget: number;

  sharesByRisk: number;
  sharesByWeight: number;
  shares: number;
  limitedBy: "risk" | "weight" | null;

  positionValue: number;
  positionPctOfEquity: number;
  riskAmount: number;
  riskPctActual: number;

  targetDistancePct: number | null;
  rewardAmount: number | null;
  rewardRisk: number | null;
  rewardRiskNet: number | null;

  roundTripCharges: number;
  breakevenMovePct: number;
  netLossAtStop: number;
  netGainAtTarget: number | null;
};

const EMPTY: SizingResult = {
  verdict: "blocked",
  blockers: [],
  warnings: [],
  stopDistancePct: 0,
  riskPerShare: 0,
  riskBudget: 0,
  sharesByRisk: 0,
  sharesByWeight: 0,
  shares: 0,
  limitedBy: null,
  positionValue: 0,
  positionPctOfEquity: 0,
  riskAmount: 0,
  riskPctActual: 0,
  targetDistancePct: null,
  rewardAmount: null,
  rewardRisk: null,
  rewardRiskNet: null,
  roundTripCharges: 0,
  breakevenMovePct: 0,
  netLossAtStop: 0,
  netGainAtTarget: null,
};

/** Stop price implied by a percentage below entry, and the inverse. Both exist
 *  so the UI can offer either input without the two drifting apart. */
export function stopPriceFromPct(entry: number, pct: number): number {
  return entry > 0 && pct > 0 ? entry * (1 - pct / 100) : 0;
}

export function stopPctFromPrice(entry: number, stop: number): number {
  return entry > 0 && stop > 0 ? ((entry - stop) / entry) * 100 : 0;
}

export function computeSizing(inputs: SizingInputs): SizingResult {
  const {
    equity, entry, stop, target,
    riskPctOfEquity, maxPositionPct, product,
    charges = DEFAULT_CHARGES,
    minRewardRisk = 2,
  } = inputs;

  const blockers: string[] = [];
  if (!(equity > 0)) blockers.push("Enter your account equity — size cannot be derived without it.");
  if (!(entry > 0)) blockers.push("Enter an entry price.");
  if (!(stop > 0)) blockers.push("Enter a stop price. Where the stop goes is the trade's real decision.");
  else if (entry > 0 && stop >= entry) {
    blockers.push("The stop is at or above the entry. A long's stop has to sit below where you buy.");
  }
  if (!(riskPctOfEquity > 0)) blockers.push("Set the share of equity you are willing to risk.");
  if (blockers.length) return { ...EMPTY, blockers };

  const riskPerShare = entry - stop;
  const stopDistancePct = (riskPerShare / entry) * 100;
  const riskBudget = equity * (riskPctOfEquity / 100);

  // Both caps floor rather than round: rounding up would put more on the line
  // than the budget the trader just set, which is the one thing this tool
  // exists to prevent.
  const sharesByRisk = Math.floor(riskBudget / riskPerShare);
  const weightCapValue = equity * (maxPositionPct > 0 ? maxPositionPct / 100 : 1);
  const sharesByWeight = Math.floor(weightCapValue / entry);
  const shares = Math.max(0, Math.min(sharesByRisk, sharesByWeight));

  const warnings: string[] = [];
  let limitedBy: "risk" | "weight" | null = null;
  if (shares > 0) {
    if (sharesByWeight < sharesByRisk) {
      limitedBy = "weight";
      warnings.push(
        `Your risk budget would allow ${sharesByRisk.toLocaleString()} shares, but the ` +
        `${maxPositionPct}% position cap holds it to ${shares.toLocaleString()}. A stop this tight ` +
        `lets you buy more than you should concentrate in one name.`,
      );
    } else if (sharesByRisk < sharesByWeight) {
      limitedBy = "risk";
    }
  }

  const positionValue = shares * entry;
  const positionPctOfEquity = equity > 0 ? (positionValue / equity) * 100 : 0;
  const riskAmount = shares * riskPerShare;
  const riskPctActual = equity > 0 ? (riskAmount / equity) * 100 : 0;

  const hasTarget = typeof target === "number" && target > entry;
  const targetDistancePct = hasTarget ? ((target - entry) / entry) * 100 : null;
  const rewardAmount = hasTarget ? shares * (target - entry) : null;
  const rewardRisk = hasTarget ? (target - entry) / riskPerShare : null;

  // Charges are computed on the real round trip — bought at entry, sold at the
  // target (or the stop) — rather than assuming the exit equals the entry. The
  // difference is small per trade and decides the R multiple over a year.
  const exitValueAtTarget = hasTarget ? shares * (target as number) : positionValue;
  const atTarget = computeCharges({
    buyValue: positionValue, sellValue: exitValueAtTarget, product, config: charges,
  });
  const atStop = computeCharges({
    buyValue: positionValue, sellValue: shares * stop, product, config: charges,
  });
  const breakevenMovePct = positionValue > 0 ? breakevenPct(positionValue, atTarget) : 0;
  const netLossAtStop = riskAmount + atStop.total;
  const netGainAtTarget = rewardAmount === null ? null : rewardAmount - atTarget.total;
  const rewardRiskNet =
    netGainAtTarget === null || netLossAtStop <= 0 ? null : netGainAtTarget / netLossAtStop;

  if (shares <= 0) {
    blockers.push(
      "This trade cannot be sized: one share already risks more than your budget allows. " +
      "Either the stop is too far from the entry, or the stock is too expensive for this account.",
    );
  }

  // A stop closer than the round-trip cost is not a stop, it is a fee. Worth
  // saying out loud because it is invisible until the statement arrives.
  if (shares > 0 && stopDistancePct > 0 && stopDistancePct < breakevenMovePct) {
    warnings.push(
      `The stop is ${stopDistancePct.toFixed(2)}% away but charges alone cost ${breakevenMovePct.toFixed(2)}%. ` +
      `You would lose money on this trade even if it never moved against you.`,
    );
  }
  if (shares > 0 && stopDistancePct > 10) {
    warnings.push(
      `A ${stopDistancePct.toFixed(1)}% stop is wide for a swing trade. It is not wrong, but it means ` +
      `a small position and a long wait to be proved right.`,
    );
  }
  if (rewardRisk !== null && rewardRisk < minRewardRisk) {
    warnings.push(
      `Reward-to-risk is ${rewardRisk.toFixed(2)}, below your ${minRewardRisk} floor. At this ratio you ` +
      `need an uncomfortably high win rate just to break even.`,
    );
  }
  if (!hasTarget) {
    warnings.push(
      "No target set, so there is no reward side to judge. Sizing is only half the decision — " +
      "a trade you cannot state a target for is a trade you cannot evaluate.",
    );
  }

  const verdict: SizingVerdict =
    shares <= 0 ? "blocked" : limitedBy === "weight" || warnings.length > 0 ? "reduced" : "take";

  return {
    verdict,
    blockers,
    warnings,
    stopDistancePct,
    riskPerShare,
    riskBudget,
    sharesByRisk,
    sharesByWeight,
    shares,
    limitedBy,
    positionValue,
    positionPctOfEquity,
    riskAmount,
    riskPctActual,
    targetDistancePct,
    rewardAmount,
    rewardRisk,
    rewardRiskNet,
    roundTripCharges: atTarget.total,
    breakevenMovePct,
    netLossAtStop,
    netGainAtTarget,
  };
}
