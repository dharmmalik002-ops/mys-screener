// ─── Dhan / Indian-equity charges calculator ────────────────────────────────────
// Pure, side-effect-free helpers that turn a buy/sell turnover pair into an
// itemised brokerage + statutory-charges breakdown, so the journal can show
// after-cost (net) P&L. All rates live in DEFAULT_CHARGES and are editable by the
// user (a settings popover persists overrides to localStorage), since SEBI /
// exchange / broker rates change periodically.

export type Product = "delivery" | "intraday";

export interface ChargesConfig {
  // Brokerage — Dhan: delivery free; intraday ₹20 or pct% per order, whichever lower.
  deliveryBrokerageFlat: number; // ₹ per order (0 = free)
  deliveryBrokeragePct: number; // % of turnover per order
  intradayBrokerageFlat: number; // ₹ cap per order
  intradayBrokeragePct: number; // % of turnover per order
  // STT (Securities Transaction Tax)
  sttDeliveryPct: number; // % on BOTH buy + sell
  sttIntradayPct: number; // % on SELL only
  // Exchange transaction charges (NSE equity)
  exchangePct: number; // % on buy + sell
  // SEBI turnover fee
  sebiPct: number; // % on buy + sell (₹10 / crore = 0.0001%)
  // Stamp duty (buy side only)
  stampDeliveryPct: number; // % on buy
  stampIntradayPct: number; // % on buy
  // GST on (brokerage + exchange + SEBI)
  gstPct: number; // %
  // DP charges — delivery SELL only, flat per scrip (+GST)
  dpCharge: number; // ₹
}

export interface ChargesBreakdown {
  brokerage: number;
  stt: number;
  exchange: number;
  sebi: number;
  stamp: number;
  gst: number;
  dp: number;
  total: number;
}

// Dhan defaults (NSE equity), as of 2026. Editable by the user.
export const DEFAULT_CHARGES: ChargesConfig = {
  deliveryBrokerageFlat: 0,
  deliveryBrokeragePct: 0,
  intradayBrokerageFlat: 20,
  intradayBrokeragePct: 0.03,
  sttDeliveryPct: 0.1,
  sttIntradayPct: 0.025,
  exchangePct: 0.00297,
  sebiPct: 0.0001,
  stampDeliveryPct: 0.015,
  stampIntradayPct: 0.003,
  gstPct: 18,
  dpCharge: 13.5,
};

const pct = (value: number, rate: number) => (value * rate) / 100;

// Brokerage for a single order leg. flat===0 && pct===0 ⇒ free; otherwise the
// lower of the flat cap and the percentage (Dhan's "₹20 or 0.03%, whichever lower").
function brokeragePerLeg(turnover: number, flat: number, pctRate: number): number {
  if (flat <= 0 && pctRate <= 0) return 0;
  if (pctRate <= 0) return flat;
  const byPct = pct(turnover, pctRate);
  if (flat <= 0) return byPct;
  return Math.min(flat, byPct);
}

export interface ComputeChargesArgs {
  buyValue: number; // entry turnover = entryPx * qty
  sellValue: number; // exit turnover = exitPx * qty
  product: Product;
  config?: ChargesConfig;
}

export function computeCharges({ buyValue, sellValue, product, config = DEFAULT_CHARGES }: ComputeChargesArgs): ChargesBreakdown {
  const buy = Math.max(0, buyValue || 0);
  const sell = Math.max(0, sellValue || 0);
  const isDelivery = product !== "intraday";

  const flat = isDelivery ? config.deliveryBrokerageFlat : config.intradayBrokerageFlat;
  const bRate = isDelivery ? config.deliveryBrokeragePct : config.intradayBrokeragePct;
  const brokerage = brokeragePerLeg(buy, flat, bRate) + brokeragePerLeg(sell, flat, bRate);

  const stt = isDelivery
    ? pct(buy + sell, config.sttDeliveryPct)
    : pct(sell, config.sttIntradayPct);

  const exchange = pct(buy + sell, config.exchangePct);
  const sebi = pct(buy + sell, config.sebiPct);
  const stamp = pct(buy, isDelivery ? config.stampDeliveryPct : config.stampIntradayPct);
  const gst = pct(brokerage + exchange + sebi, config.gstPct);
  const dp = isDelivery ? config.dpCharge * (1 + config.gstPct / 100) : 0;

  const round = (n: number) => Math.round(n * 100) / 100;
  const brokerageR = round(brokerage);
  const sttR = round(stt);
  const exchangeR = round(exchange);
  const sebiR = round(sebi);
  const stampR = round(stamp);
  const gstR = round(gst);
  const dpR = round(dp);
  const total = round(brokerageR + sttR + exchangeR + sebiR + stampR + gstR + dpR);

  return { brokerage: brokerageR, stt: sttR, exchange: exchangeR, sebi: sebiR, stamp: stampR, gst: gstR, dp: dpR, total };
}

// % the price must move from entry just to cover round-trip charges (breakeven).
export function breakevenPct(positionValue: number, breakdown: ChargesBreakdown): number {
  if (!positionValue || positionValue <= 0) return 0;
  return (breakdown.total / positionValue) * 100;
}

export const CHARGE_LABELS: Record<keyof ChargesBreakdown, string> = {
  brokerage: "Brokerage",
  stt: "STT",
  exchange: "Exchange txn",
  sebi: "SEBI fee",
  stamp: "Stamp duty",
  gst: "GST (18%)",
  dp: "DP charges",
  total: "Total charges",
};
