import type { ChartColorSettings } from "../components/ChartPanel";
import { CANDLE_DOWN, CANDLE_UP } from "./marketColors";

export const DEFAULT_CHART_COLORS: ChartColorSettings = {
  // House palette: warm, low-saturation moving averages that sit behind the
  // candles instead of competing with them. Each stays distinct from the
  // others and from both candle colours at a glance.
  ema10: "#e8a07a",
  ema20: "#d4af6a",
  ema50: "#7fb4d9",
  ema200: "#a39e93",
  vwap: "#b39ddb",
  // Teal/coral candles (TradingView's current pair): calm, high up-vs-down
  // contrast, easy on the eyes for long sessions.
  candleUp: CANDLE_UP,
  candleDown: CANDLE_DOWN,
  candleExpansion: "#e6b04e",
  volumeUp: CANDLE_UP,
  volumeDown: CANDLE_DOWN,
  rsLine: "#c3a6ec",
  rsMarker: "#c3a6ec",
  rsMarkerSize: 4,
};
/**
 * Shared crosshair for every lightweight-charts surface outside ChartPanel:
 * a fine dashed line in the house accent with warm-charcoal axis pills. The
 * canvas cannot read CSS variables, so the accent is resolved at call time
 * from the live theme.
 */
export function premiumCrosshair() {
  const styles = typeof document !== "undefined" ? getComputedStyle(document.documentElement) : null;
  const accent = styles?.getPropertyValue("--accent").trim() || "#d4af6a";
  const light = typeof document !== "undefined" && document.documentElement.dataset.theme === "light";
  const line = {
    color: `${accent}80`,
    width: 1 as const,
    style: 2 as const, // LineStyle.Dashed
    labelVisible: true,
    labelBackgroundColor: light ? "#57534a" : "#2a2722",
  };
  return { vertLine: line, horzLine: line };
}
