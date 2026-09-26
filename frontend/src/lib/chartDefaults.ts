import type { ChartColorSettings } from "../components/ChartPanel";
import { CANDLE_DOWN, CANDLE_UP } from "./marketColors";

export const DEFAULT_CHART_COLORS: ChartColorSettings = {
  // Moving averages tuned for the white chart canvas: mid-tone, so each
  // reads on white paper yet stays distinct from the others and from both
  // candle colours at a glance.
  ema10: "#e07b4f",
  ema20: "#c9971f",
  ema50: "#3f86c2",
  ema200: "#8b867b",
  vwap: "#8b6fd0",
  // Teal/coral candles (TradingView's current pair): calm, high up-vs-down
  // contrast, easy on the eyes for long sessions.
  candleUp: CANDLE_UP,
  candleDown: CANDLE_DOWN,
  candleExpansion: "#e6b04e",
  volumeUp: CANDLE_UP,
  volumeDown: CANDLE_DOWN,
  rsLine: "#7c5cc4",
  rsMarker: "#7c5cc4",
  rsMarkerSize: 4,
};
/** The big charts draw on plain white paper with no grid, in both themes. */
export const CHART_PAPER = "#ffffff";
/** Axis text on the white canvas. */
export const CHART_PAPER_TEXT = "#6b665b";

/**
 * Shared crosshair for the lightweight-charts surfaces outside ChartPanel:
 * a fine dashed bronze line with warm-charcoal axis pills, sized for the
 * white chart canvas.
 */
export function premiumCrosshair() {
  const line = {
    color: "rgba(111, 82, 20, 0.5)",
    width: 1 as const,
    style: 2 as const, // LineStyle.Dashed
    labelVisible: true,
    labelBackgroundColor: "#2a2722",
  };
  return { vertLine: line, horzLine: line };
}
