import type { ChartColorSettings } from "../components/ChartPanel";

export const DEFAULT_CHART_COLORS: ChartColorSettings = {
  ema10: "#ff7a59",
  ema20: "#f7b955",
  ema50: "#00d2ff",
  ema200: "#8b949e",
  vwap: "#39ff14",
  // Pro teal/red (TradingView-style): calmer than the old neon cyan/red,
  // higher up-vs-down contrast at a glance, and easy on the eyes for long use.
  candleUp: "#089981",
  candleDown: "#f23645",
  candleExpansion: "#ffb01f",
  volumeUp: "#089981",
  volumeDown: "#f23645",
  rsLine: "#39ff14",
  rsMarker: "#39ff14",
  rsMarkerSize: 4,
};