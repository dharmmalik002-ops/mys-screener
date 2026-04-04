import type { ScanDescriptor } from "./api";

export const DEFAULT_SCANNERS: ScanDescriptor[] = [
  { id: "custom-scan", name: "Custom Scanner", category: "Custom", description: "Build your own scan.", hit_count: 0 },
  { id: "day-high", name: "Day High", category: "Core", description: "Stocks trading at session highs.", hit_count: 0 },
  { id: "day-low", name: "Day Low", category: "Core", description: "Stocks trading at session lows.", hit_count: 0 },
  { id: "ipo", name: "IPO", category: "Core", description: "Stocks listed within the last 1 year.", hit_count: 0 },
  { id: "near-day-high", name: "Near Day High", category: "Core", description: "Hovering near day highs.", hit_count: 0 },
  { id: "near-day-low", name: "Near Day Low", category: "Core", description: "Hovering near day lows.", hit_count: 0 },
  { id: "prev-day-high-break", name: "Previous Day High Break", category: "Core", description: "Clearing previous day highs.", hit_count: 0 },
  { id: "prev-day-low-break", name: "Previous Day Low Break", category: "Core", description: "Breaking previous day lows.", hit_count: 0 },
  { id: "week-high", name: "Week High", category: "Core", description: "Stocks at weekly highs.", hit_count: 0 },
  { id: "week-low", name: "Week Low", category: "Core", description: "Stocks at weekly lows.", hit_count: 0 },
  { id: "month-high", name: "Month High", category: "Core", description: "Stocks at monthly highs.", hit_count: 0 },
  { id: "month-low", name: "Month Low", category: "Core", description: "Stocks at monthly lows.", hit_count: 0 },
  { id: "six-month-high", name: "6-Month High", category: "Core", description: "6-month highs.", hit_count: 0 },
  { id: "six-month-low", name: "6-Month Low", category: "Core", description: "6-month lows.", hit_count: 0 },
  { id: "high-52w", name: "52-Week High", category: "Core", description: "Fresh yearly highs.", hit_count: 0 },
  { id: "low-52w", name: "52-Week Low", category: "Core", description: "Fresh yearly lows.", hit_count: 0 },
  { id: "near-52w-high", name: "Near 52W High", category: "Core", description: "Near yearly highs.", hit_count: 0 },
  { id: "near-52w-low", name: "Near 52W Low", category: "Core", description: "Near yearly lows.", hit_count: 0 },
  { id: "all-time-high", name: "All-Time High", category: "Core", description: "All-time highs.", hit_count: 0 },
  { id: "all-time-low", name: "All-Time Low", category: "Core", description: "All-time lows.", hit_count: 0 },
  { id: "near-ath", name: "Near ATH", category: "Core", description: "Near all-time highs.", hit_count: 0 },
  { id: "near-atl", name: "Near ATL", category: "Core", description: "Near all-time lows.", hit_count: 0 },
  { id: "breakout-ath", name: "ATH Breakouts", category: "Setups", description: "Breakouts through ATH.", hit_count: 0 },
  { id: "breakout-52w", name: "52W Breakouts", category: "Setups", description: "Breakouts through yearly highs.", hit_count: 0 },
  { id: "breakout-range", name: "Range Breakouts", category: "Setups", description: "Range expansions.", hit_count: 0 },
  { id: "volume-price", name: "Volume + Price Move", category: "Setups", description: "Relative-volume spikes.", hit_count: 0 },
  { id: "strong-nifty", name: "Strong vs Benchmark", category: "Setups", description: "Outperforming the benchmark.", hit_count: 0 },
  { id: "strong-sector", name: "Strong vs Sector", category: "Setups", description: "Outperforming sector.", hit_count: 0 },
  { id: "clean-pullback", name: "Clean Pullbacks", category: "Setups", description: "Trend pullbacks.", hit_count: 0 },
  { id: "darvas-box", name: "Darvas Box", category: "Setups", description: "Darvas box breakouts.", hit_count: 0 },
  { id: "pivot-breakout", name: "Pivot Breakouts", category: "Setups", description: "Pivot resolutions.", hit_count: 0 },
  { id: "consolidating", name: "Consolidating", category: "Setups", description: "Run-up consolidations and names coiling below 3-year highs.", hit_count: 0 },
  { id: "relative-strength", name: "Relative Strengths", category: "Setups", description: "RS leaders.", hit_count: 0 },
  { id: "minervini-1m", name: "Minervini 1 Month", category: "Setups", description: "Trend template names with a rising 200 SMA and strong 52-week positioning.", hit_count: 0 },
  { id: "minervini-5m", name: "Minervini 5 Months", category: "Setups", description: "Trend template names with a rising 200 SMA over 1 and 5 months and stronger 52-week positioning.", hit_count: 0 },
];

export const SCANNER_BADGES: Record<string, string> = {
  "custom-scan": "CST",
  "day-high": "DH",
  "day-low": "DL",
  "ipo": "IPO",
  "near-day-high": "NDH",
  "near-day-low": "NDL",
  "prev-day-high-break": "PDH",
  "prev-day-low-break": "PDL",
  "week-high": "WH",
  "week-low": "WL",
  "month-high": "MH",
  "month-low": "ML",
  "six-month-high": "6H",
  "six-month-low": "6L",
  "high-52w": "52H",
  "low-52w": "52L",
  "near-52w-high": "N52H",
  "near-52w-low": "N52L",
  "all-time-high": "ATH",
  "all-time-low": "ATL",
  "near-ath": "NATH",
  "near-atl": "NATL",
  "breakout-ath": "BO",
  "breakout-52w": "52BO",
  "breakout-range": "RBO",
  "volume-price": "VOL",
  "strong-nifty": "RSN",
  "strong-sector": "RSS",
  "clean-pullback": "PB",
  "darvas-box": "DB",
  "pivot-breakout": "PVT",
  "consolidating": "CONS",
  "relative-strength": "RS",
  "minervini-1m": "MIN1",
  "minervini-5m": "MIN5",
};

const order = new Map(DEFAULT_SCANNERS.map((scanner, index) => [scanner.id, index]));

export function sortScanners(scanners: ScanDescriptor[]) {
  return [...scanners].sort((left, right) => (order.get(left.id) ?? 999) - (order.get(right.id) ?? 999));
}

export function applyScannerDisplayAlias(scanner: ScanDescriptor) {
  if (scanner.id !== "strong-nifty") {
    return scanner;
  }

  return {
    ...scanner,
    name: "Strong vs Benchmark",
    description: "Stocks beating the benchmark over 20D.",
  };
}

export function applyScannerDisplayAliases(scanners: ScanDescriptor[]) {
  return scanners.map((scanner) => applyScannerDisplayAlias(scanner));
}
