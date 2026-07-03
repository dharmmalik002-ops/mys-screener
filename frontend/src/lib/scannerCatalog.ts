import type { ScanDescriptor } from "./api";

export const DEFAULT_SCANNERS: ScanDescriptor[] = [
  { id: "custom-scan", name: "Custom Scanner", category: "Custom", description: "Build your own scan.", hit_count: 0 },
  { id: "volume", name: "Volume", category: "Setups", description: "Stocks that pushed a new volume high (Monthly→Yearly) in the last ~1 month, newest first.", hit_count: 0 },
  { id: "day-high", name: "Day High", category: "Core", description: "Stocks trading at session highs.", hit_count: 0 },
  { id: "ipo", name: "IPO", category: "Core", description: "Stocks listed within the last 1 year.", hit_count: 0 },
  { id: "near-day-high", name: "Near Day High", category: "Core", description: "Hovering near day highs.", hit_count: 0 },
  { id: "prev-day-high-break", name: "Previous Day High Break", category: "Core", description: "Clearing previous day highs.", hit_count: 0 },
  { id: "week-high", name: "Week High", category: "Core", description: "Stocks at weekly highs.", hit_count: 0 },
  { id: "month-high", name: "Month High", category: "Core", description: "Stocks at monthly highs.", hit_count: 0 },
  { id: "six-month-high", name: "6-Month High", category: "Core", description: "6-month highs.", hit_count: 0 },
  { id: "high-52w", name: "52-Week High", category: "Core", description: "Fresh yearly highs.", hit_count: 0 },
  { id: "near-52w-high", name: "Near 52W High", category: "Core", description: "Near yearly highs.", hit_count: 0 },
  { id: "all-time-high", name: "All-Time High", category: "Core", description: "All-time highs.", hit_count: 0 },
  { id: "near-ath", name: "Near ATH", category: "Core", description: "Near all-time highs.", hit_count: 0 },
  { id: "breakout-ath", name: "ATH Breakouts", category: "Setups", description: "Breakouts through ATH.", hit_count: 0 },
  { id: "breakout-52w", name: "52W Breakouts", category: "Setups", description: "Breakouts through yearly highs.", hit_count: 0 },
  { id: "breakout-range", name: "Range Breakouts", category: "Setups", description: "Range expansions.", hit_count: 0 },
  { id: "volume-price", name: "Volume + Price Move", category: "Setups", description: "Relative-volume spikes.", hit_count: 0 },
  { id: "strong-nifty", name: "Strong vs Benchmark", category: "Setups", description: "Outperforming the benchmark.", hit_count: 0 },
  { id: "strong-sector", name: "Strong vs Sector", category: "Setups", description: "Outperforming sector.", hit_count: 0 },
  { id: "clean-pullback", name: "Clean Pullbacks", category: "Setups", description: "Trend pullbacks.", hit_count: 0 },
  { id: "darvas-box", name: "Darvas Box", category: "Setups", description: "Darvas box breakouts.", hit_count: 0 },
  { id: "pivot-breakout", name: "Pivot Breakouts", category: "Setups", description: "Pivot resolutions.", hit_count: 0 },
  { id: "contraction", name: "Contraction", category: "Setups", description: "Tight 3-day contractions with liquidity floors and run-up confirmation.", hit_count: 0 },
  { id: "demand-zone", name: "Demand Zone Scanner", category: "Setups", description: "Stage 2 stocks within 3% of strong daily or weekly demand-zone lows.", hit_count: 0 },
  { id: "consolidating", name: "Consolidating", category: "Setups", description: "Run-up consolidations and names coiling below 3-year highs.", hit_count: 0 },
  { id: "relative-strength", name: "Relative Strengths", category: "Setups", description: "RS leaders.", hit_count: 0 },
  { id: "minervini-1m", name: "Minervini 1 Month", category: "Setups", description: "Trend template names with a rising 200 SMA and strong 52-week positioning.", hit_count: 0 },
  { id: "minervini-5m", name: "Minervini 5 Months", category: "Setups", description: "Trend template names with a rising 200 SMA over 1 and 5 months and stronger 52-week positioning.", hit_count: 0 },
  { id: "ema-expansion", name: "Expansion", category: "Setups", description: "Price gain >= 6.5%, RVOL > 3.0, and liquidity floors.", hit_count: 0 },
  { id: "episodic-pivot", name: "Episodic Pivot", category: "Setups", description: "Day-one gap-up >= 4% on 3x+ RVOL out of a flat 20-day base.", hit_count: 0 },
  { id: "rs-line-leads", name: "RS Line Leads", category: "Setups", description: "RS at a fresh high while price is still below the pivot zone.", hit_count: 0 },
  { id: "fresh-stage2", name: "Fresh Stage 2", category: "Setups", description: "New entrants to the Minervini 5M trend template vs recent sessions.", hit_count: 0 },
  { id: "high-tight-flag", name: "High Tight Flag", category: "Setups", description: "Steep pole, shallow 2-15 session flag, at or under the pivot.", hit_count: 0 },
];

export const SCANNER_BADGES: Record<string, string> = {
  "custom-scan": "CST",
  "volume": "VOL+",
  "day-high": "DH",
  "ipo": "IPO",
  "near-day-high": "NDH",
  "prev-day-high-break": "PDH",
  "week-high": "WH",
  "month-high": "MH",
  "six-month-high": "6H",
  "high-52w": "52H",
  "near-52w-high": "N52H",
  "all-time-high": "ATH",
  "near-ath": "NATH",
  "breakout-ath": "BO",
  "breakout-52w": "52BO",
  "breakout-range": "RBO",
  "volume-price": "VOL",
  "strong-nifty": "RSN",
  "strong-sector": "RSS",
  "clean-pullback": "PB",
  "darvas-box": "DB",
  "pivot-breakout": "PVT",
  "contraction": "CONT",
  "demand-zone": "DZ",
  "consolidating": "CONS",
  "relative-strength": "RS",
  "minervini-1m": "MIN1",
  "minervini-5m": "MIN5",
  "ema-expansion": "EXP",
  "episodic-pivot": "EP",
  "rs-line-leads": "RSL",
  "fresh-stage2": "S2+",
  "high-tight-flag": "HTF",
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
