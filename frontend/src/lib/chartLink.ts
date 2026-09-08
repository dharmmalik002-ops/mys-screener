// Full-chart deep links.
//
// The grid renders many small charts; each one carries an "open in a new tab"
// icon that points here. Opening the returned URL boots a fresh copy of the app
// straight into the fullscreen chart modal for that one symbol, so the grid the
// user was reading stays open in the original tab.
//
// The app has no router — App.tsx reads this at mount (before the market-route
// replaceState runs), which is why the contract is a query string and not a path.

const CHART_MODE_PARAM = "chart";
const CHART_SYMBOL_PARAM = "symbol";
const CHART_MODE_FULL = "full";

/** Absolute URL that opens `symbol` in the fullscreen chart. */
export function fullChartUrl(symbol: string): string {
  const trimmed = symbol.trim().toUpperCase();
  if (!trimmed || typeof window === "undefined") return "";
  const params = new URLSearchParams();
  params.set(CHART_SYMBOL_PARAM, trimmed);
  params.set(CHART_MODE_PARAM, CHART_MODE_FULL);
  return `${window.location.origin}${window.location.pathname}?${params.toString()}`;
}

/** The symbol this tab was opened on, or null for a normal app load. */
export function readChartDeepLink(): string | null {
  if (typeof window === "undefined") return null;
  try {
    const params = new URLSearchParams(window.location.search);
    if (params.get(CHART_MODE_PARAM) !== CHART_MODE_FULL) return null;
    const symbol = (params.get(CHART_SYMBOL_PARAM) ?? "").trim().toUpperCase();
    return symbol || null;
  } catch {
    return null;
  }
}
