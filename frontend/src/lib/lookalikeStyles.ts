/* Look-alike styles are "<trader>" (all his charts) or "<trader>_<setup>". */

export const SETUP_NAMES: Record<string, string> = {
  "": "All his charts",
  flag: "Flag & pennant",
  cup_handle: "Cup & handle",
  base: "Base",
  channel: "Channel",
  triangle: "Triangle",
  double_bottom: "Double bottom",
  gap: "Gap",
  trendline: "Trendline break",
  wedge: "Wedge",
  head_shoulders: "Head & shoulders",
};

/** Display order of the setups, "all his charts" first. */
export const SETUP_ORDER = Object.keys(SETUP_NAMES);

export const TRADER_NAMES: Record<string, string> = { zanger: "Dan Zanger", minervini: "Mark Minervini" };

export function splitStyle(style: string): [string, string] {
  const [trader, ...rest] = style.split("_");
  return [trader, rest.join("_")];
}

export function joinStyle(trader: string, setup: string) {
  return setup ? `${trader}_${setup}` : trader;
}

export function setupName(setup: string, trader?: string) {
  if (!setup) return trader === "minervini" ? "His setups" : SETUP_NAMES[""];
  return SETUP_NAMES[setup] ?? setup.replace(/_/g, " ");
}

/* `zanger_cup_handle` -> "Zanger · Cup & handle"; mirrors references.style_name. */
export function styleName(style: string) {
  if (!style) return "Library";
  const [trader, setup] = splitStyle(style);
  const name = trader.charAt(0).toUpperCase() + trader.slice(1);
  return setup ? `${name} · ${setupName(setup)}` : name;
}
