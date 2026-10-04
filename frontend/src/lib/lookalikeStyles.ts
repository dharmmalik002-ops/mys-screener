const SETUP_NAMES: Record<string, string> = {
  cup_handle: "Cup & handle",
  flag: "Flag & pennant",
  triangle: "Triangle",
  wedge: "Wedge",
  channel: "Channel",
  head_shoulders: "Head & shoulders",
  double_bottom: "Double bottom",
  base: "Base",
  trendline: "Trendline break",
  gap: "Gap",
};

/* `zanger_cup_handle` -> "Zanger · Cup & handle"; mirrors references.style_name. */
export function styleName(style: string) {
  if (!style) return "Library";
  const [trader, ...rest] = style.split("_");
  const name = trader.charAt(0).toUpperCase() + trader.slice(1);
  const setup = rest.join("_");
  return setup ? `${name} · ${SETUP_NAMES[setup] ?? setup.replace(/_/g, " ")}` : name;
}
