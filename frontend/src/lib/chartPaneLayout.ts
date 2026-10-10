// How the big chart's canvas is shared between price, the RS line and volume.
// Every value is a lightweight-charts scale margin (a fraction of the canvas).
//
// The shares were fixed fractions, which a short canvas turns into nothing:
// full screen on a landscape phone (or a laptop at high browser zoom) gave
// volume 8% of ~350px, a 28px strip the eye reads as "no volume". Each pane now
// keeps a minimum height in pixels and price gives up the difference.

export type PaneMargins = { top: number; bottom: number };
export type ChartPaneLayout = { price: PaneMargins; rs: PaneMargins | null; volume: PaneMargins };

export const VOLUME_MIN_PX = 60;
export const RS_MIN_PX = 44;

export function chartPaneLayout(
  heightPx: number | null | undefined,
  options: { hasRs: boolean; tight: boolean; priceTop: number },
): ChartPaneLayout {
  const { hasRs, tight, priceTop } = options;
  const h = typeof heightPx === "number" && heightPx > 0 ? heightPx : 0;
  const floor = (fraction: number, px: number) => (h > 0 ? Math.max(fraction, px / h) : fraction);
  // Base shares are the previous fixed layout, so a tall chart looks as before.
  const volume = Math.min(floor(hasRs ? (tight ? 0.08 : 0.12) : tight ? 0.1 : 0.18, VOLUME_MIN_PX), 0.3);
  if (!hasRs) {
    return { price: { top: priceTop, bottom: volume }, rs: null, volume: { top: 1 - volume, bottom: 0 } };
  }
  const gap = 0.02;
  const rs = Math.min(floor(tight ? 0.1 : 0.14, RS_MIN_PX), 0.22);
  const rsBottom = volume + gap;
  const rsTop = 1 - rsBottom - rs;
  return {
    price: { top: priceTop, bottom: rsBottom + rs + (tight ? 0.02 : 0.04) },
    rs: { top: rsTop, bottom: rsBottom },
    volume: { top: 1 - volume, bottom: 0 },
  };
}
