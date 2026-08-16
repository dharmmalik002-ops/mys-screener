import {
  customSeriesDefaultOptions,
  type CustomData,
  type CustomSeriesOptions,
  type CustomSeriesPricePlotValues,
  type ICustomSeriesPaneRenderer,
  type ICustomSeriesPaneView,
  type PaneRendererCustomData,
  type PriceToCoordinateConverter,
  type Time,
} from "lightweight-charts";
import type { CanvasRenderingTarget2D } from "fancy-canvas";

/**
 * Volume/range-weighted candles.
 *
 * lightweight-charts draws every candle at one width derived from bar spacing —
 * there is no per-bar width option, which is why the expansion highlight used to
 * be faked with a blurred SVG halo. A custom series owns its own renderer, so
 * here each candle is drawn at a width scaled by how much the bar actually did:
 * heavy volume on a wide range renders fat, a quiet inside day renders as a
 * hairline. Reading the chart becomes a glance instead of a volume-panel check.
 *
 * `weight` is supplied per bar by the caller (0 = dead, 1 = the loudest bar on
 * screen) so all the market logic stays in ChartPanel and this file stays a
 * renderer.
 */
export interface WeightedCandleData extends CustomData<Time> {
  open: number;
  high: number;
  low: number;
  close: number;
  /** 0..1 loudness. Missing is treated as an average bar. */
  weight?: number;
  /** Per-point overrides, matching the built-in candlestick series. */
  color?: string;
  borderColor?: string;
  wickColor?: string;
}

export interface WeightedCandleSeriesOptions extends CustomSeriesOptions {
  upColor: string;
  downColor: string;
  wickUpColor: string;
  wickDownColor: string;
  borderUpColor: string;
  borderDownColor: string;
  borderVisible: boolean;
  /** Body width as a fraction of bar spacing at weight 0 and weight 1. */
  minWidthFraction: number;
  maxWidthFraction: number;
}

export const weightedCandleDefaultOptions: WeightedCandleSeriesOptions = {
  ...customSeriesDefaultOptions,
  upColor: "#089981",
  downColor: "#f23645",
  wickUpColor: "#089981",
  wickDownColor: "#f23645",
  borderUpColor: "#089981",
  borderDownColor: "#f23645",
  borderVisible: true,
  // A quiet bar keeps ~28% of the slot so it still reads as a candle; a loud one
  // takes ~92%, near enough to touching its neighbours to look deliberate.
  minWidthFraction: 0.28,
  maxWidthFraction: 0.92,
} as WeightedCandleSeriesOptions;

/** Snap to whole device pixels so edges stay hard instead of half-lit. */
function crisp(value: number, ratio: number) {
  return Math.round(value * ratio);
}

class WeightedCandleRenderer implements ICustomSeriesPaneRenderer {
  private _data: PaneRendererCustomData<Time, WeightedCandleData> | null = null;
  private _options: WeightedCandleSeriesOptions | null = null;

  update(
    data: PaneRendererCustomData<Time, WeightedCandleData>,
    options: WeightedCandleSeriesOptions,
  ) {
    this._data = data;
    this._options = options;
  }

  draw(target: CanvasRenderingTarget2D, priceConverter: PriceToCoordinateConverter) {
    const data = this._data;
    const options = this._options;
    if (!data || !options || data.bars.length === 0 || data.visibleRange === null) {
      return;
    }
    // Held in a local so the narrowing survives into the closure below.
    const visibleRange = data.visibleRange;

    target.useBitmapCoordinateSpace((scope) => {
      const ctx = scope.context;
      const hRatio = scope.horizontalPixelRatio;
      const vRatio = scope.verticalPixelRatio;
      const { minWidthFraction, maxWidthFraction } = options;
      const spread = maxWidthFraction - minWidthFraction;

      for (let i = visibleRange.from; i < visibleRange.to; i++) {
        const bar = data.bars[i];
        const point = bar.originalData;
        if (!point || typeof point.close !== "number") continue;

        const isUp = point.close >= point.open;
        const weight = Math.max(0, Math.min(1, point.weight ?? 0.35));

        const bodyColor = point.color ?? (isUp ? options.upColor : options.downColor);
        const wickColor = point.wickColor ?? (isUp ? options.wickUpColor : options.wickDownColor);
        const borderColor =
          point.borderColor ?? (isUp ? options.borderUpColor : options.borderDownColor);

        // Width scales with loudness. Never below 1 device pixel, never wider
        // than the slot, so candles stay distinct at any zoom.
        const widthPx = data.barSpacing * (minWidthFraction + spread * weight);
        let halfWidth = Math.max(Math.round((widthPx * hRatio) / 2), 1);
        const maxHalf = Math.max(Math.floor((data.barSpacing * hRatio) / 2) - 1, 1);
        if (halfWidth > maxHalf) halfWidth = maxHalf;

        const xCentre = crisp(bar.x, hRatio);
        const left = xCentre - halfWidth;
        const bodyWidth = halfWidth * 2;

        // Wick: a hairline on quiet bars, thickening slightly with weight so a
        // heavy bar's tail carries the same visual weight as its body.
        const wickHalf = Math.max(Math.round(((1 + weight * 1.6) * hRatio) / 2), 1);
        const yHigh = crisp(priceConverter(point.high) ?? 0, vRatio);
        const yLow = crisp(priceConverter(point.low) ?? 0, vRatio);
        ctx.fillStyle = wickColor;
        ctx.fillRect(xCentre - wickHalf, yHigh, wickHalf * 2, Math.max(yLow - yHigh, 1));

        const yOpen = crisp(priceConverter(point.open) ?? 0, vRatio);
        const yClose = crisp(priceConverter(point.close) ?? 0, vRatio);
        const top = Math.min(yOpen, yClose);
        // A doji still needs a visible line across the body width.
        const height = Math.max(Math.abs(yClose - yOpen), 1);

        ctx.fillStyle = bodyColor;
        ctx.fillRect(left, top, bodyWidth, height);

        if (options.borderVisible && bodyWidth > 2) {
          ctx.strokeStyle = borderColor;
          ctx.lineWidth = 1;
          // Half-pixel offset keeps the 1px stroke on a single pixel row.
          ctx.strokeRect(left + 0.5, top + 0.5, bodyWidth - 1, height - 1);
        }
      }
    });
  }
}

export class WeightedCandleSeries
  implements ICustomSeriesPaneView<Time, WeightedCandleData, WeightedCandleSeriesOptions>
{
  private _renderer = new WeightedCandleRenderer();

  priceValueBuilder(plotRow: WeightedCandleData): CustomSeriesPricePlotValues {
    return [plotRow.low, plotRow.high, plotRow.close];
  }

  isWhitespace(data: WeightedCandleData): data is WeightedCandleData & { close: undefined } {
    return (data as Partial<WeightedCandleData>).close === undefined;
  }

  renderer(): ICustomSeriesPaneRenderer {
    return this._renderer;
  }

  update(
    data: PaneRendererCustomData<Time, WeightedCandleData>,
    options: WeightedCandleSeriesOptions,
  ) {
    this._renderer.update(data, options);
  }

  defaultOptions() {
    return weightedCandleDefaultOptions;
  }
}

/**
 * Loudness of each bar, normalised across the loaded window.
 *
 * Two things make a bar matter: how much it traded relative to its own recent
 * average, and how far it travelled. Both are compared against the dataset
 * rather than fixed thresholds, so a sleepy smallcap and a heavyweight index
 * both get a usable spread of widths instead of everything pinning to one end.
 */
export function computeCandleWeights(
  bars: Array<{ high: number; low: number; close: number; volume?: number }>,
  stats: Array<{ avgVol: number; changePct: number }>,
): number[] {
  const volScores: number[] = [];
  const moveScores: number[] = [];

  for (let i = 0; i < bars.length; i++) {
    const bar = bars[i];
    const avgVol = stats[i]?.avgVol ?? 0;
    const volume = bar.volume || 0;
    // Relative volume, capped at 3x — beyond that it is all "huge" anyway.
    volScores.push(avgVol > 0 ? Math.min(volume / avgVol, 3) : 1);

    const range = bar.high - bar.low;
    const rangePct = bar.close > 0 ? (range / bar.close) * 100 : 0;
    // Travel = the larger of the bar's own range and its close-to-close move.
    moveScores.push(Math.max(rangePct, Math.abs(stats[i]?.changePct ?? 0)));
  }

  const moveReference = percentile(moveScores, 0.9) || 1;

  return bars.map((_, i) => {
    const vol = volScores[i] / 3;
    const move = Math.min(moveScores[i] / moveReference, 1);
    // Volume leads: a big move on no volume is noise, volume without a move is
    // still accumulation worth seeing.
    const score = vol * 0.6 + move * 0.4;
    return Math.max(0, Math.min(1, score));
  });
}

function percentile(values: number[], fraction: number) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor(sorted.length * fraction)));
  return sorted[index];
}
