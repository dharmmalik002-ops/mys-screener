/**
 * Resolved market colours for canvas-drawn charts.
 *
 * `lightweight-charts` paints to a <canvas>, so it cannot read a CSS custom
 * property — `var(--positive)` reaches it as an uninterpretable string and the
 * series silently draws in the library's default colour. Everything that ends
 * up in CSS or SVG (className, style, recharts, inline <svg>) uses the tokens
 * in styles/app.css directly and must NOT import from here.
 *
 * These four values mirror the tokens of the same name in styles/app.css. That
 * duplication is unavoidable for canvas; keeping it to one file means there is
 * one place to change rather than the ten that held these hexes before.
 *
 * CANDLE_* is deliberately a different pair from POSITIVE/NEGATIVE: it is
 * TradingView's palette, chosen so the chart reads like a pro terminal, and
 * flattening the two would lose a real distinction.
 */

/** Mirrors --positive in styles/app.css. */
export const POSITIVE = "#22c55e";
/** Mirrors --negative in styles/app.css. */
export const NEGATIVE = "#ef4444";
/** Mirrors --candle-up in styles/app.css. */
export const CANDLE_UP = "#089981";
/** Mirrors --candle-down in styles/app.css. */
export const CANDLE_DOWN = "#f23645";
