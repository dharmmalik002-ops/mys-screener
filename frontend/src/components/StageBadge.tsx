import { useEffect, useRef, useState } from "react";
import { getStockStage, type MarketKey, type StockStage } from "../lib/api";
import "./StageBadge.css";

type Props = { symbol: string | null; market?: MarketKey };

/** Colour carries the stage, not an opinion about it. Stage 2 is green because
 *  it is the advancing phase, not because the stock is a buy. */
const TONE: Record<number, string> = { 1: "base", 2: "advance", 3: "top", 4: "decline" };

/**
 * Where this stock sits in its own multi-year cycle.
 *
 * One glance, no thinking: Weinstein's four stages, from the same classifier
 * the sector page uses. The detail sits in a hover card rather than on the
 * toolbar because the badge's job is to be read in passing — the evidence is
 * there for the one time in ten that the label is surprising.
 */
export function StageBadge({ symbol, market = "india" }: Props) {
  const [data, setData] = useState<StockStage | null>(null);
  const [open, setOpen] = useState(false);
  const requestId = useRef(0);

  useEffect(() => {
    if (!symbol) {
      setData(null);
      return;
    }
    const id = ++requestId.current;
    setData(null);
    setOpen(false);
    getStockStage(symbol, market)
      .then((result) => {
        // A slow fetch for a symbol the user has already navigated away from
        // must not label the new chart with the old stock's stage.
        if (id === requestId.current) setData(result);
      })
      .catch(() => {
        if (id === requestId.current) setData({ available: false, reason: "Stage unavailable." });
      });
  }, [symbol, market]);

  if (!symbol || !data) return null;

  if (!data.available) {
    return (
      <span className="stgb stgb--none" title={data.reason}>
        Stage —
      </span>
    );
  }

  const tone = TONE[data.stage ?? 1] ?? "base";
  return (
    <span
      className={`stgb stgb--${tone}`}
      onMouseEnter={() => setOpen(true)}
      onMouseLeave={() => setOpen(false)}
      onFocus={() => setOpen(true)}
      onBlur={() => setOpen(false)}
      tabIndex={0}
      role="button"
      aria-label={`Stage ${data.stage}: ${data.stage_label}`}
    >
      Stage {data.stage} · {data.stage_label}
      {data.early_advance ? <em> (off the lows)</em> : null}
      {open ? (
        <span className="stgb-card" role="tooltip">
          <strong>{data.stage_label}</strong>
          <span className="stgb-blurb">{data.blurb}</span>
          <span className="stgb-note">{data.note}</span>
          <span className="stgb-rows">
            {data.distance_from_ma_pct !== null && data.distance_from_ma_pct !== undefined ? (
              <span>
                <em>vs 30-week average</em>
                {data.distance_from_ma_pct > 0 ? "+" : ""}
                {data.distance_from_ma_pct.toFixed(1)}%
              </span>
            ) : null}
            {data.ma_slope_pct_per_week !== null && data.ma_slope_pct_per_week !== undefined ? (
              <span>
                <em>average slope</em>
                {data.ma_slope_pct_per_week > 0 ? "+" : ""}
                {data.ma_slope_pct_per_week.toFixed(2)}%/wk
              </span>
            ) : null}
            {data.position_in_2y_range_pct !== null && data.position_in_2y_range_pct !== undefined ? (
              <span>
                <em>position in 2-year range</em>
                {data.position_in_2y_range_pct.toFixed(0)}%
              </span>
            ) : null}
            {data.base?.range_pct !== null && data.base?.range_pct !== undefined ? (
              <span>
                <em>recent range</em>
                {data.base.range_pct.toFixed(1)}%{data.base.tight ? " (tight)" : ""}
              </span>
            ) : null}
          </span>
          <span className="stgb-foot">
            Measured over {data.weeks_of_history ?? "—"} weeks. A description of past price, not a forecast.
          </span>
        </span>
      ) : null}
    </span>
  );
}
