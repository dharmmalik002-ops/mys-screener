import { useEffect, useMemo, useState } from "react";
import type { SeriesMarker, UTCTimestamp } from "lightweight-charts";
import type { StudyBar } from "../../lib/api";
import { CourseCandleChart } from "./CourseCandleChart";
import { CourseChartToolbar } from "./CourseChartOptions";
import { barAtOrBefore, isoShift, loadBars, toTime } from "./courseBars";
import { caseTicker, type CaseStudy } from "./courseData";

/* A case study on a real daily chart, in the site's chart look, with his own
   buys, adds and sells marked where he logged them.

   `revealThrough` hides every bar after that date, so the trade replay can
   step through his log without the chart giving the ending away.

   Yahoo's history is restated for later splits and bonuses, so a 2021 price
   can sit at a fraction of what he quoted. When his entry price and the
   chart's close that day disagree by more than ~15%, the bars are rescaled
   onto his numbers and the chart says so. */

const BUY = "#1f6fd1";
const SELL = "#c2410c";
const NOTE = "#8b877d";

type Kind = "buy" | "sell" | "short" | "cover" | "note";

export function eventKind(action: string, isShort: boolean): Kind {
  const a = action.toLowerCase();
  if (/^cover/.test(a)) return "cover";
  if (/short/.test(a)) return "short";
  if (/^(sold|closed|exit|booked)/.test(a)) return isShort ? "cover" : "sell";
  if (/^(bought|added|re-entered|buy|entry|long)/.test(a)) return "buy";
  return "note";
}

export function caseWindow(c: CaseStudy) {
  const dates = [c.entry_date, ...c.timeline.map((t) => t.date)].filter(Boolean).sort();
  // 420 days before the first entry: room for a real 200-day average from the first drawn bar.
  return { start: isoShift(dates[0], -420), end: isoShift(dates[dates.length - 1], 60), first: dates[0] };
}

export function CaseChart({
  c,
  revealThrough,
  revealEvents,
  height = 360,
}: {
  c: CaseStudy;
  /** Show bars through this date only; omit to show the whole window. */
  revealThrough?: string;
  /** Mark only the first N log entries; omit to mark all. */
  revealEvents?: number;
  height?: number;
}) {
  const [raw, setRaw] = useState<StudyBar[] | null>(null);
  const [failed, setFailed] = useState(false);
  const ticker = caseTicker(c.symbol);
  const isShort = /short/i.test(c.symbol) || /short/i.test(c.timeline[0]?.action ?? "");
  const win = useMemo(() => caseWindow(c), [c]);

  useEffect(() => {
    let live = true;
    setRaw(null);
    setFailed(false);
    if (!ticker) {
      setFailed(true);
      return;
    }
    loadBars(ticker, win.start, win.end)
      .then((bars) => {
        if (!live) return;
        if (bars.length < 10) setFailed(true);
        else setRaw(bars);
      })
      .catch(() => live && setFailed(true));
    return () => {
      live = false;
    };
  }, [ticker, win.start, win.end]);

  // Put the bars on his price scale when a later split/bonus restated them.
  const scaled = useMemo(() => {
    if (!raw) return null;
    const anchor = c.timeline.find((t) => typeof t.price === "number" && eventKind(t.action, isShort) !== "note");
    let factor = 1;
    if (anchor && typeof anchor.price === "number") {
      const i = barAtOrBefore(raw, toTime(anchor.date));
      const close = i >= 0 ? raw[i].close : 0;
      const ratio = close > 0 ? anchor.price / close : 1;
      if (ratio > 1.15 || ratio < 0.87) factor = ratio;
    }
    const bars =
      factor === 1
        ? raw
        : raw.map((b) => ({ ...b, open: b.open * factor, high: b.high * factor, low: b.low * factor, close: b.close * factor }));
    return { bars, factor };
  }, [raw, c, isShort]);

  const markers = useMemo<SeriesMarker<UTCTimestamp>[]>(() => {
    if (!scaled) return [];
    const out: SeriesMarker<UTCTimestamp>[] = [];
    for (const e of c.timeline.slice(0, revealEvents ?? c.timeline.length)) {
      const i = barAtOrBefore(scaled.bars, toTime(e.date));
      if (i < 0) continue;
      const kind = eventKind(e.action, isShort);
      const priced = typeof e.price === "number" ? ` ${e.price}` : "";
      const time = scaled.bars[i].time as UTCTimestamp;
      if (kind === "buy" || kind === "cover") {
        out.push({ time, position: "belowBar", shape: "arrowUp", color: kind === "buy" ? BUY : SELL, text: `${kind === "buy" ? "Buy" : "Cover"}${priced}` });
      } else if (kind === "sell" || kind === "short") {
        out.push({ time, position: "aboveBar", shape: "arrowDown", color: kind === "sell" ? SELL : BUY, text: `${kind === "sell" ? "Sell" : "Short"}${priced}` });
      } else if (priced) {
        out.push({ time, position: "aboveBar", shape: "circle", color: NOTE, text: priced.trim() });
      }
    }
    return out;
  }, [scaled, c, revealEvents, isShort]);

  if (failed) {
    return (
      <p className="course-casechart-missing">
        No price history for {ticker || c.symbol} in this window (delisted, renamed or not on Yahoo). His own chart is above.
      </p>
    );
  }
  const entry = scaled ? barAtOrBefore(scaled.bars, toTime(win.first)) : 0;
  return (
    <div className="course-casechart">
      <div className="course-casechart-tools">
        <CourseChartToolbar />
      </div>
      {scaled ? (
        <CourseCandleChart
          bars={scaled.bars}
          height={height}
          showFrom={Math.max(0, entry - 90)}
          revealThrough={revealThrough ? toTime(revealThrough) : undefined}
          markers={markers}
          ariaLabel={`${ticker} daily chart with his trades marked`}
        />
      ) : (
        <p className="course-casechart-loading" style={{ height }}>
          Loading {ticker} prices…
        </p>
      )}
      <p className="course-casechart-legend">
        <span className="is-buy">▲ buy / add</span> <span className="is-sell">▼ sell</span> <span className="is-note">● update</span>
        {scaled && scaled.factor !== 1 ? ` · prices restated ×${scaled.factor.toFixed(2)} to the levels he quoted (later split/bonus)` : ""}
      </p>
    </div>
  );
}
