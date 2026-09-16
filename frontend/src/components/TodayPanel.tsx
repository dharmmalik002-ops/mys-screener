import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, Check, RefreshCw } from "lucide-react";
import {
  getMacroContext,
  getMarketsExposure,
  type IndustryGroupsResponse,
  type MacroContext,
  type MarketKey,
  type MarketsExposure,
  type XpBreadthScore,
} from "../lib/api";
import { DEFAULT_CHARGES, type ChargesConfig } from "../lib/chargesCalculator";
import {
  calculateFIFO,
  lsGet,
  LS_CHARGES,
  LS_DATA,
  LS_EQUITY,
  LS_META,
  type PosMeta,
  type Trade,
} from "./TradeJournalPanel";
import "./TodayPanel.css";

type Props = {
  market?: MarketKey;
  xpBreadth?: XpBreadthScore | null;
  groupsData?: IndustryGroupsResponse | null;
  onOpenSymbolChart?: (symbol: string) => void;
};

type StopState = "none" | "breached" | "close" | "ok" | "unknown";

const STOP_COPY: Record<StopState, { label: string; tone: string }> = {
  breached: { label: "Below your stop", tone: "bad" },
  none: { label: "No stop set", tone: "bad" },
  close: { label: "Near the stop", tone: "warn" },
  ok: { label: "Above the stop", tone: "good" },
  unknown: { label: "Price unknown", tone: "muted" },
};

function money(value: number): string {
  return value.toLocaleString("en-IN", { maximumFractionDigits: 0 });
}

/**
 * The five-minute routine.
 *
 * Every number on this page is already computed somewhere else in the app —
 * that is the point. The value is not new analysis, it is the ORDER: outside
 * world, then the tape, then the money already at risk, and only then what to
 * buy. Most bad mornings come from doing that sequence backwards, opening the
 * screener first and reasoning about the market afterwards to justify what was
 * already wanted.
 *
 * Open positions come from the journal's own `calculateFIFO` rather than a
 * second netting routine. Two implementations of "what do I hold" would drift,
 * and this page disagreeing with the Journal about the size of a position
 * would make both untrustworthy.
 */
export function TodayPanel({ market = "india", xpBreadth, groupsData, onOpenSymbolChart }: Props) {
  const [macro, setMacro] = useState<MacroContext | null>(null);
  const [exposure, setExposure] = useState<MarketsExposure | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    setLoading(true);
    // Both are independent and both degrade to null: a routine that cannot
    // render because one upstream is slow is not a routine.
    Promise.allSettled([getMacroContext(market), getMarketsExposure(market)]).then(([m, e]) => {
      if (!active) return;
      setMacro(m.status === "fulfilled" ? m.value : null);
      setExposure(e.status === "fulfilled" ? e.value : null);
      setLoading(false);
    });
    return () => {
      active = false;
    };
  }, [market]);

  // ── Step 3: the book, read from the journal's own storage ────────────────
  const book = useMemo(() => {
    const trades = lsGet<Trade[]>(LS_DATA, []);
    const equity = lsGet<number>(LS_EQUITY, 100000);
    const charges = lsGet<ChargesConfig>(LS_CHARGES, DEFAULT_CHARGES);
    const meta = lsGet<Record<string, PosMeta>>(LS_META, {});
    if (!Array.isArray(trades) || trades.length === 0) return [];

    const { openPositions, openLotsDict } = calculateFIFO(trades, equity, charges);
    return openPositions.map((position) => {
      const lots = openLotsDict[position.symbol.toUpperCase()] ?? [];
      // The most recent lot's stop is the live one: a stop moved up on a later
      // add is the trader's current intent for the whole position.
      const stop = lots.reduce<number | null>((latest, lot) => {
        const value = Number(lot.stoploss);
        return Number.isFinite(value) && value > 0 ? value : latest;
      }, null);
      const cmp = Number(meta[position.symbol]?.cmp) || null;
      const metaStop = Number(meta[position.symbol]?.sl) || null;
      const effectiveStop = metaStop ?? stop;

      let state: StopState;
      if (!effectiveStop) state = "none";
      else if (!cmp) state = "unknown";
      else if (cmp <= effectiveStop) state = "breached";
      else if (cmp / effectiveStop - 1 <= 0.02) state = "close";
      else state = "ok";

      // Risk measured from TODAY'S price, not from the entry.
      //
      // The Journal's action board reports `(avgPx - stop) * qty` — the risk
      // that was accepted when the position was opened. That is the right
      // number for grading a past decision. It is the wrong number for a
      // morning routine: on a position up 32% with a trailed stop, the risk
      // taken at entry is ancient history, while the amount that would
      // actually be lost from here is the thing to decide about. The two
      // figures legitimately differ and are labelled differently on each page.
      const openRiskPct =
        cmp && effectiveStop && equity > 0
          ? ((cmp - effectiveStop) * position.qty / equity) * 100
          : null;

      return {
        symbol: position.symbol,
        qty: position.qty,
        avgPx: position.avgPx,
        cmp,
        stop: effectiveStop,
        state,
        pnlPct: cmp ? (cmp / position.avgPx - 1) * 100 : null,
        openRiskPct,
        invested: position.totalInvested,
      };
    }).sort((a, b) => {
      // Whatever needs a decision first goes first.
      const order: Record<StopState, number> = { breached: 0, none: 1, close: 2, unknown: 3, ok: 4 };
      return order[a.state] - order[b.state];
    });
  }, []);

  // ── Step 4: where to look, from the group rankings already on screen ─────
  const leaders = useMemo(() => {
    const groups = (groupsData?.groups ?? []).slice(0, 5);
    const stocks = groupsData?.stocks ?? [];
    return groups.map((group) => ({
      group,
      picks: stocks
        .filter((stock) => stock.final_group_id === group.group_id)
        .sort((a, b) => (b.return_1w ?? -999) - (a.return_1w ?? -999))
        .slice(0, 4),
    }));
  }, [groupsData]);

  const session = macro?.as_of ?? exposure?.as_of_session ?? "today";
  const storageKey = `today-routine:${market}:${session}`;
  const [ticked, setTicked] = useState<Record<string, boolean>>({});
  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(storageKey);
      setTicked(raw ? (JSON.parse(raw) as Record<string, boolean>) : {});
    } catch {
      setTicked({});
    }
  }, [storageKey]);
  const tick = (id: string) => {
    setTicked((prev) => {
      const next = { ...prev, [id]: !prev[id] };
      try {
        window.localStorage.setItem(storageKey, JSON.stringify(next));
      } catch {
        /* blocked storage — the tick just does not persist */
      }
      return next;
    });
  };

  const needsAttention = book.filter((row) => row.state === "breached" || row.state === "none");
  const steps = [
    { id: "world", label: "Read the global and macro picture" },
    { id: "tape", label: "Checked what the tape is paying" },
    { id: "book", label: "Every open position has a stop I still believe in" },
    { id: "look", label: "Know which groups are leading before opening a scanner" },
    { id: "size", label: "Decided today's position size before seeing any chart" },
  ];
  const done = steps.filter((step) => ticked[step.id]).length;

  return (
    <div className="today">
      <header className="today-head">
        <div>
          <p className="today-eyebrow">The five-minute routine</p>
          <h2>Before you open a scanner</h2>
        </div>
        <span className="today-progress">
          {done}/{steps.length} done
          {loading ? <RefreshCw size={13} className="today-spin" aria-hidden /> : null}
        </span>
      </header>

      <ol className="today-steps">
        <li className="today-step">
          <h3><span>1</span> What the world did</h3>
          {macro?.available && macro.note ? (
            <>
              <p className={`today-stance today-stance--${macro.note.stance}`}>{macro.note.headline}</p>
              {macro.note.summary.slice(0, 2).map((paragraph, index) => (
                <p key={index} className="today-prose">{paragraph}</p>
              ))}
            </>
          ) : (
            <p className="today-prose today-muted">
              {loading ? "Loading the external picture…" : macro?.reason ?? "Market context is unavailable right now."}
            </p>
          )}
        </li>

        <li className="today-step">
          <h3><span>2</span> What the tape is paying</h3>
          {exposure?.available && exposure.verdict?.available ? (
            <>
              <p className="today-stance">
                Exposure <strong>{exposure.verdict.exposure_pct ?? "—"}%</strong> · {exposure.verdict.band}
                {xpBreadth?.regime ? ` · breadth reads ${xpBreadth.regime}` : ""}
              </p>
              <p className="today-prose">
                {exposure.verdict.win_rate !== null && exposure.verdict.breakeven_win_rate !== null
                  ? `Breakouts are paying ${exposure.verdict.win_rate.toFixed(1)}% against a ${exposure.verdict.breakeven_win_rate.toFixed(1)}% break-even. `
                  : ""}
                This is a measurement of the last few resolved weeks, not a forecast of the next one.
              </p>
            </>
          ) : (
            <p className="today-prose today-muted">
              {loading ? "Loading the tape read…" : "The exposure verdict is not available yet."}
            </p>
          )}
        </li>

        <li className="today-step">
          <h3>
            <span>3</span> Money already at risk
            {needsAttention.length ? (
              <em className="today-flag"><AlertTriangle size={12} aria-hidden /> {needsAttention.length} need a decision</em>
            ) : null}
          </h3>
          {book.length === 0 ? (
            <p className="today-prose today-muted">
              No open positions in the journal. Nothing to defend today.
            </p>
          ) : (
            <div className="today-book">
              {book.map((row) => (
                <button
                  key={row.symbol}
                  type="button"
                  className={`today-pos today-pos--${STOP_COPY[row.state].tone}`}
                  onClick={() => onOpenSymbolChart?.(row.symbol)}
                >
                  <span className="today-pos-sym">
                    <strong>{row.symbol}</strong>
                    <em>{row.qty} @ ₹{row.avgPx.toFixed(2)}</em>
                  </span>
                  <span className="today-pos-stop">{STOP_COPY[row.state].label}</span>
                  <span className="today-pos-num">
                    {row.pnlPct === null ? "—" : `${row.pnlPct > 0 ? "+" : ""}${row.pnlPct.toFixed(1)}%`}
                  </span>
                  <span className="today-pos-num today-pos-risk">
                    {row.openRiskPct === null ? "—" : `${row.openRiskPct.toFixed(2)}% still at risk`}
                  </span>
                </button>
              ))}
              <p className="today-prose today-muted">
                Quantities and averages come straight from the Journal's own FIFO, so the two pages cannot
                disagree about what you hold. The risk column is measured from today's price rather than
                from your entry — it is what you would lose from here, which is deliberately not the same
                number the Journal's action board reports. ₹{money(book.reduce((sum, row) => sum + row.invested, 0))}{" "}
                deployed across {book.length} position{book.length === 1 ? "" : "s"}.
              </p>
            </div>
          )}
        </li>

        <li className="today-step">
          <h3><span>4</span> Where to look</h3>
          {leaders.length === 0 ? (
            <p className="today-prose today-muted">Group rankings have not loaded yet.</p>
          ) : (
            <div className="today-leaders">
              {leaders.map(({ group, picks }) => (
                <div key={group.group_id} className="today-group">
                  <h4>
                    <em>#{group.rank}</em> {group.group_name}
                    <small>{group.trend_label}</small>
                  </h4>
                  <div className="today-picks">
                    {picks.length === 0 ? <span className="today-muted">No liquid names listed</span> : null}
                    {picks.map((stock) => (
                      <button key={stock.symbol} type="button" onClick={() => onOpenSymbolChart?.(stock.symbol)}>
                        {stock.symbol}
                        <em className={(stock.return_1w ?? 0) >= 0 ? "pos" : "neg"}>
                          {(stock.return_1w ?? 0) >= 0 ? "+" : ""}{(stock.return_1w ?? 0).toFixed(1)}%
                        </em>
                      </button>
                    ))}
                  </div>
                </div>
              ))}
              <p className="today-prose today-muted">
                Leading groups first, names second. A good chart in a lagging group is still swimming upstream.
              </p>
            </div>
          )}
        </li>

        <li className="today-step">
          <h3><span>5</span> Sign off</h3>
          <ul className="today-check">
            {steps.map((step) => (
              <li key={step.id}>
                <button
                  type="button"
                  className={ticked[step.id] ? "today-tick today-tick--on" : "today-tick"}
                  aria-pressed={Boolean(ticked[step.id])}
                  onClick={() => tick(step.id)}
                >
                  <span aria-hidden>{ticked[step.id] ? <Check size={12} /> : null}</span>
                  {step.label}
                </button>
              </li>
            ))}
          </ul>
          <p className="today-prose today-muted">
            Ticks reset each session. Cross them against your journal after a few months — if the losing days
            are the days you skipped this, that is the most valuable statistic the app can give you.
          </p>
        </li>
      </ol>
    </div>
  );
}
