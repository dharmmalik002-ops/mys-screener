import { useEffect, useState } from "react";
import { getBotPaper, type BotPaper } from "../lib/api";

/** The bot's journal — three plain pages: how it is doing, what it holds,
 *  what it has closed.
 *
 *  Deliberately small. The research views already carry every caveat and
 *  every distribution; this is the page you open to answer "what is it doing
 *  and is it working", and anything that does not serve that question belongs
 *  somewhere else.
 *
 *  Every number here is PAPER. No order has ever been placed.
 */
type Page = "home" | "open" | "closed";

const rupees = (v: number) =>
  v >= 1e7 ? `₹${(v / 1e7).toFixed(2)} cr` : `₹${(v / 1e5).toFixed(2)} L`;
const pct = (v: number | null | undefined, dp = 1) =>
  v === null || v === undefined || Number.isNaN(v) ? "—" : `${v >= 0 ? "+" : ""}${v.toFixed(dp)}%`;

export default function BotJournal() {
  const [page, setPage] = useState<Page>("home");
  const [data, setData] = useState<BotPaper | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    getBotPaper()
      .then((d) => alive && setData(d))
      .catch((e) => alive && setError(e?.message ?? "could not load"));
    return () => { alive = false; };
  }, []);

  if (error) return <p className="bot-message">Journal unavailable: {error}</p>;
  if (!data) return <p className="bot-message">Loading the journal…</p>;

  const s = data.summary;
  const open = data.positions ?? [];
  const closed = [...(data.recent_closed ?? [])].reverse();

  return (
    <div className="bot-view">
      <div className="bot-callout bot-callout-warn">
        <div>
          <strong>Paper trading.</strong> No order has been placed and no money is
          at risk. The bot started with {rupees(s.starting_equity)} on {s.started}.
        </div>
      </div>

      <nav className="bot-journal-tabs">
        {([["home", "How it's doing"], ["open", `Open (${open.length})`],
           ["closed", `Closed (${s.closed_trades})`]] as Array<[Page, string]>).map(([id, label]) => (
          <button key={id} type="button"
            className={`bot-view-tab ${page === id ? "is-active" : ""}`}
            onClick={() => setPage(id)}>{label}</button>
        ))}
      </nav>

      {page === "home" ? <Home data={data} /> : null}
      {page === "open" ? <Open rows={open} holdings={data.holdings} /> : null}
      {page === "closed" ? <Closed rows={closed} /> : null}
    </div>
  );
}

function Home({ data }: { data: BotPaper }) {
  const s = data.summary;
  const profit = s.equity - s.starting_equity;
  return (
    <>
      <div className="bot-stat-row">
        <Stat label="Account now" value={rupees(s.equity)} />
        <Stat label="Profit / loss" value={`${profit >= 0 ? "+" : "−"}${rupees(Math.abs(profit))}`}
              tone={profit >= 0 ? "pos" : "neg"} />
        <Stat label="Return" value={pct(s.return_pct, 2)} tone={s.return_pct >= 0 ? "pos" : "neg"} />
        <Stat label="Worst dip" value={pct(s.max_drawdown_pct, 1)} />
      </div>
      <div className="bot-stat-row">
        <Stat label="Holding" value={holdingLabel(data)} />
        <Stat label="Trades closed" value={`${s.closed_trades}`} />
        <Stat label="Winners" value={s.win_rate === null ? "—" : `${s.win_rate.toFixed(0)}%`} />
        <Stat label="Days running" value={`${s.sessions}`} />
      </div>

      {s.closed_trades === 0 ? (
        <p className="bot-note">
          No trades closed yet. The bot checks the market after every close and buys
          only when something clears its rules — on most days that is nothing.
        </p>
      ) : (
        <p className="bot-note">
          Average winner {pct(s.avg_win_pct)}, average loser {pct(s.avg_loss_pct)}. The
          worst single trade cost {pct(s.worst_trade_equity_pct, 2)} of the account.
        </p>
      )}
      <Allocation holdings={data.holdings} />
      <p className="bot-note">
        Last updated after the close on {s.last_session ?? "—"}.
      </p>
    </>
  );
}

const REGIME_NAMES: Record<string, string> = {
  bull_strong: "strong bull", bull_narrow: "narrow bull", choppy: "choppy",
  correction: "correction", bear: "bear", recovery: "recovery",
};

/** "12 stocks + Gold" — stocks first, then whatever the sleeve holds. */
function holdingLabel(data: BotPaper): string {
  const n = data.summary.open_positions;
  const mix = (data.holdings?.sleeve_mix ?? []).filter((m) => m.value > 0);
  const legs = mix.map((m) => m.name.replace(/ \(.*\)$/, ""));
  const parts = [n > 0 ? `${n} stock${n === 1 ? "" : "s"}` : "", legs.join(" + ")].filter(Boolean);
  return parts.length ? parts.join(" + ") : "Cash";
}

/** Where every rupee sits right now: stocks, the sleeve's legs, and cash. */
function Allocation({ holdings }: { holdings?: BotPaper["holdings"] }) {
  if (!holdings) return null;
  const rows: Array<{ label: string; value: number }> = [];
  if (holdings.stocks > 0) rows.push({ label: "Stocks", value: holdings.stocks });
  for (const m of holdings.sleeve_mix ?? []) if (m.value > 0) rows.push({ label: m.name, value: m.value });
  if (holdings.cash > 0.5) rows.push({ label: "Cash", value: holdings.cash });
  const total = rows.reduce((a, r) => a + r.value, 0);
  if (!rows.length || total <= 0) return null;
  const regime = holdings.regime ? REGIME_NAMES[holdings.regime] ?? holdings.regime : null;
  return (
    <>
      <table className="bot-table">
        <thead>
          <tr><th>Where the money is</th><th className="num">Value</th><th className="num">Share</th></tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.label}>
              <td>{r.label}</td>
              <td className="num">₹{Math.round(r.value).toLocaleString("en-IN")}</td>
              <td className="num">{((r.value / total) * 100).toFixed(0)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="bot-note">
        Money not in stocks is parked in the sleeve, which holds whichever of gold, the
        Nifty 500, small caps or a liquid fund has paid best in the current market type
        {regime ? <> (now: <strong>{regime}</strong>)</> : null}. Its daily moves are
        what change the balance on days the bot owns no stocks.
      </p>
    </>
  );
}

function Open({ rows, holdings }: { rows: BotPaper["positions"]; holdings?: BotPaper["holdings"] }) {
  const sleeve = (holdings?.sleeve_mix ?? []).filter((m) => m.value > 0);
  const sleeveTable = sleeve.length ? (
    <table className="bot-table">
      <thead>
        <tr><th>Sleeve (idle money)</th><th className="num">Share</th><th className="num">Value</th></tr>
      </thead>
      <tbody>
        {sleeve.map((m) => (
          <tr key={m.asset}>
            <td>{m.name}</td>
            <td className="num">{(m.weight * 100).toFixed(0)}%</td>
            <td className="num">₹{Math.round(m.value).toLocaleString("en-IN")}</td>
          </tr>
        ))}
      </tbody>
    </table>
  ) : null;
  if (!rows.length)
    return (
      <>
        <p className="bot-note">No stocks held right now{sleeve.length ? " — the money is in the sleeve:" : "."}</p>
        {sleeveTable}
      </>
    );
  return (
    <>
    {sleeveTable}
    <table className="bot-table">
      <thead>
        <tr>
          <th>Stock</th><th>Bought</th><th className="num">Price</th>
          <th className="num">Stop</th><th className="num">Value</th><th className="num">Days</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((p) => (
          <tr key={`${p.symbol}-${p.entry_day}`}>
            <td>{p.symbol}</td>
            <td>{p.entry_day}</td>
            <td className="num">₹{p.entry_price.toFixed(2)}</td>
            <td className="num">₹{p.stop_price.toFixed(2)}</td>
            <td className="num">₹{Math.round(p.shares * p.entry_price).toLocaleString("en-IN")}</td>
            <td className="num">{p.sessions_held}</td>
          </tr>
        ))}
      </tbody>
    </table>
    </>
  );
}

function Closed({ rows }: { rows: BotPaper["recent_closed"] }) {
  if (!rows.length) return <p className="bot-note">No trades closed yet.</p>;
  return (
    <table className="bot-table">
      <thead>
        <tr>
          <th>Stock</th><th>Bought</th><th>Sold</th>
          <th className="num">Result</th><th className="num">Days</th><th>Why it sold</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((t, i) => (
          <tr key={`${t.symbol}-${t.exit_day}-${i}`}>
            <td>{t.symbol}</td>
            <td>{t.entry_day}</td>
            <td>{t.exit_day}</td>
            <td className={`num ${t.net_pct >= 0 ? "pos" : "neg"}`}>{pct(t.net_pct)}</td>
            <td className="num">{t.sessions_held}</td>
            <td>{t.reason === "gap" ? "gapped past the stop"
               : t.reason === "stop" ? "hit its stop"
               : t.reason === "ceiling" ? "held too long" : t.reason}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Stat({ label, value, tone }: { label: string; value: string; tone?: "pos" | "neg" }) {
  return (
    <div className="bot-stat">
      <span className="bot-stat-label">{label}</span>
      <span className={`bot-stat-value ${tone ?? ""}`}>{value}</span>
    </div>
  );
}
