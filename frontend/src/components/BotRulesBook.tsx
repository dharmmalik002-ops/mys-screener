import { useEffect, useState } from "react";
import { AlertTriangle } from "lucide-react";
import { getBotRobust, type BotRobust } from "../lib/api";

/** The rules-based book — year by year against the Nifty Smallcap 250.
 *
 *  This view exists because the Bot tab was showing the strategy playbook's
 *  rolling walk-forward result (-1.87%/yr) while the work that had actually
 *  been done lived in an artifact nothing served. Both are real; they are
 *  different measurements, and the header says so rather than leaving the
 *  reader to assume the friendlier number is the same evidence.
 */
export default function BotRulesBook({ equity }: { equity: number }) {
  const [data, setData] = useState<BotRobust | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    getBotRobust()
      .then((d) => alive && setData(d))
      .catch((e) => alive && setError(e?.message ?? "could not load"));
    return () => {
      alive = false;
    };
  }, []);

  if (error) return <p className="bot-message">Rules book unavailable: {error}</p>;
  if (!data) return <p className="bot-message">Loading the rules book…</p>;

  const rows = (data.diagnosis ?? []).filter((r) => r.index_return !== null);
  const money = (pct: number) => equity * (1 + pct / 100);
  const fmt = (v: number) =>
    v >= 1e7 ? `₹${(v / 1e7).toFixed(2)} cr` : `₹${(v / 1e5).toFixed(2)} L`;

  return (
    <div className="bot-view">
      <div className="bot-callout bot-callout-warn">
        <AlertTriangle size={15} aria-hidden />
        <div>
          <strong>Two different tests, two different answers.</strong> This book is
          measured on a <em>single</em> train/test split and returns{" "}
          <strong>{data.cagr.toFixed(1)}%</strong> a year. The Scorecard's
          walk-forward test rebuilds the rules every January and trades the next
          year blind — that returns <strong>-1.87%</strong> a year. The second is
          the harder test and this system has never been put through it.
        </div>
      </div>

      <div className="bot-stat-row">
        <Stat label="CAGR" value={`${data.cagr.toFixed(2)}%`} />
        <Stat label="Worst drawdown" value={`${data.max_drawdown.toFixed(2)}%`} />
        <Stat label="Sharpe" value={data.sharpe.toFixed(2)} />
        <Stat label="Win rate" value={`${data.win_rate.toFixed(1)}%`} />
        <Stat label="Reward : risk" value={`1 : ${data.payoff.toFixed(1)}`} />
        <Stat
          label="Years behind index"
          value={`${data.summary?.years_behind ?? "—"} of ${data.summary?.years_total ?? rows.length}`}
        />
      </div>

      <table className="bot-table">
        <thead>
          <tr>
            <th>Year</th>
            <th className="num">Bot</th>
            <th className="num">Smallcap 250</th>
            <th className="num">Difference</th>
            <th className="num">Trades</th>
            <th className="num">{fmt(equity)} would be</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.year}>
              <td>{r.year}</td>
              <td className={`num ${r.bot_return >= 0 ? "pos" : "neg"}`}>
                {r.bot_return >= 0 ? "+" : ""}
                {r.bot_return.toFixed(1)}%
              </td>
              <td className="num">
                {(r.index_return ?? 0) >= 0 ? "+" : ""}
                {(r.index_return ?? 0).toFixed(1)}%
              </td>
              <td className={`num ${(r.alpha ?? 0) >= 0 ? "pos" : "neg"}`}>
                {(r.alpha ?? 0) >= 0 ? "+" : ""}
                {(r.alpha ?? 0).toFixed(1)}pp
              </td>
              <td className="num">{r.accepted}</td>
              <td className="num">{fmt(money(r.bot_return))}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="bot-note">
        The last column is one year in isolation on your stated book size — it does
        not compound down the table.
      </p>

      {data.caveats?.length ? (
        <>
          <h4>Before you trade this</h4>
          <ul className="bot-caveats">
            {data.caveats.map((c) => (
              <li key={c}>{c}</li>
            ))}
          </ul>
        </>
      ) : null}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bot-stat">
      <span className="bot-stat-label">{label}</span>
      <span className="bot-stat-value">{value}</span>
    </div>
  );
}
