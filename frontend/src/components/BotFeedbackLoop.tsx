import { useEffect, useState } from "react";
import { AlertTriangle, CheckCircle2, CircleDashed, Info, PauseCircle } from "lucide-react";

import { getBotCalibration, type BotCalibration, type BotCellCalibration } from "../lib/api";

/* The live feedback loop.

   Everything else on the Bot page is a study of the past. This is the part
   that watches the present: as real trades close they are recorded through
   `POST /api/bot/ledger/trade`, and each closed trade tests whether the cell
   that produced it still behaves the way the study said it would.

   It audits the model rather than retraining it, and the asymmetry is the
   point: a cell can be flagged, halved, or stood down by its live record, and
   can never be promoted by it. Twenty R-multiples have a standard error near
   half an R — wider than any edge in this book — so a good run is not
   evidence, and this system has already measured that reacting to one loses
   money. */

const STATUS_META: Record<string, { label: string; tone: string }> = {
  tracking: { label: "Tracking", tone: "good" },
  diverging: { label: "Diverging", tone: "warn" },
  suspended: { label: "Stood down", tone: "bad" },
  insufficient: { label: "Not yet known", tone: "flat" },
};

function StatusMark({ status }: { status: string }) {
  const meta = STATUS_META[status] ?? STATUS_META.insufficient;
  const Icon =
    status === "tracking" ? CheckCircle2
      : status === "diverging" ? AlertTriangle
        : status === "suspended" ? PauseCircle
          : CircleDashed;
  return (
    <span className={`bot-mark bot-mark-${meta.tone}`}>
      <Icon size={13} aria-hidden /> {meta.label}
    </span>
  );
}

export function FeedbackLoopView() {
  const [data, setData] = useState<BotCalibration | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const result = await getBotCalibration();
        if (!cancelled) setData(result);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : "Could not load calibration.");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  if (loading) return <p className="bot-empty">Checking the live record…</p>;
  if (error || !data) {
    return (
      <div className="bot-alert bot-alert-warn">
        <AlertTriangle size={15} aria-hidden />
        <span>
          {error ?? "No calibration available."} The loop needs a trade ledger on this host;
          the rest of the page is unaffected.
        </span>
      </div>
    );
  }

  const watched = data.cells.filter((c) => c.status !== "insufficient");
  const waiting = data.cells.filter((c) => c.status === "insufficient");

  return (
    <div className="bot-feedback">
      <div className="bot-alert bot-alert-info">
        <Info size={15} aria-hidden />
        <span>{data.method}</span>
      </div>

      <section>
        <h4>Is the live book behaving as modelled?</h4>
        <div className={`bot-book-status bot-book-${data.book.status}`}>
          <div>
            <p className="bot-kicker">Book calibration</p>
            <strong><StatusMark status={data.book.status} /></strong>
          </div>
          <p>{data.book.note}</p>
        </div>
        {data.suspended.length || data.diverging.length ? (
          <p className="bot-verdict bot-verdict-warn">
            <AlertTriangle size={15} aria-hidden />
            <span>
              {data.suspended.length
                ? `${data.suspended.length} setup(s) stood down by their own live record: ${data.suspended.join(", ")}. `
                : ""}
              {data.diverging.length
                ? `${data.diverging.length} flagged and halved: ${data.diverging.join(", ")}.`
                : ""}
            </span>
          </p>
        ) : null}
      </section>

      {watched.length ? (
        <section>
          <h4>Setups with a live record ({watched.length})</h4>
          <table className="bot-table bot-table-compact">
            <thead>
              <tr>
                <th>Setup</th><th>Regime</th><th className="num">Live trades</th>
                <th className="num">Live avg</th><th className="num">Expected</th>
                <th className="num">Size</th><th>Status</th>
              </tr>
            </thead>
            <tbody>
              {watched.map((cell: BotCellCalibration) => (
                <tr key={`${cell.strategy}-${cell.regime}`} title={cell.note}>
                  <td>{cell.strategy.replace(/_/g, " ")}</td>
                  <td>{cell.regime.replace(/_/g, " ")}</td>
                  <td className="num">{cell.live_trades}</td>
                  <td className={`num ${cell.live_avg_r >= 0 ? "bot-expected" : "bot-negative"}`}>
                    {cell.live_avg_r >= 0 ? "+" : ""}{cell.live_avg_r.toFixed(2)}R
                  </td>
                  <td className="num">
                    {cell.expected_r >= 0 ? "+" : ""}{cell.expected_r.toFixed(2)}R
                  </td>
                  <td className="num">{(cell.size_multiplier * 100).toFixed(0)}%</td>
                  <td><StatusMark status={cell.status} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}

      <section>
        <h4>Waiting on evidence ({waiting.length})</h4>
        <p className="bot-section-note">
          These have fewer than 25 closed live trades, so their live record cannot yet say
          anything. They trade at full size meanwhile — standing a setup down because of a
          handful of results is exactly the reflex this system measured as costly.
        </p>
        <p className="bot-footnote">
          Record a closed trade with <code>POST /api/bot/ledger/trade</code>. It joins the same
          table as the {(96401).toLocaleString("en-IN")} simulated ones, is reviewed on arrival,
          and feeds this page.
        </p>
      </section>
    </div>
  );
}
