import type { MarketsExposure } from "../../lib/api";
import { Sparkline } from "../Sparkline";
import "./ContextStrip.css";

type Props = {
  data: MarketsExposure | null;
  breadthSeries: number[];
};

function num(value: number | null | undefined, digits = 0, suffix = ""): string {
  return value === null || value === undefined ? "—" : `${value.toFixed(digits)}${suffix}`;
}

/**
 * Context beside the verdict — never inside it.
 *
 * Each of these was tested against forward index returns and drawdowns over
 * ~500 sessions and none ranked monotonically, so folding them into the
 * exposure number would encode a relationship the data does not support. They
 * are here because they describe the environment a trade has to survive, and
 * each carries the caveat that says so.
 */
export function ContextStrip({ data, breadthSeries }: Props) {
  const ctx = data?.context;
  const trend = data?.edge_trend ?? [];
  const edgeSeries = trend.filter((p) => p.eligible).map((p) => p.win_rate ?? 0);

  const participation = ctx?.participation ?? null;
  const xp = ctx?.xp_regime ?? null;
  const dist = ctx?.distribution_days ?? null;

  return (
    <div className="mkc" aria-label="Market context">
      <article className="mkc-item">
        <span className="mkc-label">Edge paying</span>
        <strong>{num(data?.verdict?.win_rate ?? null, 1, "%")}</strong>
        <Sparkline values={edgeSeries} color="var(--accent)" width={92} height={22} />
        <em>weekly win rate, resolved weeks</em>
      </article>

      <article className="mkc-item">
        <span className="mkc-label">Participation</span>
        {/* `value`, not `above_ma50_pct`: which field is populated depends on
            which source the backend served, and reading one directly rendered
            an empty tile whenever it fell through. `value` is always the blend
            (or the single average) that `label` and `universe` describe. */}
        <strong>{num(participation?.value ?? null, 0, "%")}</strong>
        <Sparkline values={breadthSeries} color="var(--text-muted)" width={92} height={22} />
        <em>
          {participation?.label ?? "participation"}
          {participation?.universe ? ` · ${participation.universe}` : ""} · context only
        </em>
      </article>

      <article className="mkc-item">
        <span className="mkc-label">Environment</span>
        <strong className="mkc-word">{xp?.regime ?? "—"}</strong>
        <em>XP {num(xp?.value ?? null, 1)} · coincident, not predictive</em>
      </article>

      <article className="mkc-item">
        <span className="mkc-label">Distribution</span>
        <strong>
          {dist ? `${dist.count} of ${dist.window_sessions}` : "—"}
        </strong>
        <em>
          {dist ? dist.pressure_label : "not enough sessions"}
          {dist && dist.trails_price_by_sessions > 0 ? " · as of prior close" : ""}
        </em>
      </article>
    </div>
  );
}
