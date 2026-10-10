import { useEffect, useMemo, useRef, useState } from "react";

import { getCatalysts, type CatalystItem, type CatalystsResponse } from "../lib/api";

/**
 * The fundamentals pane's Catalysts tab: what could move this company's
 * business, from reputed news outlets and its latest earnings call,
 * each explained in plain words by the AI (backend: services/catalysts.py).
 *
 * The server answers at once with the last record and rebuilds it in the
 * background once a day; while `refreshing` is set this polls until it lands.
 */

type Filter = "all" | "positive" | "negative";

const POLL_MS = 6000;
const POLL_LIMIT = 40; // ~4 minutes: a long transcript plus the AI call fits well inside

const STANCE_LABEL: Record<string, string> = {
  supportive: "Supportive",
  mixed: "Mixed",
  cautionary: "Cautionary",
  quiet: "Quiet",
};

const POLARITY_LABEL: Record<CatalystItem["polarity"], string> = {
  positive: "Tailwind",
  negative: "Headwind",
  mixed: "Mixed",
};

const HORIZON_LABEL: Record<CatalystItem["horizon"], string> = {
  near: "next 1-2 quarters",
  medium: "6-18 months",
  long: "beyond 18 months",
};

function fmtDate(iso: string | null | undefined) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso.slice(0, 10);
  return d.toLocaleDateString("en-IN", { day: "numeric", month: "short", year: "numeric" });
}

export function CatalystsTab({ symbol }: { symbol: string }) {
  const [data, setData] = useState<CatalystsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<Filter>("all");
  const [refreshTick, setRefreshTick] = useState(0);
  const forceRef = useRef(false);

  useEffect(() => {
    setData(null);
    setError(null);
  }, [symbol]);

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;
    let polls = 0;
    const load = async () => {
      const force = forceRef.current;
      forceRef.current = false;
      try {
        const next = await getCatalysts(symbol, force);
        if (cancelled) return;
        setData(next);
        setError(null);
        if ((next.refreshing || next.status === "building") && polls < POLL_LIMIT) {
          polls += 1;
          timer = window.setTimeout(load, POLL_MS);
        }
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Catalysts could not be loaded.");
      }
    };
    void load();
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [symbol, refreshTick]);

  const items = useMemo(() => data?.catalysts ?? [], [data]);
  const counts = useMemo(
    () => ({
      positive: items.filter((c) => c.polarity === "positive").length,
      negative: items.filter((c) => c.polarity === "negative").length,
    }),
    [items],
  );
  const shown = filter === "all" ? items : items.filter((c) => c.polarity === filter);

  if (error && !data) return <div className="rf-empty">{error}</div>;
  if (!data || (data.status !== "ready" && !items.length)) {
    return (
      <div className="rf-empty">
        Reading {symbol}'s latest news and earnings call… the first read of a stock takes up to a minute.
      </div>
    );
  }

  const overall = data.overall;
  const sources = data.sources ?? {};
  const newsFeed = data.news_feed ?? [];
  const otherNews = newsFeed.filter((n) => n.status !== "catalyst");
  const newsCount = sources.news?.count ?? newsFeed.length;
  const searchesDown = Object.entries(sources.news?.sources ?? {}).filter(([key, s]) => !key.startsWith("feed:") && !s.ok).length;
  const sourceBits: string[] = [];
  if (sources.news?.ok) {
    sourceBits.push(
      `${newsCount} ${newsCount === 1 ? "story" : "stories"} from reputed outlets (ET, Mint, Business Standard, Moneycontrol, Reuters, Bloomberg and others) in the last 60 days`,
    );
  }
  if (data.concall?.date) sourceBits.push(`the earnings call of ${fmtDate(data.concall.date)}${data.concall.read ? "" : " (transcript not readable)"}`);
  const failed: string[] = [];
  if (sources.news && !sources.news.ok) failed.push("News");
  else if (searchesDown) failed.push(`${searchesDown} of the news searches`);
  if (sources.concall && !sources.concall.ok) failed.push("The earnings-call transcript (BSE)");

  return (
    <div className="rf-section cat">
      <div className="cat-head">
        <div>
          <div className="cat-kicker">Catalysts{data.refreshed_on ? ` · updated ${fmtDate(data.refreshed_on)}` : ""}</div>
          {overall ? (
            <div className={`cat-stance cat-stance-${overall.stance}`}>{STANCE_LABEL[overall.stance] ?? overall.stance}</div>
          ) : null}
        </div>
        <button
          type="button"
          className="cat-refresh"
          disabled={data.refreshing}
          onClick={() => {
            forceRef.current = true;
            setRefreshTick((n) => n + 1);
          }}
        >
          {data.refreshing ? "Updating…" : "Refresh"}
        </button>
      </div>

      {overall?.summary ? <p className="cat-summary">{overall.summary}</p> : null}

      {overall && (overall.reasons_to_own.length || overall.reasons_to_avoid.length) ? (
        <div className="cat-case">
          <div className="cat-case-col">
            <div className="cat-case-h cat-pos">Reasons to own</div>
            {overall.reasons_to_own.length ? (
              <ul>{overall.reasons_to_own.map((r) => <li key={r}>{r}</li>)}</ul>
            ) : (
              <div className="rf-muted">None stands out.</div>
            )}
          </div>
          <div className="cat-case-col">
            <div className="cat-case-h cat-neg">Reasons to avoid</div>
            {overall.reasons_to_avoid.length ? (
              <ul>{overall.reasons_to_avoid.map((r) => <li key={r}>{r}</li>)}</ul>
            ) : (
              <div className="rf-muted">None stands out.</div>
            )}
          </div>
        </div>
      ) : null}

      {overall?.watch_next.length ? (
        <div className="cat-watch">
          <span className="cat-watch-h">Watch next</span>
          {overall.watch_next.join(" · ")}
        </div>
      ) : null}

      {!data.ai_available || data.pending_ai ? (
        <div className="cat-note">
          {data.ai_available
            ? "Some items are not analysed yet (the AI was unreachable); they are shown by keyword and will be explained on the next update."
            : "AI analysis is not configured on the server, so items are shown by keyword without an explanation."}
        </div>
      ) : null}

      <div className="cat-filters" role="tablist" aria-label="Filter catalysts">
        {(["all", "positive", "negative"] as const).map((f) => (
          <button
            key={f}
            type="button"
            role="tab"
            aria-selected={filter === f}
            className={filter === f ? "cat-chip active" : "cat-chip"}
            onClick={() => setFilter(f)}
          >
            {f === "all" ? `All ${items.length}` : f === "positive" ? `Tailwinds ${counts.positive}` : `Headwinds ${counts.negative}`}
          </button>
        ))}
      </div>

      {shown.length ? (
        <ol className="cat-list">
          {shown.map((c) => (
            <li key={c.id} className={`cat-item cat-${c.polarity}`}>
              <div className="cat-meta">
                <span className={`cat-badge cat-badge-${c.polarity}`}>{POLARITY_LABEL[c.polarity]}</span>
                <span className="cat-impact">{c.impact} impact</span>
                <span>{c.category}</span>
                <span>{fmtDate(c.date)}</span>
              </div>
              <div className="cat-title">{c.headline || c.title}</div>
              {c.what_happened ? <p className="cat-text">{c.what_happened}</p> : null}
              {c.effect_on_company ? (
                <p className="cat-text">
                  <span className="cat-label">Effect on the company · {HORIZON_LABEL[c.horizon]}</span>
                  {c.effect_on_company}
                </p>
              ) : null}
              {c.analyst_view ? (
                <p className="cat-text">
                  <span className="cat-label">Analyst view</span>
                  {c.analyst_view}
                </p>
              ) : null}
              <div className="cat-source">
                {c.link ? (
                  <a className="rf-link" href={c.link} target="_blank" rel="noreferrer">
                    {c.source}
                  </a>
                ) : (
                  c.source
                )}
                {c.source_type !== "concall" && c.headline && c.headline !== c.title ? <span className="rf-muted"> · {c.title}</span> : null}
                {!c.ai ? <span className="rf-muted"> · not yet analysed</span> : null}
              </div>
            </li>
          ))}
        </ol>
      ) : (
        <div className="rf-empty">
          {items.length
            ? "Nothing in this filter."
            : "No company-specific catalyst in recent news from reputed outlets or the latest earnings call. Market wraps, stock-tip lists and other outlets are left out on purpose."}
        </div>
      )}

      {otherNews.length ? (
        <details className="cat-news" open={!items.length}>
          <summary>
            Other recent news from reputed outlets <span className="rf-muted">· {otherNews.length}, judged not to change the business</span>
          </summary>
          <ul className="cat-news-list">
            {otherNews.map((n) => (
              <li key={n.id}>
                {n.link ? (
                  <a className="rf-link" href={n.link} target="_blank" rel="noreferrer">
                    {n.title}
                  </a>
                ) : (
                  n.title
                )}
                <span className="rf-muted">
                  {" "}
                  · {n.source} · {fmtDate(n.date)}
                  {n.status === "pending" ? " · not yet analysed" : ""}
                </span>
              </li>
            ))}
          </ul>
        </details>
      ) : null}

      <div className="cat-foot">
        {sourceBits.length ? `Read from ${sourceBits.join(", ")}. ` : ""}
        {failed.length ? `${failed.join(" and ")} could not be reached this time. ` : ""}
        Updated daily; generic market news and unknown outlets are left out. AI explanations can be wrong — open the source before acting.
      </div>
    </div>
  );
}
