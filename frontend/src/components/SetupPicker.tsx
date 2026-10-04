import { useState } from "react";
import { Sparkles } from "lucide-react";

import { getLookalikeAiReview, type LookalikeAiReview } from "../lib/api";
import { SETUP_ORDER, TRADER_NAMES, joinStyle, setupName } from "../lib/lookalikeStyles";

/* One picker for every look-alike view: whose setups, then which setup.
   `counts` is the number of the trader's own charts per style. */
export function SetupPicker({
  counts,
  trader,
  setup,
  onChange,
}: {
  counts: Record<string, number>;
  trader: string;
  setup: string;
  onChange: (trader: string, setup: string) => void;
}) {
  const traders = Array.from(new Set(Object.keys(counts).map((s) => s.split("_")[0])));
  traders.sort((a, b) => (counts[b] ?? 0) - (counts[a] ?? 0));
  const setups = SETUP_ORDER.filter((s) => joinStyle(trader, s) in counts);
  return (
    <div className="setup-picker">
      <div className="setup-picker-row" role="tablist" aria-label="Whose setups">
        {traders.map((t) => (
          <button
            key={t}
            type="button"
            role="tab"
            aria-selected={t === trader}
            className={`setup-picker-trader${t === trader ? " is-active" : ""}`}
            onClick={() => onChange(t, "")}
          >
            {TRADER_NAMES[t] ?? t}
            <span>{(counts[t] ?? 0).toLocaleString("en-IN")} charts</span>
          </button>
        ))}
      </div>
      {setups.length > 1 ? (
        <div className="setup-picker-row" role="tablist" aria-label="Setup">
          {setups.map((s) => (
            <button
              key={s || "all"}
              type="button"
              role="tab"
              aria-selected={s === setup}
              className={`lookalike-filter${s === setup ? " is-active" : ""}`}
              onClick={() => onChange(trader, s)}
            >
              {setupName(s, trader)} <span className="setup-picker-count">{(counts[joinStyle(trader, s)] ?? 0).toLocaleString("en-IN")}</span>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

const VERDICT_TEXT: Record<LookalikeAiReview["verdict"], string> = {
  yes: "Fits the setup",
  partly: "Partly fits",
  no: "Doesn't fit",
};

/** "AI second opinion": a vision model looks at the chart and says whether
    it shows the setup. One click, cached on the server; a description, not advice. */
export function AiReview({ style, symbol, date }: { style: string; symbol: string; date?: string }) {
  const [state, setState] = useState<{ loading: boolean; review?: LookalikeAiReview; note?: string }>({ loading: false });
  if (!state.review && !state.note) {
    return (
      <button
        type="button"
        className="lookalike-link ai-review-button"
        disabled={state.loading}
        onClick={() => {
          setState({ loading: true });
          getLookalikeAiReview(style, symbol, date)
            .then((r) => setState(r.available && r.review ? { loading: false, review: r.review } : { loading: false, note: r.reason ?? "No review available." }))
            .catch((e) => setState({ loading: false, note: e instanceof Error ? e.message : "The AI review failed." }));
        }}
      >
        <Sparkles size={12} /> {state.loading ? "Asking the AI…" : "AI second opinion"}
      </button>
    );
  }
  if (state.note) return <span className="lookalike-stat-sub">{state.note}</span>;
  const r = state.review!;
  return (
    <div className={`ai-review is-${r.verdict}`}>
      <strong>
        <Sparkles size={12} /> AI: {VERDICT_TEXT[r.verdict]} · {r.score}/5
      </strong>
      {r.why ? <span>{r.why}</span> : null}
      {r.look_for ? <span className="lookalike-stat-sub">Look at: {r.look_for}</span> : null}
    </div>
  );
}
