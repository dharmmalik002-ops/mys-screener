import { useEffect, useMemo, useRef, useState } from "react";
import { AlertTriangle, Plus, RefreshCw, Sparkles, X } from "lucide-react";
import {
  getAiScanCatalog,
  parseAiScan,
  runAiScan,
  type AiScanCriterion,
  type AiScanParseResult,
  type AiScanPatternRef,
  type AiScanRequest,
  type AiScanRunResult,
  type MarketKey,
} from "../lib/api";
import {
  clearCriterion,
  editLabel,
  formatMetric,
  isEditable,
  isSignedColumn,
  sortRows,
  withCriterionValue,
  withoutPattern,
  withPattern,
  type SortKey,
} from "../lib/aiScanner";
import "./AiScannerPanel.css";

type Props = {
  market?: MarketKey;
  onOpenChartWithList: (symbol: string, symbols: string[]) => void;
};

const STORAGE_KEY = "mr-malik-ai-scanner:v1";

const EXAMPLES = [
  "Within 20% of the 52-week high and sales growing 20% or more",
  "VCP or cup and handle with RS rating above 80",
  "Just under resistance, above the 50 EMA, profit growth 25%+ for the last 3 quarters",
  "Small caps near the pivot with operating margins expanding",
];

type Saved = { query: string; parse: AiScanParseResult | null; request: AiScanRequest | null };

function readSaved(): Saved {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return { query: "", parse: null, request: null };
    const parsed = JSON.parse(raw) as Saved;
    return { query: String(parsed.query ?? ""), parse: parsed.parse ?? null, request: parsed.request ?? null };
  } catch {
    return { query: "", parse: null, request: null };
  }
}

function writeSaved(value: Saved) {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
  } catch {
    /* storage is a convenience; the page works without it */
  }
}

function errorText(error: unknown): string {
  if (error instanceof Error) return error.message;
  return "Something went wrong.";
}

/**
 * Type what you are looking for; the AI turns it into filters; the scanner —
 * not the AI — picks the stocks. Every filter is shown as a chip you can edit
 * or remove, and anything the AI could not express is listed as NOT applied,
 * so a result is never quietly narrower or wider than the sentence implied.
 */
export function AiScannerPanel({ market = "india", onOpenChartWithList }: Props) {
  const saved = useRef(readSaved()).current;
  const [query, setQuery] = useState(saved.query);
  const [parse, setParse] = useState<AiScanParseResult | null>(saved.parse);
  const [request, setRequest] = useState<AiScanRequest | null>(saved.request);
  const [result, setResult] = useState<AiScanRunResult | null>(null);
  const [parsing, setParsing] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [sort, setSort] = useState<SortKey>(null);
  const [catalog, setCatalog] = useState<AiScanPatternRef[]>([]);
  const runSeq = useRef(0);

  const run = async (next: AiScanRequest) => {
    const seq = ++runSeq.current;
    setRunning(true);
    setError(null);
    try {
      const response = await runAiScan(next, market);
      if (seq !== runSeq.current) return;
      setResult(response);
    } catch (err) {
      if (seq !== runSeq.current) return;
      setError(errorText(err));
    } finally {
      if (seq === runSeq.current) setRunning(false);
    }
  };

  const apply = (next: AiScanRequest) => {
    setRequest(next);
    setEditing(null);
    writeSaved({ query, parse, request: next });
    void run(next);
  };

  // Re-run the last scan on open: it costs no AI call and today's data may be newer.
  useEffect(() => {
    if (saved.request) void run(saved.request);
    getAiScanCatalog()
      .then((value) => setCatalog(value.patterns ?? []))
      .catch(() => setCatalog([]));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [market]);

  const submit = async (text: string) => {
    const trimmed = text.trim();
    if (!trimmed || parsing) return;
    setQuery(trimmed);
    setParsing(true);
    setError(null);
    try {
      const parsed = await parseAiScan(trimmed, market);
      setParse(parsed);
      setRequest(parsed.request);
      setSort(null);
      writeSaved({ query: trimmed, parse: parsed, request: parsed.request });
      await run(parsed.request);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setParsing(false);
    }
  };

  const criteria: AiScanCriterion[] = result?.criteria ?? parse?.criteria ?? [];
  const patterns: AiScanPatternRef[] = result?.patterns ?? parse?.patterns ?? [];
  const rows = useMemo(() => sortRows(result?.items ?? [], sort), [result, sort]);
  const symbols = useMemo(() => rows.map((row) => row.symbol), [rows]);
  const hasFundamentals = criteria.some((c) => c.group === "fundamental");
  // RS is always shown, once: as its own column unless an RS filter already added it.
  const showRs = !(result?.columns ?? []).some((column) => column.id === "rs");
  const addable = catalog.filter((p) => !request?.patterns.includes(p.id));

  const toggleSort = (id: string) => {
    setSort((current) => (current?.id === id ? { id, dir: current.dir === "desc" ? "asc" : "desc" } : { id, dir: "desc" }));
  };

  const sortMark = (id: string) => (sort?.id === id ? (sort.dir === "desc" ? " ↓" : " ↑") : "");

  return (
    <div className="aiscan">
      <header className="aiscan-head">
        <p className="aiscan-eyebrow">AI scanner</p>
        <h2>Describe the stocks you want</h2>
        <p className="aiscan-sub">
          Technical, fundamental or chart-pattern criteria in plain words. The AI turns them into filters; the scanner picks the stocks.
        </p>
      </header>

      <form
        className="aiscan-ask"
        onSubmit={(event) => {
          event.preventDefault();
          void submit(query);
        }}
      >
        <textarea
          value={query}
          rows={2}
          maxLength={600}
          placeholder="e.g. Within 20% of the 52-week high, sales growth above 20%, forming a cup and handle"
          onChange={(event) => setQuery(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              void submit(query);
            }
          }}
        />
        <button type="submit" className="aiscan-go" disabled={parsing || !query.trim()}>
          {parsing ? <RefreshCw size={15} className="aiscan-spin" /> : <Sparkles size={15} />}
          {parsing ? "Reading…" : "Scan"}
        </button>
      </form>

      {!parse && !parsing ? (
        <div className="aiscan-examples">
          {EXAMPLES.map((example) => (
            <button key={example} type="button" onClick={() => void submit(example)}>
              {example}
            </button>
          ))}
        </div>
      ) : null}

      {error ? (
        <div className="aiscan-error" role="alert">
          <AlertTriangle size={15} /> {error}
        </div>
      ) : null}

      {parse && request ? (
        <section className="aiscan-plan">
          {parse.summary ? <p className="aiscan-summary">{parse.summary}</p> : null}

          <div className="aiscan-chips">
            {criteria.map((criterion) =>
              editing === criterion.key ? (
                <form
                  key={criterion.key}
                  className="aiscan-chip is-editing"
                  onSubmit={(event) => {
                    event.preventDefault();
                    const next = withCriterionValue(request, criterion, draft);
                    if (next) apply(next);
                  }}
                >
                  <span>{editLabel(criterion)}</span>
                  <input autoFocus value={draft} onChange={(event) => setDraft(event.target.value)} onBlur={() => setEditing(null)} />
                </form>
              ) : (
                <span key={criterion.key} className={`aiscan-chip is-${criterion.group}`}>
                  {isEditable(criterion) ? (
                    <button
                      type="button"
                      className="aiscan-chip-text"
                      title="Change the number"
                      onClick={() => {
                        setEditing(criterion.key);
                        setDraft(String(criterion.value));
                      }}
                    >
                      {criterion.text}
                    </button>
                  ) : (
                    <span className="aiscan-chip-text">{criterion.text}</span>
                  )}
                  <button type="button" className="aiscan-chip-x" aria-label={`Remove ${criterion.text}`} onClick={() => apply(clearCriterion(request, criterion))}>
                    <X size={12} />
                  </button>
                </span>
              ),
            )}
            {patterns.map((pattern) => (
              <span key={pattern.id} className="aiscan-chip is-pattern" title={pattern.description}>
                <span className="aiscan-chip-text">{pattern.name}</span>
                <button type="button" className="aiscan-chip-x" aria-label={`Remove ${pattern.name}`} onClick={() => apply(withoutPattern(request, pattern.id))}>
                  <X size={12} />
                </button>
              </span>
            ))}
            {patterns.length > 1 ? (
              <span className="aiscan-match" role="group" aria-label="Pattern match">
                {(["any", "all"] as const).map((mode) => (
                  <button
                    key={mode}
                    type="button"
                    className={request.pattern_match === mode ? "active" : ""}
                    onClick={() => apply({ ...request, pattern_match: mode })}
                  >
                    {mode === "any" ? "Any pattern" : "All patterns"}
                  </button>
                ))}
              </span>
            ) : null}
            {addable.length ? (
              <label className="aiscan-add">
                <Plus size={13} />
                <select
                  value=""
                  onChange={(event) => {
                    if (event.target.value) apply(withPattern(request, event.target.value));
                  }}
                >
                  <option value="">Add pattern</option>
                  {addable.map((pattern) => (
                    <option key={pattern.id} value={pattern.id}>
                      {pattern.name}
                    </option>
                  ))}
                </select>
              </label>
            ) : null}
            {!criteria.length && !patterns.length ? <span className="aiscan-muted">No filters — every stock in the universe.</span> : null}
          </div>

          {parse.unsupported.length ? (
            <div className="aiscan-unsupported">
              <strong>Not applied</strong> — the results are NOT filtered on:
              <ul>
                {parse.unsupported.map((item, index) => (
                  <li key={`${item.text}-${index}`}>
                    <span>“{item.text}”</span>
                    {item.reason ? <em> — {item.reason}</em> : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}

          {parse.notes.length ? (
            <ul className="aiscan-notes">
              {parse.notes.map((note, index) => (
                <li key={index}>{note}</li>
              ))}
            </ul>
          ) : null}
        </section>
      ) : null}

      {result ? (
        <section className="aiscan-results">
          <div className="aiscan-results-head">
            <p>
              <strong>{result.hit_count.toLocaleString("en-IN")}</strong> of {result.universe_count.toLocaleString("en-IN")} stocks
              {result.hit_count > result.items.length ? ` · showing ${result.items.length}` : ""}
              {result.session_date ? ` · close of ${result.session_date}` : ""}
            </p>
            {running ? <RefreshCw size={14} className="aiscan-spin" aria-label="Updating" /> : null}
          </div>
          {hasFundamentals ? (
            <p className="aiscan-caveat">
              Fundamentals: standalone quarterly results filed on BSE{result.fundamentals_as_of ? `, file of ${result.fundamentals_as_of.slice(0, 10)}` : ""}.
              Companies without a current filing are left out of fundamental filters; growth on a loss is not counted.
            </p>
          ) : null}

          {rows.length ? (
            <div className="aiscan-table-wrap">
              <table className="aiscan-table">
                <thead>
                  <tr>
                    <th onClick={() => toggleSort("symbol")}>Stock{sortMark("symbol")}</th>
                    <th className="num" onClick={() => toggleSort("price")}>Price{sortMark("price")}</th>
                    <th className="num" onClick={() => toggleSort("change")}>Chg{sortMark("change")}</th>
                    {result.columns
                      .filter((column) => column.id !== "price" && column.id !== "change")
                      .map((column) => (
                        <th key={column.id} className="num" onClick={() => toggleSort(column.id)}>
                          {column.label}
                          {sortMark(column.id)}
                        </th>
                      ))}
                    {showRs ? <th className="num" onClick={() => toggleSort("rs")}>RS{sortMark("rs")}</th> : null}
                    <th>Why it matched</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => (
                    <tr key={row.symbol} onClick={() => onOpenChartWithList(row.symbol, symbols)}>
                      <td>
                        <span className="aiscan-sym">{row.symbol}</span>
                        <span className="aiscan-name">{row.name}</span>
                        <span className="aiscan-sector">{row.sector}</span>
                      </td>
                      <td className="num">{row.last_price.toLocaleString("en-IN", { maximumFractionDigits: 2 })}</td>
                      <td className={`num ${row.change_pct > 0 ? "pos" : row.change_pct < 0 ? "neg" : ""}`}>
                        {row.change_pct > 0 ? "+" : ""}
                        {row.change_pct.toFixed(2)}%
                      </td>
                      {result.columns
                        .filter((column) => column.id !== "price" && column.id !== "change")
                        .map((column) => {
                          const value = row.metrics?.[column.id];
                          const tone =
                            isSignedColumn(column.id) && typeof value === "number" ? (value > 0 ? "pos" : value < 0 ? "neg" : "") : "";
                          return (
                            <td key={column.id} className={`num ${tone}`}>
                              {formatMetric(value, column)}
                            </td>
                          );
                        })}
                      {showRs ? <td className="num">{row.rs_rating ?? "—"}</td> : null}
                      <td className="aiscan-why">
                        {row.matched_patterns.length ? <span className="aiscan-tag">{row.matched_patterns.join(" + ")}</span> : null}
                        {(row.reasons ?? []).slice(0, 2).join(" · ")}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="aiscan-muted">No stock matches every criterion today. Remove or loosen a chip above to widen the search.</p>
          )}
        </section>
      ) : null}
    </div>
  );
}

export default AiScannerPanel;
