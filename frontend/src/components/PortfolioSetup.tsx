import { useCallback, useEffect, useRef, useState } from "react";
import { Plus, Search, Upload, X } from "lucide-react";
import {
  importMfStatement,
  searchMfSchemes,
  type MfImportResult,
  type MfSchemeSearchResult,
} from "../lib/api";

import "./PortfolioSetup.css";

/**
 * Getting holdings *into* the portfolio.
 *
 * Two routes, because the two situations are different: import a broker
 * statement in one go, or add a single fund by name. Both had to exist in the
 * running app — a statement parsed on a developer's machine writes to that
 * machine's state directory and never reaches the deployed server, which is
 * how a complete import went missing once already.
 *
 * The fund search deliberately queries every AMFI scheme rather than the
 * screener's Direct-Growth universe, because a real portfolio contains IDCW and
 * Payout plans and those have to be nameable.
 */

export function PortfolioSetup({
  onImported,
  onAddFund,
  heldCodes,
}: {
  onImported: (result: MfImportResult) => void;
  onAddFund: (schemeCode: string, name: string) => Promise<void> | void;
  heldCodes: Set<string>;
}) {
  const [mode, setMode] = useState<"search" | "import">("search");
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<MfSchemeSearchResult[]>([]);
  const [searching, setSearching] = useState(false);
  const [note, setNote] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [importing, setImporting] = useState(false);
  const [report, setReport] = useState<MfImportResult | null>(null);
  const fileRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    const trimmed = query.trim();
    if (trimmed.length < 3) { setResults([]); return; }
    let cancelled = false;
    setSearching(true);
    const timer = setTimeout(() => {
      searchMfSchemes(trimmed)
        .then((payload) => { if (!cancelled) setResults(payload.results); })
        .catch(() => { if (!cancelled) setResults([]); })
        .finally(() => { if (!cancelled) setSearching(false); });
    }, 260);
    return () => { cancelled = true; clearTimeout(timer); };
  }, [query]);

  const handleFile = useCallback(async (file: File | null) => {
    if (!file) return;
    setImporting(true);
    setError(null);
    setReport(null);
    try {
      const result = await importMfStatement(file, true);
      setReport(result);
      onImported(result);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Could not read that file.");
    } finally {
      setImporting(false);
      if (fileRef.current) fileRef.current.value = "";
    }
  }, [onImported]);

  return (
    <section className="pfs">
      <div className="pfs-modes">
        <button type="button" className={mode === "search" ? "is-active" : ""} onClick={() => setMode("search")}>
          <Search size={12} /> Add a fund
        </button>
        <button type="button" className={mode === "import" ? "is-active" : ""} onClick={() => setMode("import")}>
          <Upload size={12} /> Import a statement
        </button>
      </div>

      {mode === "search" ? (
        <>
          <div className="pfs-search">
            <Search size={13} />
            <input
              placeholder="Search any fund — including IDCW and Payout plans…"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
            {query ? (
              <button type="button" onClick={() => setQuery("")} aria-label="Clear"><X size={12} /></button>
            ) : null}
          </div>

          {query.trim().length >= 3 ? (
            <ul className="pfs-results">
              {results.map((result) => (
                <li key={result.scheme_code}>
                  <span className="pfs-result-name">
                    {result.name}
                    {!result.in_universe ? (
                      <i title="Outside the Direct-Growth screener universe. It values correctly from its own NAV and inherits its Growth sibling's category.">
                        not in screener
                      </i>
                    ) : null}
                  </span>
                  <button
                    type="button"
                    className="pfs-add"
                    disabled={heldCodes.has(result.scheme_code)}
                    onClick={async () => {
                      await onAddFund(result.scheme_code, result.name);
                      setNote(`Added ${result.name}. Use Edit on its row to enter units, amount or a SIP.`);
                      setQuery("");
                    }}
                  >
                    {heldCodes.has(result.scheme_code) ? "Held" : <><Plus size={11} /> Add</>}
                  </button>
                </li>
              ))}
              {!results.length && !searching ? (
                <li className="pfs-empty">Nothing matches that.</li>
              ) : null}
              {searching && !results.length ? <li className="pfs-empty">Searching…</li> : null}
            </ul>
          ) : (
            <p className="pfs-hint">
              Type at least three characters. This searches every AMFI scheme, so the exact plan you
              hold can be picked — not just the Direct-Growth ones the screener compares.
            </p>
          )}
          {note ? <p className="pfs-note">{note}</p> : null}
        </>
      ) : (
        <>
          <div className="pfs-drop">
            <input
              ref={fileRef}
              type="file"
              accept=".xlsx,.xls"
              onChange={(event) => void handleFile(event.target.files?.[0] ?? null)}
            />
            <Upload size={18} />
            <strong>{importing ? "Reading your statement…" : "Choose a P&L statement (.xlsx)"}</strong>
            <small>
              The mutual fund P&amp;L export from your broker. Units and cost come across exactly;
              because a statement carries no purchase dates, XIRR stays blank until you add them.
            </small>
          </div>

          {error ? <p className="pfs-error">{error}</p> : null}

          {report ? (
            <div className="pfs-report">
              <p className="pfs-report-head">
                Imported <b>{report.imported}</b> holdings
                {report.replaced ? `, replacing ${report.replaced}` : ""}
                {report.kept ? `, kept ${report.kept} you already had` : ""}.
              </p>
              {report.reconciliation.length ? (
                <ul className="pfs-recon">
                  {report.reconciliation.map((check) => (
                    <li key={check.label} className={check.agrees ? "is-ok" : "is-off"}>
                      {check.label}: statement ₹{check.statement.toLocaleString("en-IN")} ·
                      imported ₹{check.imported.toLocaleString("en-IN")}
                      {check.agrees ? " — matches" : " — differs, worth a look"}
                    </li>
                  ))}
                </ul>
              ) : null}
              {report.skipped.length ? (
                <div className="pfs-skipped">
                  <strong>{report.skipped.length} row(s) skipped</strong>
                  <ul>
                    {report.skipped.map((row) => (
                      <li key={row.symbol}>{row.symbol} — {row.reason}</li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          ) : null}
        </>
      )}
    </section>
  );
}
