import { useEffect, useMemo, useRef, useState } from "react";
import { Activity, BarChart3, Layers, LineChart, Search as SearchIcon } from "lucide-react";

import "./CommandPalette.css";

export type PaletteSymbol = { symbol: string; name: string };
export type PaletteGroup = { id: string; name: string; rank: number };
export type PaletteScanner = { mode: string; label: string };
export type PalettePage = { page: string; label: string };

type PaletteEntry =
  | { kind: "symbol"; key: string; label: string; sub: string; symbol: string }
  | { kind: "scanner"; key: string; label: string; sub: string; mode: string }
  | { kind: "group"; key: string; label: string; sub: string; groupId: string }
  | { kind: "page"; key: string; label: string; sub: string; page: string };

type CommandPaletteProps = {
  open: boolean;
  onClose: () => void;
  symbols: PaletteSymbol[];
  groups: PaletteGroup[];
  scanners: PaletteScanner[];
  pages: PalettePage[];
  onPickSymbol: (symbol: string) => void;
  onPickScanner: (mode: string) => void;
  onPickGroup: (groupId: string) => void;
  onPickPage: (page: string) => void;
};

export function CommandPalette({
  open,
  onClose,
  symbols,
  groups,
  scanners,
  pages,
  onPickSymbol,
  onPickScanner,
  onPickGroup,
  onPickPage,
}: CommandPaletteProps) {
  const [query, setQuery] = useState("");
  const [highlight, setHighlight] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (open) {
      setQuery("");
      setHighlight(0);
      // Focus after the dialog mounts.
      window.setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [open]);

  const entries = useMemo<PaletteEntry[]>(() => {
    const q = query.trim().toLowerCase();
    const matchesQuery = (...haystacks: string[]) => !q || haystacks.some((h) => h.toLowerCase().includes(q));

    const pageEntries: PaletteEntry[] = pages
      .filter((p) => matchesQuery(p.label))
      .map((p) => ({ kind: "page", key: `page:${p.page}`, label: p.label, sub: "Page", page: p.page }));

    const scannerEntries: PaletteEntry[] = scanners
      .filter((s) => matchesQuery(s.label))
      .map((s) => ({ kind: "scanner", key: `scan:${s.mode}`, label: s.label, sub: "Scanner", mode: s.mode }));

    const groupEntries: PaletteEntry[] = groups
      .filter((g) => matchesQuery(g.name))
      .slice(0, 6)
      .map((g) => ({ kind: "group", key: `group:${g.id}`, label: g.name, sub: `Group · rank #${g.rank}`, groupId: g.id }));

    // Symbols: prefix matches on the ticker rank first, then name matches.
    const symbolEntries: PaletteEntry[] = (
      q
        ? symbols
            .filter((s) => s.symbol.toLowerCase().includes(q) || s.name.toLowerCase().includes(q))
            .sort((a, b) => {
              const ap = a.symbol.toLowerCase().startsWith(q) ? 0 : 1;
              const bp = b.symbol.toLowerCase().startsWith(q) ? 0 : 1;
              return ap - bp || a.symbol.localeCompare(b.symbol);
            })
        : symbols
    )
      .slice(0, 8)
      .map((s) => ({ kind: "symbol", key: `sym:${s.symbol}`, label: s.symbol, sub: s.name, symbol: s.symbol }));

    // With no query: show pages + scanners (discovery). With a query: symbols first.
    return q
      ? [...symbolEntries, ...scannerEntries.slice(0, 4), ...groupEntries, ...pageEntries]
      : [...pageEntries, ...scannerEntries, ...symbolEntries];
  }, [query, symbols, groups, scanners, pages]);

  useEffect(() => {
    setHighlight((current) => Math.min(current, Math.max(0, entries.length - 1)));
  }, [entries.length]);

  if (!open) return null;

  const activate = (entry: PaletteEntry) => {
    onClose();
    if (entry.kind === "symbol") onPickSymbol(entry.symbol);
    else if (entry.kind === "scanner") onPickScanner(entry.mode);
    else if (entry.kind === "group") onPickGroup(entry.groupId);
    else onPickPage(entry.page);
  };

  return (
    <div className="cmdk-overlay" onMouseDown={onClose} role="dialog" aria-modal="true" aria-label="Command palette">
      <div className="cmdk" onMouseDown={(event) => event.stopPropagation()}>
        <div className="cmdk-input-row">
          <SearchIcon size={15} />
          <input
            ref={inputRef}
            value={query}
            placeholder="Jump to a stock, scanner, group, or page…"
            onChange={(event) => {
              setQuery(event.target.value);
              setHighlight(0);
            }}
            onKeyDown={(event) => {
              if (event.key === "Escape") {
                event.preventDefault();
                onClose();
              } else if (event.key === "ArrowDown") {
                event.preventDefault();
                setHighlight((current) => Math.min(current + 1, entries.length - 1));
              } else if (event.key === "ArrowUp") {
                event.preventDefault();
                setHighlight((current) => Math.max(current - 1, 0));
              } else if (event.key === "Enter" && entries[highlight]) {
                event.preventDefault();
                activate(entries[highlight]);
              }
            }}
          />
          <kbd>esc</kbd>
        </div>
        <div className="cmdk-list">
          {entries.length === 0 ? (
            <div className="cmdk-empty">No matches.</div>
          ) : (
            entries.map((entry, index) => (
              <button
                key={entry.key}
                type="button"
                className={`cmdk-item${index === highlight ? " is-active" : ""}`}
                onMouseEnter={() => setHighlight(index)}
                onClick={() => activate(entry)}
              >
                <span className="cmdk-item-icon">
                  {entry.kind === "symbol" ? (
                    <LineChart size={14} />
                  ) : entry.kind === "scanner" ? (
                    <Activity size={14} />
                  ) : entry.kind === "group" ? (
                    <Layers size={14} />
                  ) : (
                    <BarChart3 size={14} />
                  )}
                </span>
                <span className="cmdk-item-label">{entry.label}</span>
                <span className="cmdk-item-sub">{entry.sub}</span>
              </button>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
