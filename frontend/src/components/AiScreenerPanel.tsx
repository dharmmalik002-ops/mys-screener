import { useState, useEffect, useMemo } from "react";
import { type MarketKey, runAiScan, type AiScanResponse } from "../lib/api";
import { Panel } from "./Panel";
import { ScanTable } from "./ScanTable";

type AiScreenerPanelProps = {
  market: MarketKey;
  onPickSymbol: (symbol: string) => void;
  onRequestAddToWatchlist: (symbol: string) => void;
  onVisibleSymbolsChange?: (symbols: string[]) => void;
  selectedSymbol: string | null;
};

export function AiScreenerPanel({ market, onPickSymbol, onRequestAddToWatchlist, onVisibleSymbolsChange, selectedSymbol }: AiScreenerPanelProps) {
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<AiScanResponse | null>(null);

  const [sortMode, setSortMode] = useState<"change" | "rs">("rs");
  const [arrangementMode, setArrangementMode] = useState<"flat" | "sector">("flat");
  const [sectorSortMode, setSectorSortMode] = useState<"1W" | "1M" | "count-desc" | "count-asc">("count-desc");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError(null);
    try {
      const response = await runAiScan(query, market);
      setResult(response);
    } catch (err: any) {
      setError(err.message || "Failed to execute AI scan.");
    } finally {
      setLoading(false);
    }
  };

  const sortedItems = useMemo(() => {
    if (!result) return [];
    return [...result.results.items].sort((left, right) => {
      if (sortMode === "change") {
        return right.change_pct - left.change_pct;
      }
      return (right.rs_rating ?? Number.NEGATIVE_INFINITY) - (left.rs_rating ?? Number.NEGATIVE_INFINITY);
    });
  }, [result, sortMode]);

  useEffect(() => {
    if (onVisibleSymbolsChange) {
      onVisibleSymbolsChange(sortedItems.map(s => s.symbol));
    }
  }, [sortedItems, onVisibleSymbolsChange]);

  return (
    <div className="screener-main-stack" style={{ paddingTop: '20px' }}>
      <Panel title="AI Screener" subtitle="Describe the kinds of stocks you want using natural language." actions={null}>
        <form onSubmit={handleSubmit} style={{ padding: '0 20px 20px', display: 'flex', gap: '10px' }}>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="E.g., companies growing EPS > 30% YoY with RS > 80 tightening near 50 SMA..."
            style={{ flex: 1, padding: '12px', borderRadius: '8px', border: '1px solid var(--surface1)', background: 'var(--mantle)', color: 'var(--text)', fontSize: '1rem' }}
          />
          <button type="submit" className="nav-button primary ai-sparkle" disabled={loading || !query.trim()} style={{ whiteSpace: 'nowrap', padding: '0 20px', borderRadius: '8px' }}>
            {loading ? "Thinking..." : "Scan with AI"}
          </button>
        </form>
        {error ? <div className="error-message" style={{ margin: '0 20px 20px', color: 'var(--red)' }}>{error}</div> : null}
        {result ? (
          <div style={{ padding: '0 20px 20px' }}>
             <div style={{ padding: '12px', background: 'var(--surface0)', borderRadius: '8px', border: '1px solid var(--surface1)' }}>
               <span style={{ color: 'var(--subtext0)', fontSize: '0.85rem', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Parameters Found:</span>
               <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', marginTop: '8px' }}>
                 {Object.entries(result.parsed_request).filter(([k,v]) => v !== null && v !== false && k !== 'market' && k !== 'limit').map(([k,v]) => (
                    <span key={k} style={{ background: 'var(--surface1)', padding: '4px 8px', borderRadius: '4px', fontSize: '0.85rem', color: 'var(--text)' }}>
                      <strong>{k}</strong>: {typeof v === 'boolean' ? 'Yes' : String(v)}
                    </span>
                 ))}
                 {Object.entries(result.parsed_request).filter(([k,v]) => v !== null && v !== false && k !== 'market' && k !== 'limit').length === 0 && (
                   <span style={{ color: 'var(--subtext1)', fontSize: '0.85rem' }}>No specific filters parsed. Using default universe.</span>
                 )}
               </div>
             </div>
          </div>
        ) : null}
      </Panel>

      {result ? (
        <ScanTable
          market={market}
          loading={loading}
          sectorSummaryLoading={false}
          scan={result.results.scan}
          items={sortedItems}
          sectorSummaries={result.results.sector_summaries}
          onPickSymbol={onPickSymbol}
          onRequestAddToWatchlist={onRequestAddToWatchlist}
          selectedSymbol={selectedSymbol}
          sortMode={sortMode}
          onSortModeChange={setSortMode}
          arrangementMode={arrangementMode}
          onArrangementModeChange={setArrangementMode}
          sectorSortMode={sectorSortMode}
          onSectorSortModeChange={setSectorSortMode}
          onExport={() => {}}
        />
      ) : (
         <div className="empty-state" style={{ marginTop: '40px' }}>
            <p>Ask the AI to find exactly what you're looking for.</p>
            <p style={{ color: 'var(--subtext0)', fontSize: '0.9rem', marginTop: '8px' }}>Try exploring fundamental metrics like "EPS growth", "profit margin", or technical setups like "shakeouts" or "Mark Minervini".</p>
         </div>
      )}
    </div>
  );
}
