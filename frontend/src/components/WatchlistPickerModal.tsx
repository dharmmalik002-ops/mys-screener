import { useEffect, useRef, useState } from "react";

import type { MarketKey } from "../lib/api";
import type { LocalWatchlist } from "./WatchlistsPanel";

type WatchlistPickerModalProps = {
  market: MarketKey;
  symbol: string;
  watchlists: LocalWatchlist[];
  onClose: () => void;
  onAddToWatchlist: (watchlistId: string, symbol: string) => Promise<void> | void;
  onCreateWatchlist: (name: string, symbol?: string) => Promise<void> | void;
};

export function WatchlistPickerModal({
  market,
  symbol,
  watchlists,
  onClose,
  onAddToWatchlist,
  onCreateWatchlist,
}: WatchlistPickerModalProps) {
  const marketLabel = market === "india" ? "India" : "US";
  const [newWatchlistName, setNewWatchlistName] = useState("");
  const modalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (modalRef.current) {
      modalRef.current.scrollTop = 0;
    }
  }, [symbol]);

  return (
    <div className="watchlist-picker-backdrop" onClick={onClose}>
      <div className="watchlist-picker-modal" ref={modalRef} onClick={(event) => event.stopPropagation()}>
        <div className="watchlist-picker-head">
          <div>
            <p className="eyebrow">{marketLabel} Watchlists</p>
            <h3>Add {symbol}</h3>
          </div>
          <button type="button" className="chart-modal-close" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="watchlist-picker-list">
          {watchlists.map((watchlist) => (
            <button
              key={watchlist.id}
              type="button"
              className="watchlist-picker-row"
              style={{ borderLeftColor: watchlist.color }}
              onClick={async () => {
                  try {
                    await onAddToWatchlist(watchlist.id, symbol);
                  } catch {
                    // ignore errors but still close modal
                  } finally {
                    onClose();
                  }
                }}
            >
              <span className="watchlist-link-color" style={{ backgroundColor: watchlist.color }} aria-hidden="true" />
              <span>
                <strong>{watchlist.name}</strong>
                <small>{watchlist.symbols.length} {market === "us" ? "tickers" : "stocks"}</small>
              </span>
              <span>Add</span>
            </button>
          ))}
          {watchlists.length === 0 ? <div className="empty-state">Create your first watchlist below.</div> : null}
        </div>

        <div className="watchlist-picker-create">
          <input
            value={newWatchlistName}
            onChange={(event) => setNewWatchlistName(event.target.value)}
            placeholder="New watchlist name"
          />
          <button
            type="button"
            className="nav-button primary"
            onClick={async () => {
              const value = newWatchlistName.trim();
              if (!value) {
                return;
              }
              try {
                await onCreateWatchlist(value, symbol);
              } catch {
                // ignore
              }
              setNewWatchlistName("");
              onClose();
            }}
          >
            Create & Add
          </button>
        </div>
      </div>
    </div>
  );
}
