import { useState } from "react";
import { createPortal } from "react-dom";

import type { MarketKey, WatchlistNote } from "../lib/api";
import type { LocalWatchlist } from "./WatchlistsPanel";

type WatchlistPickerModalProps = {
  market: MarketKey;
  symbol: string;
  watchlists: LocalWatchlist[];
  onClose: () => void;
  onAddToWatchlist: (watchlistId: string, symbol: string, note?: WatchlistNote) => void;
  onCreateWatchlist: (name: string, symbol?: string) => void;
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
  // The reason is asked for at the moment of adding, which is the only moment
  // it is actually known. A symbol you cannot give a reason for is one you are
  // watching out of interest rather than intent — and those are the ones that
  // get bought on a green candle three weeks later.
  const [why, setWhy] = useState("");
  const [trigger, setTrigger] = useState("");
  const [stop, setStop] = useState("");
  const normalizedSymbol = symbol.trim().toUpperCase();

  const buildNote = (): WatchlistNote | undefined => {
    const reason = why.trim();
    const triggerPrice = Number.parseFloat(trigger);
    const stopPrice = Number.parseFloat(stop);
    const note: WatchlistNote = {
      why: reason,
      trigger: Number.isFinite(triggerPrice) ? triggerPrice : null,
      stop: Number.isFinite(stopPrice) ? stopPrice : null,
      added_at: null,
    };
    return reason || note.trigger !== null || note.stop !== null ? note : undefined;
  };
  const currentWatchlistId = watchlists.find((watchlist) => watchlist.symbols.includes(normalizedSymbol))?.id ?? null;

  return createPortal(
    <div className="watchlist-picker-backdrop" onClick={onClose}>
      <div className="watchlist-picker-modal" onClick={(event) => event.stopPropagation()}>
        <div className="watchlist-picker-head">
          <div>
            <p className="eyebrow">{marketLabel} Watchlists</p>
            <h3>Add {symbol}</h3>
          </div>
          <button type="button" className="chart-modal-close" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="watchlist-picker-why">
          <label>
            <span>Why this one?</span>
            <input
              value={why}
              onChange={(event) => setWhy(event.target.value)}
              placeholder="e.g. 8-week base, pivot 1,240, needs volume"
              maxLength={160}
              autoFocus
            />
          </label>
          <div className="watchlist-picker-levels">
            <label>
              <span>Trigger &#8377;</span>
              <input
                type="number"
                step="any"
                value={trigger}
                onChange={(event) => setTrigger(event.target.value)}
                placeholder="level that makes it live"
              />
            </label>
            <label>
              <span>Stop &#8377;</span>
              <input
                type="number"
                step="any"
                value={stop}
                onChange={(event) => setStop(event.target.value)}
                placeholder="optional"
              />
            </label>
          </div>
          <p>
            Optional, but the list is worth far more with it: a trigger means nothing needs checking until
            price reaches a level you decided while calm.
          </p>
        </div>

        <div className="watchlist-picker-list">
          {watchlists.map((watchlist) => (
            <button
              key={watchlist.id}
              type="button"
              className="watchlist-picker-row"
              style={{ borderLeftColor: watchlist.color }}
              onClick={() => {
                if (watchlist.id === currentWatchlistId) {
                  onClose();
                  return;
                }
                onAddToWatchlist(watchlist.id, symbol, buildNote());
                onClose();
              }}
            >
              <span className="watchlist-link-color" style={{ backgroundColor: watchlist.color }} aria-hidden="true" />
              <span>
                <strong>{watchlist.name}</strong>
                <small>{watchlist.symbols.length} stocks</small>
              </span>
              <span>
                {watchlist.id === currentWatchlistId
                  ? "Selected"
                  : currentWatchlistId
                    ? "Move here"
                    : "Add"}
              </span>
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
            onClick={() => {
              const value = newWatchlistName.trim();
              if (!value) {
                return;
              }
              onCreateWatchlist(value, symbol);
              setNewWatchlistName("");
              onClose();
            }}
          >
            Create & Add
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
