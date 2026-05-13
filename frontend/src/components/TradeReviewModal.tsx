import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";

import { getTradeReview, writeTradeReview, normalizeJournalSymbol } from "../lib/journal";
import "./TradeJournalPanel.css";

type TradeReviewModalProps = {
  symbol: string;
  exitDate: string;
  onClose: () => void;
};

function formatExitDate(value: string): string {
  if (!value) return "";
  const dateOnly = value.split("T")[0];
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(dateOnly);
  if (!match) return value;
  const date = new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString(undefined, { day: "2-digit", month: "short", year: "numeric" });
}

export function TradeReviewModal({ symbol, exitDate, onClose }: TradeReviewModalProps) {
  const normalizedSymbol = useMemo(() => normalizeJournalSymbol(symbol), [symbol]);
  const initial = useMemo(() => getTradeReview(symbol, exitDate), [symbol, exitDate]);
  const [soldNotes, setSoldNotes] = useState(initial?.soldNotes ?? "");
  const [reviewNotes, setReviewNotes] = useState(initial?.reviewNotes ?? "");
  const [dirty, setDirty] = useState(false);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  const handleSave = () => {
    writeTradeReview(symbol, exitDate, soldNotes, reviewNotes);
    setDirty(false);
    setSaved(true);
    window.setTimeout(() => setSaved(false), 1600);
  };

  const handleClose = () => {
    if (dirty) {
      const confirmDiscard = window.confirm("You have unsaved changes. Close anyway?");
      if (!confirmDiscard) return;
    }
    onClose();
  };

  return createPortal(
    <div className="tj-overlay trade-review-overlay" onClick={handleClose}>
      <div
        className="tj-modal trade-review-modal"
        onClick={(event) => event.stopPropagation()}
        role="dialog"
        aria-modal="true"
      >
        <button type="button" className="tj-modal-x" onClick={handleClose} aria-label="Close">✕</button>
        <div className="tj-modal-title">
          Trade Review · {normalizedSymbol || symbol}
          <div className="trade-review-subtitle">Sold on {formatExitDate(exitDate)}</div>
        </div>

        <div className="trade-review-body">
          <label className="trade-review-section">
            <span className="trade-review-section-title">Notes when sold</span>
            <span className="trade-review-section-hint">
              Captured at exit — why you sold, conviction, emotions, market context.
            </span>
            <textarea
              className="tj-textarea trade-review-textarea"
              rows={6}
              value={soldNotes}
              onChange={(event) => {
                setSoldNotes(event.target.value);
                setDirty(true);
              }}
              placeholder="What made you sell? Plan vs. action, conviction, hesitations…"
              spellCheck
            />
          </label>

          <label className="trade-review-section">
            <span className="trade-review-section-title">Review (post-trade)</span>
            <span className="trade-review-section-hint">
              Looking back — what worked, what didn't, lessons, would-do-different.
            </span>
            <textarea
              className="tj-textarea trade-review-textarea"
              rows={6}
              value={reviewNotes}
              onChange={(event) => {
                setReviewNotes(event.target.value);
                setDirty(true);
              }}
              placeholder="With hindsight: was the exit timely? What's the lesson for next time?"
              spellCheck
            />
          </label>
        </div>

        <div className="trade-review-footer">
          {saved ? <span className="trade-review-saved">Saved ✓</span> : null}
          <button type="button" className="tj-btn secondary" onClick={handleClose}>
            Close
          </button>
          <button type="button" className="tj-btn primary" onClick={handleSave}>
            Save
          </button>
        </div>
      </div>
    </div>,
    document.body,
  );
}
