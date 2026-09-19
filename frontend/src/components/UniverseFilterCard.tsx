// The universe gate's control: a metrics-strip card that opens three floors.
//
// It sits in the strip beside "Floor" (the market-cap minimum) because that is
// already where the page says what the universe is, and because these floors
// belong to the trader rather than to any one scanner — see universeFilter.ts.

import { useEffect, useRef, useState } from "react";
import { SlidersHorizontal, X } from "lucide-react";

import {
  describeUniverseFilter,
  isUniverseFilterActive,
  parseFloorInput,
  type UniverseFilter,
} from "../lib/universeFilter";
import "./UniverseFilterCard.css";

type UniverseFilterCardProps = {
  filter: UniverseFilter;
  onChange: (filter: UniverseFilter) => void;
  /** How many rows the gate removed from the current scan, for the hint line. */
  removed: number;
  /** Of those, how many had no value on record rather than a failing one. */
  removedForMissingData: number;
};

/** Floors are typed, not dragged: a trader's ADR minimum is a specific number
 *  they already know, and a slider would only make it approximate. */
const FIELDS: Array<{
  key: keyof UniverseFilter;
  label: string;
  unit: string;
  placeholder: string;
  hint: string;
  step: string;
}> = [
  {
    key: "minAdrPct",
    label: "Min ADR",
    unit: "%",
    placeholder: "e.g. 3.5",
    hint: "Average daily range over 20 sessions. A name that moves less than your stop is wide cannot pay for the risk.",
    step: "0.5",
  },
  {
    key: "minTurnoverCrore",
    label: "Min traded value",
    unit: "Cr",
    placeholder: "e.g. 10",
    hint: "Average value traded per day over 30 sessions — whether your position size can get in and out.",
    step: "1",
  },
  {
    key: "minPrice",
    label: "Min price",
    unit: "₹",
    placeholder: "e.g. 50",
    hint: "Floor on the last price, for keeping penny stocks out of every list at once.",
    step: "5",
  },
];

export function UniverseFilterCard({
  filter,
  onChange,
  removed,
  removedForMissingData,
}: UniverseFilterCardProps) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement | null>(null);
  // Drafts are strings so a half-typed "3." survives a keystroke instead of
  // being rounded to 3 and fighting the cursor.
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const active = isUniverseFilterActive(filter);

  // Re-seed the boxes from the applied filter each time the popover opens, so
  // an abandoned edit does not reappear later as if it had been saved.
  useEffect(() => {
    if (!open) return;
    setDrafts({
      minAdrPct: filter.minAdrPct === null ? "" : String(filter.minAdrPct),
      minTurnoverCrore: filter.minTurnoverCrore === null ? "" : String(filter.minTurnoverCrore),
      minPrice: filter.minPrice === null ? "" : String(filter.minPrice),
    });
  }, [open, filter]);

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (event: MouseEvent) => {
      if (wrapRef.current && !wrapRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(false);
    };
    window.addEventListener("mousedown", onPointerDown);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("mousedown", onPointerDown);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  const commit = (key: keyof UniverseFilter, text: string) => {
    setDrafts((current) => ({ ...current, [key]: text }));
    onChange({ ...filter, [key]: parseFloorInput(text) });
  };

  return (
    <div className="metric-card universe-filter-card" ref={wrapRef}>
      <span>Universe Gate</span>
      <button
        type="button"
        className={active ? "universe-filter-trigger is-active" : "universe-filter-trigger"}
        onClick={() => setOpen((current) => !current)}
        aria-expanded={open}
        title="Liquidity, volatility and price floors applied to every scanner"
      >
        <SlidersHorizontal size={12} strokeWidth={2.4} />
        <strong>{describeUniverseFilter(filter)}</strong>
      </button>
      {active && removed > 0 ? (
        <span
          className="universe-filter-removed"
          title={
            removedForMissingData > 0
              ? `${removed} of this scanner's results are hidden by the gate — ${removedForMissingData} of them because they carry no ADR or traded value on record, which cannot be shown to clear a floor.`
              : `${removed} of this scanner's results are hidden by the gate.`
          }
        >
          −{removed} hidden
        </span>
      ) : null}

      {open ? (
        <div className="universe-filter-pop" role="dialog" aria-label="Universe gate">
          <div className="universe-filter-pop-head">
            <strong>Universe gate</strong>
            <button type="button" onClick={() => setOpen(false)} aria-label="Close universe gate">
              <X size={13} strokeWidth={2.4} />
            </button>
          </div>
          <p className="universe-filter-pop-intro">
            Applied to every scanner. Leave a box empty for no floor.
          </p>
          {FIELDS.map((field) => (
            <label key={field.key} className="universe-filter-field">
              <span className="universe-filter-field-label">
                {field.label} <em>{field.unit}</em>
              </span>
              <input
                type="number"
                min="0"
                step={field.step}
                inputMode="decimal"
                value={drafts[field.key] ?? ""}
                placeholder={field.placeholder}
                onChange={(event) => commit(field.key, event.target.value)}
              />
              <small>{field.hint}</small>
            </label>
          ))}
          <div className="universe-filter-pop-foot">
            <button
              type="button"
              className="universe-filter-clear"
              disabled={!active}
              onClick={() => onChange({ minAdrPct: null, minTurnoverCrore: null, minPrice: null })}
            >
              Clear all floors
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
