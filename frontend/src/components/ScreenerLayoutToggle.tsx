export type ScreenerLayout = "research" | "classic";

/** Switches the Screener page between its two layouts. Shown in both. */
export function ScreenerLayoutToggle({ value, onChange }: { value: ScreenerLayout; onChange: (next: ScreenerLayout) => void }) {
  return (
    <div className="screener-layout-switch" role="radiogroup" aria-label="Screener layout">
      {(
        [
          ["research", "Research", "Stock list, chart and fundamentals side by side"],
          ["classic", "Classic", "Filters, results table and chart"],
        ] as const
      ).map(([key, label, title]) => (
        <button
          key={key}
          type="button"
          role="radio"
          aria-checked={value === key}
          title={title}
          className={value === key ? "active" : undefined}
          onClick={() => onChange(key)}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
