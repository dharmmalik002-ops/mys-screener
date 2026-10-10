import type { AiScanColumn, AiScanCriterion, AiScanRequest, AiScanRow } from "./api";

/** What "off" means for each kind of criterion, so removing a chip never
    leaves a filter half-set. Mirrors the defaults of the backend's AiScanRequest. */
export function clearCriterion(request: AiScanRequest, criterion: Pick<AiScanCriterion, "key" | "kind">): AiScanRequest {
  const next: AiScanRequest = { ...request };
  if (criterion.key === "price_vs_ma_mode") next.price_vs_ma_mode = "any";
  else if (criterion.key === "growth_quarters") next.growth_quarters = 1;
  else if (criterion.kind === "bool") next[criterion.key] = false;
  else next[criterion.key] = null;
  return next;
}

/** A typed-in replacement for a numeric criterion. Returns null when the text
    is not a number, so the caller can keep the old value instead of sending junk. */
export function withCriterionValue(request: AiScanRequest, criterion: Pick<AiScanCriterion, "key" | "kind">, text: string): AiScanRequest | null {
  const cleaned = String(text).replace(/[%₹,x\s]/gi, "");
  // Number("") is 0 — an emptied box must not become a zero threshold.
  if (!cleaned) return null;
  const number = Number(cleaned);
  if (!Number.isFinite(number)) return null;
  const value = criterion.kind === "int" ? Math.round(number) : number;
  return { ...request, [criterion.key]: value };
}

/** The fixed part of a chip while its number is being typed: "Sales growth YoY ≥". */
export function editLabel(criterion: Pick<AiScanCriterion, "text" | "kind">): string {
  const match = criterion.text.match(/^(.*?[≥≤])/);
  if (match) return match[1];
  return criterion.kind === "int" ? "Quarters in a row" : criterion.text;
}

export function isEditable(criterion: AiScanCriterion): boolean {
  return criterion.kind === "min" || criterion.kind === "max" || criterion.kind === "int";
}

export function withoutPattern(request: AiScanRequest, patternId: string): AiScanRequest {
  return { ...request, patterns: request.patterns.filter((id) => id !== patternId) };
}

export function withPattern(request: AiScanRequest, patternId: string): AiScanRequest {
  if (!patternId || request.patterns.includes(patternId) || request.patterns.length >= 8) return request;
  return { ...request, patterns: [...request.patterns, patternId] };
}

const SIGNED = new Set(["change", "gap", "return", "sales_yoy", "profit_yoy", "sales_qoq", "profit_qoq", "sales_ttm", "profit_ttm", "margin_change"]);

/** Growth and change columns carry a sign and a colour; distances and levels do not. */
export function isSignedColumn(columnId: string): boolean {
  return SIGNED.has(columnId);
}

export function formatMetric(value: number | null | undefined, column: Pick<AiScanColumn, "id" | "format">): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  const sign = isSignedColumn(column.id) && value > 0 ? "+" : "";
  switch (column.format) {
    case "pct":
      return `${sign}${value.toFixed(1)}%`;
    case "pp":
      return `${sign}${value.toFixed(1)}pp`;
    case "x":
      return `${value.toFixed(2)}x`;
    case "crore":
      return value >= 1000 ? `${Math.round(value).toLocaleString("en-IN")} cr` : `${value.toFixed(1)} cr`;
    case "price":
      return value.toLocaleString("en-IN", { maximumFractionDigits: 2 });
    default:
      return value.toFixed(0);
  }
}

export type SortKey = { id: string; dir: "asc" | "desc" } | null;

function sortValue(row: AiScanRow, id: string): number | string | null {
  if (id === "symbol") return row.symbol;
  if (id === "price") return row.last_price;
  if (id === "change") return row.change_pct;
  if (id === "rs") return row.rs_rating ?? null;
  if (id === "score") return row.score;
  return row.metrics?.[id] ?? null;
}

/** Client-side sort. Missing values always sink, whichever way the column runs. */
export function sortRows(rows: AiScanRow[], sort: SortKey): AiScanRow[] {
  if (!sort) return rows;
  const factor = sort.dir === "asc" ? 1 : -1;
  return [...rows].sort((a, b) => {
    const left = sortValue(a, sort.id);
    const right = sortValue(b, sort.id);
    if (left === null && right === null) return 0;
    if (left === null) return 1;
    if (right === null) return -1;
    if (typeof left === "string" || typeof right === "string") return String(left).localeCompare(String(right)) * factor;
    return (left - right) * factor;
  });
}
