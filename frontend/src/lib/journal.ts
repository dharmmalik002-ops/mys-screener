import { useEffect, useState } from "react";

export const JOURNAL_LS_KEY = "tradingJournalData";
export const JOURNAL_UPDATED_EVENT = "stock-scanner:journal-updated";
export const REVIEWS_LS_KEY = "tradingJournalReviews:v1";
export const REVIEWS_UPDATED_EVENT = "stock-scanner:reviews-updated";

export interface TradeReviewRecord {
  soldNotes: string;
  reviewNotes: string;
  updatedAt: number;
}

export type TradeReviewMap = Record<string, TradeReviewRecord>;

export function tradeReviewKey(symbol: string, exitDate: string): string {
  return `${normalizeJournalSymbol(symbol)}|${(exitDate || "").trim()}`;
}

export function readTradeReviews(): TradeReviewMap {
  if (typeof window === "undefined") return {};
  try {
    const raw = window.localStorage.getItem(REVIEWS_LS_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return {};
    const out: TradeReviewMap = {};
    for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
      if (!value || typeof value !== "object") continue;
      const v = value as Partial<TradeReviewRecord>;
      out[key] = {
        soldNotes: typeof v.soldNotes === "string" ? v.soldNotes : "",
        reviewNotes: typeof v.reviewNotes === "string" ? v.reviewNotes : "",
        updatedAt: Number.isFinite(v.updatedAt) ? Number(v.updatedAt) : 0,
      };
    }
    return out;
  } catch {
    return {};
  }
}

export function writeTradeReview(symbol: string, exitDate: string, soldNotes: string, reviewNotes: string): void {
  if (typeof window === "undefined") return;
  const key = tradeReviewKey(symbol, exitDate);
  if (!key || key === "|") return;
  const map = readTradeReviews();
  const trimmedSold = soldNotes.trim();
  const trimmedReview = reviewNotes.trim();
  if (!trimmedSold && !trimmedReview) {
    delete map[key];
  } else {
    map[key] = { soldNotes, reviewNotes, updatedAt: Date.now() };
  }
  try {
    window.localStorage.setItem(REVIEWS_LS_KEY, JSON.stringify(map));
    window.dispatchEvent(new CustomEvent(REVIEWS_UPDATED_EVENT));
  } catch {
    /* ignore */
  }
}

export function getTradeReview(symbol: string, exitDate: string): TradeReviewRecord | null {
  const key = tradeReviewKey(symbol, exitDate);
  if (!key || key === "|") return null;
  return readTradeReviews()[key] ?? null;
}

export interface JournalTradeRecord {
  symbol: string;
  type: string;
  qty: number;
  price: number;
  date: string;
}

export interface ChartTradeMarker {
  date: string;
  type: "buy" | "sell";
  price: number;
  qty: number;
}

export function normalizeJournalSymbol(symbol: string | null | undefined): string {
  return (symbol ?? "").trim().toUpperCase().replace(/\.(NS|BO|BSE|NSE)$/i, "");
}

export function readJournalTrades(): JournalTradeRecord[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(JOURNAL_LS_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (t): t is JournalTradeRecord =>
        t && typeof t === "object" && typeof (t as JournalTradeRecord).symbol === "string",
    );
  } catch {
    return [];
  }
}

export function notifyJournalUpdated(): void {
  if (typeof window === "undefined") return;
  try {
    window.dispatchEvent(new CustomEvent(JOURNAL_UPDATED_EVENT));
  } catch {
    /* ignore */
  }
}

export function useJournalTrades(): JournalTradeRecord[] {
  const [trades, setTrades] = useState<JournalTradeRecord[]>(() => readJournalTrades());
  useEffect(() => {
    const refresh = () => setTrades(readJournalTrades());
    window.addEventListener("storage", refresh);
    window.addEventListener(JOURNAL_UPDATED_EVENT, refresh);
    return () => {
      window.removeEventListener("storage", refresh);
      window.removeEventListener(JOURNAL_UPDATED_EVENT, refresh);
    };
  }, []);
  return trades;
}

export function tradeMarkersForSymbol(
  trades: JournalTradeRecord[],
  symbol: string | null | undefined,
): ChartTradeMarker[] {
  if (!symbol) return [];
  const key = normalizeJournalSymbol(symbol);
  if (!key) return [];
  const result: ChartTradeMarker[] = [];
  for (const t of trades) {
    if (!t || typeof t !== "object") continue;
    if (normalizeJournalSymbol(t.symbol) !== key) continue;
    const type = String(t.type || "").toLowerCase();
    if (type !== "buy" && type !== "sell") continue;
    const price = Number(t.price);
    if (!Number.isFinite(price)) continue;
    if (!t.date) continue;
    const qty = Number(t.qty);
    result.push({
      date: String(t.date),
      type: type as "buy" | "sell",
      price,
      qty: Number.isFinite(qty) ? qty : 0,
    });
  }
  return result;
}
