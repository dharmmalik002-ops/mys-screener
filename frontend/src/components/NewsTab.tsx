import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  getArticleProxyUrl,
  getLiveNews,
  type LiveNewsItem,
  type MarketKey,
} from "../lib/api";

// ─── Props ────────────────────────────────────────────────────────────────────

type Props = {
  market: MarketKey;
  onPickSymbol?: (symbol: string) => void;
};

// ─── Constants ────────────────────────────────────────────────────────────────

const REFRESH_MS = 2 * 60 * 1000; // 2 minutes

const ALL_CATS = [
  "All", "Markets", "Stocks", "Corporate", "IPO",
  "Economy", "AI", "Defense", "Geopolitics", "Crypto",
  "Global", "World", "Headlines", "Money",
];

// ─── Helpers ─────────────────────────────────────────────────────────────────

function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  if (isNaN(diff) || diff < 0) return "just now";
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

function hexToRgb(hex: string): string {
  const h = hex.replace("#", "");
  const n = parseInt(h, 16);
  return `${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}`;
}

// ─── Article reader modal ─────────────────────────────────────────────────────

function ArticleModal({
  item,
  onClose,
}: {
  item: LiveNewsItem;
  onClose: () => void;
}) {
  const proxyUrl = getArticleProxyUrl(item.link);

  // Fallback: if proxy fails, open original in new tab
  const handleIframeError = () => {
    window.open(item.link, "_blank", "noopener,noreferrer");
    onClose();
  };

  return createPortal(
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 9999,
        background: "rgba(0,0,0,0.75)",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        padding: "20px",
      }}
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        style={{
          width: "min(900px, 100%)",
          height: "min(85vh, 800px)",
          display: "flex",
          flexDirection: "column",
          borderRadius: "12px",
          overflow: "hidden",
          border: "1px solid var(--glass-border, rgba(255,255,255,0.12))",
          background: "var(--surface-strong, #1a1a2e)",
        }}
      >
        {/* Modal header */}
        <div
          style={{
            padding: "12px 16px",
            borderBottom: "1px solid var(--glass-border, rgba(255,255,255,0.1))",
            display: "flex",
            alignItems: "center",
            gap: "10px",
            background: "rgba(255,255,255,0.04)",
            flexShrink: 0,
          }}
        >
          <div
            style={{
              width: 8, height: 8, borderRadius: "50%",
              background: item.source.color,
              boxShadow: `0 0 6px ${item.source.color}`,
              flexShrink: 0,
            }}
          />
          <span
            style={{
              flex: 1, fontWeight: 600, fontSize: "0.85rem",
              color: "var(--text, #fff)", lineHeight: 1.4,
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
            }}
          >
            {item.title}
          </span>
          <a
            href={item.link}
            target="_blank"
            rel="noopener noreferrer"
            style={{
              fontSize: "0.72rem", color: "var(--text-muted, #aaa)",
              textDecoration: "none", whiteSpace: "nowrap", flexShrink: 0,
              borderRadius: 4, border: "1px solid var(--glass-border, rgba(255,255,255,0.15))",
              padding: "3px 8px",
            }}
          >
            Open original
          </a>
          <button
            onClick={onClose}
            style={{
              background: "none", border: "none", cursor: "pointer",
              color: "var(--text-muted, #aaa)", fontSize: "1rem", padding: 0, lineHeight: 1,
            }}
          >
            ✕
          </button>
        </div>

        {/* Iframe */}
        <iframe
          src={proxyUrl}
          title={item.title}
          style={{ flex: 1, border: "none", width: "100%", background: "#fff" }}
          onError={handleIframeError}
          sandbox="allow-scripts allow-same-origin allow-popups allow-forms"
        />
      </div>
    </div>,
    document.body
  );
}

// ─── News card ────────────────────────────────────────────────────────────────

function NewsCard({
  item,
  onRead,
  onPickSymbol,
}: {
  item: LiveNewsItem;
  onRead: (item: LiveNewsItem) => void;
  onPickSymbol?: (symbol: string) => void;
}) {
  const rgb = hexToRgb(item.source.color);

  return (
    <div
      style={{
        background: "rgba(255,255,255,0.03)",
        border: "1px solid var(--glass-border, rgba(255,255,255,0.09))",
        borderLeft: `3px solid ${item.source.color}`,
        borderRadius: "8px",
        padding: "12px 14px",
        display: "flex",
        flexDirection: "column",
        gap: "7px",
        transition: "background 0.15s",
        cursor: "pointer",
      }}
      onMouseEnter={(e) => {
        (e.currentTarget as HTMLDivElement).style.background = `rgba(${rgb}, 0.06)`;
      }}
      onMouseLeave={(e) => {
        (e.currentTarget as HTMLDivElement).style.background = "rgba(255,255,255,0.03)";
      }}
      onClick={() => onRead(item)}
    >
      {/* Meta row */}
      <div style={{ display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
        <span
          style={{
            fontSize: "0.68rem", fontWeight: 700, letterSpacing: "0.03em",
            padding: "2px 6px", borderRadius: 4,
            background: `rgba(${rgb}, 0.18)`,
            color: item.source.color,
            border: `1px solid rgba(${rgb}, 0.3)`,
            whiteSpace: "nowrap",
          }}
        >
          {item.source.name}
        </span>
        <span
          style={{
            fontSize: "0.67rem", padding: "2px 6px", borderRadius: 4,
            background: "rgba(255,255,255,0.07)",
            color: "var(--text-muted, #aaa)",
            border: "1px solid rgba(255,255,255,0.1)",
          }}
        >
          {item.category}
        </span>
        <span style={{ fontSize: "0.67rem", color: "var(--text-muted, #888)", marginLeft: "auto" }}>
          {timeAgo(item.pub_date)}
        </span>
      </div>

      {/* Title */}
      <div
        style={{
          fontSize: "0.855rem", fontWeight: 600, lineHeight: 1.45,
          color: "var(--text, #fff)",
        }}
      >
        {item.title}
      </div>

      {/* Description */}
      {item.description && (
        <div
          style={{
            fontSize: "0.775rem", color: "var(--text-secondary, #bbb)",
            lineHeight: 1.5,
            display: "-webkit-box",
            WebkitLineClamp: 2,
            WebkitBoxOrient: "vertical",
            overflow: "hidden",
          }}
        >
          {item.description}
        </div>
      )}

      {/* Company chips + read link */}
      {(item.companies.length > 0 || true) && (
        <div style={{ display: "flex", alignItems: "center", gap: "6px", flexWrap: "wrap" }}>
          {item.companies.map((sym) => (
            <button
              key={sym}
              onClick={(e) => {
                e.stopPropagation();
                onPickSymbol?.(sym);
              }}
              style={{
                fontSize: "0.67rem", fontWeight: 700, padding: "2px 7px",
                borderRadius: 4, cursor: "pointer",
                background: "rgba(0,210,255,0.1)",
                color: "var(--teal, #00d2ff)",
                border: "1px solid rgba(0,210,255,0.25)",
              }}
            >
              {sym}
            </button>
          ))}
          <span
            style={{ marginLeft: "auto", fontSize: "0.7rem", color: "var(--text-muted, #888)" }}
          >
            Read →
          </span>
        </div>
      )}
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────

export default function NewsTab({ market, onPickSymbol }: Props) {
  const [items, setItems]             = useState<LiveNewsItem[]>([]);
  const [categories, setCategories]   = useState<string[]>([]);
  const [activeCategory, setActiveCategory] = useState<string>("All");
  const [loading, setLoading]         = useState(true);
  const [error, setError]             = useState<string | null>(null);
  const [search, setSearch]           = useState("");
  const [readItem, setReadItem]       = useState<LiveNewsItem | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const timerRef                      = useRef<ReturnType<typeof setInterval> | null>(null);

  const load = useCallback(async () => {
    try {
      const res = await getLiveNews(market, undefined, 300);
      setItems(res.items);
      // Merge known categories with what server returned
      const serverCats = res.categories;
      const merged = Array.from(
        new Set(["All", ...ALL_CATS.filter((c) => serverCats.includes(c) || c === "All"), ...serverCats]),
      );
      setCategories(merged);
      setLastRefresh(new Date());
      setError(null);
    } catch (e) {
      setError("Could not load news. Retrying…");
    } finally {
      setLoading(false);
    }
  }, [market]);

  // Initial load + refresh timer
  useEffect(() => {
    setLoading(true);
    setItems([]);
    load();
    timerRef.current = setInterval(load, REFRESH_MS);
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [load]);

  // Derived: filtered items
  const filtered = items.filter((item) => {
    const matchesCat =
      activeCategory === "All" ||
      item.category.toLowerCase() === activeCategory.toLowerCase();
    const q = search.trim().toLowerCase();
    const matchesSearch =
      !q ||
      item.title.toLowerCase().includes(q) ||
      item.source.name.toLowerCase().includes(q) ||
      item.companies.some((s) => s.toLowerCase().includes(q));
    return matchesCat && matchesSearch;
  });

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        overflow: "hidden",
        background: "var(--bg, #0c0c1d)",
        color: "var(--text, #fff)",
      }}
    >
      {/* ── Header ────────────────────────────────────────────────────── */}
      <div
        style={{
          padding: "14px 20px 10px",
          borderBottom: "1px solid var(--glass-border, rgba(255,255,255,0.1))",
          flexShrink: 0,
          display: "flex",
          alignItems: "flex-start",
          gap: "14px",
          flexWrap: "wrap",
          background: "rgba(255,255,255,0.02)",
        }}
      >
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <span style={{ fontSize: "1rem", fontWeight: 700, color: "var(--text, #fff)" }}>
              Newsdesk
            </span>
            <span
              style={{
                width: 8, height: 8, borderRadius: "50%",
                background: "#22c55e",
                boxShadow: "0 0 6px #22c55e",
                animation: "nd-pulse 2s ease-in-out infinite",
              }}
            />
            {lastRefresh && (
              <span style={{ fontSize: "0.68rem", color: "var(--text-muted, #888)" }}>
                Updated {timeAgo(lastRefresh.toISOString())}
              </span>
            )}
          </div>
          <div style={{ fontSize: "0.72rem", color: "var(--text-muted, #888)", marginTop: 2 }}>
            {market === "india" ? "India markets live feed" : "US markets live feed"}
            {!loading && ` · ${filtered.length} articles`}
          </div>
        </div>

        {/* Search */}
        <input
          type="search"
          placeholder="Search news or ticker…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{
            background: "rgba(255,255,255,0.07)",
            border: "1px solid var(--glass-border, rgba(255,255,255,0.15))",
            borderRadius: 6,
            color: "var(--text, #fff)",
            fontSize: "0.8rem",
            padding: "6px 12px",
            outline: "none",
            width: "200px",
          }}
        />

        {/* Refresh button */}
        <button
          onClick={() => { setLoading(true); load(); }}
          style={{
            background: "rgba(255,255,255,0.07)",
            border: "1px solid var(--glass-border, rgba(255,255,255,0.15))",
            borderRadius: 6,
            color: "var(--text, #fff)",
            cursor: "pointer",
            padding: "6px 12px",
            fontSize: "0.8rem",
          }}
        >
          ↻ Refresh
        </button>
      </div>

      {/* ── Category pills ─────────────────────────────────────────────── */}
      <div
        style={{
          padding: "8px 20px",
          display: "flex",
          gap: "6px",
          flexWrap: "wrap",
          borderBottom: "1px solid var(--glass-border, rgba(255,255,255,0.08))",
          flexShrink: 0,
          alignItems: "center",
        }}
      >
        {categories.map((cat) => {
          const active = cat === activeCategory;
          return (
            <button
              key={cat}
              onClick={() => setActiveCategory(cat)}
              style={{
                fontSize: "0.72rem",
                padding: "3px 10px",
                borderRadius: 100,
                cursor: "pointer",
                fontWeight: active ? 700 : 500,
                transition: "all 0.15s",
                background: active
                  ? "var(--accent, #7c6aff)"
                  : "rgba(255,255,255,0.06)",
                border: active
                  ? "1px solid var(--accent, #7c6aff)"
                  : "1px solid rgba(255,255,255,0.12)",
                color: active ? "#fff" : "var(--text-muted, #aaa)",
              }}
            >
              {cat}
            </button>
          );
        })}
      </div>

      {/* ── Content ────────────────────────────────────────────────────── */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "14px 20px",
        }}
      >
        {loading && items.length === 0 && (
          <div
            style={{
              textAlign: "center",
              color: "var(--text-muted, #888)",
              padding: "60px 20px",
              fontSize: "0.85rem",
            }}
          >
            <div style={{ fontSize: "2rem", marginBottom: 12 }}>📰</div>
            Loading live news feeds…
          </div>
        )}

        {error && !loading && (
          <div
            style={{
              textAlign: "center",
              color: "#f87171",
              padding: "40px 20px",
              fontSize: "0.85rem",
            }}
          >
            {error}
          </div>
        )}

        {!loading && !error && filtered.length === 0 && (
          <div
            style={{
              textAlign: "center",
              color: "var(--text-muted, #888)",
              padding: "60px 20px",
              fontSize: "0.85rem",
            }}
          >
            No articles found
            {search ? ` for "${search}"` : ""}.
          </div>
        )}

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(340px, 1fr))",
            gap: "10px",
            alignItems: "start",
          }}
        >
          {filtered.map((item) => (
            <NewsCard
              key={item.id}
              item={item}
              onRead={setReadItem}
              onPickSymbol={onPickSymbol}
            />
          ))}
        </div>
      </div>

      {/* ── Article modal ──────────────────────────────────────────────── */}
      {readItem && (
        <ArticleModal item={readItem} onClose={() => setReadItem(null)} />
      )}

      <style>{`
        @keyframes nd-pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.35; }
        }
      `}</style>
    </div>
  );
}
