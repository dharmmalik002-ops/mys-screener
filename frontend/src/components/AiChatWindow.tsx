import { useEffect, useRef, useState } from "react";
import { type AiChatMessageInput, type ChartBar, type MarketKey, runAiChartAnalysis } from "../lib/api";

type Message = {
  id: string;
  role: "user" | "assistant";
  content: string;
  loading?: boolean;
};

type AiChatWindowProps = {
  symbol: string;
  market: MarketKey;
  timeframe: string;
  bars: ChartBar[];
  onClose: () => void;
};

export function AiChatWindow({ symbol, market, timeframe, bars, onClose }: AiChatWindowProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [width, setWidth] = useState(390);
  const [height, setHeight] = useState(530);
  const [pos, setPos] = useState({ x: 18, y: 90 });

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const dragging = useRef<{ sx: number; sy: number; px: number; py: number } | null>(null);
  const resizing = useRef<{ sx: number; sy: number; sw: number; sh: number; edge: string } | null>(null);

  // Auto-scroll
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Header drag
  const handleHeaderMouseDown = (e: React.MouseEvent) => {
    if ((e.target as HTMLElement).closest("button")) return;
    e.preventDefault();
    dragging.current = { sx: e.clientX, sy: e.clientY, px: pos.x, py: pos.y };
    const onMove = (me: MouseEvent) => {
      if (!dragging.current) return;
      setPos({
        x: Math.max(0, dragging.current.px + me.clientX - dragging.current.sx),
        y: Math.max(0, dragging.current.py + me.clientY - dragging.current.sy),
      });
    };
    const onUp = () => {
      dragging.current = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  };

  // Resize handles
  const startResize = (e: React.MouseEvent, edge: string) => {
    e.preventDefault();
    e.stopPropagation();
    resizing.current = { sx: e.clientX, sy: e.clientY, sw: width, sh: height, edge };
    const onMove = (me: MouseEvent) => {
      if (!resizing.current) return;
      const dx = me.clientX - resizing.current.sx;
      const dy = me.clientY - resizing.current.sy;
      if (resizing.current.edge === "right" || resizing.current.edge === "corner") {
        setWidth(Math.max(300, resizing.current.sw + dx));
      }
      if (resizing.current.edge === "bottom" || resizing.current.edge === "corner") {
        setHeight(Math.max(280, resizing.current.sh + dy));
      }
    };
    const onUp = () => {
      resizing.current = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  };

  const sendMessage = async () => {
    if (!input.trim() || loading) return;
    const text = input.trim();
    const history: AiChatMessageInput[] = messages
      .filter((m) => !m.loading)
      .map((m) => ({ role: m.role, content: m.content }));

    setMessages((prev) => [
      ...prev,
      { id: `u-${Date.now()}`, role: "user", content: text },
      { id: "loading", role: "assistant", content: "", loading: true },
    ]);
    setInput("");
    setLoading(true);

    try {
      const data = await runAiChartAnalysis(
        {
          symbol,
          timeframe,
          query: text,
          bars: bars.slice(-100),
          conversation_history: history,
          include_knowledge_base: true,
        },
        market,
      );
      setMessages((prev) => [
        ...prev.filter((m) => m.id !== "loading"),
        { id: `a-${Date.now()}`, role: "assistant", content: data.response },
      ]);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : "Unknown error";
      setMessages((prev) => [
        ...prev.filter((m) => m.id !== "loading"),
        { id: `e-${Date.now()}`, role: "assistant", content: `⚠ Error: ${msg}` },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  return (
    <div
      style={{
        position: "fixed",
        left: pos.x,
        top: pos.y,
        width,
        height,
        zIndex: 2000,
        display: "flex",
        flexDirection: "column",
        borderRadius: 12,
        border: "1px solid var(--glass-border, rgba(255,255,255,0.12))",
        background: "var(--surface-strong, #1a1a2e)",
        boxShadow: "0 24px 64px rgba(0,0,0,0.6), 0 0 0 1px rgba(255,255,255,0.05)",
        overflow: "hidden",
      }}
    >
      {/* Header */}
      <div
        onMouseDown={handleHeaderMouseDown}
        style={{
          padding: "10px 14px",
          display: "flex",
          alignItems: "center",
          gap: 8,
          background: "linear-gradient(90deg, color-mix(in srgb, var(--accent, #7c6aff) 20%, transparent), color-mix(in srgb, var(--teal, #00d2ff) 15%, transparent))",
          borderBottom: "1px solid var(--glass-border, rgba(255,255,255,0.1))",
          cursor: "grab",
          flexShrink: 0,
          userSelect: "none",
        }}
      >
        <span style={{ fontSize: "1.1rem", lineHeight: 1 }}>✦</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontWeight: 700, fontSize: "0.875rem", color: "var(--text, #fff)", lineHeight: 1.2 }}>
            AI Chart Analysis
          </div>
          <div style={{ fontSize: "0.7rem", color: "var(--text-muted, rgba(255,255,255,0.5))", marginTop: 2 }}>
            {symbol} · {market.toUpperCase()} · {timeframe}
          </div>
        </div>
        <button
          type="button"
          onClick={onClose}
          style={{
            background: "none",
            border: "none",
            color: "var(--text-muted, rgba(255,255,255,0.5))",
            cursor: "pointer",
            padding: "4px 6px",
            fontSize: "1rem",
            borderRadius: 6,
            lineHeight: 1,
          }}
          title="Close"
        >
          ✕
        </button>
      </div>

      {/* Messages */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "12px 12px 6px",
          display: "flex",
          flexDirection: "column",
          gap: 10,
        }}
      >
        {messages.length === 0 ? (
          <div
            style={{
              textAlign: "center",
              color: "var(--text-muted, rgba(255,255,255,0.4))",
              fontSize: "0.82rem",
              marginTop: 40,
              lineHeight: 1.8,
            }}
          >
            <div style={{ fontSize: "2rem", marginBottom: 10 }}>✦</div>
            <div style={{ fontWeight: 600, marginBottom: 6 }}>Ask me about this chart.</div>
            <div style={{ opacity: 0.75 }}>
              "Is this breaking out?" · "Key support/resistance?" · "What does the volume indicate?"
            </div>
          </div>
        ) : (
          messages.map((msg) => (
            <div
              key={msg.id}
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: msg.role === "user" ? "flex-end" : "flex-start",
              }}
            >
              {msg.loading ? (
                <div
                  style={{
                    padding: "9px 13px",
                    background: "var(--surface-soft, rgba(255,255,255,0.06))",
                    border: "1px solid var(--glass-border, rgba(255,255,255,0.1))",
                    borderRadius: "10px 10px 10px 2px",
                    fontSize: "0.82rem",
                    color: "var(--text-muted)",
                  }}
                >
                  <span className="ai-thinking-dots">Analyzing</span>
                </div>
              ) : (
                <div
                  style={{
                    padding: "9px 13px",
                    maxWidth: "92%",
                    borderRadius:
                      msg.role === "user" ? "10px 10px 2px 10px" : "10px 10px 10px 2px",
                    background:
                      msg.role === "user"
                        ? "color-mix(in srgb, var(--accent, #7c6aff) 18%, var(--surface-soft, rgba(255,255,255,0.07)))"
                        : "var(--surface-soft, rgba(255,255,255,0.06))",
                    border:
                      msg.role === "user"
                        ? "1px solid color-mix(in srgb, var(--accent, #7c6aff) 35%, transparent)"
                        : "1px solid var(--glass-border, rgba(255,255,255,0.1))",
                    fontSize: "0.83rem",
                    lineHeight: 1.65,
                    color: "var(--text, #fff)",
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                  }}
                >
                  {msg.content}
                </div>
              )}
            </div>
          ))
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div
        style={{
          padding: "8px 10px",
          borderTop: "1px solid var(--glass-border, rgba(255,255,255,0.1))",
          display: "flex",
          gap: 7,
          flexShrink: 0,
        }}
      >
        <textarea
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask about this chart… (Enter to send, Shift+Enter for new line)"
          disabled={loading}
          rows={2}
          style={{
            flex: 1,
            resize: "none",
            padding: "7px 10px",
            borderRadius: 8,
            border: "1px solid var(--glass-border, rgba(255,255,255,0.12))",
            background: "var(--surface, rgba(255,255,255,0.04))",
            color: "var(--text, #fff)",
            fontSize: "0.82rem",
            fontFamily: "inherit",
            outline: "none",
            lineHeight: 1.5,
          }}
        />
        <button
          type="button"
          onClick={sendMessage}
          disabled={loading || !input.trim()}
          style={{
            padding: "0 13px",
            borderRadius: 8,
            border: "none",
            background:
              loading || !input.trim()
                ? "var(--surface-soft, rgba(255,255,255,0.07))"
                : "var(--accent, #7c6aff)",
            color: loading || !input.trim() ? "var(--text-muted)" : "#fff",
            cursor: loading || !input.trim() ? "default" : "pointer",
            fontWeight: 700,
            fontSize: "0.78rem",
            flexShrink: 0,
            alignSelf: "stretch",
            transition: "background 0.15s",
          }}
        >
          {loading ? "…" : "Send"}
        </button>
      </div>

      {/* Resize handles */}
      <div
        onMouseDown={(e) => startResize(e, "right")}
        style={{
          position: "absolute",
          right: 0,
          top: 44,
          bottom: 8,
          width: 6,
          cursor: "ew-resize",
        }}
      />
      <div
        onMouseDown={(e) => startResize(e, "bottom")}
        style={{
          position: "absolute",
          bottom: 0,
          left: 8,
          right: 14,
          height: 6,
          cursor: "ns-resize",
        }}
      />
      <div
        onMouseDown={(e) => startResize(e, "corner")}
        style={{
          position: "absolute",
          bottom: 0,
          right: 0,
          width: 14,
          height: 14,
          cursor: "nwse-resize",
        }}
      />
    </div>
  );
}
