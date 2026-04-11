import { useRef, useState } from "react";

export type ImportResult = {
  name: string;
  color: string;
  symbols: string[];
};

type WatchlistImportModalProps = {
  defaultColor?: string;
  onConfirm: (result: ImportResult) => void;
  onClose: () => void;
};

const PRESET_COLORS = ["#4f8cff", "#00a389", "#ff9f1c", "#ef476f", "#7c5cff", "#06b6d4", "#84cc16", "#f97316"];

function parseSymbolText(raw: string): string[] {
  const lines = raw
    .split(/[\n,;\t]+/)
    .map((line) => line.trim())
    .filter(Boolean);

  const symbols: string[] = [];
  const seen = new Set<string>();

  for (const line of lines) {
    // Strip exchange prefixes like NSE:, BSE:, NYSE:, NASDAQ:, US:
    const stripped = line.replace(/^[A-Z]{1,10}:/i, "").trim().toUpperCase();
    if (stripped && !seen.has(stripped)) {
      seen.add(stripped);
      symbols.push(stripped);
    }
  }

  return symbols;
}

export function WatchlistImportModal({ defaultColor = "#4f8cff", onConfirm, onClose }: WatchlistImportModalProps) {
  const [text, setText] = useState("");
  const [name, setName] = useState("Imported Watchlist");
  const [color, setColor] = useState(defaultColor);
  const [dragOver, setDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const parsed = parseSymbolText(text);

  const handleFile = (file: File) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      const raw = e.target?.result;
      if (typeof raw === "string") {
        setText((prev) => (prev.trim() ? `${prev}\n${raw}` : raw));
        // Auto-name from filename
        const base = file.name.replace(/\.[^.]+$/, "").replace(/[-_]+/g, " ").trim();
        if (base) setName(base);
      }
    };
    reader.readAsText(file);
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  };

  const handleConfirm = () => {
    if (!parsed.length || !name.trim()) return;
    onConfirm({ name: name.trim(), color, symbols: parsed });
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal-box wl-import-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Import Watchlist</h3>
          <button type="button" className="modal-close" onClick={onClose}>✕</button>
        </div>

        <div className="wl-import-body">
          {/* Paste / Drop zone */}
          <div
            className={`wl-import-dropzone${dragOver ? " drag-over" : ""}`}
            onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
            onDragLeave={() => setDragOver(false)}
            onDrop={handleDrop}
          >
            <textarea
              className="wl-import-textarea"
              value={text}
              onChange={(e) => setText(e.target.value)}
              placeholder={"Paste symbols here — one per line or comma separated.\nFormats: NSE:CUPID, RELIANCE, NSE:INFY, AAPL…"}
              rows={9}
              spellCheck={false}
            />
            <div className="wl-import-drop-hint">
              Or{" "}
              <button type="button" className="wl-import-file-btn" onClick={() => fileInputRef.current?.click()}>
                upload a .txt file
              </button>
              <input
                ref={fileInputRef}
                type="file"
                accept=".txt,.csv"
                style={{ display: "none" }}
                onChange={(e) => { const f = e.target.files?.[0]; if (f) handleFile(f); e.target.value = ""; }}
              />
            </div>
          </div>

          {/* Preview */}
          <div className="wl-import-preview">
            <span className="wl-import-preview-label">
              {parsed.length > 0 ? `${parsed.length} symbol${parsed.length !== 1 ? "s" : ""} detected` : "No symbols detected yet"}
            </span>
            {parsed.length > 0 && (
              <div className="wl-import-chips">
                {parsed.slice(0, 40).map((s) => (
                  <span key={s} className="wl-import-chip">{s}</span>
                ))}
                {parsed.length > 40 && <span className="wl-import-chip wl-import-chip--more">+{parsed.length - 40} more</span>}
              </div>
            )}
          </div>

          {/* Name + Color */}
          <div className="wl-import-meta">
            <div className="wl-import-meta-row">
              <label>Watchlist name</label>
              <input
                type="text"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Watchlist name"
                className="wl-import-name-input"
              />
            </div>
            <div className="wl-import-meta-row">
              <label>Color</label>
              <div className="wl-import-color-row">
                {PRESET_COLORS.map((c) => (
                  <button
                    key={c}
                    type="button"
                    className={`wl-color-swatch${color === c ? " active" : ""}`}
                    style={{ background: c, borderColor: color === c ? "#fff" : "transparent" }}
                    onClick={() => setColor(c)}
                    aria-label={c}
                  />
                ))}
                <label className="wl-color-custom" title="Custom color">
                  <input type="color" value={color} onChange={(e) => setColor(e.target.value)} />
                  <span style={{ background: color }} className="wl-color-swatch" />
                </label>
              </div>
            </div>
          </div>
        </div>

        <div className="modal-footer">
          <button type="button" className="nav-button" onClick={onClose}>Cancel</button>
          <button
            type="button"
            className="nav-button primary"
            disabled={parsed.length === 0 || !name.trim()}
            onClick={handleConfirm}
          >
            Create Watchlist ({parsed.length} stocks)
          </button>
        </div>
      </div>
    </div>
  );
}
