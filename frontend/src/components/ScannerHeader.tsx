import { useEffect, useRef, useState } from "react";
import {
  Pencil,
  Play,
  Save,
  Settings2,
  Check,
  X,
  Loader2,
} from "lucide-react";

import "./ScannerHeader.css";

type ScannerHeaderProps = {
  /** Bold title shown at the top, e.g. "Custom Screener". */
  title: string;
  /** Optional description shown below the title. */
  description?: string;
  /** Number of stocks currently shown / found. */
  resultCount: number | null;
  /** Optional preset name when an active saved-scanner is loaded. */
  activeSavedName?: string | null;
  /** Called with the new name when user finishes editing the title. */
  onRenameActiveSaved?: (newName: string) => void;
  /** Whether the underlying form panel is open. */
  settingsOpen: boolean;
  /** Toggle the underlying form panel. */
  onToggleSettings: () => void;
  /** Run the active scanner (the "Apply Filters" / "Run Scanner" CTA). */
  onRunScanner: () => void;
  /** Whether a scan is in progress. */
  loading?: boolean;
  /** Whether the active mode is savable (e.g. custom-scan). */
  isSavable: boolean;
  /** Whether a saved preset already exists for this mode. */
  savedExists: boolean;
  /** Called when user wants to save / update the current scan. */
  onSaveScanner?: () => void;
  /** Whether a save is currently in flight. */
  saving?: boolean;
};

export function ScannerHeader({
  title,
  description,
  resultCount,
  activeSavedName = null,
  onRenameActiveSaved,
  settingsOpen,
  onToggleSettings,
  onRunScanner,
  loading = false,
  isSavable,
  savedExists,
  onSaveScanner,
  saving = false,
}: ScannerHeaderProps) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(activeSavedName ?? title);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const canEdit = Boolean(activeSavedName && onRenameActiveSaved);

  useEffect(() => {
    if (!editing) {
      setDraft(activeSavedName ?? title);
    }
  }, [activeSavedName, title, editing]);

  useEffect(() => {
    if (editing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [editing]);

  const commit = () => {
    const next = draft.trim();
    if (next && next !== (activeSavedName ?? title)) {
      onRenameActiveSaved?.(next);
    }
    setEditing(false);
  };

  const cancel = () => {
    setDraft(activeSavedName ?? title);
    setEditing(false);
  };

  const displayed = activeSavedName ?? title;
  const countLabel =
    resultCount === null
      ? "Awaiting first scan"
      : `${resultCount.toLocaleString("en-IN")} ${resultCount === 1 ? "stock" : "stocks"} found`;

  return (
    <header className="sh-root">
      <div className="sh-headline">
        <div className="sh-title-block">
          {editing ? (
            <div className="sh-title-edit">
              <input
                ref={inputRef}
                className="sh-title-input"
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") commit();
                  if (e.key === "Escape") cancel();
                }}
              />
              <button type="button" className="sh-icon-btn sh-icon-confirm" onClick={commit} aria-label="Save name">
                <Check size={14} strokeWidth={2.4} />
              </button>
              <button type="button" className="sh-icon-btn sh-icon-cancel" onClick={cancel} aria-label="Cancel rename">
                <X size={14} strokeWidth={2.4} />
              </button>
            </div>
          ) : (
            <div className="sh-title-row">
              <h1 className="sh-title">{displayed}</h1>
              {canEdit ? (
                <button
                  type="button"
                  className="sh-icon-btn sh-icon-edit"
                  onClick={() => setEditing(true)}
                  aria-label="Rename scanner"
                  title="Rename"
                >
                  <Pencil size={14} strokeWidth={2.2} />
                </button>
              ) : null}
            </div>
          )}

          <div className="sh-meta">
            <span className={`sh-count${loading ? " is-loading" : ""}`}>
              {loading ? <Loader2 size={12} className="sh-spin" /> : null}
              {countLabel}
            </span>
            {description ? <span className="sh-desc">{description}</span> : null}
          </div>
        </div>

        <div className="sh-actions">
          <button
            type="button"
            className="sh-btn sh-btn-ghost"
            onClick={onToggleSettings}
            title={settingsOpen ? "Hide filter panel" : "Show filter panel"}
          >
            <Settings2 size={14} strokeWidth={2.2} />
            <span>{settingsOpen ? "Hide Settings" : "Show Settings"}</span>
          </button>

          {isSavable && onSaveScanner ? (
            <button
              type="button"
              className="sh-btn sh-btn-ghost"
              onClick={onSaveScanner}
              disabled={saving}
            >
              <Save size={14} strokeWidth={2.2} />
              <span>{saving ? "Saving…" : savedExists ? "Update" : "Save"}</span>
            </button>
          ) : null}

          <button
            type="button"
            className="sh-btn sh-btn-primary"
            onClick={onRunScanner}
            disabled={loading}
          >
            {loading ? (
              <Loader2 size={14} className="sh-spin" />
            ) : (
              <Play size={14} strokeWidth={2.4} />
            )}
            <span>Run Scanner</span>
          </button>
        </div>
      </div>
    </header>
  );
}
