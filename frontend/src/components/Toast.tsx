import { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Check, Undo2, X } from "lucide-react";

import "./Toast.css";

/**
 * Toast + undo.
 *
 * The app had no feedback for ~15 mutating actions — deleting a watchlist,
 * bulk-removing symbols, deleting a trade — and no confirm or undo on any of
 * them. Rather than adding blocking confirm() dialogs (which interrupt a fast
 * workflow), destructive actions now complete immediately and offer Undo for a
 * few seconds. That keeps the app quick AND recoverable.
 */

export type ToastTone = "info" | "success" | "danger";

type Toast = {
  id: number;
  message: string;
  tone: ToastTone;
  /** When present the toast shows an Undo button that calls this. */
  onUndo?: () => void;
};

type ShowToastOptions = {
  tone?: ToastTone;
  onUndo?: () => void;
  /** Milliseconds before auto-dismiss. Undoable toasts get longer by default. */
  durationMs?: number;
};

type ToastContextValue = {
  showToast: (message: string, options?: ShowToastOptions) => void;
};

const ToastContext = createContext<ToastContextValue | null>(null);

/**
 * Safe to call from anywhere — returns a no-op when no provider is mounted, so
 * a component can adopt toasts without every test/host having to wrap it.
 */
export function useToast(): ToastContextValue {
  return useContext(ToastContext) ?? { showToast: () => {} };
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const nextId = useRef(1);
  const timers = useRef(new Map<number, number>());

  const dismiss = useCallback((id: number) => {
    setToasts((current) => current.filter((toast) => toast.id !== id));
    const timer = timers.current.get(id);
    if (timer) {
      window.clearTimeout(timer);
      timers.current.delete(id);
    }
  }, []);

  const showToast = useCallback(
    (message: string, options: ShowToastOptions = {}) => {
      const id = nextId.current++;
      const { tone = "info", onUndo } = options;
      // An undoable action needs long enough to actually notice and react.
      const durationMs = options.durationMs ?? (onUndo ? 8000 : 4000);
      setToasts((current) => [...current, { id, message, tone, onUndo }]);
      timers.current.set(id, window.setTimeout(() => dismiss(id), durationMs));
    },
    [dismiss],
  );

  const value = useMemo(() => ({ showToast }), [showToast]);

  return (
    <ToastContext.Provider value={value}>
      {children}
      {typeof document !== "undefined"
        ? createPortal(
            <div className="toast-stack" role="status" aria-live="polite">
              {toasts.map((toast) => (
                <div key={toast.id} className={`toast toast-${toast.tone}`}>
                  {toast.tone === "success" ? (
                    <Check size={14} strokeWidth={2.6} className="toast-icon" aria-hidden="true" />
                  ) : null}
                  <span className="toast-message">{toast.message}</span>
                  {toast.onUndo ? (
                    <button
                      type="button"
                      className="toast-undo"
                      onClick={() => {
                        toast.onUndo?.();
                        dismiss(toast.id);
                      }}
                    >
                      <Undo2 size={13} strokeWidth={2.4} aria-hidden="true" />
                      Undo
                    </button>
                  ) : null}
                  <button
                    type="button"
                    className="toast-close"
                    onClick={() => dismiss(toast.id)}
                    aria-label="Dismiss"
                  >
                    <X size={13} strokeWidth={2.4} aria-hidden="true" />
                  </button>
                </div>
              ))}
            </div>,
            document.body,
          )
        : null}
    </ToastContext.Provider>
  );
}
