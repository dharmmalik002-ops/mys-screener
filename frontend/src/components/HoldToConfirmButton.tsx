import { useEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import "./HoldToConfirmButton.css";

/* A destructive action that needs a deliberate press: hold for HOLD_MS while a
   fill sweeps across the button, and only then does it fire. Letting go early
   cancels. A plain click does nothing but say how to use it. Pointer and
   keyboard (Space/Enter held) both work. Replaces window.confirm(), which a
   reflexive Enter dismisses as easily as the click that opened it. */

const HOLD_MS = 800;

export function HoldToConfirmButton({
  onConfirm,
  label,
  className = "",
  children,
}: {
  onConfirm: () => void;
  /** What the hold does, for the tooltip and screen readers, e.g. "Delete this trade". */
  label: string;
  className?: string;
  children: ReactNode;
}) {
  const [holding, setHolding] = useState(false);
  const [hint, setHint] = useState(false);
  const timerRef = useRef<number | null>(null);
  const hintTimerRef = useRef<number | null>(null);
  const firedRef = useRef(false);

  const clear = () => {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    setHolding(false);
  };

  const start = () => {
    if (timerRef.current !== null) return;
    firedRef.current = false;
    setHint(false);
    setHolding(true);
    timerRef.current = window.setTimeout(() => {
      timerRef.current = null;
      firedRef.current = true;
      setHolding(false);
      onConfirm();
    }, HOLD_MS);
  };

  const cancel = () => {
    const wasEarly = timerRef.current !== null;
    clear();
    if (wasEarly && !firedRef.current) {
      setHint(true);
      if (hintTimerRef.current !== null) window.clearTimeout(hintTimerRef.current);
      hintTimerRef.current = window.setTimeout(() => setHint(false), 1600);
    }
  };

  useEffect(
    () => () => {
      if (timerRef.current !== null) window.clearTimeout(timerRef.current);
      if (hintTimerRef.current !== null) window.clearTimeout(hintTimerRef.current);
    },
    [],
  );

  return (
    <button
      type="button"
      className={`hold-confirm${holding ? " is-holding" : ""}${hint ? " show-hint" : ""} ${className}`.trim()}
      style={{ "--hold-ms": `${HOLD_MS}ms` } as CSSProperties}
      title={`Hold to confirm: ${label}`}
      aria-label={`${label} (press and hold)`}
      onPointerDown={(event) => {
        if (event.button !== 0) return;
        // Keeps the hold alive if the finger drifts off the small button.
        try {
          event.currentTarget.setPointerCapture?.(event.pointerId);
        } catch {
          // An inactive pointer id throws; the hold still works without capture.
        }
        start();
      }}
      onPointerUp={cancel}
      onPointerCancel={cancel}
      onLostPointerCapture={cancel}
      onKeyDown={(event) => {
        if ((event.key === " " || event.key === "Enter") && !event.repeat) {
          event.preventDefault();
          start();
        }
      }}
      onKeyUp={(event) => {
        if (event.key === " " || event.key === "Enter") cancel();
      }}
      onBlur={() => clear()}
      onClick={(event) => event.preventDefault()}
    >
      <span className="hold-confirm-fill" aria-hidden="true" />
      <span className="hold-confirm-label">{children}</span>
      <span className="hold-confirm-hint" role="status">{hint ? "Hold to confirm" : ""}</span>
    </button>
  );
}
