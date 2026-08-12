import { useCallback, useEffect, useRef, useState } from "react";
import "./Disclosure.css";

type Props = {
  id: string;
  summary: string;
  hint?: string;
  defaultOpen?: boolean;
  children: React.ReactNode;
};

const STORAGE_KEY = "stockScanner.marketsDisclosures.v1";

function readOpenSet(): Set<string> {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return new Set(Array.isArray(parsed) ? parsed.filter((x) => typeof x === "string") : []);
  } catch {
    return new Set();
  }
}

function persistOpenSet(open: Set<string>) {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify([...open]));
  } catch {
    /* private mode / quota — the page works fine without persistence */
  }
}

/**
 * A collapsed section of the Markets page.
 *
 * Native `<details>` because the codebase already uses it and it gives keyboard
 * and screen-reader semantics with no `aria-expanded` bookkeeping.
 *
 * The one thing native does not do is skip the work: React mounts the children
 * of a *closed* `<details>` and the browser merely hides them. Without the
 * `hasOpened` gate below, every chart, table and number on this page would
 * instantiate on load — which is the entire cost the collapsing exists to
 * avoid. Children therefore render only after the section has been opened at
 * least once, and stay mounted after that so reopening is instant.
 */
export function Disclosure({ id, summary, hint, defaultOpen = false, children }: Props) {
  const [open, setOpen] = useState(defaultOpen);
  const [hasOpened, setHasOpened] = useState(defaultOpen);
  const restored = useRef(false);

  useEffect(() => {
    if (restored.current) return;
    restored.current = true;
    if (readOpenSet().has(id)) {
      setOpen(true);
      setHasOpened(true);
    }
  }, [id]);

  const handleToggle = useCallback(
    (event: React.SyntheticEvent<HTMLDetailsElement>) => {
      const isOpen = event.currentTarget.open;
      setOpen(isOpen);
      if (isOpen) setHasOpened(true);
      const next = readOpenSet();
      if (isOpen) next.add(id);
      else next.delete(id);
      persistOpenSet(next);
    },
    [id],
  );

  return (
    <details className="mk-disclosure" open={open} onToggle={handleToggle}>
      <summary>
        <span className="mk-disclosure-title">{summary}</span>
        {hint ? <span className="mk-disclosure-hint">{hint}</span> : null}
        <span className="mk-disclosure-chevron" aria-hidden>
          ▾
        </span>
      </summary>
      <div className="mk-disclosure-body">{hasOpened ? children : null}</div>
    </details>
  );
}
