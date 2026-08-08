import { useEffect, useRef } from "react";

/**
 * Standard modal behaviour: Escape to close, background scroll lock, and focus
 * that starts inside the dialog and returns to wherever it came from.
 *
 * Only one modal in the app did any of this (HomePanel's ViewAllModal); the
 * other ~10 relied on a backdrop click alone, so keyboard users had no way out
 * and the page behind kept scrolling.
 *
 * Deliberately NOT a full focus trap — that needs every modal to declare its
 * focusable boundary and would be a much larger change. This covers the three
 * things whose absence is actually felt.
 */
export function useModalShell(
  open: boolean,
  onClose: () => void,
  options: { lockScroll?: boolean; restoreFocus?: boolean } = {},
) {
  const { lockScroll = true, restoreFocus = true } = options;
  const containerRef = useRef<HTMLElement | null>(null);
  const previouslyFocused = useRef<HTMLElement | null>(null);
  // Kept in a ref so a caller passing an inline arrow doesn't re-bind the
  // listener (and momentarily lose Escape) on every render.
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  useEffect(() => {
    if (!open || typeof document === "undefined") return;

    previouslyFocused.current = document.activeElement as HTMLElement | null;

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.stopPropagation();
        onCloseRef.current();
      }
    };
    document.addEventListener("keydown", handleKeyDown);

    const previousOverflow = lockScroll ? document.body.style.overflow : null;
    if (lockScroll) document.body.style.overflow = "hidden";

    // Move focus into the dialog so the next Tab lands inside it rather than
    // continuing through the page behind.
    const node = containerRef.current;
    if (node) {
      const focusable = node.querySelector<HTMLElement>(
        'button:not([disabled]), [href], input:not([disabled]), select, textarea, [tabindex]:not([tabindex="-1"])',
      );
      (focusable ?? node).focus?.();
    }

    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      if (lockScroll && previousOverflow !== null) {
        document.body.style.overflow = previousOverflow;
      }
      if (restoreFocus) previouslyFocused.current?.focus?.();
    };
  }, [open, lockScroll, restoreFocus]);

  return containerRef;
}
