import type { KeyboardEvent } from "react";

/**
 * Keyboard activation for elements that are clickable but not native buttons.
 *
 * A `<div onClick>` or `<tr onClick>` is invisible to keyboard and screen-reader
 * users: it cannot be tabbed to and Enter/Space do nothing. Spreading this onto
 * such an element gives it the three things a real button has — a tab stop, an
 * announced role, and Enter/Space activation.
 *
 * Prefer an actual <button> when the markup allows it. This exists for the cases
 * where it doesn't: table rows, and chips nested inside other interactive areas.
 */
export function activatable(onActivate: () => void, role: "button" | "checkbox" = "button") {
  return {
    role,
    tabIndex: 0,
    onClick: onActivate,
    onKeyDown: (event: KeyboardEvent) => {
      if (event.key === "Enter" || event.key === " ") {
        // Space scrolls the page by default, which is never what an activation
        // means here.
        event.preventDefault();
        onActivate();
      }
    },
  };
}
