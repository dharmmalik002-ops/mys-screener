import { ChevronDown, ChevronUp, ChevronsUpDown } from "lucide-react";

import "./SortableTh.css";

/**
 * Click-to-sort table header.
 *
 * Every table in the app already had sorting logic — buried in a dropdown, a
 * popover, or two loose buttons — while the header row, the universal
 * affordance for it, was an inert <span>. This makes the existing sort
 * reachable where users look for it, without changing any sort semantics.
 *
 * Deliberately controlled: the parent still owns sort state, so an existing
 * dropdown and these headers stay in sync rather than competing.
 */

export type SortDirection = "asc" | "desc";

type SortableThProps<K extends string> = {
  /** Sort key this column maps to. */
  sortKey: K;
  /** Currently active sort key. */
  activeKey: K;
  /** Direction of the active sort. */
  direction: SortDirection;
  onSort: (key: K) => void;
  children: React.ReactNode;
  /** Right-align for numeric columns. */
  numeric?: boolean;
  title?: string;
  className?: string;
  style?: React.CSSProperties;
};

export function SortableTh<K extends string>({
  sortKey,
  activeKey,
  direction,
  onSort,
  children,
  numeric = false,
  title,
  className,
  style,
}: SortableThProps<K>) {
  const active = sortKey === activeKey;
  const Icon = !active ? ChevronsUpDown : direction === "asc" ? ChevronUp : ChevronDown;

  return (
    <th
      className={[
        "sortable-th",
        numeric ? "sortable-th-num" : "",
        active ? "is-active" : "",
        className ?? "",
      ]
        .filter(Boolean)
        .join(" ")}
      style={style}
      // Screen readers announce the current sort rather than just "column".
      aria-sort={active ? (direction === "asc" ? "ascending" : "descending") : "none"}
    >
      <button
        type="button"
        className="sortable-th-btn"
        onClick={() => onSort(sortKey)}
        title={title ?? `Sort by ${typeof children === "string" ? children : sortKey}`}
      >
        <span className="sortable-th-label">{children}</span>
        <Icon size={12} strokeWidth={2.4} className="sortable-th-icon" aria-hidden="true" />
      </button>
    </th>
  );
}

/**
 * Toggle helper: clicking the active column flips direction, clicking a new one
 * adopts that column's natural default (ranks ascend, everything else descends
 * — biggest number first is what you want from a returns column).
 */
export function nextSort<K extends string>(
  current: { key: K; direction: SortDirection },
  clicked: K,
  ascendingByDefault: readonly K[] = [],
): { key: K; direction: SortDirection } {
  if (current.key === clicked) {
    return { key: clicked, direction: current.direction === "asc" ? "desc" : "asc" };
  }
  return { key: clicked, direction: ascendingByDefault.includes(clicked) ? "asc" : "desc" };
}
