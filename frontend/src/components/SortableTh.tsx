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

function SortButton({
  active,
  direction,
  onClick,
  title,
  children,
}: {
  active: boolean;
  direction: SortDirection;
  onClick: () => void;
  title?: string;
  children: React.ReactNode;
}) {
  const Icon = !active ? ChevronsUpDown : direction === "asc" ? ChevronUp : ChevronDown;
  return (
    <button type="button" className="sortable-th-btn" onClick={onClick} title={title}>
      <span className="sortable-th-label">{children}</span>
      <Icon size={12} strokeWidth={2.4} className="sortable-th-icon" aria-hidden="true" />
    </button>
  );
}

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
      <SortButton
        active={active}
        direction={direction}
        onClick={() => onSort(sortKey)}
        title={title ?? `Sort by ${typeof children === "string" ? children : sortKey}`}
      >
        {children}
      </SortButton>
    </th>
  );
}

/**
 * Same affordance for tables built as a CSS grid of <span>s rather than a real
 * <table> (ScanTable, WatchlistsPanel). Renders the button inside a span so it
 * drops into an existing grid template without disturbing the column layout.
 */
export function SortableHeader({
  active,
  direction,
  onSort,
  children,
  numeric = false,
  title,
  className,
}: {
  active: boolean;
  direction: SortDirection;
  onSort: () => void;
  children: React.ReactNode;
  numeric?: boolean;
  title?: string;
  className?: string;
}) {
  return (
    <span
      className={[
        "sortable-th",
        numeric ? "sortable-th-num" : "",
        active ? "is-active" : "",
        className ?? "",
      ]
        .filter(Boolean)
        .join(" ")}
      aria-sort={active ? (direction === "asc" ? "ascending" : "descending") : "none"}
      role="columnheader"
    >
      <SortButton active={active} direction={direction} onClick={onSort} title={title}>
        {children}
      </SortButton>
    </span>
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
