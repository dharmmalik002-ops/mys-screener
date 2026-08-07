import "./EmptyState.css";

/**
 * Empty state with a way out.
 *
 * The app had ~35 dead-end strings ("No data", "No trades yet", "No stocks
 * match…"). Exactly one of them suggested a next step and none of them linked
 * anywhere, so a zero-result screen was a full stop.
 *
 * `action` is optional on purpose: some empty states genuinely have no next
 * step (a market that simply had no gainers today). Inventing a button there
 * would be noise. Only pass one when it actually does something.
 */

type EmptyStateAction = {
  label: string;
  onClick: () => void;
};

type EmptyStateProps = {
  /** Lucide icon element, e.g. <SearchX size={22} />. */
  icon?: React.ReactNode;
  title: string;
  /** One line of explanation. Keep it factual, not apologetic. */
  body?: string;
  action?: EmptyStateAction;
  /** Secondary, lower-emphasis action. */
  secondaryAction?: EmptyStateAction;
  compact?: boolean;
  className?: string;
};

export function EmptyState({
  icon,
  title,
  body,
  action,
  secondaryAction,
  compact = false,
  className,
}: EmptyStateProps) {
  return (
    <div className={["empty-state-v2", compact ? "is-compact" : "", className ?? ""].filter(Boolean).join(" ")}>
      {icon ? <div className="empty-state-icon" aria-hidden="true">{icon}</div> : null}
      <p className="empty-state-title">{title}</p>
      {body ? <p className="empty-state-body">{body}</p> : null}
      {action || secondaryAction ? (
        <div className="empty-state-actions">
          {action ? (
            <button type="button" className="empty-state-btn" onClick={action.onClick}>
              {action.label}
            </button>
          ) : null}
          {secondaryAction ? (
            <button type="button" className="empty-state-btn ghost" onClick={secondaryAction.onClick}>
              {secondaryAction.label}
            </button>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
