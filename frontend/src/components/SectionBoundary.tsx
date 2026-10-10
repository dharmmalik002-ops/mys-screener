import { Component, Suspense, type ErrorInfo, type ReactNode } from "react";
import "./SectionBoundary.css";

type Props = {
  /** What failed, in the reader's words: "This page", "Breadth", "The chart". */
  label?: string;
  /** Changing this clears a caught error (e.g. the page or symbol shown). */
  resetKey?: unknown;
  /** Smaller notice for a section inside a page. */
  compact?: boolean;
  children: ReactNode;
};

type State = { error: Error | null; resetKey: unknown };

/**
 * Contains a render error to the section it happened in.
 *
 * Before this, the only boundary was the root one in main.tsx, so one bad API
 * response anywhere (a missing `items` array on Home was the live example)
 * replaced the whole app with "Something went wrong". Now the failing section
 * says so and offers a retry, and everything around it keeps working.
 *
 * "Try again" remounts the children, which re-runs their fetches — the usual
 * cause is a transient response, so that is usually enough.
 */
export class SectionBoundary extends Component<Props, State> {
  state: State = { error: null, resetKey: this.props.resetKey };

  static getDerivedStateFromError(error: Error): Partial<State> {
    return { error };
  }

  static getDerivedStateFromProps(props: Props, state: State): Partial<State> | null {
    // A new page / symbol is a fresh start, not the thing that failed.
    if (props.resetKey !== state.resetKey) return { error: null, resetKey: props.resetKey };
    return null;
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error(`[${this.props.label ?? "section"}] render failed`, error, info.componentStack);
  }

  private retry = () => this.setState({ error: null });

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;
    const label = this.props.label ?? "This section";
    return (
      <div className={`section-boundary${this.props.compact ? " is-compact" : ""}`} role="alert">
        <div className="section-boundary-text">
          <strong>{label} couldn't load.</strong>
          <span>The rest of the page is unaffected. {error.message ? `(${error.message})` : null}</span>
        </div>
        <button type="button" className="section-boundary-retry" onClick={this.retry}>
          Try again
        </button>
      </div>
    );
  }
}

/** A lazily-loaded page: its own error boundary around its own Suspense. */
export function PageSuspense({ label = "This page", fallback, children }: { label?: string; fallback: ReactNode; children: ReactNode }) {
  return (
    <SectionBoundary label={label}>
      <Suspense fallback={fallback}>{children}</Suspense>
    </SectionBoundary>
  );
}
