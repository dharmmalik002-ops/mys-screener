import type { MarketKey } from "../lib/api";

type AppStatusBannersProps = {
  error: string | null;
  hasBootstrappedData: boolean;
  loading: boolean;
  market: MarketKey;
  onRetry: () => void;
};

function marketLabel(market: MarketKey) {
  void market;
  return "India";
}

export function AppStatusBanners({
  error,
  hasBootstrappedData,
  loading,
  market,
  onRetry,
}: AppStatusBannersProps) {
  const showStartupToast = loading && !hasBootstrappedData;
  const startupTitle = error
    ? "Reconnecting to the backend"
    : `Loading ${marketLabel(market)} market data`;
  const startupDetail = error
    ? "Taking longer than usual. The app keeps retrying in the background."
    : "Restoring your dashboard, groups, and charts.";

  return (
    <>
      {error ? (
        <div className="error-banner error-banner--actionable">
          <span>{error}</span>
          <button type="button" className="warmup-retry-btn" onClick={onRetry}>
            Retry
          </button>
        </div>
      ) : null}

      {showStartupToast ? (
        <aside className="startup-toast" aria-live="polite">
          <div className={`startup-status-dot startup-status-dot--${error ? "failed" : "warming"}`} />
          <div className="startup-toast-copy">
            <strong>{startupTitle}</strong>
            <span>{startupDetail}</span>
          </div>
          {error ? (
            <button type="button" className="nav-button primary startup-retry-button" onClick={onRetry}>
              Retry
            </button>
          ) : null}
        </aside>
      ) : null}
    </>
  );
}
