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
  const showStartupOverlay = loading && !hasBootstrappedData;
  const startupTitle = error
    ? "Still reconnecting to the backend"
    : `Loading ${marketLabel(market)} market workspace`;
  const startupDetail = error
    ? "The backend is taking longer than usual. You can retry now while the app keeps trying in the background."
    : "Fresh deploys and cold starts can take 30-60 seconds. Restoring your dashboard, sectors, groups, and chart state.";

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

      {showStartupOverlay ? (
        <section className="startup-overlay" aria-live="polite">
          <div className="startup-overlay-card">
            <div className={`startup-status-dot startup-status-dot--${error ? "failed" : "warming"}`} />
            <p className="startup-kicker">First Load</p>
            <h2>{startupTitle}</h2>
            <p>{startupDetail}</p>
            <div className="startup-checklist">
              <div>1. Connecting to backend</div>
              <div>2. Restoring cached market data</div>
              <div>3. Loading dashboard and charts</div>
            </div>
            <button type="button" className="nav-button primary startup-retry-button" onClick={onRetry}>
              Retry Connection
            </button>
          </div>
        </section>
      ) : null}
    </>
  );
}
