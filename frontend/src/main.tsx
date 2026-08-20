import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles/app.css";
// Single override layer. Was three stacked files (premium-overrides,
// nav-modern, top-bar-modern) where a rule's winner depended on which file it
// lived in; they are now merged in that same order inside overrides.css.
import "./styles/overrides.css";
// Phone layer. Lazily-loaded panel CSS is injected AFTER this file, so it
// cannot rely on load order — every rule inside is `html `-prefixed to win on
// specificity instead. See the header comment there.
import "./styles/mobile.css";

class RootErrorBoundary extends React.Component<
  { children: React.ReactNode },
  { error: Error | null }
> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { error: null };
  }
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{
          minHeight: "100vh", display: "flex", flexDirection: "column",
          alignItems: "center", justifyContent: "center",
          background: "#0c0c1d", color: "#fff", fontFamily: "system-ui",
          padding: "2rem", textAlign: "center",
        }}>
          <div style={{ fontSize: "2rem", marginBottom: "1rem" }}>⚠️</div>
          <h2 style={{ fontSize: "1.2rem", fontWeight: 700, marginBottom: "0.5rem" }}>
            Something went wrong
          </h2>
          <p style={{ fontSize: "0.875rem", color: "rgba(255,255,255,0.6)", marginBottom: "1.5rem", maxWidth: 400 }}>
            {this.state.error.message}
          </p>
          <button
            onClick={() => window.location.reload()}
            style={{
              background: "var(--accent, #7c6aff)", color: "#fff", border: "none",
              borderRadius: "8px", padding: "0.6rem 1.4rem", cursor: "pointer", fontSize: "0.875rem",
            }}
          >
            Reload
          </button>
          <details style={{ marginTop: "1rem", fontSize: "0.75rem", color: "rgba(255,255,255,0.35)", maxWidth: 600 }}>
            <summary style={{ cursor: "pointer" }}>Stack trace</summary>
            <pre style={{ textAlign: "left", marginTop: "0.5rem", overflowX: "auto" }}>
              {this.state.error.stack}
            </pre>
          </details>
        </div>
      );
    }
    return this.props.children;
  }
}

function resolveShell() {
  if (typeof window === "undefined") {
    return <App />;
  }

  const pathname = window.location.pathname.replace(/\/+$/, "") || "/";
  if (pathname === "/india" || pathname.startsWith("/india/")) {
    return <App initialMarket="india" useMarketRoutes />;
  }
  return <App />;
}

// A tab opened before a deploy holds the previous build's chunk manifest;
// when it lazy-loads a panel, the old hashed asset can be gone and Vite emits
// vite:preloadError ("Unable to preload CSS…"). Reload once to pick up the
// fresh build instead of surfacing a dead-end error screen. The timestamp
// guard prevents a reload loop if the network itself is the problem.
window.addEventListener("vite:preloadError", (event) => {
  const KEY = "stockScanner.preloadErrorReloadAt";
  const last = Number(window.sessionStorage.getItem(KEY) || 0);
  if (Date.now() - last < 60_000) return; // let the error surface rather than loop
  try {
    window.sessionStorage.setItem(KEY, String(Date.now()));
  } catch {
    // storage unavailable — still better to reload once than to strand the tab
  }
  event.preventDefault();
  window.location.reload();
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <RootErrorBoundary>
      {resolveShell()}
    </RootErrorBoundary>
  </React.StrictMode>,
);
