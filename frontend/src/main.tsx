import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles/app.css";
import "./styles/premium-overrides.css";
import "./styles/nav-modern.css";

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

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <RootErrorBoundary>
      {resolveShell()}
    </RootErrorBoundary>
  </React.StrictMode>,
);
