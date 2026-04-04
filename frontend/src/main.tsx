import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles/app.css";

function resolveShell() {
  if (typeof window === "undefined") {
    return <App />;
  }

  const pathname = window.location.pathname.replace(/\/+$/, "") || "/";
  if (pathname === "/us" || pathname.startsWith("/us/")) {
    return <App initialMarket="us" useMarketRoutes />;
  }
  if (pathname === "/india" || pathname.startsWith("/india/")) {
    return <App initialMarket="india" useMarketRoutes />;
  }
  return <App />;
}

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {resolveShell()}
  </React.StrictMode>,
);

