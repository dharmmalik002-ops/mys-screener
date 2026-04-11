
path = "/Users/dharmender/Desktop/Stock Scanner c/frontend/src/styles/premium-overrides.css"

css = """\
/* ══════════════════════════════════════════════════════════════════════════
   PREMIUM OVERRIDES — uses CSS variables; works in dark AND light themes
   ══════════════════════════════════════════════════════════════════════════ */

/* ── Mono font for all financial data ──────────────────────────────────── */
.positive-text,
.negative-text,
.scan-row strong,
.home-stock-row strong,
.metric-card strong,
.home-macro-price,
.home-macro-price strong,
.mf-rotation-pct,
.mf-rotation-main-value,
.mf-rotation-stock-return,
.chart-grid-card-foot strong,
.pe-stat strong,
.home-group-metrics strong,
.sector-heatmap-card-return,
.market-health-score-value,
.adv-dec-stat strong,
.ticker-ribbon-item strong,
.chart-panel .panel-header h2 {
  font-family: var(--font-mono) !important;
}

/* ── Positive / negative colours ─────────────────────────────────────── */
.positive-text {
  color: var(--positive) !important;
  font-weight: 700;
}
.negative-text {
  color: var(--negative) !important;
  font-weight: 700;
}
:root:not([data-theme="light"]) .positive-text {
  text-shadow: 0 0 16px color-mix(in srgb, var(--positive) 40%, transparent);
}
:root:not([data-theme="light"]) .negative-text {
  text-shadow: 0 0 16px color-mix(in srgb, var(--negative) 40%, transparent);
}

/* ── Metric card number size ──────────────────────────────────────────── */
.metric-card strong {
  font-family: var(--font-mono);
  font-size: 1.35rem;
  font-weight: 700;
  letter-spacing: -0.02em;
}

/* ═══════════════════════════════════════════════════════════════════════════
   ACTIVE / SELECTED STATES  —  solid accent fill, impossible to miss
   ═══════════════════════════════════════════════════════════════════════════ */

/* ── Pills and tabs ───────────────────────────────────────────────────── */
.timeframe-pill.active,
.scanner-tab.active,
.indicator-pill.active,
.tool-pill.active {
  background: var(--accent) !important;
  color: #fff !important;
  border-color: var(--accent) !important;
  font-weight: 700 !important;
  box-shadow:
    0 0 0 3px color-mix(in srgb, var(--accent) 22%, transparent),
    0 4px 18px color-mix(in srgb, var(--accent) 38%, transparent) !important;
}

/* ── Primary nav button ───────────────────────────────────────────────── */
.nav-button.primary {
  background: var(--accent) !important;
  color: #fff !important;
  border-color: var(--accent) !important;
  font-weight: 700 !important;
  box-shadow:
    0 0 0 3px color-mix(in srgb, var(--accent) 22%, transparent),
    0 6px 22px color-mix(in srgb, var(--accent) 36%, transparent) !important;
}

/* ── Screener nav button ──────────────────────────────────────────────── */
.screener-nav-button.active {
  border-color: var(--accent) !important;
  background: color-mix(in srgb, var(--accent) 18%, var(--surface-strong)) !important;
  color: var(--text) !important;
  box-shadow:
    inset 0 0 0 1px color-mix(in srgb, var(--accent) 40%, transparent),
    0 0 0 3px color-mix(in srgb, var(--accent) 14%, transparent),
    0 8px 28px color-mix(in srgb, var(--accent) 22%, transparent) !important;
}
.screener-nav-button.active .screener-nav-main strong {
  color: var(--accent);
}

/* ── PE modal tab ─────────────────────────────────────────────────────── */
.pe-modal-tab.active {
  background: var(--accent) !important;
  color: #fff !important;
  border-color: var(--accent) !important;
  font-weight: 700 !important;
  box-shadow: 0 4px 14px color-mix(in srgb, var(--accent) 32%, transparent) !important;
}

/* ── MF mode tab ──────────────────────────────────────────────────────── */
.mf-mode-tab.active {
  background: var(--accent) !important;
  color: #fff !important;
  border-color: var(--accent) !important;
  font-weight: 700 !important;
}

/* ── MF week button ───────────────────────────────────────────────────── */
.mf-week-btn.active {
  background: color-mix(in srgb, var(--accent) 20%, var(--surface-strong)) !important;
  color: var(--accent) !important;
  border-color: color-mix(in srgb, var(--accent) 50%, transparent) !important;
  font-weight: 700 !important;
}

/* ── Saved scanner chip ───────────────────────────────────────────────── */
.saved-scanner-chip.active {
  background: var(--accent) !important;
  color: #fff !important;
  border-color: var(--accent) !important;
  font-weight: 700 !important;
}

/* ── Width btn / RVOL btn ─────────────────────────────────────────────── */
.width-btn.active,
.rvol-size-btn.active {
  background: var(--accent) !important;
  color: #fff !important;
}

/* ── Scan row selected ────────────────────────────────────────────────── */
.scan-row.active {
  border-left: 3px solid var(--accent) !important;
  border-color: color-mix(in srgb, var(--accent) 45%, transparent) !important;
  background: color-mix(in srgb, var(--accent) 12%, var(--surface-strong)) !important;
  box-shadow:
    inset 4px 0 0 var(--accent),
    0 4px 20px color-mix(in srgb, var(--accent) 16%, transparent) !important;
}

/* ── Sector / group / watchlist row active ────────────────────────────── */
.sector-company-row.active,
.group-stock-row.active,
.chart-group-row.active {
  background: color-mix(in srgb, var(--accent) 12%, var(--surface-strong)) !important;
  border-color: color-mix(in srgb, var(--accent) 40%, transparent) !important;
}

.watchlist-link.active {
  background: color-mix(in srgb, var(--accent) 16%, var(--surface-strong)) !important;
  border-color: color-mix(in srgb, var(--accent) 45%, transparent) !important;
  color: var(--accent) !important;
  font-weight: 700 !important;
}

/* ── MF rotation row active ───────────────────────────────────────────── */
.mf-rotation-row.active {
  background: color-mix(in srgb, var(--accent) 12%, var(--surface-strong)) !important;
  border-color: color-mix(in srgb, var(--accent) 45%, transparent) !important;
}

/* ── MF stock card active ─────────────────────────────────────────────── */
.mf-stock-card.active {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--accent) 28%, transparent) !important;
}

/* ── Screener saved item active ───────────────────────────────────────── */
.screener-saved-item.active {
  border-color: color-mix(in srgb, var(--accent) 55%, transparent) !important;
  background: color-mix(in srgb, var(--accent) 10%, var(--surface-soft)) !important;
  box-shadow: 0 0 0 1px color-mix(in srgb, var(--accent) 20%, transparent) !important;
}

/* ── Chart note selected ──────────────────────────────────────────────── */
.chart-note.selected {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 2px color-mix(in srgb, var(--accent) 28%, transparent) !important;
}

/* ── Color swatch active ──────────────────────────────────────────────── */
.wl-color-swatch.active {
  box-shadow: 0 0 0 2px var(--bg), 0 0 0 4px var(--accent) !important;
}

/* ═══════════════════════════════════════════════════════════════════════════
   PANEL — elevated using CSS vars (works in both themes)
   ═══════════════════════════════════════════════════════════════════════════ */
.panel {
  border: 1px solid color-mix(in srgb, var(--glass-border) 90%, transparent);
  background:
    linear-gradient(160deg,
      color-mix(in srgb, var(--surface-strong) 92%, white 8%),
      color-mix(in srgb, var(--surface) 88%, transparent)
    );
  backdrop-filter: blur(24px) saturate(140%);
  box-shadow:
    inset 0 1px 0 color-mix(in srgb, white 7%, transparent),
    var(--shadow);
  transition: box-shadow 260ms ease, border-color 260ms ease;
}
.panel:hover {
  border-color: color-mix(in srgb, var(--accent) 22%, var(--glass-border));
  box-shadow:
    inset 0 1px 0 color-mix(in srgb, white 9%, transparent),
    var(--shadow),
    0 0 0 1px color-mix(in srgb, var(--accent) 10%, transparent);
}
.panel-kicker {
  color: var(--accent);
  font-size: 0.66rem;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  opacity: 0.9;
}

/* ═══════════════════════════════════════════════════════════════════════════
   SCAN ROWS
   ═══════════════════════════════════════════════════════════════════════════ */
.scan-row {
  padding: 11px 16px;
  border: 1px solid color-mix(in srgb, var(--glass-border) 70%, transparent);
  border-left: 3px solid transparent;
  border-radius: var(--radius-md);
  background: color-mix(in srgb, var(--surface-soft) 60%, transparent);
  transition: all 180ms cubic-bezier(0.4, 0, 0.2, 1);
  backdrop-filter: blur(10px);
}
.scan-row:hover {
  border-color: color-mix(in srgb, var(--accent) 28%, var(--glass-border));
  border-left-color: color-mix(in srgb, var(--accent) 70%, transparent);
  background: color-mix(in srgb, var(--surface-strong) 80%, transparent);
  transform: translateY(-1px);
  box-shadow: 0 4px 20px color-mix(in srgb, var(--accent) 10%, transparent);
}

/* ═══════════════════════════════════════════════════════════════════════════
   HOME CARDS
   ═══════════════════════════════════════════════════════════════════════════ */
.home-stock-row {
  border: 1px solid color-mix(in srgb, var(--glass-border) 75%, transparent);
  border-radius: var(--radius-md);
  background: color-mix(in srgb, var(--surface-soft) 65%, transparent);
  backdrop-filter: blur(14px);
  transition: all 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.home-stock-row:hover {
  border-color: color-mix(in srgb, var(--accent) 30%, var(--glass-border));
  transform: translateY(-2px);
  box-shadow: 0 6px 24px color-mix(in srgb, var(--accent) 12%, transparent);
  background: color-mix(in srgb, var(--surface-strong) 78%, transparent);
}

.home-macro-card {
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  border-radius: var(--radius-md);
  background: color-mix(in srgb, var(--surface-soft) 65%, transparent);
  backdrop-filter: blur(16px);
  box-shadow: inset 0 1px 0 color-mix(in srgb, white 6%, transparent);
  transition: all 180ms ease;
}
.home-macro-card:hover {
  border-color: color-mix(in srgb, var(--accent) 32%, var(--glass-border));
  transform: translateY(-2px);
  box-shadow:
    0 8px 28px color-mix(in srgb, var(--accent) 12%, transparent),
    inset 0 1px 0 color-mix(in srgb, white 8%, transparent);
  background: color-mix(in srgb, var(--surface-strong) 78%, transparent);
}
.home-macro-price {
  font-family: var(--font-mono);
  font-size: 1.0rem;
  font-weight: 600;
  letter-spacing: -0.01em;
}

.home-group-card {
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  border-radius: var(--radius-lg);
  background: color-mix(in srgb, var(--surface) 72%, transparent);
  backdrop-filter: blur(18px);
  box-shadow: inset 0 1px 0 color-mix(in srgb, white 6%, transparent);
  transition: all 200ms cubic-bezier(0.4, 0, 0.2, 1);
}
.home-group-card:hover,
.home-group-card:focus-visible {
  border-color: color-mix(in srgb, var(--accent) 32%, var(--glass-border));
  transform: translateY(-2px);
  box-shadow:
    0 12px 40px color-mix(in srgb, var(--accent) 14%, transparent),
    inset 0 1px 0 color-mix(in srgb, white 8%, transparent);
  background: color-mix(in srgb, var(--surface-strong) 80%, transparent);
}

/* ── Market toggle ────────────────────────────────────────────────────── */
.home-market-toggle {
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  background: color-mix(in srgb, var(--surface-soft) 70%, transparent);
  backdrop-filter: blur(20px);
  box-shadow: inset 0 1px 0 color-mix(in srgb, white 6%, transparent);
}
.home-market-toggle-btn.active {
  background: linear-gradient(
    135deg,
    color-mix(in srgb, var(--accent) 90%, white 5%),
    color-mix(in srgb, var(--accent-2) 85%, transparent)
  ) !important;
  color: #fff !important;
  font-weight: 800 !important;
  box-shadow:
    0 6px 20px color-mix(in srgb, var(--accent) 35%, transparent),
    inset 0 1px 0 rgba(255,255,255,0.22) !important;
}

/* ── Sector heatmap ───────────────────────────────────────────────────── */
.sector-heatmap-card {
  border-radius: var(--radius-lg);
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.14), 0 4px 16px rgba(0,0,0,0.18);
  transition: transform 160ms ease, box-shadow 160ms ease;
}
.sector-heatmap-card:hover {
  transform: translateY(-3px) scale(1.02);
  box-shadow: inset 0 1px 0 rgba(255,255,255,0.18), 0 12px 36px rgba(0,0,0,0.28);
}
.sector-heatmap-card-return {
  font-family: var(--font-mono);
  font-size: 1.05rem;
  font-weight: 700;
  letter-spacing: -0.03em;
}

/* ── Home hero card ───────────────────────────────────────────────────── */
.home-hero-card {
  border: 1px solid color-mix(in srgb, var(--glass-border) 85%, transparent);
  background:
    radial-gradient(ellipse at 20% 0%, color-mix(in srgb, var(--accent) 12%, transparent), transparent 44%),
    radial-gradient(ellipse at 80% 100%, color-mix(in srgb, var(--accent-2) 8%, transparent), transparent 44%),
    color-mix(in srgb, var(--surface-strong) 90%, transparent);
  backdrop-filter: blur(28px) saturate(140%);
  box-shadow:
    inset 0 1px 0 color-mix(in srgb, white 8%, transparent),
    var(--shadow);
}

/* ── Top nav ──────────────────────────────────────────────────────────── */
.top-nav {
  background:
    linear-gradient(
      180deg,
      color-mix(in srgb, var(--surface-strong) 88%, transparent),
      color-mix(in srgb, var(--surface) 72%, transparent)
    );
  border-bottom: 1px solid color-mix(in srgb, var(--line) 90%, transparent);
  box-shadow:
    0 1px 0 color-mix(in srgb, white 4%, transparent),
    0 12px 48px color-mix(in srgb, var(--bg) 40%, transparent);
  backdrop-filter: blur(28px) saturate(160%);
}

/* ── Brand mark ───────────────────────────────────────────────────────── */
.brand-mark {
  background: linear-gradient(145deg, var(--accent), var(--accent-strong));
  box-shadow:
    0 4px 16px color-mix(in srgb, var(--accent) 36%, transparent),
    inset 0 1px 0 rgba(255,255,255,0.18);
}

/* ── Nav search ───────────────────────────────────────────────────────── */
.nav-search input {
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  background: color-mix(in srgb, var(--surface-strong) 68%, transparent);
  color: var(--text);
  box-shadow: inset 0 1px 0 color-mix(in srgb, white 5%, transparent);
}
.nav-search input:focus {
  border-color: color-mix(in srgb, var(--accent) 55%, transparent);
  box-shadow:
    0 0 0 3px color-mix(in srgb, var(--accent) 14%, transparent),
    inset 0 1px 0 color-mix(in srgb, white 6%, transparent);
  outline: none;
}

/* ── Ticker ribbon ────────────────────────────────────────────────────── */
.ticker-ribbon {
  background:
    linear-gradient(90deg,
      color-mix(in srgb, var(--accent) 6%, transparent),
      transparent 20%,
      transparent 80%,
      color-mix(in srgb, var(--accent) 6%, transparent)
    ),
    color-mix(in srgb, var(--bg-alt) 92%, var(--bg));
  border-bottom: 1px solid color-mix(in srgb, var(--line) 80%, transparent);
}

/* ── Chart grid card ──────────────────────────────────────────────────── */
.chart-grid-card {
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  background: color-mix(in srgb, var(--surface) 88%, transparent);
  box-shadow: inset 0 1px 0 color-mix(in srgb, white 5%, transparent);
  transition: all 180ms ease;
  color: var(--text);
}
.chart-grid-card.clickable:hover {
  border-color: color-mix(in srgb, var(--accent) 28%, var(--glass-border));
  box-shadow:
    0 12px 36px color-mix(in srgb, var(--accent) 12%, transparent),
    inset 0 1px 0 color-mix(in srgb, white 7%, transparent);
  transform: translateY(-2px);
}

/* ── MF badges ────────────────────────────────────────────────────────── */
.mf-badge--bull {
  background: color-mix(in srgb, var(--positive) 16%, transparent);
  color: var(--positive);
  border: 1px solid color-mix(in srgb, var(--positive) 30%, transparent);
  font-weight: 700;
}
.mf-badge--bear {
  background: color-mix(in srgb, var(--negative) 14%, transparent);
  color: var(--negative);
  border: 1px solid color-mix(in srgb, var(--negative) 28%, transparent);
  font-weight: 700;
}
.mf-sector-card--bullish {
  border-color: color-mix(in srgb, var(--positive) 32%, transparent);
  background: color-mix(in srgb, var(--positive) 8%, var(--surface-soft));
}
.mf-sector-card--bearish {
  border-color: color-mix(in srgb, var(--negative) 32%, transparent);
  background: color-mix(in srgb, var(--negative) 8%, var(--surface-soft));
}

/* ── Chart grid +/- badges ────────────────────────────────────────────── */
.chart-grid-badge.positive {
  background: color-mix(in srgb, var(--positive) 16%, transparent);
  color: var(--positive);
  border: 1px solid color-mix(in srgb, var(--positive) 26%, transparent);
  font-weight: 700;
}
.chart-grid-badge.negative {
  background: color-mix(in srgb, var(--negative) 14%, transparent);
  color: var(--negative);
  border: 1px solid color-mix(in srgb, var(--negative) 24%, transparent);
  font-weight: 700;
}

/* ── Sparklines & candles ─────────────────────────────────────────────── */
.chart-grid-sparkline.positive .chart-grid-sparkline-area { fill: color-mix(in srgb, var(--positive) 20%, transparent); }
.chart-grid-sparkline.positive .chart-grid-sparkline-line  { stroke: var(--positive); }
.chart-grid-sparkline.negative .chart-grid-sparkline-area { fill: color-mix(in srgb, var(--negative) 18%, transparent); }
.chart-grid-sparkline.negative .chart-grid-sparkline-line  { stroke: var(--negative); }
.chart-grid-candle.positive rect,
.chart-grid-candle.positive line,
.chart-grid-bar.positive line { fill: var(--positive); stroke: var(--positive); }
.chart-grid-candle.negative rect,
.chart-grid-candle.negative line,
.chart-grid-bar.negative line { fill: var(--negative); stroke: var(--negative); }

/* ── Screener sidebar panel ───────────────────────────────────────────── */
.screener-sidebar-panel {
  background:
    radial-gradient(circle at top left, color-mix(in srgb, var(--accent) 14%, transparent), transparent 38%),
    color-mix(in srgb, var(--surface-strong) 90%, transparent);
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  backdrop-filter: blur(24px);
}

/* ── Screener nav button (base) ───────────────────────────────────────── */
.screener-nav-button {
  border: 1px solid color-mix(in srgb, var(--accent) 28%, var(--glass-border));
  background:
    linear-gradient(160deg,
      color-mix(in srgb, var(--accent) 10%, transparent),
      color-mix(in srgb, var(--surface-soft) 92%, transparent)
    );
  color: var(--text);
  transition: all 160ms ease;
}
.screener-nav-button:hover {
  border-color: color-mix(in srgb, var(--accent) 42%, var(--glass-border));
  background:
    linear-gradient(160deg,
      color-mix(in srgb, var(--accent) 16%, transparent),
      color-mix(in srgb, var(--surface-strong) 90%, transparent)
    );
  transform: translateY(-1px);
  box-shadow: 0 6px 20px color-mix(in srgb, var(--accent) 12%, transparent);
}

/* ── Status dot ───────────────────────────────────────────────────────── */
.status-dot {
  width: 9px;
  height: 9px;
  background: var(--positive);
  box-shadow: 0 0 10px color-mix(in srgb, var(--positive) 55%, transparent);
}

/* ── PE / chart modals ────────────────────────────────────────────────── */
.pe-modal {
  border: 1px solid color-mix(in srgb, var(--glass-border) 85%, transparent);
  background: color-mix(in srgb, var(--surface-strong) 95%, transparent);
  box-shadow: 0 32px 80px color-mix(in srgb, var(--bg) 52%, transparent);
  backdrop-filter: blur(32px);
  color: var(--text);
}
.chart-grid-modal {
  border: 1px solid color-mix(in srgb, var(--glass-border) 85%, transparent);
  background:
    radial-gradient(circle at top left, color-mix(in srgb, var(--accent) 10%, transparent), transparent 30%),
    color-mix(in srgb, var(--surface-strong) 92%, transparent);
  box-shadow: 0 40px 100px color-mix(in srgb, var(--bg) 54%, transparent);
  backdrop-filter: blur(32px);
  color: var(--text);
}

/* ── MF stock metrics mono ────────────────────────────────────────────── */
.mf-stock-metric-card strong,
.mf-key-metric-item strong,
.mf-rotation-pct,
.mf-rotation-main-value {
  font-family: var(--font-mono);
  font-weight: 600;
  letter-spacing: -0.015em;
}

/* ── Mode switch ──────────────────────────────────────────────────────── */
.mode-switch {
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  background: color-mix(in srgb, var(--surface-soft) 70%, transparent);
  backdrop-filter: blur(14px);
  box-shadow: inset 0 1px 0 color-mix(in srgb, white 5%, transparent);
}

/* ── Skeleton shimmer ─────────────────────────────────────────────────── */
.skeleton-block,
.skeleton-row {
  background: linear-gradient(
    90deg,
    color-mix(in srgb, var(--text-muted) 18%, transparent) 0%,
    color-mix(in srgb, var(--text-soft) 22%, transparent) 40%,
    color-mix(in srgb, var(--text-muted) 18%, transparent) 80%
  );
  background-size: 400% 100%;
  animation: skeletonPulse 1.8s ease-in-out infinite;
  border-radius: 8px;
}
@keyframes skeletonPulse {
  0%   { background-position: 100% 0; }
  100% { background-position: -100% 0; }
}

/* ── Home group card entrance stagger ─────────────────────────────────── */
.home-group-card          { animation: homeCardIn 0.42s ease-out both; }
.home-group-card:nth-child(1) { animation-delay: 0ms; }
.home-group-card:nth-child(2) { animation-delay: 50ms; }
.home-group-card:nth-child(3) { animation-delay: 100ms; }
.home-group-card:nth-child(4) { animation-delay: 150ms; }
@keyframes homeCardIn {
  from { opacity: 0; transform: translateY(10px) scale(0.98); }
  to   { opacity: 1; transform: translateY(0)    scale(1); }
}

/* ── Scan row entrance ────────────────────────────────────────────────── */
.scan-table-body .scan-row {
  animation: scanRowIn 0.24s ease-out both;
}
@keyframes scanRowIn {
  from { opacity: 0; transform: translateX(-4px); }
  to   { opacity: 1; transform: translateX(0); }
}

/* ── Landing hero ─────────────────────────────────────────────────────── */
.landing-hero {
  border: 1px solid color-mix(in srgb, var(--glass-border) 80%, transparent);
  background:
    radial-gradient(ellipse at 25% 0%, color-mix(in srgb, var(--accent) 12%, transparent), transparent 44%),
    linear-gradient(160deg,
      color-mix(in srgb, var(--surface-strong) 92%, white 4%),
      color-mix(in srgb, var(--surface) 85%, transparent)
    );
  box-shadow:
    inset 0 1px 0 color-mix(in srgb, white 7%, transparent),
    var(--shadow);
  backdrop-filter: blur(24px);
}

/* ── Scrollbar ────────────────────────────────────────────────────────── */
::-webkit-scrollbar-thumb {
  background: color-mix(in srgb, var(--accent) 25%, var(--line-strong));
}
::-webkit-scrollbar-thumb:hover {
  background: color-mix(in srgb, var(--accent) 45%, var(--line-strong));
}
"""

with open(path, 'w', encoding='utf-8') as f:
    f.write(css)

lines = css.count('\n')
print(f"OK: {lines} lines written to {path}")
