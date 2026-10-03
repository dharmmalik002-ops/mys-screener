# DESIGN.md — Mr. Malik Scanner

The visual system for this app, written for whoever touches the UI next —
person or AI coding tool. Read it before styling anything. When this file and
the CSS disagree, the CSS is what ships; fix whichever one is wrong.

Two designs exist, switched by the palette button in the header
(`data-design` on `<html>`):

- **Studio** (default) — `frontend/src/styles/premium.css`. This document
  describes Studio.
- **Classic** — `frontend/src/styles/classic.css`. Obsidian/ivory with
  champagne gold, Geist + Instrument Serif. Kept working, not extended.

Each design has a light theme (default) and a dark theme (`data-theme`).

---

## 1. Principles

1. **It is a tool, not a brochure.** People read tables and charts here for
   hours. Density beats spectacle; nothing animates for its own sake.
2. **Flat.** White cards on an off-white page. No card borders, no drop
   shadows on cards, no gradients behind content. Elevation is reserved for
   things that float (menus, tooltips, the hover chart preview).
3. **Size carries hierarchy, not weight.** Regular (400) almost everywhere;
   500 for labels that must hold their own in a row. Bold is not part of the
   system.
4. **Ink is for interaction, colour is for data.** Selected tabs, chips and
   the one primary button are inverse (black on light, light on dark). The
   vivid colours only ever paint charts, meters and fills — never body text,
   never a button.
5. **One hero per page.** Each page leads with the one thing it is about,
   large: the briefing sentence on Home, realized P&L on the Journal, current
   value on the Funds portfolio. Everything else is smaller and calmer.
6. **Measured numbers stay honest.** No animated counters on prices or P&L,
   no colour that implies a judgement the data doesn't make.

---

## 2. Tokens

Defined in `styles/app.css` (base) and `styles/premium.css` (Studio). Always
use the variable, never the hex.

### Surfaces and text

| Token | Light | Dark | Use |
|---|---|---|---|
| `--bg` | `#f8f8f8` | `#111112` | Page canvas |
| `--card-flat` | `#ffffff` | `#1a1a1c` | Cards |
| `--surface-raised` | `#ffffff` | `#26262a` | Floating panels (menus, previews) |
| `--muted-bg` | `#f3f4f4` | `#222225` | Recessed wells, hover fills, icon tiles |
| `--line` | `#ebebeb` | `#2a2a2d` | Hairlines and dividers |
| `--text` | `#222222` | `#ededed` | Primary text |
| `--text-muted` | `#767676` | `#8c8c8c` | Secondary text, captions |
| `--inverse` / `--inverse-ink` | `#222` / `#fff` | `#ededed` / `#111112` | Selected tab, chip, primary button |

### Meaning

| Token | Light | Dark | Use |
|---|---|---|---|
| `--positive` | `#1a7f52` | `#4fbf8b` | Gains, advances — text and small marks |
| `--negative` | `#d42a3b` | `#f2606b` | Losses, declines, destructive actions |

### Data colours (charts and meters only)

`--viz-orange #fc6200`, `--viz-pink #ea4c7d`, `--viz-green #66c398`,
`--viz-purple #958dfc`, `--viz-red #fb3748`. The 50-day average is orange
everywhere it is drawn.

Canvas charts cannot read CSS variables; their colours live in
`lib/marketColors.ts` and `lib/chartDefaults.ts`. Change both sides together.

### Type

| Token | Stack | Use |
|---|---|---|
| `--font-sans` | System SF Pro stack | Everything by default; tabular numerals on |
| `--font-editorial` | `ui-serif`, New York, Georgia | One-sentence headlines only (the Home briefing) |
| `--font-caption` | `ui-monospace`, SF Mono | Small uppercase kickers ("MARKET BRIEFING · 01 OCT") |

Sizes in practice: hero figures 30–46px (`clamp`), card values 22–28px, body
14–15px, labels 13px, captions 11–12px. Letter-spacing tightens as size grows
(−0.01em body, −0.02 to −0.03em heroes).

### Shape and motion

- Radii: 6px (small controls), 8px (tabs, buttons, icon tiles), 12px (rail,
  menus, chart canvases), `--radius-xl` (cards).
- Easing: `--ease-out-soft` `cubic-bezier(0.23, 1, 0.32, 1)` for anything
  entering or settling. 120–240ms. Respect `prefers-reduced-motion` — every
  animation in the app has a reduced-motion branch.

---

## 3. Components

| Pattern | Where | Notes |
|---|---|---|
| **Header rail** | `components/NavGroups.tsx` | Four groups (Market / Scan / Journal / Research), dropdowns with icon + one-line blurb. Phones use the flat bottom tab bar. |
| **Sliding tab pill** | `components/GlidePill.tsx` | `<GlidePill activeSelector=".active" watch={tab} />` inside any tab container; add `has-glide-pill` to the container. Copies the active tab's own fill, so it needs no colours of its own. |
| **Editorial briefing** | `HomePanel` `.homepro-briefing` | Mono kicker, serif sentence up to 46px, a hairline, then a row of small `dt`/`dd` figures. |
| **Bento summary** | `HomePanel` `.homepro-kpis.is-bento` | The XP dial is the large tile spanning two rows; counts beside it. Collapses to one column of tiles on phones. |
| **Hero figure card** | Journal `.tj-kpi-hero` | One large number on a 8–9% positive/negative tint, with 2–3 supporting figures under a hairline. |
| **Meter card** | `HomePanel` `MeterCard` + `.ol-tick-meter` | Label, value, foot line, tick meter in a data colour. |
| **Chart marks** | `.ol-chip`, `.ol-tip`, `--hatch-*` | Axis chips, the inverse tooltip pill, hatch textures. Shared by both designs. |
| **Hover chart preview** | `components/ChartHoverPreview.tsx` | 450ms hover-intent, SVG candles + 50-DMA + volume, reads the shared chart cache. Never interactive. |
| **Hold to confirm** | `components/HoldToConfirmButton.tsx` | For destructive actions on data the user cannot get back. Replaces `window.confirm()`. |
| **Big price charts** | `ChartPanel` | White paper in both themes, no grid lines. |
| **Resizable splits** | `components/SplitResizer.tsx` | Drag seam between side-by-side panes; ratio per `storageKey`. |

---

## 4. Rules that bite

- **Scope every Studio selector** to `:root[data-design="studio"]`. Panel CSS
  loads lazily *after* `premium.css`, so a rule that must win is prefixed
  `html:root`. `mobile.css` owns phone layout; `premium.css` owns paint.
- **Full-screen overlays and floating panels portal to `<body>`.**
  `main.workspace` carries a transform, which makes it the containing block for
  `position: fixed` (CLAUDE.md gotcha 17).
- **Never fetch for decoration.** Previews and prewarms go through the shared
  chart fetch (`fetchChartShared`) and the cache, so a hover can never double a
  request to a backend with three chart slots.
- **Light is the default theme.** Check every change in light *and* dark, and
  in Classic, at 1440px, ~1000px and 375px.
- **Don't add:** WebGL/shader backgrounds, 3D icons, illustration packs,
  parallax, scroll-jacking, glassmorphism, or a second accent colour for
  interaction. All were looked at and rejected for this app.
