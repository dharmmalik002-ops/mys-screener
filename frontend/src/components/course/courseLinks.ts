import type { ScreenerMode } from "../ScreenerSidebar";

/* Where each setup lesson meets the rest of the site.

   - scanners: the closest screener on this site, so "find it today" is one
     click. Closest, not equivalent — none of these is his definition, and the
     lesson card says so.
   - deck: Chart Gym signal sets (study_deck.json) — real Indian signals with a
     graded result, failures included.
   - styles: look-alike gallery setups (lookalike_history.json) — Indian charts
     the image model matched to that trader's setup, graded +20% before -8%.

   Only lessons with a fair match are listed; a lesson missing here simply has
   no "real examples" panel rather than a misleading one. */

export type ScannerLink = { mode: ScreenerMode; label: string };
export type ExampleSource =
  | { kind: "deck"; setup: string; label: string }
  | { kind: "style"; style: string; label: string };
export type LessonLinks = { scanners?: ScannerLink[]; examples?: ExampleSource[] };

const VCP_DECK: ExampleSource = { kind: "deck", setup: "vcp", label: "VCP scanner signals" };
const HTF_DECK: ExampleSource = { kind: "deck", setup: "high-tight-flag", label: "High-tight-flag signals" };

export const LESSON_LINKS: Record<string, LessonLinks> = {
  "setups-1": {
    scanners: [{ mode: "vcp", label: "VCP" }, { mode: "near-pivot", label: "Near Pivot" }],
    examples: [VCP_DECK, { kind: "style", style: "minervini", label: "Matched to Minervini setups" }],
  },
  "setups-2": { scanners: [{ mode: "contraction", label: "Contraction" }] },
  // Entries: buying inside the tight area, before the obvious breakout point.
  "entries-1": {
    scanners: [{ mode: "near-pivot", label: "Near Pivot" }, { mode: "tight-closes", label: "3 Tight Closes" }],
    examples: [VCP_DECK, { kind: "style", style: "minervini", label: "Matched to Minervini setups" }],
  },
  "stock_selection-6": { scanners: [{ mode: "pull-backs", label: "Pull Backs" }, { mode: "bread-butter", label: "Bread & Butter" }] },
  "setups-3": {
    scanners: [{ mode: "consolidating", label: "Consolidating" }, { mode: "near-pivot", label: "Near Pivot" }],
    examples: [{ kind: "style", style: "zanger_base", label: "Matched to Zanger base breakouts" }],
  },
  "setups-4": { scanners: [{ mode: "rs-line-leads", label: "RS Line Leads" }, { mode: "near-pivot", label: "Near Pivot" }] },
  "setups-6": { scanners: [{ mode: "pull-backs", label: "Pull Backs" }, { mode: "bread-butter", label: "Bread & Butter" }] },
  "setups-7": { scanners: [{ mode: "pull-backs", label: "Pull Backs" }] },
  "setups-9": { scanners: [{ mode: "gap-up-openers", label: "Gap Up Openers" }] },
  "setups-10": {
    scanners: [{ mode: "positive-earnings", label: "Positive Earnings" }, { mode: "episodic-pivot", label: "Episodic Pivot" }],
    examples: [{ kind: "style", style: "zanger_gap", label: "Matched to Zanger gap setups" }],
  },
  "setups-15": {
    scanners: [{ mode: "tight-closes", label: "3 Tight Closes" }, { mode: "high-tight-flag", label: "High Tight Flag" }],
    examples: [HTF_DECK, VCP_DECK, { kind: "style", style: "zanger_flag", label: "Matched to Zanger flags" }],
  },
  "setups-16": {
    scanners: [{ mode: "consolidating", label: "Consolidating" }, { mode: "power-base", label: "Power Base" }],
    examples: [{ kind: "style", style: "zanger_base", label: "Matched to Zanger base breakouts" }],
  },
};

export const linksFor = (lessonId: string): LessonLinks => LESSON_LINKS[lessonId] ?? {};
export const lessonsWithExamples = () => Object.keys(LESSON_LINKS).filter((id) => LESSON_LINKS[id].examples?.length);
