export type VisualMode = "premium" | "subtle" | "performance";

export const VISUAL_MODE_KEY = "mr-malik-visual-mode:v1";

export function readVisualMode(): VisualMode {
  if (typeof window === "undefined") return "premium";
  try {
    const raw = window.localStorage.getItem(VISUAL_MODE_KEY);
    if (raw === "premium" || raw === "subtle" || raw === "performance") return raw;
  } catch {
    // ignore corrupted storage
  }
  return "premium";
}

export function visualModeLabel(mode: VisualMode) {
  if (mode === "premium") return "Premium 3D";
  if (mode === "subtle") return "Subtle";
  return "Performance";
}
