import * as THREE from "three";

import type { VisualMode } from "../../lib/visualMode";

export const MARKET_3D_COLORS = {
  strength: 0x22c55e,
  weakness: 0xef4444,
  liquidity: 0x38bdf8,
  caution: 0xf59e0b,
  insight: 0x8b5cf6,
  surface: 0x0f172a,
  line: 0x64748b,
  textGlow: 0xe2e8f0,
};

export function material(color: number, mode: VisualMode, opacity = 0.84) {
  return new THREE.MeshStandardMaterial({
    color,
    metalness: mode === "performance" ? 0.25 : 0.62,
    roughness: mode === "performance" ? 0.5 : 0.26,
    transparent: opacity < 1,
    opacity,
    emissive: color,
    emissiveIntensity: mode === "premium" ? 0.22 : mode === "subtle" ? 0.1 : 0.03,
  });
}

export function lineMaterial(color: number, mode: VisualMode, opacity = 0.62) {
  return new THREE.LineBasicMaterial({
    color,
    transparent: true,
    opacity: mode === "performance" ? Math.min(opacity, 0.35) : opacity,
  });
}

export function modeConfig(mode: VisualMode, reducedMotion: boolean) {
  if (mode === "performance") {
    return { dpr: 1, particles: 8, animate: !reducedMotion, shadows: false };
  }
  if (mode === "subtle") {
    return { dpr: 1.25, particles: 14, animate: !reducedMotion, shadows: false };
  }
  return { dpr: 1.6, particles: 26, animate: !reducedMotion, shadows: true };
}
