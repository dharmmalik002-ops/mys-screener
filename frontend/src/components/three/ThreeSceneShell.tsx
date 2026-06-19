import { useEffect, useRef, useState } from "react";
import * as THREE from "three";

import type { VisualMode } from "../../lib/visualMode";
import { MARKET_3D_COLORS, lineMaterial, material, modeConfig } from "./MarketObjectTheme";

export type ThreeSceneVariant = "home" | "scanner" | "groups" | "watchlists" | "journal" | "chart";

export type SceneDatum = {
  label: string;
  value: number;
  change?: number;
  color?: "strength" | "weakness" | "liquidity" | "caution" | "insight";
};

type ThreeSceneShellProps = {
  variant: ThreeSceneVariant;
  visualMode: VisualMode;
  data: SceneDatum[];
  positiveRatio?: number;
  active?: boolean;
};

const COLOR_MAP = {
  strength: MARKET_3D_COLORS.strength,
  weakness: MARKET_3D_COLORS.weakness,
  liquidity: MARKET_3D_COLORS.liquidity,
  caution: MARKET_3D_COLORS.caution,
  insight: MARKET_3D_COLORS.insight,
};

function canUseWebGL() {
  try {
    const canvas = document.createElement("canvas");
    return Boolean(canvas.getContext("webgl") || canvas.getContext("experimental-webgl"));
  } catch {
    return false;
  }
}

function addFloor(scene: THREE.Scene, mode: VisualMode) {
  const grid = new THREE.GridHelper(12, 18, 0x1d4ed8, 0x1e293b);
  grid.position.y = -1.18;
  const gridMaterial = grid.material as THREE.Material | THREE.Material[];
  if (Array.isArray(gridMaterial)) {
    gridMaterial.forEach((item) => {
      item.transparent = true;
      item.opacity = mode === "premium" ? 0.26 : 0.14;
    });
  } else {
    gridMaterial.transparent = true;
    gridMaterial.opacity = mode === "premium" ? 0.26 : 0.14;
  }
  scene.add(grid);
}

function addRing(root: THREE.Group, ratio: number, mode: VisualMode) {
  const safeRatio = Math.max(0.08, Math.min(0.92, ratio || 0.5));
  const green = new THREE.Mesh(
    new THREE.TorusGeometry(1.55, 0.055, 12, 56, Math.PI * 2 * safeRatio),
    material(MARKET_3D_COLORS.strength, mode, 0.92),
  );
  green.rotation.x = Math.PI / 2;
  green.position.y = 0.42;
  root.add(green);

  const red = new THREE.Mesh(
    new THREE.TorusGeometry(1.55, 0.052, 12, 56, Math.PI * 2 * (1 - safeRatio)),
    material(MARKET_3D_COLORS.weakness, mode, 0.72),
  );
  red.rotation.x = Math.PI / 2;
  red.rotation.z = Math.PI * 2 * safeRatio;
  red.position.y = 0.42;
  root.add(red);
}

function addTowers(root: THREE.Group, data: SceneDatum[], mode: VisualMode, variant: ThreeSceneVariant) {
  const items = data.slice(0, variant === "groups" ? 10 : 8);
  const max = Math.max(1, ...items.map((item) => Math.abs(item.value)));
  const spread = variant === "groups" ? 5.6 : 4.8;
  items.forEach((item, index) => {
    const normalized = Math.max(0.12, Math.abs(item.value) / max);
    const height = 0.28 + normalized * (variant === "groups" ? 2.1 : 1.55);
    const x = items.length === 1 ? 0 : (index / (items.length - 1) - 0.5) * spread;
    const z = Math.sin(index * 1.8) * 0.62;
    const colorKey = item.color ?? ((item.change ?? item.value) >= 0 ? "strength" : "weakness");
    const mesh = new THREE.Mesh(
      new THREE.BoxGeometry(0.34, height, 0.34),
      material(COLOR_MAP[colorKey], mode, 0.84),
    );
    mesh.position.set(x, -1 + height / 2, z);
    mesh.userData.phase = index * 0.35;
    root.add(mesh);
  });
}

function addConstellation(root: THREE.Group, data: SceneDatum[], mode: VisualMode) {
  const items = data.slice(0, 14);
  const points: THREE.Vector3[] = [];
  items.forEach((item, index) => {
    const angle = (index / Math.max(1, items.length)) * Math.PI * 2;
    const radius = 1.1 + (index % 3) * 0.42;
    const y = -0.2 + (Math.abs(item.value) % 7) / 9;
    const point = new THREE.Vector3(Math.cos(angle) * radius, y, Math.sin(angle) * radius);
    points.push(point);
    const sphere = new THREE.Mesh(
      new THREE.SphereGeometry(0.09 + Math.min(0.12, Math.abs(item.value) / 120), 18, 18),
      material(COLOR_MAP[item.color ?? "liquidity"], mode, 0.9),
    );
    sphere.position.copy(point);
    sphere.userData.phase = index * 0.4;
    root.add(sphere);
  });
  if (points.length > 1) {
    const geometry = new THREE.BufferGeometry().setFromPoints(points);
    root.add(new THREE.Line(geometry, lineMaterial(MARKET_3D_COLORS.liquidity, mode, 0.42)));
  }
}

function addRiskPath(root: THREE.Group, data: SceneDatum[], mode: VisualMode) {
  const values = data.length ? data.slice(0, 12) : [{ label: "Risk", value: 0 }];
  const points = values.map((item, index) => {
    const x = (index / Math.max(1, values.length - 1) - 0.5) * 5.2;
    const y = Math.max(-0.8, Math.min(1.2, item.value / 35));
    const z = Math.sin(index * 0.9) * 0.34;
    return new THREE.Vector3(x, y, z);
  });
  root.add(new THREE.Line(new THREE.BufferGeometry().setFromPoints(points), lineMaterial(MARKET_3D_COLORS.insight, mode, 0.8)));
  points.forEach((point, index) => {
    const color = values[index]?.value >= 0 ? MARKET_3D_COLORS.strength : MARKET_3D_COLORS.weakness;
    const marker = new THREE.Mesh(new THREE.SphereGeometry(0.08, 16, 16), material(color, mode, 0.9));
    marker.position.copy(point);
    marker.userData.phase = index * 0.5;
    root.add(marker);
  });
}

function addSignalEngine(root: THREE.Group, data: SceneDatum[], mode: VisualMode) {
  const core = new THREE.Mesh(
    new THREE.CylinderGeometry(0.78, 0.78, 0.28, 48),
    material(MARKET_3D_COLORS.liquidity, mode, 0.45),
  );
  core.rotation.x = Math.PI / 2;
  core.position.y = -0.12;
  root.add(core);
  addRing(root, Math.min(0.88, Math.max(0.16, data.length / 40)), mode);
  addTowers(root, data.slice(0, 6), mode, "scanner");
}

function addParticles(root: THREE.Group, count: number, mode: VisualMode) {
  const geometry = new THREE.SphereGeometry(0.025, 8, 8);
  const mat = material(MARKET_3D_COLORS.textGlow, mode, 0.7);
  for (let i = 0; i < count; i += 1) {
    const particle = new THREE.Mesh(geometry, mat);
    particle.position.set((Math.random() - 0.5) * 6, (Math.random() - 0.5) * 1.4, (Math.random() - 0.5) * 2.6);
    particle.userData.speed = 0.002 + Math.random() * 0.004;
    particle.userData.phase = Math.random() * Math.PI * 2;
    root.add(particle);
  }
}

function buildVariant(root: THREE.Group, props: ThreeSceneShellProps, mode: VisualMode, particleCount: number) {
  const { variant, data, positiveRatio = 0.55 } = props;
  if (variant === "home") {
    addRing(root, positiveRatio, mode);
    addTowers(root, data, mode, variant);
    addParticles(root, particleCount, mode);
  } else if (variant === "groups") {
    addTowers(root, data, mode, variant);
    addParticles(root, Math.floor(particleCount * 0.75), mode);
  } else if (variant === "watchlists") {
    addConstellation(root, data, mode);
    addParticles(root, Math.floor(particleCount * 0.5), mode);
  } else if (variant === "journal") {
    addRiskPath(root, data, mode);
    addParticles(root, Math.floor(particleCount * 0.45), mode);
  } else if (variant === "chart") {
    addRing(root, 0.64, mode);
    addConstellation(root, data.slice(0, 8), mode);
  } else {
    addSignalEngine(root, data, mode);
  }
}

export function ThreeSceneShell({ variant, visualMode, data, positiveRatio, active = true }: ThreeSceneShellProps) {
  const mountRef = useRef<HTMLDivElement | null>(null);
  const [fallback, setFallback] = useState(false);

  useEffect(() => {
    const mount = mountRef.current;
    if (!mount || visualMode === "performance" || !active) return undefined;
    const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (!canUseWebGL()) {
      setFallback(true);
      return undefined;
    }
    setFallback(false);
    const config = modeConfig(visualMode, reducedMotion);
    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x07111f, 0.055);
    const camera = new THREE.PerspectiveCamera(38, 1, 0.1, 100);
    camera.position.set(0, 2.4, 6.4);
    camera.lookAt(0, 0, 0);
    const renderer = new THREE.WebGLRenderer({ antialias: visualMode === "premium", alpha: true, powerPreference: "high-performance" });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, config.dpr));
    renderer.outputColorSpace = THREE.SRGBColorSpace;
    if (config.shadows) renderer.shadowMap.enabled = false;
    mount.appendChild(renderer.domElement);

    const root = new THREE.Group();
    scene.add(root);
    addFloor(scene, visualMode);
    scene.add(new THREE.AmbientLight(0xcbd5e1, 1.35));
    const key = new THREE.DirectionalLight(0x8bd3ff, 1.6);
    key.position.set(2.5, 4, 5);
    scene.add(key);
    const fill = new THREE.PointLight(0x8b5cf6, 2.2, 12);
    fill.position.set(-3.2, 1.8, 2.5);
    scene.add(fill);
    buildVariant(root, { variant, visualMode, data, positiveRatio, active }, visualMode, config.particles);

    let raf = 0;
    let visible = true;
    const resize = () => {
      const rect = mount.getBoundingClientRect();
      const width = Math.max(1, Math.floor(rect.width));
      const height = Math.max(1, Math.floor(rect.height));
      renderer.setSize(width, height, false);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
    };
    const onVisibility = () => {
      visible = document.visibilityState === "visible";
    };
    document.addEventListener("visibilitychange", onVisibility);
    const resizeObserver = new ResizeObserver(resize);
    resizeObserver.observe(mount);
    resize();

    const animate = () => {
      if (visible) {
        if (config.animate) {
          root.rotation.y += variant === "chart" ? 0.0012 : 0.0022;
          root.children.forEach((child, index) => {
            const phase = Number(child.userData.phase ?? index);
            child.position.y += Math.sin(performance.now() * 0.0015 + phase) * 0.0008;
          });
        }
        renderer.render(scene, camera);
      }
      raf = window.requestAnimationFrame(animate);
    };
    animate();

    return () => {
      window.cancelAnimationFrame(raf);
      resizeObserver.disconnect();
      document.removeEventListener("visibilitychange", onVisibility);
      renderer.dispose();
      root.traverse((obj) => {
        const mesh = obj as THREE.Mesh;
        mesh.geometry?.dispose?.();
        const mat = mesh.material as THREE.Material | THREE.Material[] | undefined;
        if (Array.isArray(mat)) mat.forEach((item) => item.dispose());
        else mat?.dispose?.();
      });
      mount.removeChild(renderer.domElement);
    };
  }, [active, data, positiveRatio, variant, visualMode]);

  if (visualMode === "performance" || fallback || !active) {
    return (
      <div className={`three-scene-fallback three-scene-fallback-${variant}`} aria-hidden="true">
        <span />
        <span />
        <span />
      </div>
    );
  }

  return <div ref={mountRef} className="three-scene-canvas" aria-hidden="true" />;
}
