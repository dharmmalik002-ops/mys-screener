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

type IconTextureKey = "chart" | "money" | "moneyBag" | "notebook";
type IconTextures = Record<IconTextureKey, THREE.Texture>;

const ICON_PATHS: Record<IconTextureKey, string> = {
  chart: "/3d-icons/chart.webp",
  money: "/3d-icons/money.webp",
  moneyBag: "/3d-icons/money-bag.webp",
  notebook: "/3d-icons/notebook.webp",
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

function addIconCard(
  root: THREE.Group,
  texture: THREE.Texture,
  position: THREE.Vector3,
  scale: number,
  rotation: THREE.Euler,
  glowColor: number,
  mode: VisualMode,
) {
  const card = new THREE.Group();
  card.position.copy(position);
  card.rotation.copy(rotation);
  card.scale.setScalar(scale);
  card.userData.phase = position.x + position.y + position.z;

  const back = new THREE.Mesh(
    new THREE.BoxGeometry(1.56, 1.56, 0.1),
    material(glowColor, mode, 0.18),
  );
  back.position.z = -0.04;
  card.add(back);

  const texturePlane = new THREE.Mesh(
    new THREE.PlaneGeometry(1.36, 1.36),
    new THREE.MeshBasicMaterial({ map: texture, transparent: false, toneMapped: false, side: THREE.DoubleSide }),
  );
  texturePlane.position.z = 0.03;
  card.add(texturePlane);

  const border = new THREE.LineSegments(
    new THREE.EdgesGeometry(new THREE.BoxGeometry(1.6, 1.6, 0.12)),
    lineMaterial(glowColor, mode, 0.5),
  );
  card.add(border);

  const glow = new THREE.PointLight(glowColor, mode === "premium" ? 1.45 : 0.75, 4.2);
  glow.position.set(0, 0.1, 0.62);
  card.add(glow);
  root.add(card);
  return card;
}

function addCandles(root: THREE.Group, mode: VisualMode, count = 9, width = 4.6) {
  for (let index = 0; index < count; index += 1) {
    const bullish = index % 4 !== 2;
    const height = 0.28 + ((index * 7) % 9) * 0.09;
    const x = (index / Math.max(1, count - 1) - 0.5) * width;
    const z = -0.96 + Math.sin(index * 1.1) * 0.22;
    const color = bullish ? MARKET_3D_COLORS.strength : MARKET_3D_COLORS.weakness;
    const candle = new THREE.Group();
    candle.position.set(x, -0.72 + height / 2, z);
    candle.userData.phase = index * 0.28;

    const body = new THREE.Mesh(
      new THREE.BoxGeometry(0.18, height, 0.14),
      material(color, mode, bullish ? 0.94 : 0.86),
    );
    candle.add(body);

    const wick = new THREE.Mesh(
      new THREE.CylinderGeometry(0.014, 0.014, height + 0.34, 8),
      material(MARKET_3D_COLORS.textGlow, mode, 0.72),
    );
    candle.add(wick);
    root.add(candle);
  }
}

function addMomentumArrow(root: THREE.Group, mode: VisualMode, color = MARKET_3D_COLORS.strength) {
  const arrow = new THREE.Group();
  arrow.position.set(0.65, 0.16, -0.7);
  arrow.rotation.z = -0.38;
  arrow.userData.phase = 0.7;
  const shaft = new THREE.Mesh(
    new THREE.CylinderGeometry(0.035, 0.035, 1.8, 14),
    material(color, mode, 0.9),
  );
  shaft.rotation.z = Math.PI / 2;
  arrow.add(shaft);
  const head = new THREE.Mesh(
    new THREE.ConeGeometry(0.18, 0.38, 24),
    material(color, mode, 0.96),
  );
  head.rotation.z = -Math.PI / 2;
  head.position.x = 0.98;
  arrow.add(head);
  root.add(arrow);
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

function addTradingObjects(root: THREE.Group, variant: ThreeSceneVariant, textures: IconTextures, mode: VisualMode) {
  if (variant === "journal") {
    addIconCard(root, textures.notebook, new THREE.Vector3(1.28, 0.16, 0.08), 1.38, new THREE.Euler(-0.08, -0.32, 0.02), MARKET_3D_COLORS.insight, mode);
    addIconCard(root, textures.moneyBag, new THREE.Vector3(-1.55, -0.36, -0.36), 0.72, new THREE.Euler(0.1, 0.44, -0.06), MARKET_3D_COLORS.caution, mode);
    addMomentumArrow(root, mode, MARKET_3D_COLORS.insight);
    return;
  }

  if (variant === "watchlists") {
    addIconCard(root, textures.money, new THREE.Vector3(-1.25, 0.02, -0.06), 1.02, new THREE.Euler(-0.1, 0.42, -0.05), MARKET_3D_COLORS.liquidity, mode);
    addIconCard(root, textures.chart, new THREE.Vector3(1.45, 0.22, -0.22), 1.04, new THREE.Euler(-0.06, -0.38, 0.05), MARKET_3D_COLORS.insight, mode);
    return;
  }

  if (variant === "groups") {
    addIconCard(root, textures.chart, new THREE.Vector3(1.65, 0.32, -0.28), 1.08, new THREE.Euler(-0.1, -0.46, 0.04), MARKET_3D_COLORS.insight, mode);
    addIconCard(root, textures.moneyBag, new THREE.Vector3(-1.72, -0.2, -0.44), 0.74, new THREE.Euler(0.08, 0.5, -0.06), MARKET_3D_COLORS.caution, mode);
    addMomentumArrow(root, mode);
    return;
  }

  if (variant === "scanner") {
    addIconCard(root, textures.chart, new THREE.Vector3(1.46, 0.25, -0.24), 1.22, new THREE.Euler(-0.12, -0.42, 0.05), MARKET_3D_COLORS.insight, mode);
    addIconCard(root, textures.money, new THREE.Vector3(-1.62, -0.36, -0.42), 0.76, new THREE.Euler(0.08, 0.5, -0.08), MARKET_3D_COLORS.liquidity, mode);
    addCandles(root, mode, 8, 3.9);
    return;
  }

  addIconCard(root, textures.chart, new THREE.Vector3(1.2, 0.3, -0.22), 1.28, new THREE.Euler(-0.1, -0.42, 0.04), MARKET_3D_COLORS.insight, mode);
  addIconCard(root, textures.money, new THREE.Vector3(-1.55, -0.22, -0.34), 0.82, new THREE.Euler(0.08, 0.48, -0.08), MARKET_3D_COLORS.liquidity, mode);
  addIconCard(root, textures.moneyBag, new THREE.Vector3(-0.22, 0.55, -0.78), 0.58, new THREE.Euler(-0.08, 0.12, 0.06), MARKET_3D_COLORS.caution, mode);
  addCandles(root, mode, 9, 4.5);
  addMomentumArrow(root, mode);
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

function buildVariant(root: THREE.Group, props: ThreeSceneShellProps, mode: VisualMode, particleCount: number, textures: IconTextures) {
  const { variant, data, positiveRatio = 0.55 } = props;
  if (variant === "home") {
    addRing(root, positiveRatio, mode);
    addTowers(root, data, mode, variant);
    addTradingObjects(root, variant, textures, mode);
    addParticles(root, particleCount, mode);
  } else if (variant === "groups") {
    addTowers(root, data, mode, variant);
    addTradingObjects(root, variant, textures, mode);
    addParticles(root, Math.floor(particleCount * 0.75), mode);
  } else if (variant === "watchlists") {
    addConstellation(root, data, mode);
    addTradingObjects(root, variant, textures, mode);
    addParticles(root, Math.floor(particleCount * 0.5), mode);
  } else if (variant === "journal") {
    addRiskPath(root, data, mode);
    addTradingObjects(root, variant, textures, mode);
    addParticles(root, Math.floor(particleCount * 0.45), mode);
  } else if (variant === "chart") {
    addRing(root, 0.64, mode);
    addConstellation(root, data.slice(0, 8), mode);
    addTradingObjects(root, variant, textures, mode);
  } else {
    addSignalEngine(root, data, mode);
    addTradingObjects(root, variant, textures, mode);
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
    const textureLoader = new THREE.TextureLoader();
    const textures = Object.fromEntries(
      Object.entries(ICON_PATHS).map(([key, path]) => {
        const texture = textureLoader.load(path);
        texture.colorSpace = THREE.SRGBColorSpace;
        texture.anisotropy = 4;
        return [key, texture];
      }),
    ) as IconTextures;
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
    buildVariant(root, { variant, visualMode, data, positiveRatio, active }, visualMode, config.particles, textures);

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
          root.rotation.y = Math.sin(performance.now() * 0.00055) * 0.035;
          root.children.forEach((child, index) => {
            const phase = Number(child.userData.phase ?? index);
            if (typeof child.userData.baseY !== "number") {
              child.userData.baseY = child.position.y;
            }
            child.position.y = child.userData.baseY + Math.sin(performance.now() * 0.0015 + phase) * 0.025;
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
      Object.values(textures).forEach((texture) => texture.dispose());
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
