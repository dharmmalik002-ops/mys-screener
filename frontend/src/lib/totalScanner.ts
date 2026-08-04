import type { ScreenerMode } from "../components/ScreenerSidebar";

/**
 * Total Scanner — combine the existing scanners with nested AND/OR logic.
 *
 * The tree is evaluated over SYMBOL SETS, not rows: each distinct leaf scanner
 * is run once, reduced to a Set<symbol>, and the groups intersect (AND) or
 * union (OR) those sets. That keeps evaluation independent of how any given
 * scanner shapes its rows, so adding a scanner to the catalog needs no changes
 * here.
 */

export type TotalScannerOp = "AND" | "OR";

/** Modes that `requestScannerResults` can run and that return a ScanResultsResponse. */
export type TotalScannerLeafMode = Exclude<ScreenerMode, "improving-rs" | "total-scanner">;

export type TotalScannerNode =
  | { kind: "scanner"; id: string; mode: TotalScannerLeafMode }
  | { kind: "group"; id: string; op: TotalScannerOp; children: TotalScannerNode[] };

/**
 * Selectable leaves, in sidebar order. `improving-rs` is excluded: it is served
 * by a different endpoint with a different payload shape, so it has no symbol
 * set to combine. Everything else here is dispatched by requestScannerResults.
 */
export const TOTAL_SCANNER_LEAVES: Array<{ mode: TotalScannerLeafMode; label: string }> = [
  { mode: "vcp", label: "VCP" },
  { mode: "power-base", label: "Power Base" },
  { mode: "tight-closes", label: "3 Tight Closes" },
  { mode: "bread-butter", label: "Bread & Butter" },
  { mode: "custom-scan", label: "Custom Scanner" },
  { mode: "volume", label: "Volume" },
  { mode: "ipo", label: "IPO" },
  { mode: "gap-up-openers", label: "Gap Up Openers" },
  { mode: "ema-expansion", label: "Expansion" },
  { mode: "contraction", label: "Contraction" },
  { mode: "momentum-burst", label: "Momentum Burst" },
  { mode: "positive-earnings", label: "Positive Earnings" },
  { mode: "minervini-1m", label: "Minervini 1 Month" },
  { mode: "minervini-5m", label: "Minervini 5 Months" },
  { mode: "episodic-pivot", label: "Episodic Pivot" },
  { mode: "rs-line-leads", label: "RS Line Leads" },
  { mode: "fresh-stage2", label: "Fresh Stage 2" },
  { mode: "high-tight-flag", label: "High Tight Flag" },
  { mode: "consolidating", label: "Consolidating" },
  { mode: "near-pivot", label: "Near Pivot" },
  { mode: "pull-backs", label: "Pull Backs" },
  { mode: "returns", label: "Returns" },
  { mode: "demand-zone", label: "Demand Zone" },
];

const LEAF_LABELS = new Map(TOTAL_SCANNER_LEAVES.map((leaf) => [leaf.mode, leaf.label]));
const LEAF_MODES = new Set<string>(TOTAL_SCANNER_LEAVES.map((leaf) => leaf.mode));

export function totalScannerLeafLabel(mode: TotalScannerLeafMode): string {
  return LEAF_LABELS.get(mode) ?? mode;
}

let nodeSeq = 0;
export function createNodeId(): string {
  nodeSeq += 1;
  return `tsn-${Date.now().toString(36)}-${nodeSeq}`;
}

export function createScannerNode(mode: TotalScannerLeafMode): TotalScannerNode {
  return { kind: "scanner", id: createNodeId(), mode };
}

export function createGroupNode(op: TotalScannerOp = "AND", children: TotalScannerNode[] = []): TotalScannerNode {
  return { kind: "group", id: createNodeId(), op, children };
}

/** A fresh tree: one AND group holding the two most-used setups. */
export function defaultTotalScannerTree(): TotalScannerNode {
  return createGroupNode("AND", [createScannerNode("vcp"), createScannerNode("power-base")]);
}

/** Every distinct leaf mode in the tree — what actually needs fetching. */
export function collectLeafModes(node: TotalScannerNode): TotalScannerLeafMode[] {
  const seen = new Set<TotalScannerLeafMode>();
  const walk = (n: TotalScannerNode) => {
    if (n.kind === "scanner") {
      seen.add(n.mode);
      return;
    }
    n.children.forEach(walk);
  };
  walk(node);
  return [...seen];
}

export function countLeaves(node: TotalScannerNode): number {
  return node.kind === "scanner" ? 1 : node.children.reduce((sum, child) => sum + countLeaves(child), 0);
}

/**
 * Evaluate the tree against per-scanner symbol sets.
 *
 * An empty group yields an empty set in BOTH modes — an AND over nothing is
 * vacuously "everything", which would dump the whole universe on screen, and
 * that is never what an empty builder should mean.
 */
export function evaluateTotalScanner(
  node: TotalScannerNode,
  sets: Map<TotalScannerLeafMode, Set<string>>,
): Set<string> {
  if (node.kind === "scanner") {
    return new Set(sets.get(node.mode) ?? []);
  }
  if (node.children.length === 0) {
    return new Set<string>();
  }
  const childSets = node.children.map((child) => evaluateTotalScanner(child, sets));
  if (node.op === "OR") {
    const union = new Set<string>();
    for (const set of childSets) for (const symbol of set) union.add(symbol);
    return union;
  }
  // AND — intersect, smallest set first so the loop stays cheap.
  childSets.sort((a, b) => a.size - b.size);
  const [first, ...rest] = childSets;
  const result = new Set<string>();
  for (const symbol of first) {
    if (rest.every((set) => set.has(symbol))) result.add(symbol);
  }
  return result;
}

/** Human-readable summary, e.g. "(VCP AND Power Base) OR IPO". */
export function describeTotalScanner(node: TotalScannerNode, top = true): string {
  if (node.kind === "scanner") return totalScannerLeafLabel(node.mode);
  if (node.children.length === 0) return "empty";
  const inner = node.children.map((child) => describeTotalScanner(child, false)).join(` ${node.op} `);
  return top || node.children.length === 1 ? inner : `(${inner})`;
}

/* ---------------- tree editing (immutable) ---------------- */

export function updateNode(
  root: TotalScannerNode,
  id: string,
  updater: (node: TotalScannerNode) => TotalScannerNode,
): TotalScannerNode {
  if (root.id === id) return updater(root);
  if (root.kind !== "group") return root;
  return { ...root, children: root.children.map((child) => updateNode(child, id, updater)) };
}

/** Remove a node by id. The root is never removed. */
export function removeNode(root: TotalScannerNode, id: string): TotalScannerNode {
  if (root.kind !== "group") return root;
  return {
    ...root,
    children: root.children.filter((child) => child.id !== id).map((child) => removeNode(child, id)),
  };
}

export function appendChild(root: TotalScannerNode, groupId: string, child: TotalScannerNode): TotalScannerNode {
  return updateNode(root, groupId, (node) =>
    node.kind === "group" ? { ...node, children: [...node.children, child] } : node,
  );
}

export function setGroupOp(root: TotalScannerNode, groupId: string, op: TotalScannerOp): TotalScannerNode {
  return updateNode(root, groupId, (node) => (node.kind === "group" ? { ...node, op } : node));
}

export function setLeafMode(root: TotalScannerNode, leafId: string, mode: TotalScannerLeafMode): TotalScannerNode {
  return updateNode(root, leafId, (node) => (node.kind === "scanner" ? { ...node, mode } : node));
}

/* ---------------- persistence ---------------- */

/**
 * Rebuild a tree from untrusted JSON (localStorage or a saved preset). Unknown
 * scanner modes are dropped rather than kept, so a leaf removed from the
 * catalog can't silently evaluate to "no matches" and skew an AND.
 */
export function normalizeTotalScannerTree(value: unknown): TotalScannerNode | null {
  const walk = (raw: unknown): TotalScannerNode | null => {
    if (!raw || typeof raw !== "object") return null;
    const node = raw as Record<string, unknown>;
    if (node.kind === "scanner") {
      const mode = typeof node.mode === "string" && LEAF_MODES.has(node.mode) ? (node.mode as TotalScannerLeafMode) : null;
      return mode ? { kind: "scanner", id: typeof node.id === "string" ? node.id : createNodeId(), mode } : null;
    }
    if (node.kind === "group") {
      const children = Array.isArray(node.children)
        ? node.children.map(walk).filter((child): child is TotalScannerNode => child !== null)
        : [];
      return {
        kind: "group",
        id: typeof node.id === "string" ? node.id : createNodeId(),
        op: node.op === "OR" ? "OR" : "AND",
        children,
      };
    }
    return null;
  };
  const parsed = walk(value);
  if (!parsed) return null;
  // The root must be a group so the UI always has something to add into.
  return parsed.kind === "group" ? parsed : createGroupNode("AND", [parsed]);
}
