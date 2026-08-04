import { Layers, Play, Plus, Trash2 } from "lucide-react";

import {
  TOTAL_SCANNER_LEAVES,
  appendChild,
  countLeaves,
  createGroupNode,
  createScannerNode,
  describeTotalScanner,
  removeNode,
  setGroupOp,
  setLeafMode,
  type TotalScannerLeafMode,
  type TotalScannerNode,
  type TotalScannerOp,
} from "../lib/totalScanner";

import "./TotalScannerPanel.css";

type TotalScannerPanelProps = {
  tree: TotalScannerNode;
  onTreeChange: (next: TotalScannerNode) => void;
  onRun: () => void;
  loading: boolean;
  matchCount: number | null;
  error: string | null;
};

const MAX_DEPTH = 3;

function GroupEditor({
  node,
  depth,
  isRoot,
  onChange,
  onRemove,
}: {
  node: Extract<TotalScannerNode, { kind: "group" }>;
  depth: number;
  isRoot: boolean;
  onChange: (next: TotalScannerNode) => void;
  onRemove?: () => void;
}) {
  const setOp = (op: TotalScannerOp) => onChange(setGroupOp(node, node.id, op));

  return (
    <div className={`ts-group ts-group-depth-${Math.min(depth, MAX_DEPTH)}`}>
      <div className="ts-group-head">
        <div className="ts-op-switch" role="group" aria-label="Match logic">
          {(["AND", "OR"] as const).map((op) => (
            <button
              key={op}
              type="button"
              className={node.op === op ? "ts-op-pill active" : "ts-op-pill"}
              onClick={() => setOp(op)}
              title={
                op === "AND"
                  ? "AND — a stock must appear in every item in this group"
                  : "OR — a stock qualifies if it appears in any item in this group"
              }
            >
              {op}
            </button>
          ))}
        </div>
        <span className="ts-group-caption">
          {node.op === "AND" ? "match ALL of" : "match ANY of"}
          {node.children.length > 0 ? ` · ${node.children.length}` : null}
        </span>
        {!isRoot && onRemove ? (
          <button type="button" className="ts-icon-btn" onClick={onRemove} title="Remove this group" aria-label="Remove group">
            <Trash2 size={13} strokeWidth={2.1} />
          </button>
        ) : null}
      </div>

      {node.children.length === 0 ? (
        <p className="ts-empty">Add a scanner to this group — an empty group matches nothing.</p>
      ) : (
        <div className="ts-children">
          {node.children.map((child) =>
            child.kind === "group" ? (
              <GroupEditor
                key={child.id}
                node={child}
                depth={depth + 1}
                isRoot={false}
                onChange={onChange}
                onRemove={() => onChange(removeNode(node, child.id))}
              />
            ) : (
              <div className="ts-leaf" key={child.id}>
                <select
                  className="ts-leaf-select"
                  value={child.mode}
                  onChange={(event) => onChange(setLeafMode(node, child.id, event.target.value as TotalScannerLeafMode))}
                  aria-label="Scanner"
                >
                  {TOTAL_SCANNER_LEAVES.map((leaf) => (
                    <option key={leaf.mode} value={leaf.mode}>
                      {leaf.label}
                    </option>
                  ))}
                </select>
                <button
                  type="button"
                  className="ts-icon-btn"
                  onClick={() => onChange(removeNode(node, child.id))}
                  title="Remove this scanner"
                  aria-label="Remove scanner"
                >
                  <Trash2 size={13} strokeWidth={2.1} />
                </button>
              </div>
            ),
          )}
        </div>
      )}

      <div className="ts-group-actions">
        <button
          type="button"
          className="ts-add-btn"
          onClick={() => onChange(appendChild(node, node.id, createScannerNode("vcp")))}
        >
          <Plus size={13} strokeWidth={2.4} /> Add scanner
        </button>
        {depth < MAX_DEPTH ? (
          <button
            type="button"
            className="ts-add-btn ghost"
            onClick={() =>
              onChange(appendChild(node, node.id, createGroupNode(node.op === "AND" ? "OR" : "AND", [])))
            }
            title="A nested group lets you mix logic, e.g. (VCP AND Power Base) OR IPO"
          >
            <Plus size={13} strokeWidth={2.4} /> Add group
          </button>
        ) : null}
      </div>
    </div>
  );
}

export function TotalScannerPanel({
  tree,
  onTreeChange,
  onRun,
  loading,
  matchCount,
  error,
}: TotalScannerPanelProps) {
  const leaves = countLeaves(tree);
  const summary = describeTotalScanner(tree);
  const runnable = leaves > 0 && !loading;

  return (
    <section className="ts-root">
      <header className="ts-head">
        <div className="ts-title-wrap">
          <h3>
            <Layers size={15} strokeWidth={2.2} /> Total Scanner
          </h3>
          <p className="ts-sub">
            Combine scanners with AND / OR. Nest a group to mix logic, e.g. (VCP AND Power Base) OR IPO.
          </p>
        </div>
        <button type="button" className="ts-run" onClick={onRun} disabled={!runnable}>
          <Play size={13} strokeWidth={2.4} />
          {loading ? "Running…" : "Run Total Scan"}
        </button>
      </header>

      <div className="ts-formula" title="The combination that will be run">
        <span className="ts-formula-label">Formula</span>
        <code>{leaves === 0 ? "—" : summary}</code>
      </div>

      <GroupEditor
        node={tree as Extract<TotalScannerNode, { kind: "group" }>}
        depth={0}
        isRoot
        onChange={onTreeChange}
      />

      {error ? <p className="ts-error">{error}</p> : null}
      {matchCount !== null && !error ? (
        <p className="ts-result-note">
          {matchCount === 0
            ? "No stocks satisfy this combination."
            : `${matchCount} stock${matchCount === 1 ? "" : "s"} matched — each row lists the scanners it came from.`}
        </p>
      ) : null}
      {leaves > 6 ? (
        <p className="ts-hint">Running {leaves} scanners — this can take a few seconds on the first run.</p>
      ) : null}
    </section>
  );
}
