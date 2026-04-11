import type { AlertItem, ScanDescriptor } from "../lib/api";
import { Panel } from "./Panel";

type AlertsPanelProps = {
  alerts: AlertItem[];
  popularScans: ScanDescriptor[];
};

export function AlertsPanel({ alerts, popularScans }: AlertsPanelProps) {
  return (
    <div className="side-stack">
      <Panel title="Popular Scans" subtitle="Pinned starting points">
        <div className="tag-grid">
          {popularScans.map((scanner) => (
            <div key={scanner.id} className="tag-chip">
              <strong>{scanner.name}</strong>
              <span>{scanner.hit_count} hits</span>
            </div>
          ))}
        </div>
      </Panel>
      <Panel title="Recent Alerts" subtitle="Scanner pulse">
        <div className="alert-list">
          {alerts.length === 0 ? <div className="empty-state">No alerts yet.</div> : null}
          {alerts.map((alert) => (
            <div key={alert.id} className="alert-row">
              <strong>{alert.symbol}</strong>
              <p>{alert.message}</p>
            </div>
          ))}
        </div>
      </Panel>
    </div>
  );
}

