import { useEffect, useMemo, useState } from "react";

import { getWatchdogTasks, type MarketKey, type WatchdogTasksResponse } from "../lib/api";

type WatchdogTasksModalProps = {
  market: MarketKey;
  onClose: () => void;
};

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return "—";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function statusLabel(status: "done" | "scheduled" | "attention") {
  if (status === "done") {
    return "Done";
  }
  if (status === "attention") {
    return "Needs attention";
  }
  return "Scheduled";
}

export function WatchdogTasksModal({ market, onClose }: WatchdogTasksModalProps) {
  const [data, setData] = useState<WatchdogTasksResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    }
  }, []);

  useEffect(() => {
    let active = true;
    let intervalId = 0;
    let midnightTimerId = 0;

    const scheduleMidnightRefresh = (nextResetAt?: string | null) => {
      if (midnightTimerId) {
        window.clearTimeout(midnightTimerId);
      }
      if (!nextResetAt) {
        return;
      }
      const delayMs = Math.max(new Date(nextResetAt).getTime() - Date.now() + 1_000, 1_000);
      midnightTimerId = window.setTimeout(() => {
        void loadTasks(true);
      }, delayMs);
    };

    const loadTasks = async (silent = false) => {
      if (!silent) {
        setLoading(true);
      }
      try {
        const response = await getWatchdogTasks(market);
        if (!active) {
          return;
        }
        setData(response);
        setError(null);
        scheduleMidnightRefresh(response.next_reset_at);
      } catch (taskError) {
        if (!active) {
          return;
        }
        setError(taskError instanceof Error ? taskError.message : "Failed to load watchdog tasks");
      } finally {
        if (active && !silent) {
          setLoading(false);
        }
      }
    };

    void loadTasks();
    intervalId = window.setInterval(() => {
      void loadTasks(true);
    }, 60_000);

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };

    window.addEventListener("keydown", handleEscape);
    return () => {
      active = false;
      window.clearInterval(intervalId);
      window.clearTimeout(midnightTimerId);
      window.removeEventListener("keydown", handleEscape);
    };
  }, [market, onClose]);

  const counts = useMemo(() => {
    const tasks = data?.tasks ?? [];
    return {
      done: tasks.filter((task) => task.status === "done").length,
      scheduled: tasks.filter((task) => task.status === "scheduled").length,
      attention: tasks.filter((task) => task.status === "attention").length,
    };
  }, [data]);

  return (
    <div className="modal-backdrop watchdog-task-backdrop" onClick={onClose}>
      <div className="modal-box watchdog-task-modal" onClick={(event) => event.stopPropagation()} role="dialog" aria-modal="true">
        <div className="modal-header">
          <div>
            <h3>{market === "us" ? "US" : "India"} Watchdog Task Board</h3>
            <p className="watchdog-task-subtitle">
              Today&apos;s scheduled auto-heal and refresh jobs, with live status and reasons.
            </p>
          </div>
          <button type="button" className="modal-close" onClick={onClose}>
            ✕
          </button>
        </div>

        <div className="watchdog-task-body">
          <div className="watchdog-task-summary-strip">
            <span className="watchdog-task-summary-pill watchdog-task-summary-pill--done">Done {counts.done}</span>
            <span className="watchdog-task-summary-pill watchdog-task-summary-pill--scheduled">Scheduled {counts.scheduled}</span>
            <span className="watchdog-task-summary-pill watchdog-task-summary-pill--attention">Attention {counts.attention}</span>
          </div>

          <div className="watchdog-task-meta-bar">
            <span>As of {formatDateTime(data?.local_time)}</span>
            <span>Auto resets at {formatDateTime(data?.next_reset_at)}</span>
          </div>

          {loading && !data ? <div className="watchdog-task-loading">Loading today&apos;s watchdog schedule…</div> : null}
          {error ? <div className="watchdog-task-error">{error}</div> : null}

          <div className="watchdog-task-list">
            {(data?.tasks ?? []).map((task) => (
              <article key={task.id} className={`watchdog-task-card watchdog-task-card--${task.status}`}>
                <div className="watchdog-task-card-top">
                  <div>
                    <div className="watchdog-task-title-row">
                      <h4>{task.title}</h4>
                      <span className="watchdog-task-source">{task.source}</span>
                    </div>
                    <p className="watchdog-task-schedule">{task.schedule}</p>
                  </div>
                  <span className={`watchdog-task-badge watchdog-task-badge--${task.status}`}>
                    {statusLabel(task.status)}
                  </span>
                </div>

                <p className="watchdog-task-detail">{task.detail}</p>

                <div className="watchdog-task-card-footer">
                  <span>{task.done_today ? "Recorded for today" : "Waiting on today"}</span>
                  {task.last_event_at ? <span>Last event: {formatDateTime(task.last_event_at)}</span> : null}
                </div>
              </article>
            ))}
          </div>
        </div>

        <div className="modal-footer">
          <button type="button" className="nav-button" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

export default WatchdogTasksModal;
