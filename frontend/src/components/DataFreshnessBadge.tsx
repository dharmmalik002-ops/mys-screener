import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Database } from "lucide-react";
import { getDataFreshness, type DataFreshnessReport } from "../lib/api";
import { behindText, summarizeFreshness } from "../lib/dataFreshness";
import { longDate } from "../lib/marketsBrief";
import "./DataFreshnessBadge.css";

// Feeds move at most once an evening; a quarter-hour recheck is plenty.
const RECHECK_MS = 15 * 60 * 1000;

/**
 * Header badge: one dot that turns amber or red when any committed data feed
 * (prices, breadth, XP score, breakout replay, bot, look-alikes, funds) falls
 * behind the session it should describe. Click for the per-feed list.
 *
 * The list portals to <body> (gotcha 17) and is positioned from the button's
 * rect, so the header's overflow can never clip it.
 */
export function DataFreshnessBadge() {
  const [report, setReport] = useState<DataFreshnessReport | null>(null);
  const [failed, setFailed] = useState(false);
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState<{ top: number; right: number } | null>(null);
  const buttonRef = useRef<HTMLButtonElement | null>(null);
  const panelRef = useRef<HTMLDivElement | null>(null);

  const load = useCallback(() => {
    getDataFreshness()
      .then((resp) => {
        setReport(resp);
        setFailed(false);
      })
      .catch(() => setFailed(true));
  }, []);

  useEffect(() => {
    load();
    const id = window.setInterval(load, RECHECK_MS);
    const onVisible = () => {
      if (document.visibilityState === "visible") load();
    };
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      window.clearInterval(id);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [load]);

  const place = useCallback(() => {
    const rect = buttonRef.current?.getBoundingClientRect();
    if (!rect) return;
    setPos({ top: rect.bottom + 8, right: Math.max(8, window.innerWidth - rect.right) });
  }, []);

  useLayoutEffect(() => {
    if (!open) return;
    place();
    window.addEventListener("resize", place);
    window.addEventListener("scroll", place, true);
    return () => {
      window.removeEventListener("resize", place);
      window.removeEventListener("scroll", place, true);
    };
  }, [open, place]);

  useEffect(() => {
    if (!open) return;
    const onDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (panelRef.current?.contains(target) || buttonRef.current?.contains(target)) return;
      setOpen(false);
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setOpen(false);
        buttonRef.current?.focus();
      }
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const summary = summarizeFreshness(report);
  const tone = failed && !report ? "idle" : summary.tone;
  const label = failed && !report ? "Could not check data freshness" : summary.label;

  return (
    <>
      <button
        ref={buttonRef}
        type="button"
        className={`icon-btn data-fresh-btn tone-${tone}`}
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        aria-haspopup="dialog"
        aria-label={`Data freshness: ${label}`}
        title={label}
      >
        <Database size={15} strokeWidth={2.2} aria-hidden="true" />
        <span className="data-fresh-dot" aria-hidden="true" />
      </button>
      {open && pos
        ? createPortal(
            <div
              ref={panelRef}
              className="data-fresh-panel"
              role="dialog"
              aria-label="Data freshness"
              style={{ top: pos.top, right: pos.right }}
            >
              <div className="data-fresh-head">
                <strong>{label}</strong>
                {report?.expected_session ? (
                  <span>Latest session expected: {longDate(report.expected_session)}</span>
                ) : null}
              </div>
              {report ? (
                <ul className="data-fresh-list">
                  {report.feeds.map((feed) => (
                    <li key={feed.key} className={`status-${feed.status}`}>
                      <span className="data-fresh-dot" aria-hidden="true" />
                      <span className="data-fresh-name">
                        {feed.label}
                        <small>{feed.used_by}</small>
                      </span>
                      <span className="data-fresh-when">
                        {feed.as_of ? longDate(feed.as_of) : "—"}
                        <small>{behindText(feed)}</small>
                      </span>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="data-fresh-empty">
                  {failed ? "The server did not answer." : "Checking…"}{" "}
                  <button type="button" onClick={load}>
                    Check again
                  </button>
                </p>
              )}
              <p className="data-fresh-foot">
                Each feed is dated by the market session it describes. One session behind is normal on an exchange
                holiday or early in the evening.
              </p>
            </div>,
            document.body,
          )
        : null}
    </>
  );
}
