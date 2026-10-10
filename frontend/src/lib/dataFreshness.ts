/**
 * What the header's data badge says, from /api/data-freshness.
 *
 * The badge exists because this site's outages were quiet: a feed stopped
 * updating and the page kept rendering last week's numbers as if they were
 * today's. Rules:
 *
 *  - The badge takes the WORST feed's status. One stale feed is worth a glance.
 *  - "unknown" (unreadable file, no date) is shown as a problem, not hidden.
 *  - No report at all (the request failed) is its own state, never "ok".
 */
import type { DataFreshnessReport, FreshnessFeed, FreshnessStatus } from "./api";

export type BadgeTone = "ok" | "warn" | "bad" | "idle";

export type BadgeSummary = {
  tone: BadgeTone;
  /** Short text for the button's accessible name and tooltip. */
  label: string;
  /** Feeds that are not "ok", worst first. */
  problems: FreshnessFeed[];
};

const RANK: Record<FreshnessStatus, number> = { ok: 0, late: 1, stale: 2, unknown: 3 };

export function badgeTone(status: FreshnessStatus): BadgeTone {
  if (status === "ok") return "ok";
  if (status === "late") return "warn";
  return "bad";
}

export function behindText(feed: FreshnessFeed): string {
  if (feed.status === "unknown") return feed.error ? `No date (${feed.error})` : "No date";
  const n = feed.sessions_behind ?? 0;
  if (n <= 0) return "Current";
  return `${n} session${n === 1 ? "" : "s"} behind`;
}

export function summarizeFreshness(report: DataFreshnessReport | null): BadgeSummary {
  if (!report) return { tone: "idle", label: "Data freshness not checked yet", problems: [] };
  const problems = report.feeds
    .filter((feed) => feed.status !== "ok")
    .sort((a, b) => RANK[b.status] - RANK[a.status] || (b.sessions_behind ?? 0) - (a.sessions_behind ?? 0));
  if (!report.feeds.length) return { tone: "bad", label: "No data feeds reported", problems };
  const worst = report.feeds.reduce<FreshnessStatus>((acc, feed) => (RANK[feed.status] > RANK[acc] ? feed.status : acc), "ok");
  if (worst === "ok") return { tone: "ok", label: "All data is current", problems };
  const names = problems.map((feed) => feed.label);
  const lead = names.length === 1 ? names[0] : `${names.length} feeds`;
  return { tone: badgeTone(worst), label: `${lead} ${names.length === 1 ? "is" : "are"} behind`, problems };
}
