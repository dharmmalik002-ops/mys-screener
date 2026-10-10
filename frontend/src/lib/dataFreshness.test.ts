import { describe, expect, it } from "vitest";
import { normalizeDataFreshness, type FreshnessFeed } from "./api";
import { behindText, summarizeFreshness } from "./dataFreshness";

const feed = (key: string, status: FreshnessFeed["status"], behind: number | null = 0): FreshnessFeed => ({
  key,
  label: key.toUpperCase(),
  used_by: "",
  as_of: behind === null ? null : "2026-10-09",
  sessions_behind: behind,
  status,
  error: behind === null ? "file not found" : null,
});

describe("summarizeFreshness", () => {
  it("is idle, never ok, before a report arrives", () => {
    expect(summarizeFreshness(null).tone).toBe("idle");
  });

  it("is ok only when every feed is ok", () => {
    const s = summarizeFreshness({ expected_session: null, checked_at: null, status: "ok", feeds: [feed("a", "ok"), feed("b", "ok")] });
    expect(s.tone).toBe("ok");
    expect(s.problems).toEqual([]);
  });

  it("takes the worst feed and lists problems worst first", () => {
    const s = summarizeFreshness({
      expected_session: null,
      checked_at: null,
      status: "stale",
      feeds: [feed("a", "ok"), feed("late", "late", 3), feed("stale", "stale", 5)],
    });
    expect(s.tone).toBe("bad");
    expect(s.problems.map((f) => f.key)).toEqual(["stale", "late"]);
    expect(s.label).toBe("2 feeds are behind");
  });

  it("names a single lagging feed", () => {
    const s = summarizeFreshness({ expected_session: null, checked_at: null, status: "late", feeds: [feed("a", "ok"), feed("picks", "late", 3)] });
    expect(s.tone).toBe("warn");
    expect(s.label).toBe("PICKS is behind");
  });

  it("treats an empty feed list as a problem", () => {
    expect(summarizeFreshness({ expected_session: null, checked_at: null, status: "unknown", feeds: [] }).tone).toBe("bad");
  });
});

describe("behindText", () => {
  it("reads in sessions, and explains a missing date", () => {
    expect(behindText(feed("a", "ok", 0))).toBe("Current");
    expect(behindText(feed("a", "ok", 1))).toBe("1 session behind");
    expect(behindText(feed("a", "stale", 5))).toBe("5 sessions behind");
    expect(behindText(feed("a", "unknown", null))).toBe("No date (file not found)");
  });
});

describe("normalizeDataFreshness", () => {
  it("survives a malformed payload", () => {
    const r = normalizeDataFreshness({ status: "weird", feeds: [{ key: "x", status: "late", sessions_behind: "2" }, null, { label: "no key" }] });
    expect(r.status).toBe("unknown");
    expect(r.feeds).toHaveLength(1);
    expect(r.feeds[0].status).toBe("late");
    expect(normalizeDataFreshness(null).feeds).toEqual([]);
  });
});
