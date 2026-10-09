import { useCallback, useEffect, useRef, useState } from "react";
import { getCourseProgress, saveCourseProgress, type CourseProgress } from "../../lib/api";
import { readJson, writeJson } from "./courseData";

/* The learner's progress (studied lessons, notes, flashcard schedule, drill
   scores), kept on the server so it follows them between devices, with this
   browser's copy as a cache for first paint and for working offline.

   The rule that matters is CLAUDE.md gotcha 128's: never push before the
   server copy has been read. A fresh browser starts empty, and pushing that
   would wipe the record. So edits made before the first read are kept
   locally and marked dirty, and the first read merges them in:
   - not dirty: the server copy wins (this browser has nothing newer);
   - dirty: union of both — studied lessons from either, this browser's notes
     and flashcards over the server's, the higher drill count.
   The server additionally refuses a save that would drop more than a few
   items (409), which the status line reports rather than retrying. */

const KEYS = {
  done: "mr-malik-course-done:v1",
  notes: "mr-malik-course-notes:v1",
  cards: "mr-malik-course-cards:v1",
  scores: "mr-malik-course-scores:v1",
  dirty: "mr-malik-course-dirty:v1",
} as const;

export type Progress = Omit<CourseProgress, "updated_at">;
export type SyncStatus = "loading" | "synced" | "saving" | "local" | "conflict";

const EMPTY: Progress = { done: {}, notes: {}, cards: {}, scores: {} };
const SAVE_DELAY_MS = 1200;

function readLocal(): Progress {
  return {
    done: readJson(KEYS.done, {}),
    notes: readJson(KEYS.notes, {}),
    cards: readJson(KEYS.cards, {}),
    scores: readJson(KEYS.scores, {}),
  };
}

function writeLocal(p: Progress, dirty: boolean) {
  writeJson(KEYS.done, p.done);
  writeJson(KEYS.notes, p.notes);
  writeJson(KEYS.cards, p.cards);
  writeJson(KEYS.scores, p.scores);
  writeJson(KEYS.dirty, dirty);
}

const isEmpty = (p: Progress) => !Object.keys(p.done).length && !Object.keys(p.notes).length && !Object.keys(p.cards).length && !Object.keys(p.scores).length;

export function mergeProgress(server: Progress, local: Progress): Progress {
  const scores: Progress["scores"] = { ...server.scores };
  for (const [k, v] of Object.entries(local.scores)) {
    const s = scores[k];
    scores[k] = !s || v.total > s.total ? v : s;
  }
  return {
    done: { ...server.done, ...local.done },
    notes: { ...server.notes, ...local.notes },
    cards: { ...server.cards, ...local.cards },
    scores,
  };
}

export function useCourseProgress() {
  const [progress, setProgress] = useState<Progress>(() => ({ ...EMPTY, ...readLocal() }));
  const [status, setStatus] = useState<SyncStatus>("loading");
  const loaded = useRef(false);
  const timer = useRef<number | null>(null);
  const latest = useRef(progress);
  latest.current = progress;

  const push = useCallback(async (p: Progress) => {
    setStatus("saving");
    try {
      await saveCourseProgress(p);
      writeJson(KEYS.dirty, false);
      setStatus("synced");
    } catch (e) {
      const conflict = e instanceof Error && /409|drop \d+ saved/.test(e.message);
      setStatus(conflict ? "conflict" : "local");
    }
  }, []);

  useEffect(() => {
    let live = true;
    getCourseProgress()
      .then((server) => {
        if (!live) return;
        const remote: Progress = {
          done: server?.done ?? {},
          notes: server?.notes ?? {},
          cards: server?.cards ?? {},
          scores: server?.scores ?? {},
        };
        const local = latest.current;
        const dirty = readJson<boolean>(KEYS.dirty, false);
        const merged = dirty || isEmpty(remote) ? mergeProgress(remote, local) : remote;
        loaded.current = true;
        setProgress(merged);
        const needsPush = JSON.stringify(merged) !== JSON.stringify(remote);
        writeLocal(merged, needsPush);
        if (needsPush) void push(merged);
        else setStatus("synced");
      })
      .catch(() => live && setStatus("local"));
    return () => {
      live = false;
    };
  }, [push]);

  const update = useCallback(
    (fn: (prev: Progress) => Progress) => {
      setProgress((prev) => {
        const next = fn(prev);
        if (next === prev) return prev;
        writeLocal(next, true);
        if (loaded.current) {
          if (timer.current) window.clearTimeout(timer.current);
          timer.current = window.setTimeout(() => void push(latest.current), SAVE_DELAY_MS);
        }
        return next;
      });
    },
    [push],
  );

  // Flush a pending save when the tab is hidden or closed.
  useEffect(() => {
    const flush = () => {
      if (timer.current && loaded.current) {
        window.clearTimeout(timer.current);
        timer.current = null;
        void push(latest.current);
      }
    };
    const onHide = () => document.visibilityState === "hidden" && flush();
    document.addEventListener("visibilitychange", onHide);
    window.addEventListener("pagehide", flush);
    return () => {
      document.removeEventListener("visibilitychange", onHide);
      window.removeEventListener("pagehide", flush);
      flush();
    };
  }, [push]);

  return { progress, update, status };
}

export const STATUS_TEXT: Record<SyncStatus, string> = {
  loading: "Loading your progress…",
  synced: "Progress saved to your account",
  saving: "Saving…",
  local: "Saved in this browser only (server unreachable)",
  conflict: "Not saved: the server holds more progress. Reload the page.",
};
