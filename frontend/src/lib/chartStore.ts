// Persistent chart store in IndexedDB.
//
// localStorage holds a handful of charts at most (~70 KB of JSON each against a
// ~5 MB quota), so after a reload nearly every chart open went to the network:
// ~0.3 s for a warm backend, 1-1.5 s for a symbol the Space had not cached, and
// more when the proxy dropped the request. IndexedDB takes hundreds of charts,
// so a chart the browser has ever shown opens from here instantly and is then
// refreshed in the background if it is behind.
//
// Every call fails soft: a browser without IndexedDB (private mode, blocked
// storage) simply behaves as if the store were empty.

import type { ChartResponse } from "./api";

export type StoredChart = { key: string; saved_at: string; payload: ChartResponse };

const DB_NAME = "mr-malik-charts";
const DB_VERSION = 1;
const STORE = "charts";
// Bump when ChartResponse changes shape in a way old payloads cannot survive.
const SCHEMA = 1;
const MAX_ENTRIES = 400;
const PRUNE_EVERY_PUTS = 25;
// A store read that has not answered by now is not worth waiting on — the
// network request is already in flight.
const READ_TIMEOUT_MS = 250;

type Row = StoredChart & { schema: number };

let dbPromise: Promise<IDBDatabase | null> | null = null;
let putsSincePrune = 0;

function openAt(version: number | undefined, resolve: (db: IDBDatabase | null) => void, allowRepair: boolean) {
  const request = version === undefined ? indexedDB.open(DB_NAME) : indexedDB.open(DB_NAME, version);
  request.onupgradeneeded = () => {
    const db = request.result;
    if (!db.objectStoreNames.contains(STORE)) {
      const store = db.createObjectStore(STORE, { keyPath: "key" });
      store.createIndex("saved_at", "saved_at");
    }
  };
  request.onsuccess = () => {
    const db = request.result;
    if (db.objectStoreNames.contains(STORE)) {
      resolve(db);
      return;
    }
    // A database of this name exists without the store (opened elsewhere
    // with no upgrade handler). Without a repair every read would throw and
    // the store would silently never work — reopen one version up.
    const nextVersion = db.version + 1;
    db.close();
    if (!allowRepair) {
      resolve(null);
      return;
    }
    openAt(nextVersion, resolve, false);
  };
  request.onerror = () => {
    // Already at a higher version than ours (e.g. after a repair): open it as is.
    if (version !== undefined && allowRepair) {
      openAt(undefined, resolve, true);
      return;
    }
    resolve(null);
  };
  request.onblocked = () => resolve(null);
}

function openDb(): Promise<IDBDatabase | null> {
  if (dbPromise) return dbPromise;
  dbPromise = new Promise((resolve) => {
    try {
      if (typeof indexedDB === "undefined") {
        resolve(null);
        return;
      }
      openAt(DB_VERSION, resolve, true);
    } catch {
      resolve(null);
    }
  });
  return dbPromise;
}

export async function getStoredChart(key: string): Promise<StoredChart | null> {
  const db = await openDb();
  if (!db) return null;
  return new Promise((resolve) => {
    let settled = false;
    const finish = (value: StoredChart | null) => {
      if (settled) return;
      settled = true;
      resolve(value);
    };
    const timer = setTimeout(() => finish(null), READ_TIMEOUT_MS);
    try {
      const request = db.transaction(STORE, "readonly").objectStore(STORE).get(key);
      request.onsuccess = () => {
        clearTimeout(timer);
        const row = request.result as Row | undefined;
        finish(row && row.schema === SCHEMA && row.payload ? { key: row.key, saved_at: row.saved_at, payload: row.payload } : null);
      };
      request.onerror = () => {
        clearTimeout(timer);
        finish(null);
      };
    } catch {
      clearTimeout(timer);
      finish(null);
    }
  });
}

function prune(db: IDBDatabase) {
  try {
    const store = db.transaction(STORE, "readwrite").objectStore(STORE);
    const countRequest = store.count();
    countRequest.onsuccess = () => {
      let excess = countRequest.result - MAX_ENTRIES;
      if (excess <= 0) return;
      // Oldest first along the saved_at index.
      const cursorRequest = store.index("saved_at").openCursor();
      cursorRequest.onsuccess = () => {
        const cursor = cursorRequest.result;
        if (!cursor || excess <= 0) return;
        cursor.delete();
        excess -= 1;
        cursor.continue();
      };
    };
  } catch {
    // Pruning is housekeeping; a failure only means the store stays larger.
  }
}

export function putStoredChart(key: string, payload: ChartResponse, savedAt: string = new Date().toISOString()) {
  void openDb().then((db) => {
    if (!db) return;
    try {
      const row: Row = { key, saved_at: savedAt, payload, schema: SCHEMA };
      db.transaction(STORE, "readwrite").objectStore(STORE).put(row);
      putsSincePrune += 1;
      if (putsSincePrune >= PRUNE_EVERY_PUTS) {
        putsSincePrune = 0;
        prune(db);
      }
    } catch {
      // Quota or a structured-clone failure: the chart still shows, it just
      // is not remembered across reloads.
    }
  });
}
