"""👍 / 👎 on similar charts: stored, applied at once, learned from nightly.

**Stored.** One vote per (stock, session, match): voting again replaces it and
a vote of 0 removes it. Postgres when DATABASE_URL is set (the same durable
store as the trade journal — the Space's own disk does not survive a restart),
a JSON file in APP_STATE_DIR otherwise. The evening run also commits a copy
(`data/lookalike_feedback_votes.json`) and the Space merges it back on start,
so a restart without a database loses nothing.

**Applied at once.** A 👎 hides that match for that stock from then on
(`hidden_for`); a 👍 is shown as confirmed. No model needed for either.

**Learned nightly.** Each vote becomes a training example: how alike the pair
looked, and how far apart they were on each measured shape feature. A small
logistic model learns which of those gaps make *you* say "not similar", and
its weights replace the equal weights of the shape check — but only once there
are `MIN_EACH` votes of each kind and the learned weights predict votes they
were not fitted on better than the current ranking does (`learn`). Until then
it reports what it is waiting for.
"""

from __future__ import annotations

import json
import threading
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

from . import model, shape

try:
    import psycopg
except Exception:  # pragma: no cover - optional in some deployments
    psycopg = None

VOTES_FILE = "lookalike_feedback.json"          # APP_STATE_DIR fallback
BACKUP_FILE = "lookalike_feedback_votes.json"   # committed copy, under data/
MODEL_FILE = "feedback_model.json"              # under data/lookalike_state/
KINDS = ("ref", "peer")
MIN_EACH = 20
MIN_GAIN = 0.03
FOLDS = 5
HIDE_DAYS = 60


def vote_id(query: str, session: str, kind: str, target: str) -> str:
    return f"{query}|{session}|{kind}|{target}"


class FeedbackStore:
    def __init__(self, database_url: str | None, state_dir: Path | None, backup: Path | None = None):
        self._db = str(database_url or "").strip() or None
        self._file = (state_dir / VOTES_FILE) if state_dir else None
        self._backup = backup
        self._lock = threading.Lock()
        self._schema = False
        self._restored = False

    # ── storage ───────────────────────────────────────────────────────────
    def _use_db(self) -> bool:
        return bool(self._db) and psycopg is not None

    def _connect(self):
        return psycopg.connect(self._db, autocommit=True, connect_timeout=10)

    def _ensure(self, cur) -> None:
        if not self._schema:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS lookalike_feedback (
                       id TEXT PRIMARY KEY, payload JSONB NOT NULL,
                       updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"""
            )
            self._schema = True

    def _read_file(self) -> dict[str, dict]:
        if self._file is None or not self._file.exists():
            return {}
        try:
            return json.loads(self._file.read_text())
        except (OSError, ValueError):
            return {}

    def _write_file(self, votes: dict[str, dict]) -> None:
        if self._file is None:
            return
        self._file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text(json.dumps(votes, separators=(",", ":")))
        tmp.replace(self._file)

    def _restore_backup_once(self) -> None:
        """After a restart without a database the file store is empty; merge
        the committed copy back so no vote is lost."""
        if self._restored or self._backup is None or not self._backup.exists():
            self._restored = True
            return
        self._restored = True
        try:
            saved = json.loads(self._backup.read_text()).get("votes", [])
        except (OSError, ValueError):
            return
        current = self.all()
        have = {v["id"]: v for v in current}
        for v in saved:
            if v.get("id") not in have:
                self._put(v)

    def all(self) -> list[dict]:
        if self._use_db():
            with self._connect() as conn, conn.cursor() as cur:
                self._ensure(cur)
                cur.execute("SELECT payload FROM lookalike_feedback")
                return [r[0] if isinstance(r[0], dict) else json.loads(str(r[0])) for r in cur.fetchall()]
        return list(self._read_file().values())

    def _put(self, v: dict) -> None:
        if self._use_db():
            with self._connect() as conn, conn.cursor() as cur:
                self._ensure(cur)
                cur.execute(
                    """INSERT INTO lookalike_feedback (id, payload, updated_at) VALUES (%s, %s::jsonb, NOW())
                       ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()""",
                    (v["id"], json.dumps(v)),
                )
            return
        with self._lock:
            votes = self._read_file()
            votes[v["id"]] = v
            self._write_file(votes)

    def _delete(self, vid: str) -> None:
        if self._use_db():
            with self._connect() as conn, conn.cursor() as cur:
                self._ensure(cur)
                cur.execute("DELETE FROM lookalike_feedback WHERE id = %s", (vid,))
            return
        with self._lock:
            votes = self._read_file()
            votes.pop(vid, None)
            self._write_file(votes)

    # ── API ───────────────────────────────────────────────────────────────
    def record(self, query: str, session: str, kind: str, target: str, vote: int, style: str | None = None) -> dict:
        self._restore_backup_once()
        query, target = query.strip().upper(), target.strip()
        if kind not in KINDS or not query or not target or vote not in (-1, 0, 1):
            raise ValueError("bad vote")
        date.fromisoformat(session)  # raises on a malformed date
        vid = vote_id(query, session, kind, target)
        if vote == 0:
            self._delete(vid)
        else:
            self._put({
                "id": vid, "query": query, "session": session, "kind": kind, "target": target,
                "vote": vote, "style": style, "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            })
        return {"id": vid, "vote": vote}

    def for_symbol(self, query: str) -> list[dict]:
        self._restore_backup_once()
        q = query.strip().upper()
        return [v for v in self.all() if v.get("query") == q]

    def counts(self) -> dict:
        self._restore_backup_once()
        votes = self.all()
        return {"up": sum(v["vote"] > 0 for v in votes), "down": sum(v["vote"] < 0 for v in votes)}


def hidden_for(votes: list[dict], query: str, today: date | None = None) -> set[tuple[str, str]]:
    """(kind, target) pairs this stock's 👎 votes hide — recent ones only: a
    chart changes, and a match rejected months ago may be right now."""
    today = today or date.today()
    out = set()
    for v in votes:
        if v.get("query") == query and v.get("vote", 0) < 0:
            try:
                if (today - date.fromisoformat(v["session"])).days <= HIDE_DAYS:
                    out.add((v["kind"], v["target"]))
            except (KeyError, ValueError):
                continue
    return out


# ── nightly learning ──────────────────────────────────────────────────────

def _cv_auc(X: np.ndarray, y: np.ndarray, seed: int = 0) -> float | None:
    rng = np.random.default_rng(seed)
    folds = rng.permutation(len(y)) % FOLDS
    scores = np.full(len(y), np.nan)
    for f in range(FOLDS):
        train = folds != f
        if len(set(y[train])) < 2:
            return None
        scores[~train] = model.fit(X[train], y[train], l2=1.0).logit(X[~train])
    return model.auc(scores, y)


def learn(examples: list[dict]) -> dict:
    """`examples`: one per vote with `look` (0-1 alike), `gaps` (per shape
    feature, already scaled) and `vote`. Returns the model file's content:
    per-feature shape weights, used only when they beat the current ranking on
    votes they were not fitted on."""
    up = sum(e["vote"] > 0 for e in examples)
    down = sum(e["vote"] < 0 for e in examples)
    out = {"votes": {"up": up, "down": down}, "in_use": False, "weights": None, "names": list(shape.NAMES)}
    if min(up, down) < MIN_EACH:
        out["status"] = (
            f"Learning from your feedback starts at {MIN_EACH} 👍 and {MIN_EACH} 👎 — so far {up} 👍 and {down} 👎. "
            "Every 👎 already hides that match for that stock."
        )
        return out
    gaps = np.array([np.nan_to_num(np.asarray(e["gaps"], dtype=float), nan=1.0) for e in examples])
    look = np.array([e["look"] for e in examples])
    y = np.array([1.0 if e["vote"] > 0 else 0.0 for e in examples])
    # The current ranking's view of a pair: half look, half mean shape gap.
    baseline = model.auc(0.5 * look - 0.5 * gaps.mean(axis=1), y)
    X = np.column_stack([look, gaps])
    learned = _cv_auc(X, y)
    out["auc"] = {"current_ranking": None if baseline is None else round(baseline, 3), "learned": None if learned is None else round(learned, 3)}
    if learned is None or baseline is None or learned < baseline + MIN_GAIN:
        out["status"] = (
            f"Learned from {up} 👍 and {down} 👎, but the learned weights did not predict your votes better than the "
            f"current ranking on votes they were not fitted on ({(learned or 0) * 100:.0f}% vs {baseline * 100:.0f}%), "
            "so the ranking is unchanged. Every 👎 still hides that match."
        )
        return out
    coef = model.fit(X, y, l2=1.0).w
    # A bigger gap on a feature you care about makes 👎 likelier: negative
    # coefficient -> weight. Features you ignore get no weight.
    w = np.maximum(0.0, -coef[1:])
    if w.sum() <= 0:
        out["status"] = "Your votes did not single out any shape feature, so the ranking is unchanged."
        return out
    w = w / w.mean()
    out["weights"] = [round(float(x), 4) for x in w]
    out["in_use"] = True
    top = [shape.NAMES[i] for i in np.argsort(-w)[:3]]
    out["status"] = (
        f"In use: learned from {up} 👍 and {down} 👎 and predicts your votes better than before "
        f"({learned * 100:.0f}% vs {baseline * 100:.0f}% on votes it did not learn from). "
        f"You weigh most: {', '.join(n.replace('_', ' ') for n in top)}."
    )
    return out


def load_weights(state_dir: Path) -> np.ndarray | None:
    path = state_dir / MODEL_FILE
    try:
        m = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not m.get("in_use") or not m.get("weights"):
        return None
    return np.asarray(m["weights"], dtype=float)


def build_examples(votes: list[dict], universe, library) -> list[dict]:
    """Turn votes into training examples: the stock's chart on the session it
    was voted on, the match's chart (a reference from the library, or another
    stock on the same session), how alike they look, and their gap on each
    shape feature."""
    from . import embed, render, similarity

    by_symbol = {b.symbol: b for b in universe}
    ref_pos = {s: {f"{s}:{r['ticker']}@{r['date']}": j for j, r in enumerate(refs)} for s, refs in library.refs.items()}
    F_all = np.vstack([F for F in library.F_ref.values()]) if library.F_ref else None
    scale = shape.robust_scale(F_all) if F_all is not None else np.ones(len(shape.NAMES))

    def chart_at(symbol: str, session: str):
        b = by_symbol.get(symbol)
        if b is None:
            return None
        i = int(np.searchsorted(b.dates, date.fromisoformat(session), side="right")) - 1
        if i < 0 or b.dates[i].isoformat() != session:
            return None
        pics = {sc: render.picture(b.open, b.high, b.low, b.close, b.volume, i, sc) for sc in render.SCALES}
        return pics, shape.features(b.open, b.high, b.low, b.close, b.volume, i)

    examples = []
    for v in votes:
        if v.get("vote", 0) == 0:
            continue
        q = chart_at(v.get("query", ""), v.get("session", ""))
        if q is None or q[1] is None:
            continue
        if v["kind"] == "ref":
            style = v["target"].split(":", 1)[0]
            j = ref_pos.get(style, {}).get(v["target"])
            if j is None or style not in library.F_ref:
                continue
            T = {sc: X[j : j + 1] for sc, X in library.X_ref_scales[style].items()}
            Ft = library.F_ref[style][j]
        else:
            t = chart_at(v["target"], v["session"])
            if t is None or t[1] is None:
                continue
            T = {sc: (embed.fingerprints([p[0]]) if p else np.zeros((1, 768), np.float32)) for sc, p in t[0].items()}
            Ft = t[1]
        Q = {sc: (embed.fingerprints([p[0]]) if p else np.zeros((1, 768), np.float32)) for sc, p in q[0].items()}
        look = float(similarity.blended_cosine(Q, T, library.head)[0, 0])
        gaps = np.minimum(np.abs(q[1] - Ft) / scale, 3.0)
        examples.append({"look": look, "gaps": [None if not np.isfinite(g) else float(g) for g in gaps], "vote": v["vote"]})
    return examples


def learn_from_votes(data_dir: Path, universe, library) -> dict:
    """The evening step: read the committed copy of the votes, learn, write the
    model file the next scoring reads. Returns the model file's content."""
    from .pipeline import library_dir

    backup = data_dir / BACKUP_FILE
    try:
        votes = json.loads(backup.read_text()).get("votes", []) if backup.exists() else []
    except (OSError, ValueError):
        votes = []
    result = learn(build_examples(votes, universe, library))
    result["learned_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    (library_dir(data_dir) / MODEL_FILE).write_text(json.dumps(result, indent=1))
    return result
