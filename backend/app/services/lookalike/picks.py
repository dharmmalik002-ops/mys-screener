"""Daily picks: choose, explain, remember, review, learn.

Choose — each day, up to `MAX_PICKS` Indian charts that look like the style's
setups (beat `MIN_PERCENTILE`% of ordinary charts) AND pass all 8 of its Trend
Template rules. Both conditions are the trader's own: the picture and his
written rules. A stock picked in the last `REPICK_GAP_DAYS` is the same setup
still forming, not a new pick, so it is not counted twice.

Explain — every pick carries a description built from measured numbers only
(gotcha 24): how much it resembles the style, which rules it passes and by how
much, how tight its base is, how dry its volume is, and the closest example
with what that example went on to do.

Remember, then review — a pick is recorded the day it is made and is NOT used
for learning while its result is unknown. Each run grades every open pick
against the same rule the references were graded by (+20% before -8% within 40
sessions, from the next open); only a finished pick becomes evidence.

Learn — finished picks train a small model of worked-vs-failed on the same
fingerprints plus the rule checks. It is only allowed to reorder picks once it
has beaten a coin flip on picks it did not learn from (`feedback_status`).
Until then it watches. This restraint is the lesson of gotchas 40, 59 and 69:
a model that changes itself on every new result learns the noise.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from . import model, outcome, rules
from .scoring import Library, Scored, tradingview_india, tradingview_us

PICK_STYLES = ("minervini",)
MAX_PICKS = 10
MIN_PERCENTILE = 95.0
REQUIRED_TEMPLATE = 8
REPICK_GAP_DAYS = 28

LEDGER_FILE = "picks.json"            # under data/lookalike/ (private)
FINGERPRINTS_FILE = "picks_fingerprints.npz"
PUBLIC_FILE = "lookalike_picks.json"  # under data/ (served)

# The learner may reorder picks only after this much evidence, judged on picks
# it did not learn from. Declared, not tuned.
FEEDBACK_MIN_TRAIN_EACH = 30
FEEDBACK_MIN_TEST_EACH = 15
FEEDBACK_MIN_AUC = 0.55
LESSON_MIN_EACH = 20


# ── ledger ────────────────────────────────────────────────────────────────

def _lib_dir(data_dir: Path) -> Path:
    return data_dir / "lookalike"


def load_ledger(data_dir: Path) -> dict:
    path = _lib_dir(data_dir) / LEDGER_FILE
    if path.exists():
        return json.loads(path.read_text())
    return {"picks": {}}


def save_ledger(data_dir: Path, ledger: dict) -> None:
    path = _lib_dir(data_dir) / LEDGER_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(ledger, separators=(",", ":")))
    tmp.replace(path)


def load_fingerprints(data_dir: Path) -> dict[str, np.ndarray]:
    path = _lib_dir(data_dir) / FINGERPRINTS_FILE
    if not path.exists():
        return {}
    arr = np.load(path)
    return {k: arr[k] for k in arr.files}


def save_fingerprints(data_dir: Path, fps: dict[str, np.ndarray]) -> None:
    path = _lib_dir(data_dir) / FINGERPRINTS_FILE
    np.savez_compressed(path, **fps)


# ── explain ───────────────────────────────────────────────────────────────

def _ref_public(ref: dict) -> dict:
    return {
        "name": f"{ref['ticker']} · {ref['date']}",
        "ticker": ref["ticker"],
        "date": ref["date"],
        "label": ref["label"],
        "max_gain_pct": ref["max_gain_pct"],
        "max_loss_pct": ref["max_loss_pct"],
        "days_to_result": ref["days_to_result"],
        "link": tradingview_us(ref["ticker"]),
    }


def describe(style: str, pct: float, m: dict, f: dict, nearest: list[dict]) -> str:
    """A pick's reason, from measured numbers only."""
    name = style.title()
    parts = [f"Its last 120 sessions look more like {name}'s setups than {pct:.0f}% of ordinary charts."]
    tmpl = rules.template_score(f) or 0
    rel = None if m.get("index_return_6m_pct") is None else m["return_6m_pct"] - m["index_return_6m_pct"]
    rel_txt = f", and it beat the Nifty 500 by {rel:.0f} points over 6 months" if rel is not None else ""
    parts.append(
        f"It passes {tmpl} of his 8 Trend Template rules: above its 50, 150 and 200-day averages with the 200-day rising, "
        f"{m['above_low_pct']:.0f}% above its 52-week low and {m['below_high_pct']:.1f}% below its high{rel_txt}."
    )
    if f.get("tightening"):
        parts.append(
            f"The last two weeks' range ({m['range_2w_pct']:.1f}%) is tighter than its base average "
            f"({m['range_base_pct']:.1f}%) — the contraction his VCP looks for."
        )
    else:
        parts.append(
            f"The base is not tightening yet: the last two weeks' range ({m['range_2w_pct']:.1f}%) is wider than its "
            f"average ({m['range_base_pct']:.1f}%)."
        )
    vr = m.get("volume_ratio")
    if vr is not None and np.isfinite(vr):
        parts.append(
            f"Volume has dried up to {vr:.2f}x its 50-day average." if vr < 1
            else f"Volume is running {vr:.2f}x its 50-day average, so it has not dried up."
        )
    if nearest:
        n0 = nearest[0]
        if n0["label"] == outcome.WORKED:
            what = f"went on to rise {outcome.TARGET_PCT:.0f}%+ (best +{n0['max_gain_pct']:.0f}%)"
        elif n0["label"] == outcome.FAILED:
            what = f"failed (fell {abs(n0['max_loss_pct']):.0f}% before rising {outcome.TARGET_PCT:.0f}%)"
        else:
            what = "has not finished yet"
        parts.append(f"Closest example: {n0['name']} ({n0['similarity'] * 100:.0f}% alike), which {what}.")
    return " ".join(parts)


# ── choose ────────────────────────────────────────────────────────────────

def _recent_symbols(ledger: dict, style: str, day: date) -> set[str]:
    out = set()
    for p in ledger["picks"].values():
        if p["style"] != style:
            continue
        d = date.fromisoformat(p["date"])
        if 0 <= (day - d).days < REPICK_GAP_DAYS and d != day:
            out.add(p["symbol"])
    return out


def choose(
    ledger: dict,
    fps: dict[str, np.ndarray],
    scored: Scored,
    library: Library,
    source: str,
    feedback: "Feedback | None" = None,
) -> list[dict]:
    """Record the day's picks in the ledger and return them. Re-running a day
    replaces that day's picks (same ids), never adds to them."""
    day = scored.as_of
    made = []
    for style in PICK_STYLES:
        if style not in library.styles:
            continue
        refs = library.refs[style]
        recent = _recent_symbols(ledger, style, day)
        pct = scored.percentile[style]
        eligible = [
            i for i in range(len(scored.symbols))
            if pct[i] >= MIN_PERCENTILE
            and rules.template_score(scored.flags[i]) == REQUIRED_TEMPLATE
            and scored.symbols[i] not in recent
        ]
        use_feedback = feedback is not None and feedback.in_use
        key = (lambda i: -feedback.predict(scored.X[i], scored.flags[i])) if use_feedback else (lambda i: -scored.logits[style][i])
        chosen = sorted(eligible, key=key)[:MAX_PICKS]

        # drop any earlier record of this day for this style before re-adding
        for pid in [k for k, p in ledger["picks"].items() if p["date"] == day.isoformat() and p["style"] == style]:
            ledger["picks"].pop(pid)
            fps.pop(pid, None)

        for rank, i in enumerate(chosen, start=1):
            near_idx = np.argsort(-scored.sims[style][i])[:3]
            nearest = [{**_ref_public(refs[j]), "similarity": round(float(scored.sims[style][i, j]), 3)} for j in near_idx]
            m = {k: (None if v is None else round(float(v), 3)) for k, v in scored.metrics[i].items()}
            f = scored.flags[i]
            pid = f"{day.isoformat()}:{style}:{scored.symbols[i]}"
            pick = {
                "id": pid,
                "date": day.isoformat(),
                "style": style,
                "rank": rank,
                "symbol": scored.symbols[i],
                "session": scored.sessions[i].isoformat(),
                "close": round(scored.closes[i], 2),
                "turnover_crore": round(scored.turnover[i], 1),
                "score": round(float(scored.logits[style][i]), 3),
                "percentile": round(float(pct[i]), 1),
                "template": rules.template_score(f),
                "rules": f,
                "metrics": m,
                "nearest": nearest,
                "reason": describe(style, float(pct[i]), scored.metrics[i], f, nearest),
                "ranked_by": "learned_outcome" if use_feedback else "style_score",
                "links": {"tradingview": tradingview_india(scored.symbols[i])},
                "source": source,
                "outcome": {"label": outcome.PENDING},
            }
            ledger["picks"][pid] = pick
            fps[pid] = scored.X[i]
            made.append(pick)
    return made


# ── review ────────────────────────────────────────────────────────────────

def review(ledger: dict, universe, today: date) -> int:
    """Grade every pick whose result was still unknown. Returns how many
    changed. Uses the same rule the references were graded by."""
    by_symbol = {b.symbol: b for b in universe}
    changed = 0
    for p in ledger["picks"].values():
        if p["outcome"]["label"] != outcome.PENDING:
            continue
        bars = by_symbol.get(p["symbol"])
        if bars is None:
            continue
        idx = int(np.searchsorted(bars.dates, date.fromisoformat(p["session"]), side="right")) - 1
        if idx < 0 or bars.dates[idx].isoformat() != p["session"]:
            continue
        g = outcome.grade(bars.open, bars.high, bars.low, idx)
        resolved_on = None
        if g.label != outcome.PENDING and g.days_to_result:
            resolved_on = bars.dates[min(len(bars.dates) - 1, idx + g.days_to_result)].isoformat()
        p["outcome"] = {
            "label": g.label,
            "max_gain_pct": g.max_gain_pct,
            "max_loss_pct": g.max_loss_pct,
            "sessions_observed": g.sessions,
            "days_to_result": g.days_to_result,
            "resolved_on": resolved_on,
            "reviewed_on": today.isoformat(),
        }
        changed += g.label != outcome.PENDING
    return changed


# ── learn ─────────────────────────────────────────────────────────────────

RULE_KEYS = [k for k, _ in rules.RULES]


def _features(fp: np.ndarray, flags: dict | None) -> np.ndarray:
    f = np.array([1.0 if (flags or {}).get(k) else 0.0 for k in RULE_KEYS])
    return np.concatenate([fp, f * 0.1])


@dataclass
class Feedback:
    in_use: bool
    status: str
    clf: model.Logistic | None
    train: dict
    test: dict
    auc: float | None

    def predict(self, fp: np.ndarray, flags: dict | None) -> float:
        return float(self.clf.logit(_features(fp, flags)[None, :])[0]) if self.clf else 0.0


def _decided(ledger: dict, before: date | None = None) -> list[dict]:
    out = []
    for p in ledger["picks"].values():
        o = p["outcome"]
        if o["label"] not in (outcome.WORKED, outcome.FAILED):
            continue
        # A pick is evidence only from the day its result was known.
        if before is not None and (o.get("resolved_on") is None or date.fromisoformat(o["resolved_on"]) >= before):
            continue
        out.append(p)
    return sorted(out, key=lambda p: p["date"])


def feedback_status(ledger: dict, fps: dict[str, np.ndarray], as_of: date | None = None) -> Feedback:
    """Fit worked-vs-failed on finished picks; allow it to rank only if it
    beats a coin flip on the newest 30% of finished picks it did not see."""
    decided = [p for p in _decided(ledger, as_of) if p["id"] in fps]
    n_w = sum(p["outcome"]["label"] == outcome.WORKED for p in decided)
    n_f = len(decided) - n_w
    cut = int(len(decided) * 0.7)
    train, test = decided[:cut], decided[cut:]
    counts = lambda rows: {
        "worked": sum(p["outcome"]["label"] == outcome.WORKED for p in rows),
        "failed": sum(p["outcome"]["label"] == outcome.FAILED for p in rows),
    }
    tr, te = counts(train), counts(test)
    if min(tr.values()) < FEEDBACK_MIN_TRAIN_EACH or min(te.values()) < FEEDBACK_MIN_TEST_EACH:
        return Feedback(False, (
            f"Watching, not yet learning: {n_w} worked and {n_f} failed picks have finished. It needs "
            f"{FEEDBACK_MIN_TRAIN_EACH} of each to learn from plus {FEEDBACK_MIN_TEST_EACH} of each to test on."
        ), None, tr, te, None)

    def xy(rows):
        X = np.stack([_features(fps[p["id"]], p["rules"]) for p in rows])
        y = np.array([1.0 if p["outcome"]["label"] == outcome.WORKED else 0.0 for p in rows])
        return X, y

    Xtr, ytr = xy(train)
    Xte, yte = xy(test)
    auc = model.auc(model.fit(Xtr, ytr).logit(Xte), yte)
    if auc is None or auc < FEEDBACK_MIN_AUC:
        return Feedback(False, (
            f"Learned from {len(train)} finished picks but could not tell winners from losers among the next "
            f"{len(test)} ({0 if auc is None else auc * 100:.0f}%, where 50% is a coin flip and {FEEDBACK_MIN_AUC * 100:.0f}% is required). "
            "Picks stay ranked by resemblance."
        ), None, tr, te, auc)
    Xall, yall = xy(decided)
    return Feedback(True, (
        f"In use: it told winners from losers {auc * 100:.0f}% of the time on {len(test)} picks it did not learn from, "
        "so picks are now ranked by what has worked."
    ), model.fit(Xall, yall), tr, te, auc)


def lessons(ledger: dict) -> list[dict]:
    """Which conditions separated winners from losers among finished picks —
    reported only where both sides have enough picks to mean something."""
    decided = _decided(ledger)
    out = []

    def add(label, test):
        yes = [p for p in decided if test(p)]
        no = [p for p in decided if not test(p)]
        wr = lambda rows: round(100 * sum(p["outcome"]["label"] == outcome.WORKED for p in rows) / len(rows), 1) if rows else None
        out.append({
            "condition": label,
            "with": len(yes), "without": len(no),
            "worked_with_pct": wr(yes), "worked_without_pct": wr(no),
            "enough": len(yes) >= LESSON_MIN_EACH and len(no) >= LESSON_MIN_EACH,
        })

    add("Base tightening (last 2 weeks tighter than the base)", lambda p: bool(p["rules"].get("tightening")))
    add("Volume dried up", lambda p: bool(p["rules"].get("volume_dry_up")))
    add("Within 5% of its 52-week high", lambda p: (p["metrics"].get("below_high_pct") or 99) <= 5)
    add("Resemblance in the top 1% (99th percentile+)", lambda p: p["percentile"] >= 99)
    add("Closest example worked", lambda p: bool(p["nearest"]) and p["nearest"][0]["label"] == outcome.WORKED)
    add("Ranked in the top 3 that day", lambda p: p["rank"] <= 3)
    return out


def _best(ps: list[dict]) -> str | None:
    """The pick that ran furthest that week, among picks that have been reviewed."""
    seen = [p for p in ps if p["outcome"].get("max_gain_pct") is not None]
    if not seen:
        return None
    top = max(seen, key=lambda p: p["outcome"]["max_gain_pct"])
    return f"{top['symbol']} +{top['outcome']['max_gain_pct']:.0f}%"


def weekly(ledger: dict) -> list[dict]:
    """Picks grouped by the week they were made, with how they turned out —
    the weekly / fortnightly review."""
    weeks: dict[str, list] = defaultdict(list)
    for p in ledger["picks"].values():
        d = date.fromisoformat(p["date"])
        monday = d - timedelta(days=d.weekday())
        weeks[monday.isoformat()].append(p)
    rows = []
    for wk, ps in sorted(weeks.items(), reverse=True):
        labels = [p["outcome"]["label"] for p in ps]
        w, f = labels.count(outcome.WORKED), labels.count(outcome.FAILED)
        rows.append({
            "week_of": wk,
            "picks": len(ps),
            "worked": w,
            "failed": f,
            "pending": labels.count(outcome.PENDING),
            "worked_pct": round(100 * w / (w + f), 1) if (w + f) else None,
            "best": _best(ps),
        })
    return rows


# ── baselines ─────────────────────────────────────────────────────────────

BASELINES_FILE = "pick_baselines.json"  # under data/lookalike/


def update_baselines(data_dir: Path, ledger: dict, universe, index) -> dict:
    """For every pick date: how often ANY stock worked under the same rule,
    and how often a stock passing all 8 Trend Template rules did. Without
    these a pick's success rate has nothing to be compared with. A date is
    cached once every stock's result on it is known."""
    from . import render, scoring

    path = _lib_dir(data_dir) / BASELINES_FILE
    cache = json.loads(path.read_text()) if path.exists() else {}
    for d in sorted({p["date"] for p in ledger["picks"].values()}):
        if cache.get(d, {}).get("complete"):
            continue
        day = date.fromisoformat(d)
        counts = {"all": [0, 0], "template8": [0, 0]}
        pending = 0
        for b in universe:
            end = int(np.searchsorted(b.dates, day, side="right")) - 1
            if end < render.min_bars_needed() - 1 or (day - b.dates[end]).days > scoring.STALE_DAYS:
                continue
            if scoring._turnover_crore(b.close, b.volume, end) < scoring.MIN_TURNOVER_CRORE:
                continue
            g = outcome.grade(b.open, b.high, b.low, end)
            if g.label == outcome.PENDING:
                pending += 1
                continue
            ret = rules.index_return(index[0], index[1], b.dates[end], b.dates[end - 126]) if index and end >= 126 else None
            m = rules.metrics(b.open, b.high, b.low, b.close, b.volume, end, ret)
            if m is None:
                continue
            w = int(g.label == outcome.WORKED)
            counts["all"][0] += w
            counts["all"][1] += 1
            if rules.template_score(rules.flags_from(m)) == REQUIRED_TEMPLATE:
                counts["template8"][0] += w
                counts["template8"][1] += 1
        cache[d] = {**counts, "complete": pending == 0}
    path.write_text(json.dumps(cache, separators=(",", ":")))
    return cache


def _baseline_summary(ledger: dict, cache: dict) -> dict:
    """Pooled over the dates whose picks have finished, so the comparison is
    like for like."""
    dates = {p["date"] for p in _decided(ledger)}
    out = {}
    for key in ("all", "template8"):
        w = sum(cache.get(d, {}).get(key, [0, 0])[0] for d in dates)
        n = sum(cache.get(d, {}).get(key, [0, 0])[1] for d in dates)
        out[key] = {"worked": w, "charts": n, "worked_pct": round(100 * w / n, 1) if n else None}
    decided = _decided(ledger)
    w = sum(p["outcome"]["label"] == outcome.WORKED for p in decided)
    n = len(decided)
    out["picks"] = {"worked": w, "charts": n, "worked_pct": round(100 * w / n, 1) if n else None}
    if n:
        se = (w / n * (1 - w / n) / n) ** 0.5 * 100
        out["picks"]["margin_pct"] = round(1.96 * se, 1)
    learned = [p for p in decided if p.get("ranked_by") == "learned_outcome"]
    lw = sum(p["outcome"]["label"] == outcome.WORKED for p in learned)
    out["learned_ranked"] = {"worked": lw, "charts": len(learned), "worked_pct": round(100 * lw / len(learned), 1) if learned else None}
    return out


# ── export ────────────────────────────────────────────────────────────────

def export(data_dir: Path, ledger: dict, feedback: Feedback, library: Library, baselines: dict | None = None) -> dict:
    days: dict[str, list] = defaultdict(list)
    for p in sorted(ledger["picks"].values(), key=lambda p: (p["date"], p["rank"])):
        days[p["date"]].append({k: v for k, v in p.items() if k not in ("metrics",)})
    decided = _decided(ledger)
    w = sum(p["outcome"]["label"] == outcome.WORKED for p in decided)
    by_source = {}
    for src in ("backfill", "live"):
        rows = [p for p in decided if p["source"] == src]
        ww = sum(p["outcome"]["label"] == outcome.WORKED for p in rows)
        by_source[src] = {"decided": len(rows), "worked": ww, "worked_pct": round(100 * ww / len(rows), 1) if rows else None}
    style = PICK_STYLES[0]
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "selection": {
            "styles": list(PICK_STYLES),
            "max_picks": MAX_PICKS,
            "min_percentile": MIN_PERCENTILE,
            "required_template": REQUIRED_TEMPLATE,
            "repick_gap_days": REPICK_GAP_DAYS,
        },
        "outcome_rule": {"target_pct": outcome.TARGET_PCT, "stop_pct": outcome.STOP_PCT, "horizon_sessions": outcome.HORIZON},
        "summary": {
            "picks": len(ledger["picks"]),
            "days": len(days),
            "decided": len(decided),
            "worked": w,
            "failed": len(decided) - w,
            "worked_pct": round(100 * w / len(decided), 1) if decided else None,
            "by_source": by_source,
            "style_base_rate_pct": (library.styles.get(style, {}).get("worked_rate_pct") or {}).get("setups"),
        },
        "learning": {
            "in_use": feedback.in_use,
            "status": feedback.status,
            "auc": None if feedback.auc is None else round(feedback.auc, 3),
            "train": feedback.train,
            "test": feedback.test,
        },
        "baselines": _baseline_summary(ledger, baselines or {}),
        "lessons": lessons(ledger),
        "weekly": weekly(ledger),
        "rule_labels": dict(rules.RULES),
        "days": days,
    }
    (data_dir / PUBLIC_FILE).write_text(json.dumps(payload, separators=(",", ":")))
    return payload
