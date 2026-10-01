"""Build the reference library, and scan the Indian market against it.

`build_library` is the learning step, run separately for each STYLE (one
trader's setups): redraw every reference, grade what it went on to do, draw
random days in the same stocks as the contrast, fingerprint everything, test
the classifier out of sample, measure the learning curve, and fit the final
one. Styles never share a model — Minervini's charts are no evidence about
what a Zanger setup looks like, and a pooled model would be whichever style
had more charts. Its
output lives in `data/lookalike/` (gitignored) — the reference list is the
user's paid material and has no business in a public repository.

`scan_india` is the daily step: redraw every Indian stock ending on its latest
session, score it, and write `data/lookalikes.json`, which is all the website
reads. That file carries each match's and each cited reference's window in
normalised 0..1 units only, so the page can draw them side by side without
republishing anyone's prices or images.
"""

from __future__ import annotations

import json
import logging
import zlib
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from . import embed, model, outcome, render, rules, us_bars
from .references import Reference

logger = logging.getLogger(__name__)

STATE_DIR = "lookalike_state"
RESULT_FILE = "lookalikes.json"
# Random days per reference: the first TRAIN_CONTROLS teach the model what an
# ordinary day in that stock looks like; the rest are held back to turn a raw
# score into "resembles the setups more than X% of ordinary charts".
TRAIN_CONTROLS = 5
CALIBRATION_CONTROLS = 3
# A control closer than this to any reference in the same stock would be the
# setup itself, a few days early or late.
CONTROL_EXCLUSION_SESSIONS = 60
# A newsletter dated more than this after the last available bar is not
# describing that bar.
MAX_DATE_SLIP_DAYS = 6

# Daily scan filters, declared rather than tuned.
MIN_TURNOVER_CRORE = 2.0
STALE_DAYS = 6
INDEX_SYMBOLS = {"NIFTY", "NIFTY500", "BANKNIFTY"}
NEAREST = 3
TOP_MATCHES = 80
# The benchmark the relative-strength rule is measured against: the S&P 500
# for the (US) reference charts, the Nifty 500 for the Indian scan.
US_INDEX = "SPY"
INDIA_INDEX = "NIFTY500"


def library_dir(data_dir: Path) -> Path:
    """Where the trained library and the pick ledger live: a COMMITTED folder,
    because the daily run happens on a GitHub runner that starts empty every
    time (.github/workflows/lookalike-daily.yml). The raw material behind the
    library — reference lists, US bars, page extracts — stays private in
    data/lookalike/ and is only needed to rebuild it."""
    return data_dir / STATE_DIR


def _controls(series: us_bars.Series, ref_indices: list[int], seed: str) -> list[int]:
    lo = render.min_bars_needed() - 1
    hi = len(series.dates) - 1
    allowed = [
        i for i in range(lo, hi + 1)
        if all(abs(i - r) >= CONTROL_EXCLUSION_SESSIONS for r in ref_indices)
    ]
    rng = np.random.default_rng(zlib.crc32(seed.encode()))
    want = (TRAIN_CONTROLS + CALIBRATION_CONTROLS) * len(ref_indices)
    if not allowed:
        return []
    picks = rng.choice(len(allowed), size=min(want, len(allowed)), replace=False)
    return [allowed[p] for p in picks]


def _rule_flags(series, idx: int, index) -> dict[str, bool] | None:
    ret = None
    if index is not None and idx >= 126:
        ret = rules.index_return(index[0], index[1], series.dates[idx], series.dates[idx - 126])
    return rules.check(series.o, series.h, series.l, series.c, series.v, idx, ret)


def _gather(data_dir: Path, references: list[Reference], today: date):
    """Rendered references and control days for one style."""
    earliest_all = min(r.day for r in references)
    spy = us_bars.load(data_dir, US_INDEX, earliest_all, need_through=today - timedelta(days=4))
    index = (spy.dates, spy.c) if spy is not None else None
    by_ticker: dict[str, list[Reference]] = defaultdict(list)
    for ref in references:
        by_ticker[ref.ticker].append(ref)

    ref_rows: list[dict] = []
    ref_images = []
    ctrl_images, ctrl_meta = [], []  # (ticker, day, role, outcome label)
    skipped: list[dict] = []

    for n, (ticker, refs) in enumerate(sorted(by_ticker.items()), start=1):
        if n % 50 == 0:
            logger.info("  %d/%d tickers", n, len(by_ticker))
        earliest = min(r.day for r in refs)
        series = us_bars.load(data_dir, ticker, earliest, need_through=today - timedelta(days=4))
        if series is None or not series.dates:
            skipped.extend({"ticker": ticker, "date": r.day.isoformat(), "reason": "no price history"} for r in refs)
            continue
        indices: list[int] = []
        for ref in refs:
            idx = series.index_on_or_before(ref.day)
            if idx is None or (ref.day - series.dates[idx]).days > MAX_DATE_SLIP_DAYS:
                skipped.append({"ticker": ticker, "date": ref.day.isoformat(), "reason": "no bar near that date"})
                continue
            pic = render.picture(series.o, series.h, series.l, series.c, series.v, idx)
            if pic is None:
                skipped.append({"ticker": ticker, "date": ref.day.isoformat(), "reason": "not enough history before the date"})
                continue
            graded = outcome.grade(series.o, series.h, series.l, idx)
            image, window = pic
            ref_images.append(image)
            indices.append(idx)
            ref_rows.append({
                "rules": _rule_flags(series, idx, index),
                "key": ref.key,
                "style": ref.style,
                "ticker": ticker,
                "date": ref.day.isoformat(),
                "session": series.dates[idx].isoformat(),
                "label": graded.label,
                "sessions_observed": graded.sessions,
                "max_gain_pct": graded.max_gain_pct,
                "max_loss_pct": graded.max_loss_pct,
                "days_to_result": graded.days_to_result,
                "window": window,
            })
        per_ref = TRAIN_CONTROLS + CALIBRATION_CONTROLS
        for pos, idx in enumerate(_controls(series, indices, f"{ticker}:{refs[0].style}")):
            pic = render.picture(series.o, series.h, series.l, series.c, series.v, idx)
            if pic is None:
                continue
            role = "train" if pos % per_ref < TRAIN_CONTROLS else "calibration"
            ctrl_images.append(pic[0])
            ctrl_meta.append((
                ticker, series.dates[idx], role,
                outcome.grade(series.o, series.h, series.l, idx).label,
                _rule_flags(series, idx, index),
            ))
    return ref_rows, ref_images, ctrl_images, ctrl_meta, skipped


def _worked_rate(labels: list[str]) -> float | None:
    decided = [x for x in labels if x in (outcome.WORKED, outcome.FAILED)]
    return round(100 * sum(x == outcome.WORKED for x in decided) / len(decided), 1) if decided else None


def _rules_report(ref_rows: list[dict], ctrl_meta: list) -> list[dict]:
    """Per rule: does it describe what the trader buys (pass rate on his setups
    vs on ordinary days), and does it pick his winners (worked rate among his
    setups that pass vs those that fail)?"""
    report = []
    keys = [(k, label) for k, label in rules.RULES] + [("template_8", "All 8 Trend Template rules")]

    def flag(f, key):
        if f is None:
            return None
        return all(f.get(k) for k in rules.TEMPLATE) if key == "template_8" else bool(f.get(key))

    for key, label in keys:
        setup_flags = [flag(r["rules"], key) for r in ref_rows]
        ctrl_flags = [flag(m[4], key) for m in ctrl_meta]
        s_known = [x for x in setup_flags if x is not None]
        c_known = [x for x in ctrl_flags if x is not None]
        passing = [r["label"] for r, f in zip(ref_rows, setup_flags) if f is True]
        failing = [r["label"] for r, f in zip(ref_rows, setup_flags) if f is False]

        def decided(labels):
            return sum(1 for x in labels if x in (outcome.WORKED, outcome.FAILED))

        report.append({
            "rule": key,
            "label": label,
            "setups_pass_pct": round(100 * sum(s_known) / len(s_known), 1) if s_known else None,
            "ordinary_pass_pct": round(100 * sum(c_known) / len(c_known), 1) if c_known else None,
            "worked_when_pass_pct": _worked_rate(passing),
            "worked_when_fail_pct": _worked_rate(failing),
            "decided_pass": decided(passing),
            "decided_fail": decided(failing),
        })
    return report


def _build_style(data_dir: Path, style: str, references: list[Reference], today: date):
    ref_rows, ref_images, ctrl_images, ctrl_meta, skipped = _gather(data_dir, references, today)
    if not ref_rows:
        logger.warning("style %s: no usable reference charts", style)
        return None

    logger.info("style %s: fingerprinting %d references and %d control days", style, len(ref_images), len(ctrl_images))
    X_ref = embed.fingerprints(ref_images)
    X_ctrl = embed.fingerprints(ctrl_images)
    train_mask = np.array([m[2] == "train" for m in ctrl_meta], dtype=bool)
    X_train_ctrl = X_ctrl[train_mask]
    X_cal = X_ctrl[~train_mask]

    X = np.vstack([X_ref, X_train_ctrl])
    y = np.concatenate([np.ones(len(X_ref)), np.zeros(len(X_train_ctrl))])
    groups = [r["ticker"] for r in ref_rows] + [m[0] for m, t in zip(ctrl_meta, train_mask) if t]
    days = [date.fromisoformat(r["session"]) for r in ref_rows] + [m[1] for m, t in zip(ctrl_meta, train_mask) if t]
    labels = [r["label"] for r in ref_rows]

    ev = model.evaluate(X, y, groups, days, labels)
    curve = model.learning_curve(X, y, days)
    final = model.fit(X, y)
    cal_logits = final.logit(X_cal) if len(X_cal) else final.logit(X_train_ctrl)

    counts = {k: sum(1 for r in ref_rows if r["label"] == k) for k in (outcome.WORKED, outcome.FAILED, outcome.PENDING)}
    summary = {
        "style": style,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": embed.MODEL_ID,
        "window_sessions": render.WINDOW,
        "outcome_rule": {"target_pct": outcome.TARGET_PCT, "stop_pct": outcome.STOP_PCT, "horizon_sessions": outcome.HORIZON},
        "submitted": len(references),
        "references": len(ref_rows),
        "tickers": len({r["ticker"] for r in ref_rows}),
        "first_date": min(r["date"] for r in ref_rows),
        "last_date": max(r["date"] for r in ref_rows),
        "outcomes": counts,
        # The plainest test of whether the setups carry an edge at all: how
        # often they worked, against random days in the same stocks graded by
        # the identical rule.
        "worked_rate_pct": {
            "setups": _worked_rate(labels),
            "ordinary_days": _worked_rate([m[3] for m in ctrl_meta]),
        },
        "controls": {"train": int(train_mask.sum()), "calibration": int((~train_mask).sum())},
        "rules": _rules_report(ref_rows, ctrl_meta),
        "skipped": skipped,
        "evaluation": {
            "method": ev.method,
            "setup_vs_random_auc": None if ev.setup_vs_random_auc is None else round(ev.setup_vs_random_auc, 3),
            "outcome_auc": None if ev.outcome_auc is None else round(ev.outcome_auc, 3),
            "worked": ev.worked,
            "failed": ev.failed,
            "warnings": ev.warnings,
            "learning_curve": curve,
        },
    }
    arrays = {
        f"{style}__X_ref": X_ref,
        f"{style}__w": final.w,
        f"{style}__b": np.array([final.b]),
        f"{style}__mean": final.mean,
        f"{style}__cal_logits": cal_logits,
    }
    return summary, ref_rows, arrays


def build_library(data_dir: Path, references: list[Reference], today: date | None = None) -> dict:
    """One model per style. Returns {style: summary}."""
    today = today or date.today()
    by_style: dict[str, list[Reference]] = defaultdict(list)
    for ref in references:
        by_style[ref.style or "default"].append(ref)

    summaries: dict[str, dict] = {}
    all_rows: list[dict] = []
    arrays: dict[str, np.ndarray] = {}
    for style, refs in sorted(by_style.items()):
        built = _build_style(data_dir, style, refs, today)
        if built is None:
            continue
        summary, rows, arr = built
        summaries[style] = summary
        all_rows.extend(rows)
        arrays.update(arr)
    if not summaries:
        raise RuntimeError("No usable reference charts in any style.")

    out = library_dir(data_dir)
    out.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(out / "library.npz", **arrays)
    (out / "library.json").write_text(json.dumps({"styles": summaries, "references": all_rows}, indent=1))
    return summaries


def scan_india(
    data_dir: Path,
    top: int = TOP_MATCHES,
    show_reference_names: bool = False,
    universe=None,
    scored=None,
    library=None,
) -> dict:
    """Today's look-alikes for every style, written to `data/lookalikes.json`.
    Pass `universe` / `scored` / `library` to reuse work the daily run has
    already done."""
    from . import picks as picks_mod
    from . import scoring

    library = library or scoring.load_library(data_dir)
    if scored is None:
        universe, index = universe or scoring.load_universe(data_dir)
        scored = scoring.score(universe, index, library)
    if scored is None:
        raise RuntimeError("No Indian charts to scan — is data/deep_history/ built?")

    public_refs: dict[str, dict] = {}
    styles_out: dict[str, dict] = {}
    for style, summary in library.styles.items():
        refs = library.refs[style]
        logits = scored.logits[style]
        sims = scored.sims[style]
        matches = []
        for rank, i in enumerate(np.argsort(-logits)[:top], start=1):
            near = np.argsort(-sims[i])[:NEAREST]
            for j in near:
                ref = refs[j]
                key = _public_key(ref, show_reference_names)
                if key not in public_refs:
                    row = {k: ref[k] for k in ("style", "label", "max_gain_pct", "max_loss_pct", "days_to_result", "window")}
                    if show_reference_names:
                        row["name"] = f"{ref['ticker']} · {ref['date']}"
                        row["ticker"] = ref["ticker"]
                        row["date"] = ref["date"]
                        row["link"] = scoring.tradingview_us(ref["ticker"])
                        row["source"] = library.sources.get(ref["key"])
                    else:
                        row["name"] = f"{style.title()} setup · {date.fromisoformat(ref['date']).strftime('%b %Y')}"
                    public_refs[key] = row
            nearest_rows = [{**public_refs[_public_key(refs[j], show_reference_names)], "similarity": round(float(sims[i, j]), 3)} for j in near]
            flags = scored.flags[i]
            matches.append({
                "rank": rank,
                "symbol": scored.symbols[i],
                "session": scored.sessions[i].isoformat(),
                "close": round(scored.closes[i], 2),
                "turnover_crore": round(scored.turnover[i], 1),
                "score": round(float(logits[i]), 3),
                "percentile": round(float(scored.percentile[style][i]), 1),
                "window": scored.windows[i],
                "rules": flags,
                "template": rules.template_score(flags),
                "reason": (
                    picks_mod.describe(style, float(scored.percentile[style][i]), scored.metrics[i], flags, nearest_rows)
                    if scored.metrics[i] is not None else None
                ),
                "links": {"tradingview": scoring.tradingview_india(scored.symbols[i])},
                "nearest": [{"key": _public_key(refs[j], show_reference_names), "similarity": round(float(sims[i, j]), 3)} for j in near],
            })
        public_summary = dict(summary)
        public_summary["skipped"] = len(summary.get("skipped", []))
        styles_out[style] = {"library": public_summary, "matches": matches}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "session": scored.as_of.isoformat(),
        "scanned": len(scored.symbols),
        "filters": {"min_turnover_crore": scoring.MIN_TURNOVER_CRORE, "stale_days": scoring.STALE_DAYS},
        "rule_labels": dict(rules.RULES),
        "reference_names_shown": show_reference_names,
        "styles": styles_out,
        # Only the references a match actually cites.
        "references": public_refs,
    }
    (data_dir / RESULT_FILE).write_text(json.dumps(payload, separators=(",", ":")))
    return payload


def _public_key(ref: dict, show_names: bool) -> str:
    return ref["key"] if show_names else f"ref-{zlib.crc32((ref['style'] + ref['key']).encode()):08x}"
