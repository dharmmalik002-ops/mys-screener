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

from . import chart_bars, embed, model, outcome, render, rules, shape, us_bars
from .references import Reference, style_name

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
SETUP_STYLE_MATCHES = 30
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


EXTRA_SCALES = tuple(s for s in render.SCALES if s != render.WINDOW)


def _ref_extras(series, idx: int):
    """The other-scale pictures and the measured shape of one reference, for
    the similarity layers. None where the history is too short."""
    pics = {}
    for scale in EXTRA_SCALES:
        pic = render.picture(series.o, series.h, series.l, series.c, series.v, idx, scale)
        pics[scale] = pic[0] if pic else None
    return pics, shape.features(series.o, series.h, series.l, series.c, series.v, idx)


class _Prints:
    """Fingerprints taken as pictures arrive. A style of tens of thousands of
    charts — eight ordinary days drawn for each — would otherwise hold every
    picture (~150 KB) in memory until the end. A None stands for a missing
    picture and becomes a zero row."""

    BATCH = 512

    def __init__(self):
        self.done: list[np.ndarray] = []
        self.pending: list = []

    def add(self, image) -> None:
        self.pending.append(image)
        if len(self.pending) >= self.BATCH:
            self._flush()

    def _flush(self) -> None:
        if not self.pending:
            return
        have = [i for i, im in enumerate(self.pending) if im is not None]
        X = np.zeros((len(self.pending), 768), dtype=np.float32)
        if have:
            X[have] = embed.fingerprints([self.pending[i] for i in have])
        self.done.append(X)
        self.pending = []

    def array(self) -> np.ndarray:
        self._flush()
        return np.concatenate(self.done) if self.done else np.zeros((0, 768), dtype=np.float32)


def _extra_arrays(style: str, extras: list) -> dict[str, np.ndarray]:
    """Fingerprints per extra scale (zero rows where a picture is missing) and
    the shape matrix (nan rows where it is missing), keyed for library.npz."""
    out = {}
    for scale in EXTRA_SCALES:
        imgs = [e[0][scale] for e in extras]
        have = [i for i, im in enumerate(imgs) if im is not None]
        X = np.zeros((len(imgs), 768), dtype=np.float32)
        if have:
            X[have] = embed.fingerprints([imgs[i] for i in have])
        out[f"{style}__X_ref{scale}"] = X
    out[f"{style}__F_ref"] = np.array(
        [e[1] if e[1] is not None else np.full(len(shape.NAMES), np.nan) for e in extras], dtype=np.float64
    )
    return out


def extend_library(data_dir: Path) -> dict:
    """Add the similarity layers' inputs (60/250-session fingerprints and the
    shape of every reference) to an existing library without rebuilding it.
    Reads the cached US bars, so workstation only."""
    lib_dir = library_dir(data_dir)
    meta = json.loads((lib_dir / "library.json").read_text())
    arrays = dict(np.load(lib_dir / "library.npz"))
    cache: dict[str, us_bars.Series | None] = {}
    counts = {}
    for style in meta["styles"]:
        extras = []
        for r in [r for r in meta["references"] if r["style"] == style]:
            if r["ticker"] not in cache:
                cache[r["ticker"]] = us_bars._read(us_bars.cache_dir(data_dir) / f"{r['ticker']}.json")
            series = cache[r["ticker"]]
            idx = series.index_on_or_before(date.fromisoformat(r["session"])) if series else None
            if series is None or idx is None or series.dates[idx].isoformat() != r["session"]:
                extras.append(({s: None for s in EXTRA_SCALES}, None))
            else:
                extras.append(_ref_extras(series, idx))
        arrays.update(_extra_arrays(style, extras))
        counts[style] = {
            str(sc): int((np.abs(arrays[f"{style}__X_ref{sc}"]).sum(axis=1) > 0).sum()) for sc in EXTRA_SCALES
        } | {"shape": int(np.isfinite(arrays[f"{style}__F_ref"]).all(axis=1).sum()), "references": len(extras)}
    np.savez_compressed(lib_dir / "library.npz", **arrays)
    return counts


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
    ref_prints = _Prints()
    extra_prints = {scale: _Prints() for scale in EXTRA_SCALES}
    ref_shapes: list = []
    ctrl_prints, ctrl_meta = _Prints(), []  # (ticker, day, role, outcome label, rules)
    skipped: list[dict] = []

    def add(series, idx: int, ref: Reference, prices: str) -> bool:
        try:
            pic = render.picture(series.o, series.h, series.l, series.c, series.v, idx)
        except ValueError as exc:  # one malformed series must not stop a build of thousands
            logger.warning("%s: chart could not be drawn (%s)", ref.key, exc)
            skipped.append({"ticker": ref.ticker, "date": ref.day.isoformat(), "reason": "chart could not be drawn"})
            return False
        if pic is None:
            skipped.append({"ticker": ref.ticker, "date": ref.day.isoformat(), "reason": "not enough history before the date"})
            return False
        graded = outcome.grade(series.o, series.h, series.l, idx)
        image, window = pic
        pics, feat = _ref_extras(series, idx)
        for scale in EXTRA_SCALES:
            extra_prints[scale].add(pics[scale])
        ref_shapes.append(feat)
        ref_prints.add(image)
        ref_rows.append({
            "chart": render.extended(series.o, series.h, series.l, series.c, series.v, idx, series.dates),
            "rules": _rule_flags(series, idx, index),
            "key": ref.key,
            "style": ref.style,
            "ticker": ref.ticker,
            "date": ref.day.isoformat(),
            "session": series.dates[idx].isoformat(),
            "label": graded.label,
            "sessions_observed": graded.sessions,
            "max_gain_pct": graded.max_gain_pct,
            "max_loss_pct": graded.max_loss_pct,
            "days_to_result": graded.days_to_result,
            "window": window,
            **({"prices": prices} if prices != chart_bars.YAHOO else {}),
        })
        return True

    use = chart_bars.verdicts(data_dir)
    for n, (ticker, refs) in enumerate(sorted(by_ticker.items()), start=1):
        if n % 50 == 0:
            logger.info("  %d/%d tickers", n, len(by_ticker))
        # Prices read off the newsletter's own chart: a delisted stock, or a
        # symbol that now belongs to another company (chart_bars.py). No
        # sessions follow the chart, so these carry no controls and no outcome.
        for ref in [r for r in refs if use.get(r.key) == chart_bars.CHART]:
            series = chart_bars.read(data_dir, ref.key)
            if series is None or not series.dates:
                skipped.append({"ticker": ticker, "date": ref.day.isoformat(), "reason": "chart could not be read"})
                continue
            add(series, len(series.dates) - 1, ref, chart_bars.CHART)
        for ref in [r for r in refs if use.get(r.key) == chart_bars.NONE]:
            skipped.append({"ticker": ticker, "date": ref.day.isoformat(), "reason": "no price history"})
        refs = [r for r in refs if use.get(r.key, chart_bars.YAHOO) == chart_bars.YAHOO]
        if not refs:
            continue
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
            if add(series, idx, ref, chart_bars.YAHOO):
                indices.append(idx)
        per_ref = TRAIN_CONTROLS + CALIBRATION_CONTROLS
        for pos, idx in enumerate(_controls(series, indices, f"{ticker}:{refs[0].style}")):
            pic = render.picture(series.o, series.h, series.l, series.c, series.v, idx)
            if pic is None:
                continue
            role = "train" if pos % per_ref < TRAIN_CONTROLS else "calibration"
            ctrl_prints.add(pic[0])
            ctrl_meta.append((
                ticker, series.dates[idx], role,
                outcome.grade(series.o, series.h, series.l, idx).label,
                _rule_flags(series, idx, index),
            ))
    extras = {f"X_ref{scale}": extra_prints[scale].array() for scale in EXTRA_SCALES}
    extras["F_ref"] = np.array(
        [f if f is not None else np.full(len(shape.NAMES), np.nan) for f in ref_shapes], dtype=np.float64
    ).reshape(len(ref_shapes), len(shape.NAMES))
    return ref_rows, ref_prints.array(), ctrl_prints.array(), ctrl_meta, skipped, extras


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
    logger.info("style %s: drawing and fingerprinting %d references and their ordinary days", style, len(references))
    ref_rows, X_ref, X_ctrl, ctrl_meta, skipped, ref_extras = _gather(data_dir, references, today)
    if not ref_rows:
        logger.warning("style %s: no usable reference charts", style)
        return None
    train_mask = np.array([m[2] == "train" for m in ctrl_meta], dtype=bool)
    X_train_ctrl = X_ctrl[train_mask]
    X_cal = X_ctrl[~train_mask]

    # The classifier learns from Yahoo-priced references only. A chart-read
    # reference has no ordinary days of its own to contrast with, and its bars
    # carry the reader's small artefacts — trained on, the model could learn
    # "read off an image" as the style. Those references still serve as
    # look-alikes (X_ref), which is what they are for.
    trainable = np.array([r.get("prices", chart_bars.YAHOO) == chart_bars.YAHOO for r in ref_rows], dtype=bool)
    if not trainable.any():
        logger.warning("style %s: no Yahoo-priced references to train on", style)
        return None
    train_rows = [r for r, t in zip(ref_rows, trainable) if t]
    X = np.vstack([X_ref[trainable], X_train_ctrl])
    y = np.concatenate([np.ones(int(trainable.sum())), np.zeros(len(X_train_ctrl))])
    groups = [r["ticker"] for r in train_rows] + [m[0] for m, t in zip(ctrl_meta, train_mask) if t]
    days = [date.fromisoformat(r["session"]) for r in train_rows] + [m[1] for m, t in zip(ctrl_meta, train_mask) if t]
    labels = [r["label"] for r in train_rows]

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
        "references_from_chart_prices": int((~trainable).sum()),
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
        **{f"{style}__{k}": v for k, v in ref_extras.items()},
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
    save_library(out, summaries, all_rows, arrays)
    return summaries


LIBRARY_VERSION = 2
CHART_LEN = render.WINDOW + render.AFTER_SESSIONS
CHART_SERIES = ("o", "h", "l", "c", "sma", "v")
SKIPPED_KEPT = 200


def save_library(out: Path, summaries: dict, rows: list[dict], arrays: dict) -> None:
    """Store every reference ONCE, however many styles cite it.

    A Zanger chart named a cup and handle sits in both `zanger` and
    `zanger_cup_handle`; storing it per style doubled the library. And at tens
    of thousands of references the per-row JSON charts and float32
    fingerprints outgrew what git takes (100 MB a file). So: one row per
    reference in library.json (no arrays), its before-and-after chart packed
    float16 in library.npz, each scale's fingerprints in its own
    library_x<scale>.npz, and each style an index list into them."""
    unique: dict[str, int] = {}
    base_rows: list[dict] = []
    charts: list[dict | None] = []
    xs: dict[int, list] = {sc: [] for sc in render.SCALES}
    feats: list = []
    style_idx: dict[str, list[int]] = defaultdict(list)
    pos: dict[str, int] = defaultdict(int)
    for r in rows:
        st = r["style"]
        k = pos[st]
        pos[st] += 1
        if r["key"] not in unique:
            unique[r["key"]] = len(base_rows)
            base = {kk: v for kk, v in r.items() if kk not in ("chart", "window", "style")}
            ch = r.get("chart")
            base["chart_meta"] = {m: ch[m] for m in ("setup_index", "lo", "hi", "dates") if m in ch} if ch else None
            base_rows.append(base)
            charts.append(ch)
            xs[render.WINDOW].append(arrays[f"{st}__X_ref"][k])
            for sc in EXTRA_SCALES:
                xs[sc].append(arrays[f"{st}__X_ref{sc}"][k])
            feats.append(arrays[f"{st}__F_ref"][k])
        style_idx[st].append(unique[r["key"]])

    packed = np.full((len(charts), len(CHART_SERIES), CHART_LEN), np.nan, dtype=np.float16)
    lengths = np.zeros(len(charts), dtype=np.int16)
    for i, ch in enumerate(charts):
        if not ch:
            continue
        n = min(CHART_LEN, len(ch["c"]))
        lengths[i] = n
        for j, name in enumerate(CHART_SERIES):
            packed[i, j, :n] = ch[name][:n]
    keep = {k: v for k, v in arrays.items() if k.split("__", 1)[1] in ("w", "b", "mean", "cal_logits")}
    for st, idx in style_idx.items():
        keep[f"{st}__idx"] = np.array(idx, dtype=np.int32)
    keep["refs__F"] = np.array(feats, dtype=np.float32).reshape(len(feats), len(shape.NAMES))
    keep["refs__chart"] = packed
    keep["refs__chart_len"] = lengths
    np.savez_compressed(out / "library.npz", **keep)
    for sc in render.SCALES:
        X = np.array(xs[sc], dtype=np.float16).reshape(len(base_rows), 768)
        np.savez_compressed(out / f"library_x{sc}.npz", X=X)
    for summary in summaries.values():
        skipped = summary.get("skipped") or []
        if isinstance(skipped, list):
            summary["skipped_count"] = len(skipped)
            reasons: dict[str, int] = defaultdict(int)
            for row in skipped:
                reasons[row["reason"]] += 1
            summary["skipped_reasons"] = dict(reasons)
            summary["skipped"] = skipped[:SKIPPED_KEPT]
    (out / "library.json").write_text(json.dumps(
        {"version": LIBRARY_VERSION, "styles": summaries, "references": base_rows}, separators=(",", ":")))


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
    notes = style_notes(data_dir)
    for style in scoring.shown(library):
        summary = library.styles[style]
        refs = library.refs[style]
        logits = scored.logits[style]
        sims = scored.sims[style]
        matches = []
        # A setup group (zanger_flag) shows fewer matches than a trader's main
        # style: twelve styles x 80 matches made the page's one file 18 MB.
        n_show = top if "_" not in style else min(top, SETUP_STYLE_MATCHES)
        for rank, i in enumerate(np.argsort(-logits)[:n_show], start=1):
            near = scored.nearest(style, i, NEAREST)
            for j in near:
                ref = refs[j]
                key = _public_key(ref, show_reference_names)
                if key not in public_refs:
                    row = {k: ref[k] for k in ("style", "label", "max_gain_pct", "max_loss_pct", "days_to_result")}
                    row["window"] = library.window(ref)
                    if ref.get("prices"):
                        row["prices"] = ref["prices"]
                    row["chart"] = library.chart(ref)
                    if show_reference_names:
                        row["name"] = f"{ref['ticker']} · {ref['date']}"
                        row["ticker"] = ref["ticker"]
                        row["date"] = ref["date"]
                        row["link"] = scoring.tradingview_us(ref["ticker"])
                        row["source"] = library.sources.get(ref["key"])
                    else:
                        row["name"] = f"{style_name(style)} setup · {date.fromisoformat(ref['date']).strftime('%b %Y')}"
                    public_refs[key] = row
            nearest_rows = [{**public_refs[_public_key(refs[j], show_reference_names)], "similarity": round(scored.alike(style, i, j), 3)} for j in near]
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
                "nearest": [{"key": _public_key(refs[j], show_reference_names), "similarity": round(scored.alike(style, i, j), 3)} for j in near],
            })
        public_summary = dict(summary)
        public_summary["skipped"] = summary.get("skipped_count", len(summary.get("skipped", [])))
        public_summary.pop("skipped_reasons", None)
        styles_out[style] = {"library": public_summary, "matches": matches, "notes": notes_for(notes, style)}

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


NOTES_FILE = "style_notes.json"


def style_notes(data_dir: Path) -> dict:
    """Each trader's setups described in our own words, written from his
    comments (scripts/import_zanger_newsletters.py): {trader: {setup: {...}}}
    with "general" for his overall rules. Never his text — a paraphrase."""
    try:
        return json.loads((library_dir(data_dir) / NOTES_FILE).read_text())
    except (OSError, ValueError):
        return {}


def notes_for(notes: dict, style: str) -> dict | None:
    trader, _, setup = style.partition("_")
    return (notes.get(trader) or {}).get(setup or "general")


def _public_key(ref: dict, show_names: bool) -> str:
    return ref["key"] if show_names else f"ref-{zlib.crc32((ref['style'] + ref['key']).encode()):08x}"
