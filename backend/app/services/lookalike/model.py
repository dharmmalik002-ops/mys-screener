"""The small model on top of the fingerprints, and the test it has to pass.

Question it answers: does this chart look like one of the reference setups,
or like an ordinary day in the same stocks? Positives are the references;
negatives are random days in the SAME tickers, so the model cannot win by
learning "US tech stock" or "volatile name" — only by learning the setup.

Three rules keep the test honest:

* **Folds are grouped by ticker.** A random NVDA day looks like the NVDA
  setup simply because it is NVDA. With a ticker's reference and its controls
  split across train and test, the model would be graded on recognising the
  stock, not the pattern.
* **Chronological when the library spans time.** Once references cover more
  than a year, the test is "learn on the earlier charts, score the later ones",
  which is how it will actually be used.
* **The daily percentile is taken against calibration days the model never
  saw.** Scoring candidates against the controls it was trained on would
  flatter every candidate, because training pushes those controls down.

Plain numpy logistic regression with an L2 penalty: with a few hundred
examples and 768 dimensions, anything more flexible memorises the library.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

L2 = 1.0
ITERATIONS = 400
LEARNING_RATE = 0.1
FOLDS = 5
# Below these counts a measurement is printed with a warning rather than
# presented as a result. They are not tuning knobs.
MIN_REFERENCES_TO_TRUST = 200
MIN_OUTCOMES_TO_JUDGE = 20
CHRONO_MIN_SPAN_DAYS = 365
CHRONO_TRAIN_FRACTION = 0.7


@dataclass
class Logistic:
    w: np.ndarray
    b: float
    mean: np.ndarray

    def logit(self, X: np.ndarray) -> np.ndarray:
        return (X - self.mean) @ self.w + self.b


def fit(X: np.ndarray, y: np.ndarray, l2: float = L2) -> Logistic:
    """Class-balanced L2 logistic regression by full-batch gradient descent."""
    mean = X.mean(axis=0)
    Z = X - mean
    n, d = Z.shape
    pos = max(1, int(y.sum()))
    neg = max(1, n - pos)
    weight = np.where(y == 1, n / (2 * pos), n / (2 * neg))
    w = np.zeros(d)
    b = 0.0
    # Adam: the fingerprints are unit vectors, so feature scales are similar
    # and a fixed schedule converges without tuning per library.
    mw = np.zeros(d); vw = np.zeros(d); mb = vb = 0.0
    beta1, beta2, eps = 0.9, 0.999, 1e-8
    for t in range(1, ITERATIONS + 1):
        p = 1 / (1 + np.exp(-(Z @ w + b)))
        err = (p - y) * weight
        gw = Z.T @ err / n + l2 * w / n
        gb = float(err.mean())
        mw = beta1 * mw + (1 - beta1) * gw; vw = beta2 * vw + (1 - beta2) * gw * gw
        mb = beta1 * mb + (1 - beta1) * gb; vb = beta2 * vb + (1 - beta2) * gb * gb
        w -= LEARNING_RATE * (mw / (1 - beta1 ** t)) / (np.sqrt(vw / (1 - beta2 ** t)) + eps)
        b -= LEARNING_RATE * (mb / (1 - beta1 ** t)) / (np.sqrt(vb / (1 - beta2 ** t)) + eps)
    return Logistic(w, b, mean)


def auc(scores: np.ndarray, labels: np.ndarray) -> float | None:
    """Probability a random positive outscores a random negative (ties half)."""
    pos = scores[labels == 1]
    neg = scores[labels == 0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    order = np.argsort(np.concatenate([pos, neg]), kind="mergesort")
    ranks = np.empty(len(order))
    allv = np.concatenate([pos, neg])[order]
    # average ranks for ties
    i = 0
    while i < len(allv):
        j = i
        while j + 1 < len(allv) and allv[j + 1] == allv[i]:
            j += 1
        ranks[order[i : j + 1]] = (i + j) / 2 + 1
        i = j + 1
    r_pos = ranks[: len(pos)].sum()
    return float((r_pos - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def grouped_folds(groups: list[str], k: int = FOLDS) -> np.ndarray:
    """Fold id per row, every row of one group (ticker) in the same fold.
    Deterministic: groups are sorted and dealt round-robin."""
    uniq = sorted(set(groups))
    k = max(2, min(k, len(uniq)))
    fold_of = {g: i % k for i, g in enumerate(uniq)}
    return np.array([fold_of[g] for g in groups])


@dataclass
class Evaluation:
    method: str
    setup_vs_random_auc: float | None
    references: int
    controls: int
    outcome_auc: float | None
    worked: int
    failed: int
    warnings: list[str]
    oof_reference_scores: np.ndarray


def evaluate(X: np.ndarray, y: np.ndarray, groups: list[str], days: list, labels: list[str]) -> Evaluation:
    """Out-of-sample scores for every row, then the two questions:
    does the score separate setups from random days, and among the setups,
    does it separate the ones that worked from the ones that failed?"""
    oof = np.full(len(y), np.nan)
    ordinals = np.array([d.toordinal() for d in days])
    ref_ordinals = ordinals[y == 1]
    span = int(ref_ordinals.max() - ref_ordinals.min()) if len(ref_ordinals) else 0

    if span >= CHRONO_MIN_SPAN_DAYS:
        method = "chronological"
        cut = np.quantile(ref_ordinals, CHRONO_TRAIN_FRACTION)
        train = ordinals < cut
        test = ~train
        if train.sum() and test.sum() and len(set(y[train])) == 2:
            oof[test] = fit(X[train], y[train]).logit(X[test])
    else:
        method = "grouped_by_ticker"
        folds = grouped_folds(groups)
        for f in np.unique(folds):
            train = folds != f
            if len(set(y[train])) < 2:
                continue
            oof[~train] = fit(X[train], y[train]).logit(X[~train])

    mask = ~np.isnan(oof)
    separation = auc(oof[mask], y[mask]) if mask.any() else None

    ref_idx = np.where(y == 1)[0]
    ref_scores = oof[ref_idx]
    lab = np.array(labels)
    decided = (lab == "worked") | (lab == "failed")
    usable = decided & ~np.isnan(ref_scores)
    worked = int((lab[usable] == "worked").sum())
    failed = int((lab[usable] == "failed").sum())
    outcome = auc(ref_scores[usable], (lab[usable] == "worked").astype(int)) if usable.any() else None

    warnings: list[str] = []
    n_refs = int(y.sum())
    if n_refs < MIN_REFERENCES_TO_TRUST:
        warnings.append(
            f"Built from {n_refs} reference charts; at least {MIN_REFERENCES_TO_TRUST} are needed before "
            "the separation score means anything. Treat the matches as a demonstration."
        )
    if method != "chronological":
        warnings.append(
            "All reference charts fall inside one year, so the test could not be run the way it will be "
            "used (learn on earlier charts, score later ones)."
        )
    if min(worked, failed) < MIN_OUTCOMES_TO_JUDGE:
        warnings.append(
            f"Only {worked} worked and {failed} failed references have finished; at least "
            f"{MIN_OUTCOMES_TO_JUDGE} of each are needed to say whether resemblance predicts a move."
        )
        outcome = outcome if min(worked, failed) > 0 else None

    return Evaluation(method, separation, n_refs, int(len(y) - n_refs), outcome, worked, failed, warnings, ref_scores)


def percentile_against(scores: np.ndarray, reference_scores: np.ndarray) -> np.ndarray:
    """Share of `reference_scores` (calibration days) each score beats, 0-100."""
    ref = np.sort(reference_scores)
    if len(ref) == 0:
        return np.full(len(scores), np.nan)
    return np.searchsorted(ref, scores, side="right") / len(ref) * 100


LEARNING_CURVE_SIZES = (25, 50, 100, 200, 400, 800, 1600, 3200)
LEARNING_CURVE_REPEATS = 5
MIN_REFERENCES_FOR_CURVE = 60


def learning_curve(X: np.ndarray, y: np.ndarray, days: list, seed: int = 0) -> list[dict]:
    """Out-of-sample separation as a function of how many reference charts the
    model learned from. The test set is fixed — the latest 30% of references
    and every row dated after the cut — and only the training set shrinks, so
    the points differ in one thing. Each size is the mean of several random
    draws. This is the answer to "would more charts help?": a curve still
    rising at its last point says yes; a flat one says the charts are not
    the constraint.

    Empty when the library does not span a year, because without a time split
    the test would be scored on charts from the same weeks it learned from."""
    ordinals = np.array([d.toordinal() for d in days])
    ref_ord = ordinals[y == 1]
    if len(ref_ord) < MIN_REFERENCES_FOR_CURVE or int(ref_ord.max() - ref_ord.min()) < CHRONO_MIN_SPAN_DAYS:
        return []
    cut = np.quantile(ref_ord, CHRONO_TRAIN_FRACTION)
    train, test = ordinals < cut, ordinals >= cut
    pos_idx = np.where(train & (y == 1))[0]
    neg_idx = np.where(train & (y == 0))[0]
    if len(set(y[test])) < 2:
        return []
    ratio = len(neg_idx) / max(1, len(pos_idx))
    rng = np.random.default_rng(seed)
    sizes = [n for n in LEARNING_CURVE_SIZES if n < len(pos_idx)] + [len(pos_idx)]
    curve = []
    for n in sizes:
        scores = []
        repeats = 1 if n == len(pos_idx) else LEARNING_CURVE_REPEATS
        for _ in range(repeats):
            p = rng.choice(pos_idx, size=n, replace=False)
            q = rng.choice(neg_idx, size=min(len(neg_idx), int(round(n * ratio))), replace=False)
            rows = np.concatenate([p, q])
            clf = fit(X[rows], y[rows])
            a = auc(clf.logit(X[test]), y[test])
            if a is not None:
                scores.append(a)
        if scores:
            curve.append({"charts": int(n), "auc": round(float(np.mean(scores)), 3)})
    return curve
