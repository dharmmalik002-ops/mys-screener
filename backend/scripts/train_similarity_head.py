#!/usr/bin/env python3
"""Train the chart-trained similarity layer, and test it before it is used.

    python3 scripts/train_similarity_head.py              # ~20 min on Apple GPU
    python3 scripts/train_similarity_head.py --dates 2    # quick check

Data: Indian charts from data/deep_history/ at random past dates, at each of the
60 / 120 / 250-session scales. Each chart is paired with the same stock's chart
a few sessions later (`SHIFT`) — the same pattern, slightly moved — and the
layer learns to find that twin among every other chart in a batch
(InfoNCE / contrastive learning).

Test: a fifth of the stocks never take part in training. For each of their
charts, rank its twin against every other held-out chart of the same scale,
once with the raw image fingerprint and once through the trained layer. The
layer is switched on (`use_head`) only if it finds the twin more often. The
result is written to data/lookalike_state/similarity_report.json.

Needs torch + transformers (workstation only). Fingerprints are cached in the
private data/lookalike/ so a re-run trains in a minute.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import embed, projection, render, scoring  # noqa: E402
from app.services.lookalike.pipeline import library_dir  # noqa: E402

SHIFT = (2, 3, 4, 5, 6)
# The twin is also stretched or squeezed in time: the same base taking 15%
# more or fewer sessions. "Same idea, different numbers" — a 6-week base and
# a 7-week one should still be found as the same pattern.
STRETCH = (0.85, 1.0, 1.15)
HOLDOUT_SHARE = 0.2
HIDDEN, OUT = 256, 128
TEMPERATURE = 0.07
EPOCHS = 40
BATCH = 256
CACHE = "similarity_training.npz"


def held_out(symbol: str) -> bool:
    return (zlib.crc32(symbol.encode()) % 1000) / 1000 < HOLDOUT_SHARE


def build_pairs(universe, dates_per_symbol: int, seed: int = 0):
    rng = np.random.default_rng(seed)
    anchors, twins, meta = [], [], []
    for b in universe:
        n = len(b)
        for scale in render.SCALES:
            lo = render.min_bars_needed(scale) + 5
            hi = n - max(SHIFT) - 1
            if hi <= lo:
                continue
            for end in rng.choice(np.arange(lo, hi), size=min(dates_per_symbol, hi - lo), replace=False):
                k = int(rng.choice(SHIFT))
                stretch = float(rng.choice(STRETCH))
                twin_window = int(round(scale * stretch))
                if int(end) + k < render.min_bars_needed(twin_window) - 1:
                    continue
                a = render.picture(b.open, b.high, b.low, b.close, b.volume, int(end), scale)
                t = render.picture(b.open, b.high, b.low, b.close, b.volume, int(end) + k, twin_window)
                if a is None or t is None:
                    continue
                anchors.append(a[0])
                twins.append(t[0])
                meta.append((b.symbol, scale))
    return anchors, twins, meta


def retrieval(A: np.ndarray, T: np.ndarray) -> dict:
    """For each anchor, the rank of its own twin among all twins (1 = found)."""
    A = A / (np.linalg.norm(A, axis=1, keepdims=True) + 1e-12)
    T = T / (np.linalg.norm(T, axis=1, keepdims=True) + 1e-12)
    S = A @ T.T
    own = np.diag(S)
    ranks = (S > own[:, None]).sum(axis=1) + 1
    return {
        "candidates": int(len(T)),
        "top1_pct": round(float((ranks == 1).mean() * 100), 1),
        "top5_pct": round(float((ranks <= 5).mean() * 100), 1),
        "median_rank": float(np.median(ranks)),
    }


def train(Xa: np.ndarray, Xt: np.ndarray, epochs: int):
    import torch

    device = "mps" if torch.backends.mps.is_available() else "cpu"
    torch.manual_seed(0)
    net = torch.nn.Sequential(torch.nn.Linear(Xa.shape[1], HIDDEN), torch.nn.ReLU(), torch.nn.Linear(HIDDEN, OUT)).to(device)
    opt = torch.optim.Adam(net.parameters(), lr=1e-3, weight_decay=1e-4)
    A = torch.from_numpy(Xa).float().to(device)
    T = torch.from_numpy(Xt).float().to(device)
    n = len(A)
    for epoch in range(epochs):
        perm = torch.randperm(n, device=device)
        total = 0.0
        for s in range(0, n - 1, BATCH):
            idx = perm[s : s + BATCH]
            za = torch.nn.functional.normalize(net(A[idx]), dim=1)
            zt = torch.nn.functional.normalize(net(T[idx]), dim=1)
            logits = za @ zt.T / TEMPERATURE
            target = torch.arange(len(idx), device=device)
            loss = (torch.nn.functional.cross_entropy(logits, target) + torch.nn.functional.cross_entropy(logits.T, target)) / 2
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(idx)
        if epoch % 10 == 0 or epoch == epochs - 1:
            print(f"  epoch {epoch + 1}/{epochs}: loss {total / n:.3f}", flush=True)
    w = [p.detach().cpu().numpy() for p in net.parameters()]
    # torch Linear stores (out, in); the numpy head multiplies X @ W
    return projection.Head(w[0].T.copy(), w[1].copy(), w[2].T.copy(), w[3].copy())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--dates", type=int, default=4, help="random past dates per stock per scale")
    parser.add_argument("--epochs", type=int, default=EPOCHS)
    parser.add_argument("--rebuild", action="store_true", help="ignore cached fingerprints")
    args = parser.parse_args()
    t0 = time.time()

    cache = args.data_dir / "lookalike" / CACHE
    if cache.exists() and not args.rebuild:
        z = np.load(cache, allow_pickle=True)
        Xa, Xt, symbols, scales = z["Xa"], z["Xt"], list(z["symbols"]), list(z["scales"])
        print(f"cached fingerprints: {len(Xa)} pairs")
    else:
        universe, _ = scoring.load_universe(args.data_dir)
        print(f"universe {len(universe)} symbols; rendering and fingerprinting pairs…", flush=True)
        # In chunks of stocks: the pictures are ~150 KB each and there are tens
        # of thousands; only the fingerprints are kept.
        fa, ft, symbols, scales = [], [], [], []
        chunk = 40
        for k in range(0, len(universe), chunk):
            anchors, twins, meta = build_pairs(universe[k : k + chunk], args.dates, seed=k)
            if anchors:
                fa.append(embed.fingerprints(anchors))
                ft.append(embed.fingerprints(twins))
                symbols += [m[0] for m in meta]
                scales += [m[1] for m in meta]
            if (k // chunk) % 5 == 0:
                print(f"  {min(k + chunk, len(universe))}/{len(universe)} stocks, {len(symbols)} pairs ({time.time() - t0:.0f}s)", flush=True)
        Xa, Xt = np.concatenate(fa), np.concatenate(ft)
        cache.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(cache, Xa=Xa, Xt=Xt, symbols=np.array(symbols), scales=np.array(scales))
        print(f"fingerprints done ({time.time() - t0:.0f}s)")

    test = np.array([held_out(s) for s in symbols])
    scales = np.array(scales)
    print(f"train pairs {int((~test).sum())}, held-out pairs {int(test.sum())} "
          f"({len({s for s, t in zip(symbols, test) if t})} stocks never seen in training)")
    head = train(Xa[~test], Xt[~test], args.epochs)

    report = {"trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "by_scale": {}}
    for scale in render.SCALES:
        m = test & (scales == scale)
        if m.sum() < 50:
            continue
        raw = retrieval(Xa[m], Xt[m])
        trained = retrieval(head(Xa[m]), head(Xt[m]))
        report["by_scale"][str(scale)] = {"raw_fingerprint": raw, "trained_layer": trained}
        print(f"  {scale} sessions, {raw['candidates']} held-out charts: "
              f"finds its twin first time raw {raw['top1_pct']}% -> trained {trained['top1_pct']}% "
              f"(top-5 {raw['top5_pct']}% -> {trained['top5_pct']}%; median rank {raw['median_rank']} -> {trained['median_rank']})")
    gains = [v["trained_layer"]["top1_pct"] - v["raw_fingerprint"]["top1_pct"] for v in report["by_scale"].values()]
    report["use_head"] = bool(gains) and min(gains) > 0
    report["verdict"] = (
        "The trained layer finds the twin more often at every scale on stocks it never saw, so it is used for similarity."
        if report["use_head"] else
        "The trained layer did not beat the raw fingerprint at every scale on held-out stocks, so it is NOT used."
    )
    state = library_dir(args.data_dir)
    state.mkdir(parents=True, exist_ok=True)
    np.savez(state / projection.HEAD_FILE, W1=head.W1.astype(np.float32), b1=head.b1.astype(np.float32),
             W2=head.W2.astype(np.float32), b2=head.b2.astype(np.float32))
    (state / projection.REPORT_FILE).write_text(json.dumps(report, indent=1))
    print(report["verdict"], f"({time.time() - t0:.0f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
