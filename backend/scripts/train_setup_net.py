#!/usr/bin/env python3
"""Teach the image model Zanger's setup names, and test whether it recognises
them better than the current per-setup models.

    python3 scripts/train_setup_net.py            # ~1-1.5 h on an Apple GPU
    python3 scripts/train_setup_net.py --epochs 1 --limit 2000   # quick check
    python3 scripts/train_setup_net.py --calibrate                # after training: the
        # ordinary-day scores "beats X% of ordinary charts" is measured against

Today each setup is recognised by a straight-line model on frozen DINOv2
fingerprints ("setup X vs ordinary days in the same stocks"). Here the image
model itself learns: its last TRAIN_BLOCKS transformer blocks are retrained,
with one head that sorts every chart into one of his setups or "ordinary day".

Data: every Yahoo-priced reference in the zanger_<setup> styles (chart-read
references stay out, as in the classifier — gotcha 147), redrawn in the house
style, plus CONTROLS_PER_REF random days in the same stocks. Split by date:
the newest TEST_SHARE of references (and controls dated after the cut) are
never trained on.

The test, per setup: how well P(setup) separates that setup's unseen charts
from unseen ordinary days (AUC), against the straight-line model refitted on
the same training split. The verdict and per-setup numbers go to
data/lookalike_state/setup_net_report.json; the weights to
data/lookalike_state/setup_net.npz (only the retrained part, float16), and a
setup uses the net only where it beat the straight-line model by MIN_GAIN.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import zlib
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import chart_bars, embed, model, render, scoring, us_bars  # noqa: E402
from app.services.lookalike.pipeline import CONTROL_EXCLUSION_SESSIONS, library_dir  # noqa: E402

SETUPS = ("base", "channel", "cup_handle", "double_bottom", "flag", "gap", "head_shoulders", "trendline", "triangle", "wedge")
CLASSES = SETUPS + ("ordinary",)
TRAIN_BLOCKS = 2
CONTROLS_PER_REF = 3
TEST_SHARE = 0.3
EPOCHS = 3
BATCH = 48
MIN_GAIN = 0.02
NET_FILE = "setup_net.npz"
REPORT_FILE = "setup_net_report.json"


def auc(pos: np.ndarray, neg: np.ndarray) -> float | None:
    if len(pos) < 10 or len(neg) < 10:
        return None
    scores = np.concatenate([pos, neg])
    ranks = scores.argsort().argsort() + 1
    return float((ranks[: len(pos)].sum() - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg)))


def gather(data_dir: Path, limit: int | None):
    """[(series, idx, class, date, ticker)] for references and their controls."""
    lib = scoring.load_library(data_dir)
    use = chart_bars.verdicts(data_dir)
    items, seen = [], set()
    by_ticker: dict[str, list] = defaultdict(list)
    for k, setup in enumerate(SETUPS):
        for r in lib.refs.get(f"zanger_{setup}", []):
            if r.get("prices") or use.get(r["key"], "yahoo") != "yahoo" or r["key"] in seen:
                continue
            seen.add(r["key"])
            by_ticker[r["ticker"]].append((date.fromisoformat(r["session"]), k))
    cache: dict[str, us_bars.Series | None] = {}
    tickers = sorted(by_ticker)
    for n, t in enumerate(tickers, 1):
        s = us_bars._read(us_bars.cache_dir(data_dir) / f"{t}.json")
        if s is None or not s.dates:
            continue
        idxs = []
        for day, k in by_ticker[t]:
            i = s.index_on_or_before(day)
            if i is None or s.dates[i] != day or i < render.min_bars_needed() - 1:
                continue
            items.append((t, i, k, day))
            idxs.append(i)
        cache[t] = s
        lo, hi = render.min_bars_needed() - 1, len(s.dates) - 1
        allowed = [i for i in range(lo, hi + 1) if all(abs(i - r) >= CONTROL_EXCLUSION_SESSIONS for r in idxs)]
        rng = np.random.default_rng(zlib.crc32(t.encode()))
        for p in rng.choice(len(allowed), size=min(len(allowed), CONTROLS_PER_REF * len(idxs)), replace=False) if allowed else []:
            items.append((t, allowed[p], len(SETUPS), s.dates[allowed[p]]))
        if limit and len(items) >= limit:
            break
    return items, cache


def picture(cache, item):
    t, i = item[0], item[1]
    s = cache[t]
    pic = render.picture(s.o, s.h, s.l, s.c, s.v, i)
    return pic[0] if pic else None


def to_tensor(images, torch, device):
    arr = np.stack([np.asarray(im.convert("RGB"), dtype=np.float32) / 255.0 for im in images])
    arr = (arr - embed._MEAN) / embed._STD
    return torch.from_numpy(arr.transpose(0, 3, 1, 2)).to(device)


class SetupNet:
    """DINOv2-small with its last TRAIN_BLOCKS blocks retrained and a setup head."""

    def __init__(self, torch):
        from transformers import AutoModel

        self.torch = torch
        self.device = "mps" if torch.backends.mps.is_available() else "cpu"
        self.base = AutoModel.from_pretrained(embed.MODEL_ID).to(self.device).eval()
        for p in self.base.parameters():
            p.requires_grad = False
        self.blocks = self.base.encoder.layer
        self.split = len(self.blocks) - TRAIN_BLOCKS
        for blk in self.blocks[self.split:]:
            for p in blk.parameters():
                p.requires_grad = True
        for p in self.base.layernorm.parameters():
            p.requires_grad = True
        self.head = torch.nn.Linear(768, len(CLASSES)).to(self.device)

    def trainable(self):
        ps = [p for blk in self.blocks[self.split:] for p in blk.parameters()]
        return ps + list(self.base.layernorm.parameters()) + list(self.head.parameters())

    @staticmethod
    def _run(layer, h):
        out = layer(h)
        return out[0] if isinstance(out, tuple) else out

    def features(self, x, train: bool):
        torch = self.torch
        with torch.no_grad():
            h = self.base.embeddings(x)
            for blk in self.blocks[: self.split]:
                h = self._run(blk, h)
        h = h.detach()
        with torch.set_grad_enabled(train):
            for blk in self.blocks[self.split:]:
                h = self._run(blk, h)
            h = self.base.layernorm(h)
            return torch.cat([h[:, 0], h[:, 1:].mean(dim=1)], dim=1)

    def logits(self, x, train: bool = False):
        return self.head(self.features(x, train))


def calibrate(data_dir: Path, cache, ordinary) -> int:
    """Log-odds of held-out ordinary days (never trained on) for each setup the
    net is used for — what a stock's score is ranked against."""
    from app.services.lookalike import setup_net

    state = library_dir(data_dir)
    report = json.loads((state / REPORT_FILE).read_text())
    arrays = dict(np.load(state / NET_FILE))
    use = [s for s, r in report["setups"].items() if r["use_net"] and r["test_setups"] >= setup_net.MIN_UNSEEN]
    net = setup_net.SetupNet(classes=report["classes"], use=use, auc={}, weights={k: v for k, v in arrays.items() if not k.startswith("cal_")})
    images = [im for im in (picture(cache, it) for it in ordinary) if im is not None]
    lo = net.log_odds(images)
    for s in use:
        arrays[f"cal_{s}"] = lo[s].astype(np.float32)
    np.savez_compressed(state / NET_FILE, **arrays)
    print(f"calibrated {use} on {len(images)} unseen ordinary days")
    return 0


def main() -> int:
    import torch

    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", type=Path, default=ROOT / "data")
    ap.add_argument("--epochs", type=int, default=EPOCHS)
    ap.add_argument("--limit", type=int)
    ap.add_argument("--calibrate", action="store_true", help="score held-out ordinary days with the saved net")
    args = ap.parse_args()
    t0 = time.time()
    items, cache = gather(args.data_dir, args.limit)
    ref_days = sorted(it[3] for it in items if it[2] < len(SETUPS))
    cut = ref_days[int(len(ref_days) * (1 - TEST_SHARE))]
    train = [it for it in items if it[3] < cut]
    test = [it for it in items if it[3] >= cut]
    if args.calibrate:
        return calibrate(args.data_dir, cache, [it for it in test if it[2] == len(SETUPS)])
    counts = {c: sum(1 for it in items if it[2] == k) for k, c in enumerate(CLASSES)}
    print(f"{len(items)} charts {counts}; train {len(train)} / test {len(test)} (cut {cut}) ({time.time() - t0:.0f}s)", flush=True)

    torch.manual_seed(0)
    net = SetupNet(torch)
    n_cls = np.array([max(1, sum(1 for it in train if it[2] == k)) for k in range(len(CLASSES))], dtype=np.float32)
    weights = torch.tensor((n_cls.sum() / n_cls) / (n_cls.sum() / n_cls).mean(), device=net.device)
    loss_fn = torch.nn.CrossEntropyLoss(weight=weights)
    opt = torch.optim.AdamW([
        {"params": [p for blk in net.blocks[net.split:] for p in blk.parameters()] + list(net.base.layernorm.parameters()), "lr": 2e-5},
        {"params": net.head.parameters(), "lr": 1e-3},
    ], weight_decay=1e-4)
    rng = np.random.default_rng(0)
    for epoch in range(args.epochs):
        order = rng.permutation(len(train))
        total, seen = 0.0, 0
        for s in range(0, len(order), BATCH):
            batch = [train[j] for j in order[s:s + BATCH]]
            imgs, ys = [], []
            for it in batch:
                im = picture(cache, it)
                if im is not None:
                    imgs.append(im)
                    ys.append(it[2])
            if not imgs:
                continue
            x = to_tensor(imgs, torch, net.device)
            y = torch.tensor(ys, device=net.device)
            loss = loss_fn(net.logits(x, train=True), y)
            opt.zero_grad()
            loss.backward()
            opt.step()
            total += float(loss.detach()) * len(ys)
            seen += len(ys)
            if (s // BATCH) % 100 == 0:
                print(f"  epoch {epoch + 1} {s}/{len(order)} loss {total / max(1, seen):.3f} ({time.time() - t0:.0f}s)", flush=True)
                if net.device == "mps":
                    torch.mps.empty_cache()
        print(f"epoch {epoch + 1}: loss {total / max(1, seen):.3f} ({time.time() - t0:.0f}s)", flush=True)

    # evaluation: the net's P(setup), and the frozen-fingerprint straight-line model refitted on the same split
    def run(split):
        P, F, Y = [], [], []
        for s in range(0, len(split), 64):
            batch = split[s:s + 64]
            imgs, ys = [], []
            for it in batch:
                im = picture(cache, it)
                if im is not None:
                    imgs.append(im)
                    ys.append(it[2])
            if not imgs:
                continue
            x = to_tensor(imgs, torch, net.device)
            with torch.no_grad():
                P.append(torch.softmax(net.logits(x), dim=1).float().cpu().numpy())
            F.append(embed.fingerprints(imgs))
            Y.extend(ys)
            if net.device == "mps":
                torch.mps.empty_cache()
        return np.concatenate(P), np.concatenate(F), np.array(Y)

    print("evaluating…", flush=True)
    P_te, F_te, Y_te = run(test)
    _, F_tr, Y_tr = run(train)
    ordinary = len(SETUPS)
    report = {"trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "cut": cut.isoformat(),
              "train": len(train), "test": len(test), "epochs": args.epochs, "train_blocks": TRAIN_BLOCKS, "setups": {}}
    for k, setup in enumerate(SETUPS):
        pos_te, neg_te = Y_te == k, Y_te == ordinary
        net_auc = auc(P_te[pos_te, k], P_te[neg_te, k])
        tr = (Y_tr == k) | (Y_tr == ordinary)
        lin_auc = None
        if (Y_tr == k).sum() >= 20:
            clf = model.fit(F_tr[tr], (Y_tr[tr] == k).astype(float))
            lg = clf.logit(F_te)
            lin_auc = auc(lg[pos_te], lg[neg_te])
        use = net_auc is not None and lin_auc is not None and net_auc >= lin_auc + MIN_GAIN
        report["setups"][setup] = {"test_setups": int(pos_te.sum()), "net_auc": None if net_auc is None else round(net_auc, 3),
                                   "straight_line_auc": None if lin_auc is None else round(lin_auc, 3), "use_net": bool(use)}
        print(f"  {setup:15s} unseen {int(pos_te.sum()):4d}: net {net_auc} vs straight-line {lin_auc} -> {'NET' if use else 'keep'}", flush=True)
    # which setup is it? among unseen setup charts, is the top setup class the right one
    is_setup = Y_te < ordinary
    top = P_te[is_setup, :ordinary].argmax(axis=1)
    acc = float((top == Y_te[is_setup]).mean()) if is_setup.any() else None
    share = np.bincount(Y_te[is_setup], minlength=ordinary) / max(1, is_setup.sum())
    report["names_the_setup_pct"] = None if acc is None else round(100 * acc, 1)
    report["guessing_the_commonest_pct"] = round(100 * float(share.max()), 1)
    print(f"names the right setup {report['names_the_setup_pct']}% (always guessing the commonest: {report['guessing_the_commonest_pct']}%)")

    state = library_dir(args.data_dir)
    weights_out = {}
    for j, blk in enumerate(net.blocks[net.split:]):
        for name, p in blk.state_dict().items():
            weights_out[f"block{j}.{name}"] = p.detach().float().cpu().numpy().astype(np.float16)
    for name, p in net.base.layernorm.state_dict().items():
        weights_out[f"layernorm.{name}"] = p.detach().float().cpu().numpy().astype(np.float16)
    for name, p in net.head.state_dict().items():
        weights_out[f"head.{name}"] = p.detach().float().cpu().numpy().astype(np.float16)
    np.savez_compressed(state / NET_FILE, **weights_out)
    report["classes"] = list(CLASSES)
    (state / REPORT_FILE).write_text(json.dumps(report, indent=1))
    print(f"done ({time.time() - t0:.0f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
