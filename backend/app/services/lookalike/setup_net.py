"""The image model retrained on Zanger's setup names (scripts/train_setup_net.py).

DINOv2-small with its last blocks retrained and a head that sorts a chart into
one of his setups or "ordinary day". It is used only for the setups where it
beat the straight-line model on unseen charts (`use_net` in the report, at
least MIN_UNSEEN of them): for those, a stock's resemblance score is the net's
log-odds for that setup, turned into "beats X% of ordinary charts" against
held-out ordinary days (`cal_<setup>`). Everything else — similarity, nearest
examples, picks — is unchanged.

Weights live in the committed state folder as float16 (only the retrained
part); torch + transformers load them on the workstation and the GitHub runner.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

NET_FILE = "setup_net.npz"
REPORT_FILE = "setup_net_report.json"
MIN_UNSEEN = 50


@dataclass
class SetupNet:
    classes: list[str]
    use: list[str]                       # setups scored by the net
    auc: dict[str, float]                # its unseen-chart score per setup
    cal: dict[str, np.ndarray] = field(default_factory=dict)
    weights: dict[str, np.ndarray] = field(default_factory=dict)
    _model: object = None

    def _build(self):
        import torch
        from transformers import AutoModel

        from . import embed

        base = AutoModel.from_pretrained(embed.MODEL_ID).eval()
        n_trained = len({k.split(".")[0] for k in self.weights if k.startswith("block")})
        blocks = base.encoder.layer[len(base.encoder.layer) - n_trained:]
        for j, blk in enumerate(blocks):
            blk.load_state_dict({k.split(".", 1)[1]: torch.from_numpy(v.astype(np.float32))
                                 for k, v in self.weights.items() if k.startswith(f"block{j}.")})
        base.layernorm.load_state_dict({k.split(".", 1)[1]: torch.from_numpy(v.astype(np.float32))
                                        for k, v in self.weights.items() if k.startswith("layernorm.")})
        head = torch.nn.Linear(768, len(self.classes))
        head.load_state_dict({k.split(".", 1)[1]: torch.from_numpy(v.astype(np.float32))
                              for k, v in self.weights.items() if k.startswith("head.")})
        device = "mps" if torch.backends.mps.is_available() else "cpu"
        self._model = (base.to(device), head.to(device).eval(), device)

    def log_odds(self, images, batch: int = 32) -> dict[str, np.ndarray]:
        """{setup: log-odds that each picture is that setup} for the setups in `use`."""
        import torch

        from . import embed

        if self._model is None:
            self._build()
        base, head, device = self._model
        out = []
        for s in range(0, len(images), batch):
            arr = np.stack([np.asarray(im.convert("RGB"), dtype=np.float32) / 255.0 for im in images[s:s + batch]])
            arr = (arr - embed._MEAN) / embed._STD
            x = torch.from_numpy(arr.transpose(0, 3, 1, 2)).to(device)
            with torch.no_grad():
                h = base(pixel_values=x).last_hidden_state
                feats = torch.cat([h[:, 0], h[:, 1:].mean(dim=1)], dim=1)
                out.append(torch.softmax(head(feats), dim=1).float().cpu().numpy())
        P = np.concatenate(out) if out else np.zeros((0, len(self.classes)))
        res = {}
        for setup in self.use:
            p = np.clip(P[:, self.classes.index(setup)], 1e-6, 1 - 1e-6)
            res[setup] = np.log(p / (1 - p))
        return res


def load(state_dir: Path) -> SetupNet | None:
    """The net, or None when it was never trained or wins no setup."""
    try:
        report = json.loads((state_dir / REPORT_FILE).read_text())
        arrays = dict(np.load(state_dir / NET_FILE))
    except (OSError, ValueError):
        return None
    use = [s for s, r in (report.get("setups") or {}).items()
           if r.get("use_net") and (r.get("test_setups") or 0) >= MIN_UNSEEN and f"cal_{s}" in arrays]
    if not use:
        return None
    return SetupNet(
        classes=report["classes"],
        use=use,
        auc={s: report["setups"][s]["net_auc"] for s in use},
        cal={s: arrays[f"cal_{s}"].astype(np.float64) for s in use},
        weights={k: v for k, v in arrays.items() if not k.startswith("cal_")},
    )
