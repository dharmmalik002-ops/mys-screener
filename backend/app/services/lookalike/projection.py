"""The chart-trained layer on top of the image fingerprint.

DINOv2 learned to describe photographs, not price charts, so two charts of the
same base a few sessions apart can land further apart than two unrelated
charts that share a colour balance. This small two-layer network is trained
(`scripts/train_similarity_head.py`) on thousands of Indian charts to put the
same pattern together and different patterns apart: each chart is paired with
the same stock's chart a few sessions later, and the network learns to pick
that twin out of a crowd of other charts.

It is used for SIMILARITY only — which references and which stocks look alike.
The style classifier that decides how much a chart looks like a trader's
setups still reads the raw fingerprint it was validated on.

Inference is plain numpy, so the GitHub runner needs no extra dependency. The
weights live in the committed state folder; without them the layer is the
identity and similarity falls back to the raw fingerprint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np

HEAD_FILE = "similarity_head.npz"
REPORT_FILE = "similarity_report.json"


@dataclass
class Head:
    W1: np.ndarray
    b1: np.ndarray
    W2: np.ndarray
    b2: np.ndarray

    def __call__(self, X: np.ndarray) -> np.ndarray:
        Z = np.maximum(X @ self.W1 + self.b1, 0.0) @ self.W2 + self.b2
        return Z / (np.linalg.norm(Z, axis=1, keepdims=True) + 1e-12)


def _state(data_dir: Path) -> Path:
    from .pipeline import library_dir

    return library_dir(data_dir)


def load(data_dir: Path) -> Head | None:
    """The trained layer, or None when it was never trained or its own test
    said it did not beat the raw fingerprint (`use_head` in the report)."""
    path = _state(data_dir) / HEAD_FILE
    report = _state(data_dir) / REPORT_FILE
    if not path.exists():
        return None
    try:
        if report.exists() and not json.loads(report.read_text()).get("use_head", False):
            return None
        a = np.load(path)
        return Head(a["W1"], a["b1"], a["W2"], a["b2"])
    except Exception:
        return None


def embed(head: Head | None, X: np.ndarray) -> np.ndarray:
    if head is None:
        return X / (np.linalg.norm(X, axis=1, keepdims=True) + 1e-12)
    return head(X)
