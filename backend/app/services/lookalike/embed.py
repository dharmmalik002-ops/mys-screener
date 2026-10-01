"""DINOv2 fingerprints for rendered charts.

DINOv2 (Meta, Apache 2.0) was trained to describe images by their structure
without labels, which is what "the same idea with different numbers" needs:
two bases with 3% and 6% last contractions produce nearby fingerprints. The
small variant (22M parameters) runs on a laptop CPU or Apple GPU in well under
a second per batch.

The fingerprint is the CLS token concatenated with the mean of the patch tokens,
L2-normalised, so cosine similarity is a dot product. Pictures are fed at
exactly 224x224 and normalised here: the stock image processor resizes to 256
and centre-crops, which would cut off the most recent sessions — the part of a
setup that matters most.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Sequence

import numpy as np

MODEL_ID = "facebook/dinov2-small"
_MEAN = np.array([0.485, 0.456, 0.406], dtype=np.float32)
_STD = np.array([0.229, 0.224, 0.225], dtype=np.float32)


@lru_cache(maxsize=1)
def _model():
    import torch
    from transformers import AutoModel

    model = AutoModel.from_pretrained(MODEL_ID)
    model.eval()
    device = "mps" if torch.backends.mps.is_available() else "cuda" if torch.cuda.is_available() else "cpu"
    return model.to(device), device


def fingerprints(images: Sequence, batch_size: int = 32) -> np.ndarray:
    """(n, 768) float32, one L2-normalised row per image."""
    import torch

    if not images:
        return np.zeros((0, 768), dtype=np.float32)
    model, device = _model()
    out: list[np.ndarray] = []
    for start in range(0, len(images), batch_size):
        batch = images[start : start + batch_size]
        arr = np.stack([np.asarray(img.convert("RGB"), dtype=np.float32) / 255.0 for img in batch])
        arr = (arr - _MEAN) / _STD
        tensor = torch.from_numpy(arr.transpose(0, 3, 1, 2)).to(device)
        with torch.no_grad():
            hidden = model(pixel_values=tensor).last_hidden_state
        feats = torch.cat([hidden[:, 0], hidden[:, 1:].mean(dim=1)], dim=1).float().cpu().numpy()
        out.append(feats)
    feats = np.concatenate(out)
    feats /= np.linalg.norm(feats, axis=1, keepdims=True) + 1e-12
    return feats.astype(np.float32)
