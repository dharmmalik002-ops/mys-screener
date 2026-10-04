"""An AI second opinion on one chart: does it show this setup?

A vision model (Gemini, the app's existing AI) is shown the chart drawn large
— the same 120 sessions, 50-day average and volume the look-alike model saw —
with the setup described in our own words (the setup guide), and asked one
question: how well does the latest part of this chart fit the setup. It
describes the shape; it does not advise (gotcha 12): the prompt forbids buy,
sell, target and stop talk, and the answer is shown as a description.

Asked on demand from the page (one click per chart), cached per
(style, symbol, date) in APP_STATE_DIR, so a chart is only ever reviewed once.
"""

from __future__ import annotations

import io
import json
import logging
import threading
from pathlib import Path

from . import render

logger = logging.getLogger(__name__)

CACHE_FILE = "lookalike_ai_reviews.json"
MODELS = ("gemini-2.5-flash", "gemini-2.0-flash")
VERDICTS = ("yes", "partly", "no")
_lock = threading.Lock()

MINERVINI = (
    "Mark Minervini's style: a stock in a strong stage-2 uptrend (above rising 50/150/200-day averages, near its "
    "52-week high) building a base whose pullbacks get smaller from left to right (volatility contraction), with "
    "volume drying up near the right side before a breakout."
)


def describe_setup(style: str, notes: dict | None, setup_label: str) -> str:
    if style == "minervini" or not notes:
        return MINERVINI if style.startswith("minervini") else f"Dan Zanger's {setup_label}."
    parts = [notes.get("summary") or ""]
    for item in (notes.get("what_it_looks_like") or [])[:6]:
        parts.append(f"- {item}")
    return "\n".join(p for p in parts if p)


def prompt(setup_label: str, description: str) -> str:
    return (
        "You are reviewing the shape of one daily stock chart. It shows the last 120 trading sessions as candles "
        "(grey = up day, black = down day), the 50-day moving average as a blue line, and volume as grey bars at the "
        "bottom. The right edge is the latest session.\n\n"
        f"Question: how well does the latest part of this chart fit this setup — {setup_label}?\n"
        f"How the setup is described:\n{description}\n\n"
        "Answer only about the chart's shape. Do not give buy or sell advice, price targets, stops or predictions.\n"
        'Reply as JSON: {"verdict": "yes" | "partly" | "no", "score": 1-5 (5 = a textbook example), '
        '"why": one plain-English sentence on what in the chart fits or does not, '
        '"look_for": one short sentence on which part of the chart shows it}.'
    )


class Reviewer:
    def __init__(self, api_key: str | None, state_dir: Path | None):
        self.api_key = api_key
        self.path = (state_dir / CACHE_FILE) if state_dir else None
        self._cache: dict | None = None
        self._client = None

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def _load(self) -> dict:
        if self._cache is None:
            try:
                self._cache = json.loads(self.path.read_text()) if self.path and self.path.exists() else {}
            except (OSError, ValueError):
                self._cache = {}
        return self._cache

    def cached(self, key: str) -> dict | None:
        return self._load().get(key)

    def review(self, key: str, chart: dict, setup_label: str, description: str) -> dict:
        hit = self.cached(key)
        if hit:
            return hit
        if not self.api_key:
            raise RuntimeError("No AI key is configured on the server.")
        img = render.draw_large({k: chart[k][: render.WINDOW] for k in ("o", "h", "l", "c", "v", "sma") if k in chart})
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        from google import genai

        if self._client is None:
            self._client = genai.Client(api_key=self.api_key)
        last_error = None
        for model_name in MODELS:
            try:
                resp = self._client.models.generate_content(
                    model=model_name,
                    contents=[genai.types.Part.from_bytes(data=buf.getvalue(), mime_type="image/png"), prompt(setup_label, description)],
                    config=genai.types.GenerateContentConfig(temperature=0.2, max_output_tokens=400, response_mime_type="application/json"),
                )
                raw = json.loads(resp.text)
                out = {
                    "verdict": raw.get("verdict") if raw.get("verdict") in VERDICTS else "partly",
                    "score": max(1, min(5, int(raw.get("score") or 3))),
                    "why": str(raw.get("why") or "")[:300],
                    "look_for": str(raw.get("look_for") or "")[:200],
                    "model": model_name,
                }
                break
            except Exception as exc:  # try the next model, then give up
                last_error = exc
                logger.info("ai review %s with %s failed: %s", key, model_name, exc)
        else:
            raise RuntimeError(f"The AI review failed: {last_error}")
        with _lock:
            cache = self._load()
            cache[key] = out
            if self.path:
                try:
                    self.path.parent.mkdir(parents=True, exist_ok=True)
                    self.path.write_text(json.dumps(cache))
                except OSError as exc:
                    logger.warning("ai review cache not saved: %s", exc)
        return out
