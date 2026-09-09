from __future__ import annotations

import concurrent.futures
import struct
import subprocess
import sys
import unittest
import warnings
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import chart_render as cr

PNG_MAGIC = b"\x89PNG\r\n\x1a\n"


def make_bars(count: int, *, base: float = 100.0, drift: float = 1.0, volume: int = 1000):
    bars = []
    for i in range(count):
        close = base + i * drift
        bars.append(
            {
                "time": 1_700_000_000 + i * 86_400,
                "open": close - 0.5,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": volume + i,
            }
        )
    return bars


def png_dimensions(data: bytes) -> tuple[int, int]:
    """Parse IHDR directly so the test does not depend on Pillow."""
    return struct.unpack(">II", data[16:24])


NOTE = cr.ChartAnnotation(
    symbol="TESTCO",
    name="Test Industries Ltd.",
    exchange="NSE",
    sector="Refineries",
    market_cap_crore=1_942_100,
    last_price=1412.60,
    change_pct=3.24,
    rs_rating=92,
    relative_volume=3.2,
    scanner_name="VCP Contraction",
    is_new=True,
    also_in=("Minervini 5M", "3 Tight Closes"),
    session_date="2026-08-20",
)


@unittest.skipUnless(cr.CHART_RENDER_AVAILABLE, "matplotlib not installed")
class ChartRenderPngTests(unittest.TestCase):
    def test_produces_a_png_of_the_requested_size(self) -> None:
        png = cr.render_candles_png(make_bars(520), NOTE)
        self.assertTrue(png.startswith(PNG_MAGIC))
        self.assertEqual(png_dimensions(png), (1280, 800))

    def test_dimensions_track_the_spec(self) -> None:
        # Catches figsize/dpi arithmetic errors.
        spec = cr.RenderSpec(width_px=800, height_px=500)
        png = cr.render_candles_png(make_bars(520), NOTE, spec)
        self.assertEqual(png_dimensions(png), (800, 500))

    def test_byte_length_proves_candles_were_actually_drawn(self) -> None:
        png = cr.render_candles_png(make_bars(520), NOTE)
        # A blank 1280x800 solid fill is only a few KB; the upper bound catches
        # an accidental dpi blowup. Loose enough to survive a matplotlib bump.
        self.assertGreater(len(png), 20_000)
        self.assertLess(len(png), 400_000)

    def test_render_is_deterministic(self) -> None:
        bars = make_bars(520)
        self.assertEqual(
            cr.render_candles_png(bars, NOTE), cr.render_candles_png(bars, NOTE)
        )

    def test_threaded_rendering_matches_serial_output(self) -> None:
        """The test that catches someone reintroducing pyplot or mutating
        rcParams -- both would randomly mis-style charts under concurrency."""
        jobs = [make_bars(300 + i * 10, base=50.0 + i) for i in range(8)]
        serial = [cr.render_candles_png(bars, NOTE) for bars in jobs]

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            threaded = list(pool.map(lambda bars: cr.render_candles_png(bars, NOTE), jobs))

        self.assertEqual(len(threaded), 8)
        for index, (want, got) in enumerate(zip(serial, threaded)):
            with self.subTest(index=index):
                self.assertEqual(png_dimensions(got), (1280, 800))
                self.assertEqual(want, got)

    def test_module_never_imports_pyplot(self) -> None:
        # Subprocess so other tests' imports cannot pollute sys.modules.
        code = (
            "import sys; sys.path.insert(0, %r);"
            "from app.services import chart_render;"
            "assert 'matplotlib.pyplot' not in sys.modules, 'pyplot was imported';"
            "print('ok')" % str(BACKEND_ROOT)
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=120
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ok", result.stdout)

    def test_no_missing_glyph_warnings_for_the_symbols_we_emit(self) -> None:
        note = cr.ChartAnnotation(
            symbol="TESTCO",
            name="M&M Financial · Dr. Reddy's — Ltd.",
            exchange="NSE",
            sector="Refineries",
            market_cap_crore=1_942_100,
            last_price=1412.60,
            change_pct=-1.25,
            rs_rating=88,
            relative_volume=2.1,
            scanner_name="VCP Contraction",
            is_new=True,
            also_in=("Minervini 5M",),
            session_date="2026-08-20",
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            cr.render_candles_png(make_bars(520), note)
        missing = [str(w.message) for w in caught if "missing from font" in str(w.message)]
        self.assertEqual(missing, [])


@unittest.skipUnless(cr.CHART_RENDER_AVAILABLE, "matplotlib not installed")
class ChartRenderDegenerateInputTests(unittest.TestCase):
    def test_empty_and_single_bar_raise_value_error(self) -> None:
        for bars in ([], make_bars(1)):
            with self.subTest(count=len(bars)):
                with self.assertRaises(ValueError):
                    cr.render_candles_png(bars, NOTE)

    def test_short_history_renders_without_long_moving_averages(self) -> None:
        png = cr.render_candles_png(make_bars(3), NOTE)
        self.assertTrue(png.startswith(PNG_MAGIC))
        self.assertEqual(png_dimensions(png), (1280, 800))

    def test_flat_series_does_not_divide_by_zero(self) -> None:
        # A circuit-locked stock: every OHLC identical, so spread collapses.
        bars = [
            {
                "time": 1_700_000_000 + i * 86_400,
                "open": 500.0, "high": 500.0, "low": 500.0, "close": 500.0,
                "volume": 1000,
            }
            for i in range(250)
        ]
        png = cr.render_candles_png(bars, NOTE)
        self.assertTrue(png.startswith(PNG_MAGIC))

    def test_zero_volume_throughout_suppresses_the_volume_band(self) -> None:
        bars = make_bars(250, volume=0)
        for bar in bars:
            bar["volume"] = 0
        png = cr.render_candles_png(bars, NOTE)
        self.assertTrue(png.startswith(PNG_MAGIC))

    def test_renders_with_no_annotation_fields_populated(self) -> None:
        png = cr.render_candles_png(make_bars(250), cr.ChartAnnotation(symbol="BARE"))
        self.assertTrue(png.startswith(PNG_MAGIC))

    def test_trade_levels_can_be_disabled(self) -> None:
        spec = cr.RenderSpec(show_trade_levels=False)
        png = cr.render_candles_png(make_bars(250), NOTE, spec)
        self.assertTrue(png.startswith(PNG_MAGIC))


@unittest.skipUnless(cr.CHART_RENDER_AVAILABLE, "matplotlib not installed")
class ChartRenderAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_wrapper_returns_the_same_bytes(self) -> None:
        bars = make_bars(300)
        got = await cr.render_candles_png_async(bars, NOTE)
        self.assertEqual(got, cr.render_candles_png(bars, NOTE))


if __name__ == "__main__":
    unittest.main()
