#!/usr/bin/env python3
"""The look-alike day: pick, review, learn, publish.

    python3 scripts/run_lookalike_day.py                          # today
    python3 scripts/run_lookalike_day.py --backfill-from 2024-01-01   # + every Friday since

1. (backfill) For each past Friday, score the market as of that date and record
   up to 10 picks. The learner at each Friday sees only picks whose results were
   known before it, so the backfilled record is a fair walk-forward test.
2. Review: grade every open pick (+20% before -8% within 40 sessions).
3. Pick today, ranked by resemblance — or by the learned outcome model once it
   has proved itself on picks it did not learn from.
4. Write data/lookalikes.json (today's look-alikes) and
   data/lookalike_picks.json (the calendar, reviews and what was learned).

Reads data/deep_history/ (top it up first with scripts/build_deep_history.py)
and data/lookalike/. Needs torch + transformers.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.services.lookalike import picks, pipeline, scoring  # noqa: E402

# The user chose to show every reference by name, including course examples.
SHOW_REFERENCE_NAMES = True


def fridays(start: date, end: date):
    d = start + timedelta(days=(4 - start.weekday()) % 7)
    while d < end:
        yield d
        d += timedelta(days=7)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data")
    parser.add_argument("--backfill-from", type=date.fromisoformat)
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(message)s")

    t0 = time.time()
    library = scoring.load_library(args.data_dir)
    universe, index = scoring.load_universe(args.data_dir)
    latest = max(b.last_date for b in universe)
    print(f"library: {', '.join(f'{k} {v['references']}' for k, v in library.styles.items())}; "
          f"universe {len(universe)} symbols through {latest} ({time.time() - t0:.0f}s)")

    ledger = picks.load_ledger(args.data_dir)
    fps = picks.load_fingerprints(args.data_dir)

    if args.backfill_from:
        days = list(fridays(args.backfill_from, latest - timedelta(days=2)))
        for n, day in enumerate(days, start=1):
            s = scoring.score(universe, index, library, as_of=day)
            if s is None:
                continue
            fb = picks.feedback_status(ledger, fps, as_of=s.as_of)
            made = picks.choose(ledger, fps, s, library, source="backfill", feedback=fb)
            picks.review(ledger, universe, latest)
            print(f"  [{n}/{len(days)}] {s.as_of}: {len(made)} picks"
                  f"{' (ranked by learned outcome)' if fb.in_use else ''}  {time.time() - t0:.0f}s", flush=True)
            if n % 10 == 0:
                picks.save_ledger(args.data_dir, ledger)
                picks.save_fingerprints(args.data_dir, fps)

    picks.review(ledger, universe, latest)
    today = scoring.score(universe, index, library)
    fb = picks.feedback_status(ledger, fps, as_of=today.as_of)
    made = picks.choose(ledger, fps, today, library, source="live", feedback=fb)
    picks.save_ledger(args.data_dir, ledger)
    picks.save_fingerprints(args.data_dir, fps)

    fb = picks.feedback_status(ledger, fps)
    pipeline.scan_india(args.data_dir, show_reference_names=SHOW_REFERENCE_NAMES, scored=today, library=library)
    baselines = picks.update_baselines(args.data_dir, ledger, universe, index)
    out = picks.export(args.data_dir, ledger, fb, library, baselines)

    s = out["summary"]
    print(f"\ntoday {today.as_of}: {len(made)} picks — " + ", ".join(p["symbol"] for p in made))
    print(f"ledger: {s['picks']} picks on {s['days']} days; finished {s['decided']} "
          f"(worked {s['worked']}, {s['worked_pct']}%); style base rate {s['style_base_rate_pct']}%")
    b = out["baselines"]
    print(f"compare: picks {b['picks']['worked_pct']}% (±{b['picks'].get('margin_pct')}) | "
          f"all 8 template rules {b['template8']['worked_pct']}% | every stock {b['all']['worked_pct']}%")
    print("learning:", out["learning"]["status"])
    for row in out["lessons"]:
        flag = "" if row["enough"] else "  (too few to judge)"
        print(f"  {row['condition']}: {row['worked_with_pct']}% with ({row['with']}) vs "
              f"{row['worked_without_pct']}% without ({row['without']}){flag}")
    print(f"done in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
