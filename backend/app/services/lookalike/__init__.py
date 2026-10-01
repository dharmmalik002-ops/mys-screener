"""Chart look-alikes: learn what a library of reference setups looks like, then
find Indian stocks whose charts look the same today.

The references are NEVER learned from as pictures. A newsletter screenshot
carries its own colours, zoom, indicators and watermarks, and an image model fed
those learns the house style rather than the setup. Each reference contributes
only a ticker and a date; the chart is then redrawn from real price data in one
fixed style (`render.py`), so the reference and every stock it is compared
against differ in the pattern and in nothing else.

Pipeline:
  references.py  filenames / newsletter HTML / text list -> (ticker, date)
  us_bars.py     daily bars for each reference ticker (Yahoo), cached locally
  render.py      one standard picture per (bars, end index)
  outcome.py     what the reference went on to do -> worked / failed / pending
  embed.py       DINOv2 fingerprint per picture
  model.py       setup-vs-random-day classifier, grouped CV, scoring
"""
