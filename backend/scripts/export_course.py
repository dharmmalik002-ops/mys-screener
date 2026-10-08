"""Export the verified @iManasArora study course to frontend/public/course/course.json.

Input is the gitignored archive in backend/data/x_archive/iManasArora (tweets, the
verified lesson files in consol/verified_*.json). Output is the one file the
Course page reads.

Charts are NOT copied into the repo: each example points at the image on X's own
CDN (pbs.twimg.com), the same URL his tweet uses. The lesson text is our own
paraphrase, every bullet carrying the ids of the tweets it rests on.

Years and strength are recomputed here from the cited tweets, never taken from the
writers' labels (an LLM consolidation once claimed years its evidence did not show
for 88 of 91 lessons).

    cd backend && python3 scripts/export_course.py
"""
from __future__ import annotations

import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVE = os.path.join(ROOT, "data", "x_archive", "iManasArora")
CONSOL = os.path.join(ARCHIVE, "consol")
OUT = os.path.join(os.path.dirname(ROOT), "frontend", "public", "course", "course.json")

MODULES = [
    ("philosophy", "How he thinks about the game"),
    ("market_conditions", "Reading the market"),
    ("playbook", "Market-condition playbook"),
    ("stock_selection", "Choosing stocks"),
    ("setups", "Setups"),
    ("entries", "Entries"),
    ("risk_sizing", "Risk and position size"),
    ("trade_management", "Managing open trades"),
    ("selling", "Selling"),
    ("psychology", "Psychology"),
    ("tools", "Tools and routines"),
    ("cases", "Case studies"),
]


def load_tweets() -> tuple[dict, dict]:
    dates, media = {}, {}
    with open(os.path.join(ARCHIVE, "tweets.jsonl")) as fh:
        for line in fh:
            r = json.loads(line)
            dates[r["id"]] = r["date"][:10]
            urls = {f"{i}": m.get("url") for i, m in enumerate(r.get("media") or [], 1)}
            urls.update({f"q{i}": m.get("url") for i, m in enumerate((r.get("quoted") or {}).get("media") or [], 1)})
            media[r["id"]] = urls
    return dates, media


DATES, MEDIA = load_tweets()


def cdn(local_path: str | None) -> str | None:
    """`<archive>/media/<tweet>_<n>.<ext>` -> the original pbs.twimg.com URL (without size)."""
    if not local_path:
        return None
    m = re.match(r"(\d+)_(q?\d+)\.", os.path.basename(local_path))
    if not m:
        return None
    url = MEDIA.get(m.group(1), {}).get(m.group(2))
    if not url or "pbs.twimg.com/media/" not in url:
        return None
    base, ext = url.rsplit(".", 1)
    return f"{base}?format={ext}"


def ids(seq) -> list[str]:
    return [str(i) for i in seq or [] if str(i) in DATES]


def bullets(seq) -> list[dict]:
    out = []
    for b in seq or []:
        if isinstance(b, dict):
            out.append({"text": b.get("text", ""), "ids": ids(b.get("ids"))})
        else:
            out.append({"text": str(b), "ids": []})
    return out


def lesson(l: dict) -> dict:
    how, mistakes = bullets(l.get("how")), bullets(l.get("mistakes"))
    cited = set(ids(l.get("evidence")))
    for b in how + mistakes:
        cited |= set(b["ids"])
    examples = []
    for e in l.get("examples") or []:
        url = cdn(e.get("image"))
        if url and str(e.get("tweet_id")) in DATES:
            examples.append({"tweet": str(e["tweet_id"]), "image": url, "caption": e.get("caption", "")})
            cited.add(str(e["tweet_id"]))
    cited_sorted = sorted(cited, key=lambda i: DATES[i])
    years = sorted({int(DATES[i][:4]) for i in cited_sorted})
    return {
        "id": l["id"],
        "setup": l.get("setup"),
        "title": l.get("title", ""),
        "rule": l.get("rule", ""),
        "how": how,
        "when": l.get("when", ""),
        "mistakes": mistakes,
        "evolution": l.get("evolution", ""),
        "examples": examples[:4],
        "evidence": cited_sorted,
        "years": years,
        # Core = recurs across time and in volume, measured from the tweets themselves.
        "strength": "core" if len(years) >= 3 and len(cited_sorted) >= 6 else "supporting",
    }


def main() -> int:
    if not os.path.isdir(CONSOL):
        print(f"archive not found at {CONSOL}; nothing exported", file=sys.stderr)
        return 1
    modules = []
    for key, title in MODULES:
        if key == "playbook":
            pb = json.load(open(os.path.join(CONSOL, "verified_playbook.json")))
            conditions = []
            for c in pb.get("conditions") or []:
                ev = set(ids(c.get("evidence")))
                periods = []
                for p in c.get("example_periods") or []:
                    pids = ids(p.get("evidence"))
                    ev |= set(pids)
                    periods.append({"from": p.get("from"), "to": p.get("to"), "note": p.get("note", ""), "ids": pids})
                conditions.append({
                    "name": c.get("name", ""), "reads": c.get("how_he_reads_it", ""),
                    "does": c.get("what_he_does", ""), "periods": periods,
                    "lessons": c.get("lesson_ids") or [],
                    "evidence": sorted(ev, key=lambda i: DATES[i]),
                })
            modules.append({"key": key, "title": title,
                            "intro": "When the market looks like this, this is what he does.",
                            "conditions": conditions})
        elif key == "cases":
            cases = []
            for c in json.load(open(os.path.join(CONSOL, "verified_case_studies.json"))):
                cases.append({
                    "symbol": c.get("symbol"), "setup": c.get("setup", ""), "entry_date": c.get("entry_date"),
                    "context": c.get("context", ""), "story": c.get("story", ""), "takeaway": c.get("takeaway", ""),
                    "result_pct": c.get("result_pct"), "result_note": c.get("result_note", ""),
                    "image": cdn(c.get("entry_image")), "root": str(c.get("root_tweet_id")),
                    "lessons": c.get("lesson_ids") or [],
                    "timeline": [{"date": t.get("date"), "action": t.get("action"), "price": t.get("price"),
                                  "text": t.get("text", ""), "tweet": str(t.get("tweet_id"))}
                                 for t in c.get("timeline") or []],
                })
            modules.append({"key": key, "title": title,
                            "intro": "Real trades from his logs, entry to exit, tied to the lessons they show.",
                            "cases": cases})
        else:
            d = json.load(open(os.path.join(CONSOL, f"verified_{key}.json")))
            modules.append({"key": key, "title": title, "intro": d.get("intro", ""),
                            "lessons": [lesson(l) for l in d["lessons"]]})

    all_ids = {i for m in modules for l in m.get("lessons", []) for i in l["evidence"]}
    payload = {
        "source": "@iManasArora",
        "span": ["2021-01-01", max(DATES.values())],
        "tweet_dates": {i: DATES[i] for i in sorted(all_ids | {i for m in modules for c in m.get("conditions", []) for i in c["evidence"]}
                                                   | {t["tweet"] for m in modules for c in m.get("cases", []) for t in c["timeline"] if t["tweet"] in DATES})},
        "modules": modules,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
    lessons = [l for m in modules for l in m.get("lessons", [])]
    missing = sum(1 for l in lessons for e in l["examples"] if not e["image"])
    print(f"{len(lessons)} lessons ({sum(l['strength'] == 'core' for l in lessons)} core), "
          f"{len(all_ids)} cited tweets, {sum(len(l['examples']) for l in lessons)} chart links, "
          f"{missing} without image -> {os.path.relpath(OUT, os.path.dirname(ROOT))} "
          f"({os.path.getsize(OUT) / 1e3:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
