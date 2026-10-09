# Handoff Log

Append-only. One report-back per session, newest last (template in `HANDOFF.md` §8).

---

```
Date / operator: 2026-10-09 / Claude Code (cloud session)
Branch(es) and commit hashes: claude/optimistic-rubin-vljhd6 — 8d1a56a7 (fix), plus the commit adding this log. Not merged; main is untouched.

1. What I was asked to do:
   Onboarding checks (local run, pytest, production health, workflow status), then:
   fix the Groups page Rotation chart (Improving groups that were not improving,
   Leading groups that were not leading), check the Market Map heat map, verify the rankings.

2. What I changed (files + one line each):
   - backend/app/services/group_rotation.py (new): per-group and per-sector price momentum
     vs the Nifty 500 (% change of EMA5 of group/benchmark over 10 sessions; EMA10 over 15 weekly).
   - backend/app/services/dashboard_service.py: get_group_rotation(); the groups build now drops
     stale rows with the scanners' rule (_group_eligible_snapshots).
   - backend/app/api/routes.py: GET /api/groups/rotation (+ namespaced).
   - backend/app/services/industry_groups.py: score EMA ignores history older than 30 days.
   - backend/scripts/generate_bhavcopy_patch.py: rewrites data/close_history.json nightly from the
     bars the indicator step already downloads; drops Yahoo holiday rows; refuses <1,000 symbols.
   - .github/workflows/daily-bhavcopy.yml: stages close_history.json in the daily commit.
   - frontend/src/lib/rotation.ts, RotationGraph.tsx, lib/api.ts: up axis = backend price momentum;
     across axis = ranking score (today pinned to the table), centred on the median of ALL groups;
     default view "All" groups; tooltip shows last-5-session move vs Nifty 500.
   - frontend/src/components/SectorTreemap.tsx: a sector too small to nest is drawn as one coloured
     tile (it was a blank white frame — Real Estate and Transport on 8 Oct).
   - frontend/src/components/GroupsPanel.tsx: Rotation subtitle wording.
   - backend/tests/test_group_rotation.py (new, 12 tests), test_industry_groups.py (+1 test).
   - CLAUDE.md: codebase-map line + gotcha 153.

3. What I ran to verify (commands + results; test counts; failures that remain):
   - cd backend && pytest tests: 1143 passed, 5 failed, 1 skipped. The 5 are pre-existing:
     2 dashboard_service + 1 date-dependent free_provider (the known 4, one of which passes today),
     and 2 in test_breakout_stats_pull, which assume the machine has >1 h uptime
     (time.monotonic() vs a 3600 s throttle seeded at 0.0) — this VM had ~5 min. Not product bugs.
     Run `pytest tests`, not bare `pytest`: backend/test_yf.py etc. are network scripts and error on collection.
   - cd frontend && npx --no-install tsc --noEmit: clean.
   - Python 3.11 py_compile of every changed backend file (the Space and CI run 3.11): clean.
   - Replay: rebuilt group scores for 80 sessions from committed closes with the real backend code,
     applied production's smoothing and the browser's old rotation maths, compared with what group
     prices did. Old chart: 51/79 quadrants agreed; 50% of "Improving" groups had a falling RS line
     over 2 weeks. New axis: 15% (the rest is smoothed-vs-raw lag), "Leading but falling" 10%.
   - Tests prove the key cases discriminate: the old ratio method reads +1.39 (Improving) on a group
     still falling; keeping the holiday row changes momentum 4.06 -> 3.67.
   - Local backend: /api/groups 2.7 s, no stale rows; /api/groups/rotation 0.7 s, 94/96 groups,
     14 sectors, 90 sessions, 1 holiday row dropped. Headless Chromium screenshots of Rotation
     (daily/weekly, groups/sectors) and Market Map at 1440 px: no app console errors.
   - Rankings: contiguous, sorted by score; score vs relative return Spearman 1m 0.58, 3m 0.84,
     6m 0.80, breadth 0.84 (anchored on 1-3 months by design).

4. Deployed? Not deployed. Nothing is on main; Vercel and the Space are unchanged.
   No PR opened (not asked for).

5. Data or workflow state changed:
   - daily-bhavcopy.yml will commit backend/data/close_history.json every evening (~2.3 MB, text).
     It is JSON, so nothing was added to hf-deploy-excludes.txt.
   - No schema version bumped; no new binary files.

6. New gotchas learned (candidates for CLAUDE.md) — 153 is written; also:
   - Yahoo returns some NSE holidays as rows with every close copied forward and zero volume
     (2026-09-14, 2026-10-02). They are in every snapshot's recent_closes, so 5d/20d returns span
     one session less after a holiday. Fixed for close_history; the indicator block (rc, b5, b20...)
     still carries them.
   - close_history.closes_for estimates drift as days*5/7 and appends recent closes by position;
     with holidays it can misplace bars by a session. Moot once the artifact is refreshed nightly.
   - HANDOFF.md is stale in two places: backend/run_local.py does not exist (use
     `python3 -m uvicorn app.main:app --port 8000` from backend/), and pytest is not in requirements.txt.

7. Things I did NOT finish or am unsure about:
   - Production could not be checked from this sandbox (hf.space is blocked by its network policy),
     so /api/health and /api/bhavcopy/status are unverified. GitHub shows the 8 Oct bhavcopy commit
     and green Deploy runs on 9 Oct.
   - Until the first nightly run after merge rewrites close_history.json, the rotation uses the
     current artifact (ends 2026-09-18) spliced with 20 trailing closes. That splice stops working
     around 2026-10-16 if this is not merged — and Power Base / VCP degrade with it regardless.
   - Production rank history lives in Postgres, which I could not read; the tail shapes there are
     unverified. The local check used seeded replay history.
   - Look-alike Daily Picks is broken on main: backend/app/services/lookalike/picks.py:153 has a
     backslash inside an f-string, which Python 3.11 rejects. Every repository_dispatch run on
     8 Oct failed at "Check the scripts load"; the later green run was the 17 s "already ran" skip.
     Not fixed (not assigned).

8. Anything that needs the owner's decision or credentials:
   - Merge claude/optimistic-rubin-vljhd6 (deploys frontend + backend).
   - Parent buckets (1-6 stocks merged from small groups) are ranked and plotted on the Groups page
     beside real groups — "Unclassified (Parent bucket)" with 1 stock is #27. Home already hides them
     (unstable_flag). Hide or mark them on Groups too?
   - Whether to fix the look-alike f-string (one line).
   - Network access to the Space for future sessions, if you want production checked from the cloud.

9. Uncommitted / local-only state the owner should know about:
   - In this cloud checkout: backend/data/bhavcopy_status.json and
     backend/app/data/groups/needs_review.csv were rewritten by the local backend run, and
     backend/app/data/rank_history/ranks_20261008.json was created. Runtime artifacts, deliberately
     not committed; reverting them was blocked by the session's permission rules.
   - On the owner's machine (from HANDOFF.md, not visible here): the rank_history files and
     .claude/launch.json — still the owner's call. wip/chart-journal-markers is not on origin.
```
