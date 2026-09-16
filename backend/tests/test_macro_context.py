"""Tests for the macro context note.

The point of this module is that a trader reads the paragraphs and trusts them,
so the tests are mostly about honesty: a missing measurement must never read as
zero, the prose must never claim a trend it does not have the history for, and
the model must never be allowed to overrule the arithmetic.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.services import macro_context as mc


def _closes(start: float, pct_change: float, n: int = 120) -> list[float]:
    """Flat, then a smooth ramp over the final 20 sessions.

    Shaped this way on purpose: the rules read the 20-session change, so a
    helper that spread the move across all 120 bars would produce a ~4% window
    for a "25% move" and quietly test nothing.
    """
    ramp = 21
    step = (1.0 + pct_change / 100.0) ** (1.0 / (ramp - 1))
    flat = [start] * (n - ramp)
    return flat + [start * (step ** i) for i in range(ramp)]


def _raw(**by_symbol: list[float]) -> dict[str, dict]:
    return {sym: {"closes": vals, "as_of": "2026-09-15"} for sym, vals in by_symbol.items()}


# ───────────────────────────── series arithmetic ─────────────────────────────


def test_missing_history_reports_none_not_zero():
    """A flat market and an unmeasurable one are different facts."""
    assert mc._pct_change([100.0, 101.0], 20) is None
    assert mc._pct_change([100.0, 100.0], 1) == 0.0


def test_summarise_rejects_a_series_too_short_to_describe():
    spec = mc.SERIES_BY_KEY["brent"]
    assert mc.summarise_series(spec, [80.0], "2026-09-15") is None


def test_summarise_marks_position_against_the_50_day_average():
    spec = mc.SERIES_BY_KEY["sp500"]
    row = mc.summarise_series(spec, _closes(4000, 20.0), "2026-09-15")
    assert row is not None
    assert row["above_50dma"] is True
    assert row["vs_50dma_pct"] > 0


# ────────────────────────────── the verdict ──────────────────────────────────


def test_expensive_crude_and_a_broken_sp500_read_as_hostile():
    series = {
        "brent": mc.summarise_series(mc.SERIES_BY_KEY["brent"], _closes(70, 25.0), "2026-09-15"),
        "sp500": mc.summarise_series(mc.SERIES_BY_KEY["sp500"], _closes(5000, -12.0), "2026-09-15"),
    }
    verdict = mc.assess_pressure(series)
    assert verdict["stance"] == "hostile"
    assert verdict["score"] <= -3
    assert verdict["headwinds"] and not verdict["tailwinds"]


def test_cheap_crude_and_a_healthy_sp500_read_as_supportive():
    series = {
        "brent": mc.summarise_series(mc.SERIES_BY_KEY["brent"], _closes(95, -20.0), "2026-09-15"),
        "sp500": mc.summarise_series(mc.SERIES_BY_KEY["sp500"], _closes(4000, 15.0), "2026-09-15"),
    }
    verdict = mc.assess_pressure(series)
    assert verdict["stance"] == "supportive"
    assert verdict["tailwinds"]


def test_no_data_at_all_is_mixed_rather_than_bearish():
    """The failure mode that matters: absent inputs must not manufacture a
    negative verdict, the way `.fillna(0)` on breadth once did."""
    verdict = mc.assess_pressure({})
    assert verdict["stance"] == "mixed"
    assert verdict["score"] == 0


def test_every_reason_is_a_sentence_not_a_code():
    """The reasons are the deterministic writer's raw material, so they have to
    read as English — a fallback that prints `brent_20d_up` is not a fallback."""
    series = {
        "brent": mc.summarise_series(mc.SERIES_BY_KEY["brent"], _closes(70, 25.0), "2026-09-15"),
    }
    for reason in mc.assess_pressure(series)["headwinds"]:
        assert reason[0].isupper() and " " in reason and len(reason) > 40


# ─────────────────────────────── the prose ───────────────────────────────────


def test_note_is_complete_even_with_no_data_at_all():
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    note = mc.deterministic_sections(facts)
    assert note["source"] == "computed"
    assert [s["id"] for s in note["sections"]] == [sid for sid, _, _ in mc.SECTION_SPEC]
    for section in note["sections"]:
        assert section["paragraphs"], f"{section['id']} rendered empty"


def test_index_names_keep_their_capitalisation():
    """`str.capitalize()` turns "S&P 500" into "S&p 500" — it must not be used
    on text containing instrument names."""
    facts = mc.build_facts(
        _raw(**{"^GSPC": _closes(5000, -12.0), "^IXIC": _closes(16000, -14.0)}),
        None, [], today=date(2026, 9, 16),
    )
    prose = " ".join(mc._overnight_paragraphs(facts["series"]))
    assert "S&p" not in prose
    assert "S&P 500" in prose


def test_asia_is_described_on_its_own_terms_not_the_us_direction():
    """Asia rising after a weak US close must not be called "following through
    on that strength" — the divergence is the whole point of the paragraph."""
    facts = mc.build_facts(
        _raw(**{
            "^GSPC": _closes(5000, -12.0),
            "^N225": _closes(38000, 15.0),
            "^HSI": _closes(19000, 12.0),
        }),
        None, [], today=date(2026, 9, 16),
    )
    prose = " ".join(mc._overnight_paragraphs(facts["series"]))
    assert "following through on that strength" not in prose
    assert "shrugging off" in prose


def test_a_single_session_of_flows_is_never_called_a_ten_day_trend():
    """The one-day vs ten-day distinction is the entire value of the flows
    paragraph, so a single session must say so rather than restate itself."""
    flows = mc.merge_flow_history(None, {"days": [{"date": "15-Sep-2026", "fii_net_crore": -2978.0,
                                                  "dii_net_crore": 2686.0}]})
    prose = " ".join(mc._flow_paragraphs(flows))
    assert "ten sessions" not in prose
    assert "no ten-day trend" in prose


def test_ten_day_flow_totals_accumulate_across_runs():
    """NSE returns one session at a time, so the running total only exists if
    history is merged forward out of the cache."""
    first = mc.merge_flow_history(None, {"days": [{"date": "2026-09-14", "fii_net_crore": -1000.0}]})
    second = mc.merge_flow_history(first, {"days": [{"date": "2026-09-15", "fii_net_crore": -2000.0}]})
    assert second is not None
    assert second["sessions_in_10d"] == 2
    assert second["fii_net_10d_crore"] == pytest.approx(-3000.0)
    # Re-seeing a day must update it, not double-count it.
    again = mc.merge_flow_history(second, {"days": [{"date": "2026-09-15", "fii_net_crore": -2000.0}]})
    assert again["fii_net_10d_crore"] == pytest.approx(-3000.0)


def test_missing_flows_say_so_instead_of_reading_as_zero():
    prose = " ".join(mc._flow_paragraphs(None))
    assert "not available" in prose
    assert "0 crore" not in prose


# ──────────────────────────── the event calendar ─────────────────────────────


def test_expiry_is_the_last_thursday_of_the_month():
    assert mc._last_thursday(2026, 9) == date(2026, 9, 24)
    assert mc._last_thursday(2026, 12) == date(2026, 12, 31)


def test_events_are_ordered_and_carry_their_distance():
    events = mc.upcoming_events(date(2026, 9, 16), horizon_days=21)
    assert events == sorted(events, key=lambda e: e["date"])
    assert all(0 <= e["days_away"] <= 21 for e in events)


# ────────────────────────────── the AI boundary ──────────────────────────────


def _good_payload() -> dict:
    return {
        "headline": "The global backdrop is leaning against Indian equities today.",
        "stance": "supportive",
        "summary": ["A paragraph about the whole picture and what it means for size."],
        "sections": [
            {"id": sid, "title": title, "paragraphs": ["A paragraph."]}
            for sid, title, _ in mc.SECTION_SPEC
        ],
    }


def test_the_model_may_phrase_the_stance_but_not_overrule_it():
    """The stance is arithmetic. Letting the prose layer change it would mean a
    hallucinated adjective could flip the page's verdict."""
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    facts["pressure"]["stance"] = "hostile"
    note = mc.validate_narrative(_good_payload(), facts)
    assert note is not None
    assert note["stance"] == "hostile"


def test_a_half_written_note_is_rejected_in_favour_of_the_computed_one():
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    payload = _good_payload()
    payload["sections"] = payload["sections"][:3]
    assert mc.validate_narrative(payload, facts) is None


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(stance="bullish"),
    lambda p: p.update(headline=""),
    lambda p: p.update(summary=[]),
    lambda p: p.update(sections=[]),
])
def test_malformed_notes_are_rejected(mutate):
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    payload = _good_payload()
    mutate(payload)
    assert mc.validate_narrative(payload, facts) is None


def test_validated_note_still_carries_the_checklist():
    """The checklist is derived from facts, never from the model, so it must
    survive the AI path exactly as it does the computed one."""
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    note = mc.validate_narrative(_good_payload(), facts)
    assert note is not None
    assert [item["id"] for item in note["checklist"]] == [
        item["id"] for item in mc.deterministic_sections(facts)["checklist"]
    ]


# ────────────────────────────── staleness ────────────────────────────────────


def test_stale_context_reports_its_real_age():
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    stale = mc.mark_stale(facts, "2020-01-01T00:00:00+00:00")
    assert stale["stale"] is True
    assert "Do not trade off it" in stale["stale_reason"]


def test_cache_round_trips(tmp_path):
    payload = {"generated_at": "2026-09-16T00:00:00+00:00", "facts": {"as_of": "2026-09-15"}}
    mc.save_cache(tmp_path, payload)
    assert mc.load_cache(tmp_path) == payload


# ───────────────────────── prose hygiene regressions ─────────────────────────


def test_a_move_that_rounds_to_zero_is_called_flat_not_minus_zero():
    """`f"{-0.04:+.1f}%"` renders "-0.0%", which reads as a fall that did not
    happen."""
    assert mc._signed("the dollar index is", -0.04) == "the dollar index is flat"
    assert mc._signed("the dollar index is", -0.4) == "the dollar index is -0.4%"


def test_summary_terminates_each_reason_and_does_not_repeat_the_headline():
    series = {
        "brent": mc.summarise_series(mc.SERIES_BY_KEY["brent"], _closes(70, 25.0), "2026-09-15"),
        "sp500": mc.summarise_series(mc.SERIES_BY_KEY["sp500"], _closes(5000, -12.0), "2026-09-15"),
    }
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    facts["series"] = series
    facts["pressure"] = mc.assess_pressure(series)
    note = mc.deterministic_sections(facts)
    first = note["summary"][0]
    assert note["headline"] not in first, "the headline is rendered above; repeating it is padding"
    # Two sentences joined without a full stop is the bug this pins.
    assert "bill The" not in first
    assert first.endswith(".")


def test_checklist_reports_the_flow_window_it_actually_has():
    facts = mc.build_facts({}, None, [], today=date(2026, 9, 16))
    flows = mc.merge_flow_history(None, {"days": [{"date": "2026-09-15", "fii_net_crore": -2978.0}]})
    items = mc.build_checklist(facts["pressure"], facts["linkage"], facts["events"], flows)
    answer = next(i["answer"] for i in items if i["id"] == "flows")
    assert "1 session." in answer
    assert "ten sessions" not in answer


def test_sector_clauses_are_separated_by_semicolons():
    """Each clause contains its own commas, so comma-joining three of them
    produced one unreadable run-on sentence."""
    joined = mc._join_clauses(["IT — it earns in dollars, and sells abroad", "Metals — it tracks China, and the dollar"])
    assert "; and " in joined


def test_headwinds_are_listed_as_separate_sentences():
    """Three full sentences comma-joined became one run-on in which the
    separate arguments blurred together."""
    series = {
        "brent": mc.summarise_series(mc.SERIES_BY_KEY["brent"], _closes(70, 25.0), "2026-09-15"),
        "sp500": mc.summarise_series(mc.SERIES_BY_KEY["sp500"], _closes(5000, -12.0), "2026-09-15"),
    }
    prose = " ".join(mc._macro_paragraphs(series, mc.assess_pressure(series)))
    assert "bill, the US" not in prose
    assert "expensive and the S&P 500" not in prose


def test_acronyms_survive_being_spliced_after_a_label():
    """The reasons are sentences spliced after "Working against you right now:",
    so the leading capital is dropped — but not on "S&P 500" or "US"."""
    assert mc._lower_first("The dollar index is up") == "the dollar index is up"
    assert mc._lower_first("S&P 500 has lost its average") == "S&P 500 has lost its average"
    assert mc._lower_first("US 10-year yields rose") == "US 10-year yields rose"


def test_the_sp500_headwind_keeps_its_name_when_listed_first():
    series = {"sp500": mc.summarise_series(mc.SERIES_BY_KEY["sp500"], _closes(5000, -12.0), "2026-09-15")}
    prose = " ".join(mc._macro_paragraphs(series, mc.assess_pressure(series)))
    assert "s&P" not in prose
    assert "S&P 500" in prose


def test_every_series_declares_which_direction_helps_india():
    """The evidence table colours by effect, not by sign — an unset effect
    would silently paint a rising crude price green."""
    allowed = {"up_helps", "up_hurts", "neutral"}
    for spec in mc.ALL_SERIES:
        assert spec.effect in allowed, spec.key
    assert mc.SERIES_BY_KEY["brent"].effect == "up_hurts"
    assert mc.SERIES_BY_KEY["sp500"].effect == "up_helps"


def test_the_effect_reaches_the_summarised_row():
    row = mc.summarise_series(mc.SERIES_BY_KEY["brent"], _closes(70, 25.0), "2026-09-15")
    assert row is not None and row["effect"] == "up_hurts"
