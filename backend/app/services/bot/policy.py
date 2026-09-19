"""Turn the attribution table into a playbook: what to run, and how hard.

The rule this module enforces is that a strategy is only allowed to trade in a
regime where it earned the right — meaning the cell survived the held-out
period, not merely the full-sample average. Everything else stands down.

"Stand down" is a real output and the most valuable one the system produces. A
bot that always has an opinion will always find something to buy, and in a bear
market that is precisely the behaviour that empties an account. If no strategy
has a validated edge in the current regime, the correct playbook is cash, and
this module will say so rather than surfacing the least-bad option.

Sizing is expressed in *risk per trade*, not position value. Risking a fixed
fraction of equity per trade is what makes a 32%-win-rate trend profile
survivable — the losses arrive in clusters, and a fixed rupee position size
lets a cluster in a volatile name do far more damage than the same cluster in a
quiet one. Position value then falls out of the stop distance.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict

from .attribution import ValidatedCell
from .regime import REGIME_LABELS, REGIME_NOTES, REGIMES
from .strategies import BY_ID

# Only these verdicts earn capital. `confirmed_weak` is allowed in at reduced
# size: it was positive in both periods but did not clear the multiple-testing
# correction, which is a reason for caution and not for exclusion.
TRADEABLE_VERDICTS = {"confirmed", "confirmed_weak"}

# Risk per trade as a percentage of equity, by how strong the evidence is.
RISK_CONFIRMED = 0.75
RISK_CONFIRMED_WEAK = 0.40
# Total risk the book may carry at once. A trend profile takes many small
# losses in a row when a regime turns; this is the cap that makes that
# survivable rather than terminal.
MAX_PORTFOLIO_RISK = 6.0
MAX_CONCURRENT_POSITIONS = 8


@dataclass
class PlaybookEntry:
    """One strategy cleared to trade in one regime."""

    strategy: str
    label: str
    family: str
    verdict: str
    in_sample_r: float
    out_sample_r: float
    out_sample_trades: int
    win_rate: float
    risk_per_trade_pct: float
    note: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class RegimePlaybook:
    regime: str
    label: str
    note: str
    stance: str                  # engaged | selective | stand_down
    entries: list[PlaybookEntry]
    max_concurrent: int
    max_portfolio_risk_pct: float
    rationale: str

    def to_dict(self) -> dict:
        out = asdict(self)
        out["entries"] = [e.to_dict() for e in self.entries]
        return out


def _risk_for(verdict: str) -> float:
    return RISK_CONFIRMED if verdict == "confirmed" else RISK_CONFIRMED_WEAK


def build_playbooks(validated: list[ValidatedCell]) -> list[RegimePlaybook]:
    """One playbook per regime, built only from cells that survived validation."""
    by_regime: dict[str, list[ValidatedCell]] = {r: [] for r in REGIMES}
    for cell in validated:
        if cell.regime in by_regime:
            by_regime[cell.regime].append(cell)

    playbooks: list[RegimePlaybook] = []
    for regime in REGIMES:
        cleared = [
            c for c in by_regime[regime]
            if c.verdict in TRADEABLE_VERDICTS and c.out_sample and c.out_sample.avg_r > 0
        ]
        cleared.sort(key=lambda c: -(c.out_sample.avg_r if c.out_sample else 0))

        entries: list[PlaybookEntry] = []
        for cell in cleared:
            spec = BY_ID.get(cell.strategy)
            ins, outs = cell.in_sample, cell.out_sample
            if spec is None or ins is None or outs is None:
                continue
            # A strategy trading in a regime it did not predict is a legitimate
            # finding, but it is worth flagging: it is the case most likely to
            # be a statistical accident that survived by luck.
            surprise = regime not in spec.expects
            entries.append(
                PlaybookEntry(
                    strategy=spec.id,
                    label=spec.label,
                    family=spec.family,
                    verdict=cell.verdict,
                    in_sample_r=ins.avg_r,
                    out_sample_r=outs.avg_r,
                    out_sample_trades=outs.trades,
                    win_rate=outs.win_rate,
                    risk_per_trade_pct=_risk_for(cell.verdict),
                    note=(
                        ("Not predicted for this regime — held up anyway, treat with care. " if surprise else "")
                        + cell.note
                    ),
                )
            )

        if not entries:
            stance = "stand_down"
            rationale = (
                "No strategy in the library held a positive edge here through the held-out "
                "period. The playbook is cash: sitting out is a position, and it is the one "
                "the evidence supports."
            )
        elif len(entries) <= 2 or all(e.verdict == "confirmed_weak" for e in entries):
            stance = "selective"
            rationale = (
                f"{len(entries)} setup(s) cleared validation here, on thin or unconfirmed "
                "evidence. Trade them at reduced size and expect long quiet stretches."
            )
        else:
            stance = "engaged"
            rationale = (
                f"{len(entries)} setups held up out-of-sample in this regime. This is where "
                "the book should be working."
            )

        playbooks.append(
            RegimePlaybook(
                regime=regime,
                label=REGIME_LABELS[regime],
                note=REGIME_NOTES[regime],
                stance=stance,
                entries=entries,
                max_concurrent=MAX_CONCURRENT_POSITIONS if stance == "engaged" else max(3, len(entries)),
                max_portfolio_risk_pct=MAX_PORTFOLIO_RISK if stance == "engaged" else MAX_PORTFOLIO_RISK / 2,
                rationale=rationale,
            )
        )

    return playbooks


def position_size(
    equity: float,
    entry: float,
    stop: float,
    risk_per_trade_pct: float,
) -> dict:
    """Shares to buy so that a stop-out costs exactly `risk_per_trade_pct`.

    Returns the arithmetic as well as the answer — a sizing number the user
    cannot check is a sizing number they will not trust, and should not.
    """
    risk_per_share = entry - stop
    if risk_per_share <= 0 or entry <= 0 or equity <= 0:
        return {"shares": 0, "reason": "invalid stop or entry"}
    risk_budget = equity * risk_per_trade_pct / 100.0
    shares = int(risk_budget // risk_per_share)
    value = shares * entry
    # A position that needs more capital than the book has is not a position.
    capped = False
    if value > equity:
        shares = int(equity // entry)
        value = shares * entry
        capped = True
    return {
        "shares": shares,
        "position_value": round(value, 2),
        "risk_budget": round(risk_budget, 2),
        "risk_per_share": round(risk_per_share, 2),
        "actual_risk": round(shares * risk_per_share, 2),
        "pct_of_equity": round(value / equity * 100.0, 1) if equity else 0.0,
        "capped_by_capital": capped,
    }
