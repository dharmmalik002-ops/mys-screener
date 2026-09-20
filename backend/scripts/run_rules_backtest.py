import json, sys, numpy as np
from datetime import date, timedelta
from pathlib import Path
S="/private/tmp/claude-501/-Users-dharmender-Desktop-Stock-Scanner-c/b819157b-9c2e-430b-ae71-a7f7c06ac14f/scratchpad/"
B=Path("/Users/dharmender/Desktop/Stock Scanner c/backend"); sys.path.insert(0,str(B))
from app.services.bot import portfolio as pf
import yfinance as yf
D=json.load(open(S+"trades.json")); CUT="2018-01-01"
tr=[t for t in D["trail8_500"] if t["exit_day"]]
train=[t for t in tr if t["entry_day"]<CUT]
RISK=np.quantile([t["risk_pct"] for t in train],.40)
TURN=np.quantile([t["turnover_crore_at_entry"] for t in train],.60)
GOOD={"squeeze_release","earnings_drift","earnings_gap_hold","high_tight_flag",
      "minervini_breakout","pullback_ema21"}
k=[t for t in tr if (t["risk_pct"]<=RISK and t["turnover_crore_at_entry"]<=TURN
   and t["ret_63_at_entry"]>0 and t["strategy"] in GOOD
   and t["regime"] in ("bull_strong","bull_narrow","recovery"))]
cfg=pf.PortfolioConfig(risk_per_trade_pct=0.25,watch_risk_pct=0.25,max_concurrent=60,
    max_portfolio_risk_pct=15.0,max_deployed_pct=100.0,max_position_pct=10.0)
r=pf.simulate(k,[],cfg,label="rules")
print(f"RULES: CAGR {r.cagr_pct:+.2f}%  maxDD {r.max_drawdown_pct:.2f}%  Sharpe {r.sharpe:.2f}  "
      f"trades {r.trades_taken}  win {r.win_rate:.1f}%  payoff {r.payoff:.2f}  "
      f"thresholds risk<={RISK:.2f}% turnover<={TURN:.1f}cr")
def daily(curve):
    c={date.fromisoformat(p["day"]):p["equity"] for p in curve}
    ds=sorted(c); out={}; last=c[ds[0]]; d=ds[0]
    while d<=ds[-1]:
        if d in c: last=c[d]
        out[d]=last; d+=timedelta(days=1)
    return out
def yearly(s):
    ds=sorted(s); y={}
    for yy in range(2008,2027):
        dd=[d for d in ds if d.year==yy]
        if len(dd)<150: continue
        pr=[d for d in ds if d<date(yy,1,1)]
        st=s[pr[-1]] if pr else s[dd[0]]
        y[yy]=(s[dd[-1]]/st-1.0)*100.0
    return y
bot=yearly(daily(r.equity_curve))
bench={}
for nm,tk in (("SC250","NIFTYSMLCAP250.NS"),("MC150","NIFTYMIDCAP150.NS")):
    h=yf.Ticker(tk).history(period="max")
    if not len(h): continue
    s={d.date():float(c) for d,c in zip(h.index,h["Close"])}
    bench[nm]=yearly(s)
json.dump({"bot":bot,"bench":bench,"cagr":r.cagr_pct,"dd":r.max_drawdown_pct,
           "sharpe":r.sharpe,"win":r.win_rate,"payoff":r.payoff,"trades":r.trades_taken},
          open(S+"FINAL.json","w"))
print("\nYEAR      BOT    SMALLCAP250   MIDCAP150")
for y in sorted(bot):
    a=bench.get("SC250",{}).get(y); b=bench.get("MC150",{}).get(y)
    print(f"{y}  {bot[y]:+7.1f}%  {('%+.1f%%'%a) if a is not None else 'n/a':>10}  {('%+.1f%%'%b) if b is not None else 'n/a':>10}")
for nm in ("SC250","MC150"):
    if nm in bench:
        ys=[y for y in bot if y in bench[nm]]
        g=np.prod([1+bot[y]/100 for y in ys])**(1/len(ys))-1
        gb=np.prod([1+bench[nm][y]/100 for y in ys])**(1/len(ys))-1
        print(f"\n{nm} overlap {ys[0]}-{ys[-1]}: bot {100*g:+.2f}%/yr vs {nm} {100*gb:+.2f}%/yr")
