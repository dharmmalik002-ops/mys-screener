import json, urllib.request

url = "http://127.0.0.1:8000/api/fundamentals/TCS"
print("Fetching TCS from fresh server...")
with urllib.request.urlopen(url, timeout=120) as resp:
    data = json.load(resp)

print(f"Quarterly Results: {len(data.get('quarterly_results',[]))}")
for q in data.get("quarterly_results", [])[:3]:
    print(f"  {q['period']}: Sales={q.get('sales_crore')}Cr, NP={q.get('net_profit_crore')}Cr")
v = data.get("valuation")
print(f"Valuation: PE={v.get('pe_ratio') if v else None}, MCap={v.get('market_cap_crore') if v else None}")
g = data.get("growth")
print(f"Growth: {g.get('latest_period') if g else None} SalesYoY={g.get('sales_yoy_pct') if g else None}%")
print(f"Recent Updates: {len(data.get('recent_updates',[]))}")
print(f"Balance Sheet: {len(data.get('balance_sheet',[]))}")
print(f"AI Summary: {'YES' if data.get('ai_news_summary') else 'NO'}")
print(f"Mgmt Guidance: {len(data.get('management_guidance',[]))}")
print(f"Competitive: {'YES' if data.get('competitive_position') else 'NO'}")
print(f"Warnings: {data.get('data_warnings')}")
