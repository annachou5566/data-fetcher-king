import json
from pathlib import Path

p = Path("./scripts/grayscale_all.json")

if not p.exists():
    print("grayscale_all.json not found")
    raise SystemExit(0)

with p.open("r", encoding="utf-8") as f:
    data = json.load(f)

print("total_funds_tested:", data.get("total_funds_tested"))
print("success_count:", data.get("success_count"))
print("funds_with_holdings_data:", data.get("funds_with_holdings_data"))
print()

for ticker, entry in data.get("results", {}).items():
    print(f"--- {ticker} ({entry.get('kind')}) ---")
    print("  success:", entry.get("success"))
    if entry.get("success"):
        m = entry.get("metrics", {}) or {}
        print("  total_in_trust:", m.get("total_in_trust"), m.get("coin_symbol_detected"))
        print("  aum_non_gaap:", m.get("aum_non_gaap"))
        print("  gaap_aum:", m.get("gaap_aum"))
        print("  nav_per_share:", m.get("nav_per_share"))
        print("  market_price:", m.get("market_price"))
        print("  shares_outstanding:", m.get("shares_outstanding"))
        print("  sponsors_fee:", m.get("sponsors_fee"))
        print("  as_of_date:", m.get("as_of_date"))
    else:
        print("  error:", entry.get("error_message"))
        if entry.get("blocked_snippet"):
            print("  blocked_snippet:", repr(entry.get("blocked_snippet")))
    print()
