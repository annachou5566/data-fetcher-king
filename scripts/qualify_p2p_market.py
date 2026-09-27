#!/usr/bin/env python3
"""Read-only runtime qualification for the canonical P2P market snapshot.

No R2 credentials are read and no storage mutation is possible. The script uses
exactly the same Binance/curl_cffi fetch path as the production writer, then
projects representative amount tiers in memory.
"""

from fetch_p2p import BNC_ASSETS, fetch_binance_side, requests

AMOUNTS = (1_000_000, 5_000_000, 10_000_000, 50_000_000)


def eligible(ad, amount_vnd):
    try:
        price = float(ad.get("price") or 0)
        min_fiat = float(ad.get("minFiat") or 0)
        max_fiat = float(ad.get("maxFiat") or 0)
        available = float(ad.get("availableCrypto") or 0)
    except Exception:
        return False

    if price <= 10_000:
        return False
    if min_fiat > 0 and amount_vnd < min_fiat:
        return False
    if max_fiat > 0 and amount_vnd > max_fiat:
        return False
    if available > 0 and amount_vnd > available * price:
        return False
    return True


def avg5(prices):
    if not prices:
        return None
    top = prices[:5]
    return round(sum(top) / len(top))


def method_count(ads):
    methods = set()
    for ad in ads:
        for method in ad.get("payTypes") or []:
            if isinstance(method, str) and method.strip():
                methods.add(method.strip())
    return len(methods)


def main():
    print("P2P_MARKET_QUAL_BEGIN")
    session = requests.Session(impersonate="chrome116")

    complete = True
    total_sanitized = 0

    for asset in BNC_ASSETS:
        for side in ("BUY", "SELL"):
            stats, ads = fetch_binance_side(session, asset, side)
            total_sanitized += len(ads)
            reported = stats.get("reported_ad_count")
            partial = bool(stats.get("is_partial"))
            if partial or not ads:
                complete = False

            print(
                f"side asset={asset} side={side} "
                f"reported={reported} sanitized={len(ads)} partial={str(partial).lower()}"
            )

            for amount in AMOUNTS:
                rows = [ad for ad in ads if eligible(ad, amount)]
                prices = [round(float(ad["price"])) for ad in rows]
                print(
                    f"tier asset={asset} side={side} amount={amount} "
                    f"eligible={len(rows)} best={prices[0] if prices else None} "
                    f"avg5={avg5(prices)} methods={method_count(rows)}"
                )

    print(f"summary complete={str(complete).lower()} sanitized_ads={total_sanitized}")
    print("P2P_MARKET_QUAL_END")

    if not complete:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
