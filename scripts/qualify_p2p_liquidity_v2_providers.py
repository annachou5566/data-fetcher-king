#!/usr/bin/env python3
"""Read-only ads-level qualification for OKX + Bybit P2P Liquidity v2.

No R2/storage credentials are read. No provider authentication is used.
Output is bounded metadata/capability evidence only; public ad identities are
hashed locally instead of printed.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from collections import Counter
from urllib.parse import urlencode

from curl_cffi import requests

FIAT = "VND"
ASSETS = ("USDT", "USDC")
SIDES = ("BUY", "SELL")  # Wave/taker perspective
TIMEOUT = 15
MAX_PAGES = 20
BYBIT_PAGE_SIZE = 50
OKX_PAGE_SIZE = 1000

OKX_BOOKS = "https://www.okx.com/v3/c2c/tradingOrders/books"
OKX_MARKET = "https://www.okx.com/v3/c2c/tradingOrders/getMarketplaceAdsPrelogin"
BYBIT_ONLINE = "https://api2.bybit.com/fiat/otc/item/online"


def first(d, keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        v = d.get(k)
        if v not in (None, "", [], {}):
            return v
    return None


def num(v):
    try:
        n = float(v)
        return n if math.isfinite(n) else None
    except Exception:
        return None


def identity_value(d, keys):
    if not isinstance(d, dict):
        return None
    for k in keys:
        v = d.get(k)
        if v in (None, ""):
            continue
        if str(v).strip().lower() in {"0", "-1", "none", "null"}:
            continue
        return v
    return None


def short_hash(v):
    if v in (None, ""):
        return None
    return hashlib.sha256(str(v).encode()).hexdigest()[:12]


def listish(v):
    return v if isinstance(v, list) else []


def okx_items(body, user_side):
    data = body.get("data") if isinstance(body, dict) else None
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        preferred = user_side.lower()
        if isinstance(data.get(preferred), list):
            return data[preferred]
        # books endpoint often keys by provider-side book.
        provider_key = "sell" if user_side == "BUY" else "buy"
        if isinstance(data.get(provider_key), list):
            return data[provider_key]
        arrays = [v for v in data.values() if isinstance(v, list)]
        if len(arrays) == 1:
            return arrays[0]
    return []


def summarize(provider, asset, side, items, response_meta, complete, pagination, authless, endpoint):
    ad_ids = []
    merchants = []
    good_price = good_limits = good_inventory = good_payments = 0
    good_orders = good_rate = good_ad_id = good_merchant = 0
    freshness_fields = set()
    prices = []

    for x in items:
        if not isinstance(x, dict):
            continue
        if provider == "okx":
            price = num(first(x, ("price",)))
            mn = num(first(x, ("quoteMinAmountPerOrder", "minAmount")))
            mx = num(first(x, ("quoteMaxAmountPerOrder", "maxAmount")))
            inv = num(first(x, ("availableAmount", "tradableAmount")))
            aid = first(x, ("id", "advertisementId", "advNo"))
            mid = identity_value(x, ("merchantId", "publicUserId", "userId", "nickName"))
            orders = num(first(x, ("completedOrderQuantity", "completedOrderCount", "orderCount")))
            rate = num(first(x, ("completedRate", "completionRate", "finishRate")))
            pays = first(x, ("paymentMethods", "payments", "payTypes"))
        else:
            price = num(first(x, ("price",)))
            mn = num(first(x, ("minAmount",)))
            mx = num(first(x, ("maxAmount",)))
            inv = num(first(x, ("lastQuantity", "quantity")))
            aid = first(x, ("id", "itemId", "advNo"))
            # userId is commonly the anonymous sentinel "0" on the keyless
            # endpoint. Prefer advertiser/account identities that actually
            # distinguish public ads.
            mid = identity_value(x, ("accountId", "userMaskId", "merchantId", "nickName", "userId"))
            orders = num(first(x, ("recentOrderNum", "orderNum", "completedOrderQuantity")))
            rate = num(first(x, ("recentExecuteRate", "completionRate")))
            pays = first(x, ("payments", "paymentMethods", "payTypes"))

        if price and price > 10_000:
            good_price += 1
            prices.append(price)
        if mn is not None and mx is not None and mn >= 0 and mx > 0 and mx >= mn:
            good_limits += 1
        if inv is not None and inv > 0:
            good_inventory += 1
        if isinstance(pays, list) and len(pays) > 0:
            good_payments += 1
        if orders is not None and orders >= 0:
            good_orders += 1
        if rate is not None and rate >= 0:
            good_rate += 1
        if aid not in (None, ""):
            good_ad_id += 1
            ad_ids.append(str(aid))
        if mid not in (None, ""):
            good_merchant += 1
            merchants.append(str(mid))
        for k in ("createTime", "updateTime", "createdAt", "updatedAt", "createDate", "updateDate"):
            if x.get(k) not in (None, ""):
                freshness_fields.add(k)

    c = Counter(merchants)
    duplicate_merchants = sum(1 for n in c.values() if n > 1)
    unique_ads = len(set(ad_ids))
    unique_merchants = len(set(merchants))
    n = len(items)
    order = "unknown"
    if len(prices) >= 2:
        asc = all(a <= b for a, b in zip(prices, prices[1:]))
        desc = all(a >= b for a, b in zip(prices, prices[1:]))
        order = "asc" if asc else "desc" if desc else "mixed"

    field_keys = sorted({k for x in items[:5] if isinstance(x, dict) for k in x.keys()})
    merchant_identity_usable = bool(
        n and good_merchant == n and (unique_merchants > 1 or n == 1)
    )
    provider_side_values = sorted({
        str(x.get("side")) for x in items
        if isinstance(x, dict) and x.get("side") not in (None, "")
    })[:10]

    evidence = {
        "provider": provider,
        "asset": asset,
        "side": side,
        "fiat": FIAT,
        "endpoint": endpoint,
        "authless": bool(authless),
        "items": n,
        "complete": bool(complete),
        "pagination": pagination,
        "response_meta": response_meta,
        "coverage": {
            "price": good_price,
            "fiat_limits": good_limits,
            "inventory_crypto": good_inventory,
            "payment_methods": good_payments,
            "order_count": good_orders,
            "completion_rate": good_rate,
            "ad_identity": good_ad_id,
            "merchant_identity": good_merchant,
        },
        "unique_ads": unique_ads,
        "unique_merchants": unique_merchants,
        "duplicate_merchants": duplicate_merchants,
        "merchant_identity_usable": merchant_identity_usable,
        "provider_side_values": provider_side_values,
        "price_order": order,
        "freshness_fields": sorted(freshness_fields),
        "sample_keys": field_keys[:80],
        "sample_ad_hash": short_hash(ad_ids[0]) if ad_ids else None,
        "sample_merchant_hash": short_hash(merchants[0]) if merchants else None,
        "amount_filter_possible": bool(n and good_price == n and good_limits == n and good_inventory == n),
        "capacity_without_guessing": bool(n and good_inventory == n),
    }
    print("evidence " + json.dumps(evidence, separators=(",", ":"), sort_keys=True))
    return evidence


def get_json(session, url, *, params=None, body=None):
    headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}
    if body is None:
        r = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
    else:
        headers["Content-Type"] = "application/json"
        r = session.post(url, json=body, headers=headers, timeout=TIMEOUT)
    status = int(r.status_code)
    if status != 200:
        return status, None
    try:
        return status, r.json()
    except Exception:
        return status, None


def qualify_okx(session, asset, side):
    # First try the marketplace endpoint because it exposes explicit paging controls
    # and richer ad fields. This remains unauthenticated/read-only.
    params = {
        "paymentMethod": "all",
        "userType": "all",
        "hideOverseasVerificationAds": "false",
        "sortType": "price_asc",
        "limit": str(OKX_PAGE_SIZE),
        "currentPage": "1",
        "numberPerPage": str(OKX_PAGE_SIZE),
        "side": side,
        "fiatCurrency": FIAT,
        "cryptoCurrency": asset,
    }
    s1, b1 = get_json(session, OKX_MARKET, params=params)
    items1 = okx_items(b1 or {}, side) if b1 else []
    endpoint = "marketplace-prelogin"
    authless = s1 == 200

    if not items1:
        # Current writer's legacy price-context endpoint fallback.
        provider_side = "sell" if side == "BUY" else "buy"
        params = {
            "quoteCurrency": FIAT,
            "baseCurrency": asset,
            "side": provider_side,
            "paymentMethod": "all",
            "userType": "all",
            "showTrade": "false",
            "showFollow": "false",
            "showAlreadyTraded": "false",
            "isAbleFilter": "false",
            "limit": "50",
        }
        s1, b1 = get_json(session, OKX_BOOKS, params=params)
        items1 = okx_items(b1 or {}, side) if b1 else []
        endpoint = "books"
        authless = s1 == 200
        meta = {"http": s1, "root_keys": sorted((b1 or {}).keys())[:30] if isinstance(b1, dict) else []}
        return summarize("okx", asset, side, items1, meta, False, "not_proven_books_no_page_walk", authless, endpoint)

    # Explicit page-2 probe. If page 2 is empty after a page smaller than the
    # requested page size, the bounded terminal condition is directly observed.
    p2 = dict(params)
    p2["currentPage"] = "2"
    s2, b2 = get_json(session, OKX_MARKET, params=p2)
    items2 = okx_items(b2 or {}, side) if b2 else []
    ids1 = {str(first(x, ("id", "advertisementId", "advNo"))) for x in items1 if isinstance(x, dict)}
    ids2 = {str(first(x, ("id", "advertisementId", "advNo"))) for x in items2 if isinstance(x, dict)}
    same_nonempty_page = bool(ids1 and ids2 and ids1 == ids2)
    terminal = s2 == 200 and not items2 and len(items1) < OKX_PAGE_SIZE
    complete = bool(terminal)
    pagination = "page2_empty_terminal" if terminal else "page2_same_as_page1" if same_nonempty_page else "page2_nonempty_or_unresolved"
    meta = {
        "http_page1": s1,
        "http_page2": s2,
        "page1_items": len(items1),
        "page2_items": len(items2),
        "root_keys": sorted((b1 or {}).keys())[:30] if isinstance(b1, dict) else [],
    }
    return summarize("okx", asset, side, items1, meta, complete, pagination, authless, endpoint)


def qualify_bybit(session, asset, side):
    # Bybit keyless web endpoint. side=1 is taker BUY, side=0 taker SELL in
    # the current Wave source; qualification reports actual fields/completeness.
    provider_side = "1" if side == "BUY" else "0"
    all_items = []
    total = None
    statuses = []
    page = 1
    complete = False

    while page <= MAX_PAGES:
        body = {
            "userId": "",
            "tokenId": asset,
            "currencyId": FIAT,
            "payment": [],
            "side": provider_side,
            "size": str(BYBIT_PAGE_SIZE),
            "page": str(page),
            "amount": "",
            "authMaker": False,
            "canTrade": False,
        }
        st, data = get_json(session, BYBIT_ONLINE, body=body)
        statuses.append(st)
        if st != 200 or not isinstance(data, dict):
            break
        result = data.get("result") or {}
        items = result.get("items") if isinstance(result, dict) else None
        items = items if isinstance(items, list) else []
        if total is None:
            try:
                total = int(result.get("count"))
            except Exception:
                total = None
        if not items:
            complete = total is None or len(all_items) >= total
            break
        all_items.extend(items)
        if total is not None and len(all_items) >= total:
            complete = True
            break
        if len(items) < BYBIT_PAGE_SIZE:
            complete = total is None or len(all_items) >= total
            break
        page += 1
        time.sleep(0.12)

    if page > MAX_PAGES and (total is None or len(all_items) < total):
        complete = False

    pagination = f"walked_pages={min(page, MAX_PAGES)};max_pages={MAX_PAGES};reported_total={total}"
    meta = {
        "http_statuses": statuses,
        "reported_total": total,
        "collected": len(all_items),
        "max_capacity": BYBIT_PAGE_SIZE * MAX_PAGES,
    }
    return summarize("bybit", asset, side, all_items, meta, complete, pagination, bool(statuses and statuses[0] == 200), "online")


def main():
    print("P2P_LIQ_V2_PROVIDER_QUAL_BEGIN")
    print("mode=read_only auth=none storage=none fiat=VND assets=USDT,USDC sides=BUY,SELL")
    results = []
    for provider in ("okx", "bybit"):
        for asset in ASSETS:
            for side in SIDES:
                session = requests.Session(impersonate="chrome116")
                try:
                    if provider == "okx":
                        r = qualify_okx(session, asset, side)
                    else:
                        r = qualify_bybit(session, asset, side)
                    results.append(r)
                except Exception as exc:
                    print(f"error provider={provider} asset={asset} side={side} type={type(exc).__name__} msg={str(exc)[:160]}")
                    results.append({"provider": provider, "asset": asset, "side": side, "complete": False, "error": type(exc).__name__})
                time.sleep(0.15)

    for provider in ("okx", "bybit"):
        rows = [r for r in results if r.get("provider") == provider]
        full = bool(len(rows) == 4 and all(r.get("complete") for r in rows))
        amount = bool(rows and all(r.get("amount_filter_possible") for r in rows))
        capacity = bool(rows and all(r.get("capacity_without_guessing") for r in rows))
        authless = bool(rows and all(r.get("authless") for r in rows))
        identity = bool(rows and all(r.get("merchant_identity_usable") for r in rows))
        classification = "QUALIFIED_CANDIDATE" if full and amount and capacity and authless and identity else "PARTIAL_EXCLUDE_ALL"
        print(f"summary provider={provider} classification={classification} complete={str(full).lower()} amount={str(amount).lower()} capacity={str(capacity).lower()} authless={str(authless).lower()} identity={str(identity).lower()}")

    print("P2P_LIQ_V2_PROVIDER_QUAL_END")


if __name__ == "__main__":
    main()
