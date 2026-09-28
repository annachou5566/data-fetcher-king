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
OKX_PAGE_SIZE = 50

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
    good_orders = good_rate = good_ad_id = good_merchant = good_units = 0
    freshness_fields = set()
    prices = []
    completion_rates = []
    fiat_limit_inventory_relation = 0

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
            unit_ok = str(x.get("quoteCurrency") or "").upper() == FIAT and str(x.get("baseCurrency") or "").upper() == asset
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
            unit_ok = str(x.get("currencyId") or "").upper() == FIAT and str(x.get("tokenId") or "").upper() == asset

        if unit_ok:
            good_units += 1
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
            completion_rates.append(rate)
        if (
            price and price > 0 and mn is not None and mx is not None and inv is not None
            and mn >= 0 and mx >= mn and inv > 0 and mx <= inv * price * 1.05
        ):
            fiat_limit_inventory_relation += 1
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
    if completion_rates:
        rmin = min(completion_rates)
        rmax = max(completion_rates)
        rate_scale = "fraction" if rmax <= 1.000001 else "percent" if rmax <= 100.000001 else "invalid"
    else:
        rmin = rmax = None
        rate_scale = "missing"
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
            "unit_labels": good_units,
        },
        "unique_ads": unique_ads,
        "unique_merchants": unique_merchants,
        "duplicate_merchants": duplicate_merchants,
        "merchant_identity_usable": merchant_identity_usable,
        "provider_side_values": provider_side_values,
        "price_order": order,
        "completion_rate_scale": rate_scale,
        "completion_rate_min": rmin,
        "completion_rate_max": rmax,
        "fiat_limit_within_inventory_value": fiat_limit_inventory_relation,
        "freshness_fields": sorted(freshness_fields),
        "sample_keys": field_keys[:80],
        "sample_ad_hash": short_hash(ad_ids[0]) if ad_ids else None,
        "sample_merchant_hash": short_hash(merchants[0]) if merchants else None,
        "unit_labels_match": bool(n and good_units == n),
        "amount_filter_possible": bool(n and good_price == n and good_limits == n and good_inventory == n and good_units == n),
        "capacity_without_guessing": bool(n and good_inventory == n and good_units == n),
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
    # OKX endpoint side is maker-ad perspective. Wave/user BUY consumes maker
    # SELL ads; Wave/user SELL consumes maker BUY ads.
    provider_side = "sell" if side == "BUY" else "buy"
    endpoint = "marketplace-prelogin"
    all_items = []
    seen_ids = set()
    statuses = []
    page_sizes = []
    repeated_page = False
    complete = False
    first_body = None

    for page in range(1, MAX_PAGES + 1):
        params = {
            "paymentMethod": "all",
            "userType": "all",
            "hideOverseasVerificationAds": "false",
            "sortType": "price_asc",
            "limit": str(OKX_PAGE_SIZE),
            "currentPage": str(page),
            "numberPerPage": str(OKX_PAGE_SIZE),
            "side": provider_side,
            "fiatCurrency": FIAT,
            "cryptoCurrency": asset,
        }
        st, body = get_json(session, OKX_MARKET, params=params)
        statuses.append(st)
        if page == 1:
            first_body = body
        if st != 200 or not isinstance(body, dict):
            break

        items = okx_items(body, provider_side.upper())
        page_sizes.append(len(items))
        if not items:
            complete = page > 1
            break

        page_ids = []
        new_count = 0
        for idx, item in enumerate(items):
            aid = first(item, ("id", "advertisementId", "advNo")) if isinstance(item, dict) else None
            key = str(aid) if aid not in (None, "") else f"page={page}:idx={idx}"
            page_ids.append(key)
            if key not in seen_ids:
                seen_ids.add(key)
                all_items.append(item)
                new_count += 1

        if page > 1 and new_count == 0:
            repeated_page = True
            break

        # A short page is an observed terminal condition. Otherwise walk until
        # an empty page or the hard safety bound.
        if len(items) < OKX_PAGE_SIZE:
            complete = True
            break
        time.sleep(0.12)

    if not all_items:
        # Existing writer endpoint can still provide price context, but without
        # a proven page walk it is not sufficient for Liquidity-v2 completeness.
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
        st, body = get_json(session, OKX_BOOKS, params=params)
        items = okx_items(body or {}, side) if body else []
        meta = {
            "http": st,
            "root_keys": sorted((body or {}).keys())[:30] if isinstance(body, dict) else [],
        }
        return summarize(
            "okx", asset, side, items, meta, False,
            "not_proven_books_no_page_walk", st == 200, "books"
        )

    if len(page_sizes) >= MAX_PAGES and page_sizes[-1] >= OKX_PAGE_SIZE:
        complete = False

    # Completeness cross-check: the endpoint historically accepts a much larger
    # first-page limit even though currentPage pagination can return empty page 2.
    # If that wide request exposes more ads than the page walk, page-walk
    # completeness is disproven and this provider must be excluded from ALL.
    wide_params = {
        "paymentMethod": "all",
        "userType": "all",
        "hideOverseasVerificationAds": "false",
        "sortType": "price_asc",
        "limit": "1000",
        "currentPage": "1",
        "numberPerPage": "1000",
        "side": provider_side,
        "fiatCurrency": FIAT,
        "cryptoCurrency": asset,
    }
    wide_status, wide_body = get_json(session, OKX_MARKET, params=wide_params)
    wide_items = okx_items(wide_body or {}, provider_side.upper()) if wide_body else []
    wide_count = len(wide_items)
    if wide_status != 200 or wide_count > len(all_items):
        complete = False

    pagination = (
        f"walked_pages={len(page_sizes)};page_size={OKX_PAGE_SIZE};"
        f"page_sizes={','.join(str(x) for x in page_sizes)};"
        f"wide_count={wide_count};"
        f"repeated_page={str(repeated_page).lower()}"
    )
    meta = {
        "http_statuses": statuses,
        "wide_http_status": wide_status,
        "wide_count": wide_count,
        "collected_unique": len(all_items),
        "page_sizes": page_sizes,
        "max_capacity": OKX_PAGE_SIZE * MAX_PAGES,
        "root_keys": sorted((first_body or {}).keys())[:30] if isinstance(first_body, dict) else [],
    }
    # The public OKX prelogin surface exposes rich ad fields but no
    # provider-native total count / last-page marker. currentPage also does not
    # establish a trustworthy page walk: bounded probes can return an empty
    # page 2 even when raising limit yields more page-1 ads. Therefore field
    # semantics are qualified, but whole-book completeness remains NOT PROVEN.
    observed_terminal = bool(complete and not repeated_page)
    meta["observed_terminal"] = observed_terminal
    meta["completeness_reason"] = "no_provider_total_or_last_page_marker"
    return summarize(
        "okx", asset, side, all_items, meta,
        False,
        pagination + ";completeness=not_proven",
        bool(statuses and all(st == 200 for st in statuses)),
        endpoint,
    )


def qualify_bybit(session, asset, side):
    # Bybit keyless web endpoint. side=1 is taker BUY, side=0 taker SELL.
    provider_side = "1" if side == "BUY" else "0"
    items_by_id = {}
    reported_totals = []
    statuses = []
    page_sizes = []
    complete = False

    for page in range(1, MAX_PAGES + 1):
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
        page_sizes.append(len(items))
        try:
            reported_totals.append(int(result.get("count")))
        except Exception:
            pass

        if not items:
            complete = page > 1 or not reported_totals
            break

        for idx, item in enumerate(items):
            aid = first(item, ("id", "itemId", "advNo")) if isinstance(item, dict) else None
            key = str(aid) if aid not in (None, "") else f"page={page}:idx={idx}"
            items_by_id[key] = item

        current_total = reported_totals[-1] if reported_totals else None
        if current_total is not None and len(items_by_id) >= current_total:
            complete = True
            break
        if len(items) < BYBIT_PAGE_SIZE:
            complete = True
            break
        time.sleep(0.12)

    if len(page_sizes) >= MAX_PAGES and page_sizes[-1] >= BYBIT_PAGE_SIZE and not complete:
        complete = False

    all_items = list(items_by_id.values())
    pagination = (
        f"walked_pages={len(page_sizes)};page_size={BYBIT_PAGE_SIZE};"
        f"reported_totals={','.join(str(x) for x in reported_totals)};"
        f"page_sizes={','.join(str(x) for x in page_sizes)}"
    )
    meta = {
        "http_statuses": statuses,
        "reported_totals": reported_totals,
        "collected_unique": len(all_items),
        "page_sizes": page_sizes,
        "max_capacity": BYBIT_PAGE_SIZE * MAX_PAGES,
    }
    return summarize(
        "bybit", asset, side, all_items, meta, complete,
        pagination,
        bool(statuses and all(st == 200 for st in statuses)),
        "online",
    )


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
