#!/usr/bin/env python3
"""Read-only audit for P2P Liquidity methodology R2.

No R2 credentials are read. No storage/write function is called.
The script fetches current public/provider P2P books and prints bounded
aggregate diagnostics only; it never prints merchant identifiers or raw ads.
"""

import math
import time

import fetch_p2p as p2p


BINANCE_SELL_GUARD_USDT = 10_000.0
OKX_LIMITS = (5, 20, 50)


def fnum(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else 0.0
    except Exception:
        return 0.0


def norm_rate(value):
    rate = fnum(value)
    if rate > 1:
        rate /= 100.0
    return rate


def merchant_reduce(items, side):
    by_merchant = {}
    qualified_ads = 0
    dynamic_present = 0

    for item in items:
        adv = item
        merchant = adv.get("advertiser", {}) or {}
        merchant_id = str(
            p2p._first_present(merchant, ["userNo", "advNo", "nickName"]) or ""
        ).strip()
        price = fnum(adv.get("price"))
        available = fnum(
            p2p._first_present(
                adv, ["surplusAmount", "tradableAmount", "tradableQuantity"]
            )
        )
        static_max_fiat = fnum(
            p2p._first_present(adv, ["maxSingleTransAmount", "maxTransAmount"])
        )
        dynamic_raw = p2p._first_present(
            adv, ["dynamicMaxSingleTransAmount", "dynamicMaxTransAmount"]
        )
        dynamic_max_fiat = fnum(dynamic_raw) if dynamic_raw not in (None, "") else 0.0
        order_count = int(fnum(p2p._first_present(merchant, ["monthOrderCount"])))
        completion = norm_rate(p2p._first_present(merchant, ["monthFinishRate"]))

        if (
            not merchant_id
            or price <= 10_000
            or available <= 0
            or static_max_fiat <= 0
            or order_count < p2p.VERIFIED_MIN_ORDER_COUNT
            or completion < p2p.VERIFIED_MIN_FINISH_RATE
        ):
            continue

        qualified_ads += 1
        if dynamic_max_fiat > 0:
            dynamic_present += 1

        static_single = min(available, static_max_fiat / price)
        dynamic_single = min(
            available,
            (dynamic_max_fiat / price) if dynamic_max_fiat > 0 else static_max_fiat / price,
        )
        legacy_v1 = (
            available
            if side == "BUY"
            else min(
                available,
                p2p.SELL_CAP_FLAT_USDT,
                (static_max_fiat / price) * p2p.SELL_CAP_MULTIPLIER,
            )
        )
        guarded = dynamic_single if side == "BUY" else min(dynamic_single, BINANCE_SELL_GUARD_USDT)

        row = {
            "available": available,
            "static_single": static_single,
            "dynamic_single": dynamic_single,
            "legacy_v1": legacy_v1,
            "guarded": guarded,
        }
        old = by_merchant.get(merchant_id)
        if old is None:
            by_merchant[merchant_id] = row
        else:
            for key, value in row.items():
                if value > old[key]:
                    old[key] = value

    return by_merchant, qualified_ads, dynamic_present


def summarize(values):
    vals = sorted((float(v) for v in values if float(v) > 0), reverse=True)
    total = sum(vals)
    if not vals or total <= 0:
        return {
            "total": 0.0, "max": 0.0, "top1_share": 0.0,
            "top5_share": 0.0, "top10_share": 0.0, "gt10k": 0,
        }
    return {
        "total": total,
        "max": vals[0],
        "top1_share": vals[0] / total,
        "top5_share": sum(vals[:5]) / total,
        "top10_share": sum(vals[:10]) / total,
        "gt10k": sum(1 for v in vals if v > 10_000),
    }


def fetch_binance_book(session, asset, side):
    page = 1
    items = []
    reported = None
    partial = False
    while page <= p2p.MAX_PAGE_SAFETY:
        rows, total, ok = p2p.fetch_binance_ads_page(session, asset, side, page)
        if not ok:
            partial = True
            break
        if reported is None:
            reported = int(total or 0)
        if not rows:
            break
        items.extend(rows)
        if reported and len(items) >= reported:
            break
        if len(rows) < p2p.PAGE_SIZE:
            break
        page += 1
        time.sleep(0.12)
    else:
        partial = True
    return items, reported, page, partial


def audit_binance(session):
    print("BINANCE_AUDIT_BEGIN")
    totals = {}
    for asset in p2p.BNC_ASSETS:
        for side in ("BUY", "SELL"):
            items, reported, pages, partial = fetch_binance_book(session, asset, side)
            merchants, qualified_ads, dynamic_present = merchant_reduce(items, side)
            metrics = {
                key: summarize(row[key] for row in merchants.values())
                for key in ("available", "static_single", "dynamic_single", "legacy_v1", "guarded")
            }
            totals[(asset, side)] = metrics
            cur = metrics["static_single"]
            dyn = metrics["dynamic_single"]
            guard = metrics["guarded"]
            legacy = metrics["legacy_v1"]
            print(
                "BINANCE "
                f"asset={asset} side={side} raw_ads={len(items)} reported={reported} "
                f"pages={pages} partial={int(partial)} qualified_ads={qualified_ads} "
                f"merchants={len(merchants)} dynamic_max_ads={dynamic_present} "
                f"v2_static={cur['total']:.2f} v2_dynamic={dyn['total']:.2f} "
                f"guard10k={guard['total']:.2f} legacy_v1={legacy['total']:.2f} "
                f"max_merchant={cur['max']:.2f} gt10k_merchants={cur['gt10k']} "
                f"top1={cur['top1_share']:.4f} top5={cur['top5_share']:.4f} "
                f"top10={cur['top10_share']:.4f}"
            )

    for asset in p2p.BNC_ASSETS:
        buy = totals[(asset, "BUY")]
        sell = totals[(asset, "SELL")]
        for key in ("static_single", "dynamic_single", "guarded", "legacy_v1"):
            b = buy[key]["total"]
            s = sell[key]["total"]
            ratio = (s / b) if b > 0 else None
            print(
                f"BINANCE_RATIO asset={asset} method={key} "
                f"sell_over_buy={ratio:.4f}" if ratio is not None
                else f"BINANCE_RATIO asset={asset} method={key} sell_over_buy=NA"
            )
    print("BINANCE_AUDIT_END")


def okx_items_and_total(body, side):
    data = body.get("data", []) if isinstance(body, dict) else []
    if isinstance(data, dict):
        items = data.get(side, []) or []
        total = None
        for key in ("total", "count", "totalCount", "totalNum"):
            if data.get(key) not in (None, ""):
                total = data.get(key)
                break
        if total is None:
            for key in ("total", "count", "totalCount", "totalNum"):
                if body.get(key) not in (None, ""):
                    total = body.get(key)
                    break
        return items if isinstance(items, list) else [], total
    if isinstance(data, list):
        total = None
        for key in ("total", "count", "totalCount", "totalNum"):
            if isinstance(body, dict) and body.get(key) not in (None, ""):
                total = body.get(key)
                break
        return data, total
    return [], None


def audit_okx(session):
    print("OKX_AUDIT_BEGIN")
    side_complete = {}
    for side in ("sell", "buy"):
        seen = []
        reported_totals = []
        for limit in OKX_LIMITS:
            try:
                res = session.get(
                    p2p.OKX_URL,
                    params={
                        "quoteCurrency": p2p.FIAT,
                        "baseCurrency": "USDT",
                        "side": side,
                        "paymentMethod": "all",
                        "userType": "all",
                        "showTrade": "false",
                        "receivingAds": "false",
                        "showFollow": "false",
                        "showAlreadyTraded": "false",
                        "isAbleFilter": "false",
                        "limit": str(limit),
                    },
                    timeout=15,
                )
                http = res.status_code
                body = res.json() if http == 200 else {}
                items, total = okx_items_and_total(body, side)
            except Exception:
                http, items, total = 0, [], None
            seen.append(len(items))
            if total not in (None, ""):
                try:
                    reported_totals.append(int(total))
                except Exception:
                    pass
            print(
                f"OKX side={side} limit={limit} http={http} "
                f"items={len(items)} reported_total={total}"
            )
            time.sleep(0.15)

        total = reported_totals[-1] if reported_totals else None
        complete = bool(total is not None and seen and max(seen) >= total)
        side_complete[side] = complete
        print(
            f"OKX_SIDE_GATE side={side} counts={','.join(map(str, seen))} "
            f"reported_total={total} completeness={'PROVEN' if complete else 'NOT_PROVEN'}"
        )

    print(
        "OKX_GATE="
        + ("PROVEN" if side_complete.get("sell") and side_complete.get("buy") else "NOT_PROVEN")
    )
    print("OKX_AUDIT_END")


def main():
    print("P2P_LIQUIDITY_METHODOLOGY_R2_AUDIT_BEGIN")
    print("mode=read_only storage=none raw_ads=not_printed merchant_ids=not_printed")
    session = p2p.requests.Session(impersonate="chrome116")
    audit_binance(session)
    audit_okx(session)
    print("P2P_LIQUIDITY_METHODOLOGY_R2_AUDIT_END")


if __name__ == "__main__":
    main()
