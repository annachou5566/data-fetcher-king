#!/usr/bin/env python3
"""Read-only runtime qualification for P2P Liquidity v2 R1 source.

Runs the exact writer fetch/composition path in memory only.
No R2 credentials are read and no storage write function is called.
"""

import time

import fetch_p2p as p2p


EXPECTED = {
    (provider, asset, side)
    for provider in p2p.LIQUIDITY_V2_QUALIFIED_PROVIDERS
    for asset in p2p.BNC_ASSETS
    for side in ("BUY", "SELL")
}


def main():
    print("P2P_LIQ_V2_R1_RUNTIME_BEGIN")
    print(
        "mode=read_only storage=none "
        f"methodology={p2p.LIQUIDITY_V2_METHODOLOGY} "
        f"qualified={','.join(p2p.LIQUIDITY_V2_QUALIFIED_PROVIDERS)} "
        f"excluded={','.join(sorted(p2p.LIQUIDITY_V2_EXCLUDED_PROVIDERS))}"
    )

    session = p2p.requests.Session(impersonate="chrome116")
    ts = int(time.time())
    records, market = p2p.build_liquidity_and_market(session, ts)

    if not market or market.get("complete") is not True:
        raise SystemExit("FAIL canonical Binance market not complete")
    if market.get("schema_version") != 1:
        raise SystemExit(f"FAIL market schema drift={market.get('schema_version')}")
    if "providers" in market or "liquidity_v2" in market:
        raise SystemExit("FAIL canonical market payload unexpectedly expanded")

    v2 = [
        r for r in records
        if r.get("record_type") == "liquidity_v2_snapshot"
    ]
    got = {(r.get("exchange"), r.get("asset"), r.get("side")) for r in v2}

    missing = sorted(EXPECTED - got)
    extra = sorted(got - EXPECTED)
    if missing or extra:
        raise SystemExit(f"FAIL v2 membership missing={missing} extra={extra}")

    if any(r.get("exchange") in {"okx", "all", "ALL"} for r in v2):
        raise SystemExit("FAIL excluded provider/aggregate appeared in v2 records")

    for r in sorted(v2, key=lambda x: (x["exchange"], x["asset"], x["side"])):
        if r.get("methodology_version") != p2p.LIQUIDITY_V2_METHODOLOGY:
            raise SystemExit("FAIL methodology version mismatch")
        if r.get("complete") is not True or r.get("is_partial"):
            raise SystemExit(
                f"FAIL incomplete {r['exchange']} {r['asset']} {r['side']}"
            )
        if not (float(r.get("capacity_crypto") or 0) > 0):
            raise SystemExit(
                f"FAIL zero capacity {r['exchange']} {r['asset']} {r['side']}"
            )
        if not (int(r.get("qualified_merchant_count") or 0) > 0):
            raise SystemExit(
                f"FAIL zero qualified merchants {r['exchange']} {r['asset']} {r['side']}"
            )
        print(
            "record "
            f"exchange={r['exchange']} asset={r['asset']} side={r['side']} "
            f"capacity_crypto={r['capacity_crypto']} "
            f"capacity_vnd={r['capacity_vnd']} "
            f"ads={r['qualified_ad_count']} "
            f"merchants={r['qualified_merchant_count']} "
            f"reported={r.get('reported_ad_count')} "
            f"pages={r.get('pages_fetched')}"
        )

    print(
        "summary "
        f"records={len(v2)} market_schema={market['schema_version']} "
        f"aggregate_status={p2p.LIQUIDITY_V2_AGGREGATE_STATUS}"
    )
    print("P2P_LIQ_V2_R1_RUNTIME_END")


if __name__ == "__main__":
    main()
