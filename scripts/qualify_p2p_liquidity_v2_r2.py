#!/usr/bin/env python3
"""Read-only runtime qualification for P2P Liquidity v2 R2.

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
    print("P2P_LIQ_V2_R2_RUNTIME_BEGIN")
    print(
        "mode=read_only storage=none "
        f"methodology={p2p.LIQUIDITY_V2_METHODOLOGY} "
        f"qualified={','.join(p2p.LIQUIDITY_V2_QUALIFIED_PROVIDERS)} "
        f"excluded={','.join(sorted(p2p.LIQUIDITY_V2_EXCLUDED_PROVIDERS))}"
    )

    if p2p.LIQUIDITY_V2_METHODOLOGY != "p2p-liquidity-v2-r2":
        raise SystemExit("FAIL methodology version is not R2")
    if p2p.LIQUIDITY_V2_MAKER_BUY_CAP_CRYPTO != {"USDT": 10_000.0, "USDC": 10_000.0}:
        raise SystemExit("FAIL maker-buy cap configuration drift")

    session = p2p.requests.Session(impersonate="chrome116")
    ts = int(time.time())
    records, market = p2p.build_liquidity_and_market(session, ts)

    if not market or market.get("complete") is not True:
        raise SystemExit("FAIL canonical Binance market not complete")
    if market.get("schema_version") != 1:
        raise SystemExit(f"FAIL market schema drift={market.get('schema_version')}")
    if "providers" in market or "liquidity_v2" in market:
        raise SystemExit("FAIL canonical market payload unexpectedly expanded")

    v2 = [r for r in records if r.get("record_type") == "liquidity_v2_snapshot"]
    got = {(r.get("exchange"), r.get("asset"), r.get("side")) for r in v2}
    missing = sorted(EXPECTED - got)
    extra = sorted(got - EXPECTED)
    if missing or extra:
        raise SystemExit(f"FAIL v2 membership missing={missing} extra={extra}")

    if any(r.get("exchange") in {"okx", "all", "ALL"} for r in v2):
        raise SystemExit("FAIL excluded provider/aggregate appeared in v2 records")

    by_key = {}
    for r in v2:
        key = (r["exchange"], r["asset"], r["side"])
        by_key[key] = r
        if r.get("methodology_version") != "p2p-liquidity-v2-r2":
            raise SystemExit(f"FAIL methodology mismatch {key}")
        if r.get("complete") is not True or r.get("is_partial"):
            raise SystemExit(f"FAIL incomplete {key}")
        if not (float(r.get("capacity_crypto") or 0) > 0):
            raise SystemExit(f"FAIL zero capacity {key}")
        if not (int(r.get("qualified_merchant_count") or 0) > 0):
            raise SystemExit(f"FAIL zero qualified merchants {key}")
        if r.get("capacity_policy") != "side_aware_dynamic_order_maker_buy_cap10k_v2":
            raise SystemExit(f"FAIL capacity policy drift {key}")
        if r["side"] == "SELL":
            if float(r.get("maker_buy_cap_crypto") or 0) != 10_000:
                raise SystemExit(f"FAIL SELL cap missing {key}")
        elif r.get("maker_buy_cap_crypto") is not None:
            raise SystemExit(f"FAIL BUY unexpectedly capped {key}")

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

    for provider in p2p.LIQUIDITY_V2_QUALIFIED_PROVIDERS:
        for asset in p2p.BNC_ASSETS:
            buy = float(by_key[(provider, asset, "BUY")]["capacity_crypto"])
            sell = float(by_key[(provider, asset, "SELL")]["capacity_crypto"])
            ratio = sell / buy if buy > 0 else None
            print(
                f"ratio exchange={provider} asset={asset} "
                + (f"sell_over_buy={ratio:.4f}" if ratio is not None else "sell_over_buy=NA")
            )

    print(
        "summary "
        f"records={len(v2)} market_schema={market['schema_version']} "
        f"aggregate_status={p2p.LIQUIDITY_V2_AGGREGATE_STATUS}"
    )
    print("P2P_LIQ_V2_R2_RUNTIME_END")


if __name__ == "__main__":
    main()
