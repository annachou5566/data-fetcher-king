#!/usr/bin/env python3
"""Read-only Production R2 verification for P2P Liquidity v2 R1."""

import json
import os
from datetime import datetime, timezone

import boto3

METHOD = "p2p-liquidity-v2-r1"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
DAILY = f"p2p-snapshots/{TODAY}.json"
MANIFEST = "p2p-snapshots/_manifest.json"
MARKET = "p2p-snapshots/_market-latest.json"
LEGACY = "p2p-data.json"


def r2_client():
    required = (
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_ENDPOINT_URL",
        "R2_BUCKET_NAME",
    )
    if not all(os.getenv(k) for k in required):
        raise RuntimeError("required R2 environment is unavailable")
    return (
        boto3.client(
            "s3",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
            endpoint_url=os.environ["R2_ENDPOINT_URL"],
        ),
        os.environ["R2_BUCKET_NAME"],
    )


def get_json(client, bucket, key):
    obj = client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def iso(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def main():
    print("P2P_LIQ_V2_R2_VERIFY_BEGIN")
    client, bucket = r2_client()

    daily = get_json(client, bucket, DAILY)
    records = daily.get("records") or []
    v2 = [
        r for r in records
        if r.get("record_type") == "liquidity_v2_snapshot"
        and r.get("methodology_version") == METHOD
    ]
    if not v2:
        raise SystemExit("FAIL no v2 records in current daily partition")

    exchanges = sorted({str(r.get("exchange")) for r in v2})
    assets = sorted({str(r.get("asset")) for r in v2})
    sides = sorted({str(r.get("side")) for r in v2})
    if set(exchanges) - {"binance", "bybit"}:
        raise SystemExit(f"FAIL unexpected v2 exchange(s): {exchanges}")
    if "okx" in exchanges or "all" in {x.lower() for x in exchanges}:
        raise SystemExit("FAIL excluded OKX/ALL record persisted")

    required = {
        "record_type", "methodology_version", "ts", "collected_at",
        "exchange", "asset", "fiat", "side", "capacity_crypto",
        "capacity_vnd", "qualified_ad_count", "qualified_merchant_count",
        "source_ad_count", "complete", "is_partial", "qualification_policy",
        "capacity_policy", "merchant_dedupe", "aggregate_eligible",
        "aggregate_status", "aggregate_exclusion_reason",
    }
    missing_shapes = 0
    invalid_rows = 0
    for r in v2:
        if required - set(r):
            missing_shapes += 1
        if (
            r.get("complete") is not True
            or r.get("is_partial") is True
            or r.get("aggregate_eligible") is not False
            or r.get("aggregate_status") != "EXCLUDED"
            or r.get("fiat") != "VND"
        ):
            invalid_rows += 1

    if missing_shapes or invalid_rows:
        raise SystemExit(
            f"FAIL schema/flags missing_shapes={missing_shapes} invalid_rows={invalid_rows}"
        )

    first_ts = min(int(r["ts"]) for r in v2)
    latest_ts = max(int(r["ts"]) for r in v2)
    latest = [r for r in v2 if int(r["ts"]) == latest_ts]
    latest_membership = sorted(
        f"{r['exchange']}:{r['asset']}:{r['side']}" for r in latest
    )

    manifest = get_json(client, bucket, MANIFEST)
    if TODAY not in (manifest.get("dates") or []):
        raise SystemExit("FAIL current date missing from manifest")

    market = get_json(client, bucket, MARKET)
    if (
        market.get("schema_version") != 1
        or market.get("record_type") != "market_snapshot"
        or market.get("complete") is not True
        or "providers" in market
        or "liquidity_v2" in market
    ):
        raise SystemExit("FAIL canonical market shape drift")

    legacy = get_json(client, bucket, LEGACY)
    legacy_count = int(legacy.get("count") or len(legacy.get("snapshots") or []))
    if legacy_count <= 0:
        raise SystemExit("FAIL legacy object empty")

    v1_count = sum(1 for r in records if r.get("record_type") == "liquidity_snapshot")
    imbalance_count = sum(1 for r in records if r.get("record_type") == "imbalance_index")

    print(f"date={TODAY} daily_count={len(records)} v2_count={len(v2)}")
    print(
        f"v2_exchanges={','.join(exchanges)} assets={','.join(assets)} "
        f"sides={','.join(sides)}"
    )
    print(f"v2_first_ts={first_ts} v2_first_iso={iso(first_ts)}")
    print(f"v2_latest_ts={latest_ts} v2_latest_iso={iso(latest_ts)}")
    print("v2_latest_membership=" + ",".join(latest_membership))
    print(f"v1_liquidity_rows={v1_count} v1_imbalance_rows={imbalance_count}")
    print(
        f"market_schema={market.get('schema_version')} "
        f"market_exchange={market.get('exchange')} "
        f"market_assets={','.join(sorted((market.get('assets') or {}).keys()))}"
    )
    print(f"legacy_count={legacy_count} manifest_has_today=true")
    print("schema_required_fields=PASS flags=PASS excluded_okx_all=PASS")
    print("P2P_LIQ_V2_R2_VERIFY_END")


if __name__ == "__main__":
    main()
