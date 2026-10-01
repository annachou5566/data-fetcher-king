import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

CT_CONTRACT = "0x0a092e544da31150b439a1aaa1a3a2214a867f46"
CT_SYMBOL = "CT"
CT_DATE = "2026-09-30"
CT_TIME = "08:00"
R2_KEYS = (
    "alpha-events/all.json",
    "alpha-events/history.json",
)


def build_ct_event(source_path):
    doc = json.loads(Path(source_path).read_text(encoding="utf-8"))
    rows = doc.get("airdrops")
    if not isinstance(rows, list):
        raise RuntimeError("airdrops source schema invalid")

    matches = [
        r for r in rows
        if isinstance(r, dict)
        and str(r.get("token") or "").upper() == CT_SYMBOL
        and str(r.get("contract_address") or "").lower() == CT_CONTRACT
        and r.get("date") == CT_DATE
        and r.get("time") == CT_TIME
    ]
    if len(matches) != 1:
        raise RuntimeError(f"exact CT source row cardinality={len(matches)}")

    r = matches[0]
    if str(r.get("points")) != "223" or str(r.get("amount")) != "200":
        raise RuntimeError("CT source facts mismatch")

    return {
        "project_name": r.get("name") or CT_SYMBOL,
        "symbol": CT_SYMBOL,
        "event_type": str(r.get("type") or "grab").lower(),
        "points_threshold": str(r.get("points") or ""),
        "amount_per_user": r.get("amount"),
        "contract_address": CT_CONTRACT,
        "chain_id": "56",
        "chain_name": "BSC",
        "event_time": "2026-09-30T08:00:00+00:00",
        "status": "ended",
        "phase": r.get("phase"),
        "spot_listed": bool(r.get("spot_listed")),
        "futures_listed": bool(r.get("futures_listed")),
        "completed": bool(r.get("completed")),
        "source_channel": "historical",
    }


def minute_key(row):
    symbol = str(row.get("symbol") or row.get("token") or "").upper()
    contract = str(
        row.get("contract_address") or row.get("contract") or ""
    ).lower()
    event_time = str(row.get("event_time") or "")
    if not event_time and row.get("date"):
        event_time = f"{row.get('date')}T{row.get('time') or '00:00'}:00+00:00"
    return symbol, contract, event_time[:16]


def merge_ct(existing, candidate):
    if not isinstance(existing, list):
        raise RuntimeError("R2 all.json is not a list")

    target = minute_key(candidate)
    exact = [r for r in existing if isinstance(r, dict) and minute_key(r) == target]

    core = (
        "project_name", "symbol", "event_type", "points_threshold",
        "amount_per_user", "contract_address", "chain_id",
        "event_time", "status",
    )

    if exact:
        if len(exact) != 1:
            raise RuntimeError("duplicate exact CT identities")
        for k in core:
            a = str(exact[0].get(k) or "").lower()
            b = str(candidate.get(k) or "").lower()
            if a != b:
                raise RuntimeError(f"existing CT conflict at {k}")
        return existing, False, "ALREADY_PRESENT"

    same_day_conflicts = []
    for row in existing:
        if not isinstance(row, dict):
            continue
        sym, con, tm = minute_key(row)
        if tm[:10] != CT_DATE:
            continue
        if sym == CT_SYMBOL or con == CT_CONTRACT:
            same_day_conflicts.append(row)

    if same_day_conflicts:
        raise RuntimeError("same-day CT identity conflict")

    def event_epoch(row):
        raw = str(row.get("event_time") or "").strip()
        if not raw and row.get("date"):
            raw = f"{row.get('date')}T{row.get('time') or '00:00'}:00+00:00"
        if not raw:
            return None
        try:
            value = raw.replace("Z", "+00:00")
            if " " in value and "T" not in value:
                value = value.replace(" ", "T", 1)
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return None

    candidate_ts = event_epoch(candidate)
    if candidate_ts is None:
        raise RuntimeError("CT event_time invalid")

    out = list(existing)
    insert_at = len(out)

    # Existing canonical history is newest-first. Preserve every existing
    # row and its relative order; insert CT immediately before the first
    # older parseable event. Never sort/rewrite historical rows globally.
    for i, row in enumerate(out):
        if not isinstance(row, dict):
            continue
        ts = event_epoch(row)
        if ts is not None and ts < candidate_ts:
            insert_at = i
            break

    out.insert(insert_at, candidate)
    return out, True, "INSERT_CT"


def r2_client():
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/airdrops.json")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    candidate = build_ct_event(args.source)
    r2 = r2_client()
    bucket = os.environ["R2_BUCKET_NAME"]

    plans = []

    for key in R2_KEYS:
        obj = r2.get_object(Bucket=bucket, Key=key)
        existing = json.loads(obj["Body"].read().decode("utf-8"))

        merged, changed, action = merge_ct(existing, candidate)

        plans.append((key, existing, merged, changed, action))

        print(f"R2_KEY={key}")
        print(f"COUNT_BEFORE={len(existing)}")
        print(f"COUNT_AFTER={len(merged)}")
        print(f"ACTION={action}")

    print(f"MODE={'APPLY' if args.apply else 'DRY_RUN'}")
    print("CT_EVENT=" + json.dumps(
        candidate,
        separators=(",", ":"),
        sort_keys=True,
    ))

    if not args.apply:
        print("R2_WRITES=0")
        return

    writes = 0

    for key, existing, merged, changed, action in plans:
        if not changed:
            continue

        body = json.dumps(
            merged,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        r2.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            CacheControl="public, max-age=60",
        )
        writes += 1
        print(f"R2_WRITE_KEY={key}")

    print(f"R2_WRITES={writes}")


if __name__ == "__main__":
    main()
