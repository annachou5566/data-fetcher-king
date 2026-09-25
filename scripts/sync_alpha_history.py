"""
sync_alpha_history.py
─────────────────────
Merge-preserving import of data/airdrops.json into the canonical Alpha
History R2 objects.

Ownership:
  - this script may write only alpha-events/all.json and alpha-events/history.json
  - realtime storage owns pending.json, upcoming.json, live.json and blindbox.json
  - sync_listing_prices.py owns price enrichment on all.json/history.json

Safety contract:
  - DRY-RUN is the default; --apply is required for any R2 write.
  - Existing canonical rows win. Source data only fills missing base fields.
  - Existing enrichment (listing_price, max_since, spot_listing_price, etc.)
    is never replaced by this importer.
  - Only source rows with completed=true are eligible for historical import.
  - Failure to read either canonical R2 list is fatal; never overwrite from [].
"""

import argparse
import hashlib
import json
import os
import sys
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from botocore.config import Config

try:
    import boto3
except ImportError:
    sys.exit("Missing boto3. Run: pip install boto3")

BINANCE_TOKEN_LIST = (
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/"
    "buw/wallet/cex/alpha/all/token/list"
)

CHAIN_NAMES = {
    "56": "BSC", "1": "ETH", "8453": "Base",
    "501": "SOL", "784": "SUI", "42161": "ARB",
    "146": "SONIC", "59144": "LINEA",
}

R2_ALL_KEY = "alpha-events/all.json"
R2_HISTORY_KEY = "alpha-events/history.json"

# This importer may fill these fields when an existing canonical row has no
# value. It must not replace price/enrichment fields maintained downstream.
BASE_FILL_FIELDS = (
    "project_name", "symbol", "event_type", "points_threshold",
    "amount_per_user", "total_amount", "contract_address",
    "chain_id", "chain_name", "market_cap", "fdv",
    "price_snapshot", "value_usd", "event_time", "phase",
    "pretge", "source_channel", "raw_text",
)

ENRICHMENT_FIELDS = (
    "listing_price", "ath_since_listing_price", "ath_since_listing_date",
    "spot_listing_price", "spot_listed_at", "spot_ath_price", "spot_ath_date",
    "air_number", "_vwap_daybound_checked", "listing_price_unavailable",
)


def fetch_live_prices():
    """Return symbol -> {price, marketCap} from Binance Alpha token list."""
    prices = {}
    try:
        req = urllib.request.Request(
            BINANCE_TOKEN_LIST,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        tokens = data.get("data") or []
        for t in tokens:
            sym = t.get("symbol")
            if sym and t.get("price"):
                prices[sym] = {
                    "price": float(t["price"]),
                    "marketCap": float(t.get("marketCap") or 0),
                }
        print(f"[prices] Fetched {len(prices)} tokens from Binance Alpha")
    except Exception as exc:
        print(f"[prices] Warning: could not fetch live prices: {exc}")
    return prices


def _norm_text(value):
    return str(value or "").strip()


def _norm_contract(value):
    v = _norm_text(value)
    # EVM addresses are case-insensitive. Keep non-EVM identifiers verbatim.
    return v.lower() if v.startswith("0x") else v


def _norm_phase(value):
    v = _norm_text(value)
    return v.lower()


def _event_minute(value):
    """Normalize event_time/date to a stable UTC minute string where possible."""
    raw = _norm_text(value)
    if not raw:
        return ""
    candidate = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M")
    except Exception:
        # Preserve enough precision for legacy date-only values.
        return raw[:16]


def event_identity(event):
    """
    Stable event identity.

    Prefer immutable contract+chain over ticker because Binance Alpha tickers can
    be reused. Keep event minute/type/phase so repeated rounds of one contract do
    not collapse into one row. If contract is missing, symbol is the fallback.
    """
    contract = _norm_contract(
        event.get("contract_address") or event.get("contract")
    )
    symbol = _norm_text(event.get("symbol") or event.get("token")).upper()
    anchor = contract or f"symbol:{symbol}"
    chain = _norm_text(event.get("chain_id") or event.get("chainId") or "56")
    event_time = _event_minute(event.get("event_time") or event.get("date"))
    event_type = _norm_text(event.get("event_type") or event.get("type") or "grab").lower()
    phase = _norm_phase(event.get("phase"))
    return "|".join((anchor, chain, event_time, event_type, phase))


def map_event(source, prices, now=None):
    symbol = _norm_text(source.get("token"))
    date_str = _norm_text(source.get("date"))
    time_str = _norm_text(source.get("time")) or "00:00"
    if not time_str or not time_str[0].isdigit():
        time_str = "00:00"

    now = now or datetime.now(timezone.utc)
    try:
        # data/airdrops.json stores its human-readable event clock in UTC+8.
        # Canonical Alpha History stores UTC. Existing canonical rows prove
        # this consistently (e.g. 17:00 source -> 09:00 UTC).
        if _norm_text(source.get("time")):
            source_tz = timezone(timedelta(hours=8))
            dt_local = datetime.strptime(
                f"{date_str}T{time_str}:00", "%Y-%m-%dT%H:%M:%S"
            ).replace(tzinfo=source_tz)
            dt = dt_local.astimezone(timezone.utc)
            event_iso = dt.isoformat()
        else:
            # Do not invent a midnight timezone shift for date-only records.
            event_iso = date_str
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        delta = (dt - now).total_seconds()
        if delta > 3600:
            status = "upcoming"
        elif delta > -3600:
            status = "live"
        else:
            status = "ended"
    except Exception:
        event_iso = date_str
        status = "ended"

    live = prices.get(symbol, {})
    price_now = live.get("price")
    mc_now = live.get("marketCap") or source.get("market_cap")

    amount_raw = source.get("amount")
    value_usd = None
    if price_now and amount_raw:
        try:
            value_usd = round(float(amount_raw) * price_now, 2)
        except Exception:
            pass

    chain_id = _norm_text(source.get("chain_id") or "56")

    return {
        "project_name": source.get("name") or symbol,
        "symbol": symbol,
        "event_type": (_norm_text(source.get("type")) or "grab").lower(),
        "points_threshold": _norm_text(source.get("points")),
        "amount_per_user": amount_raw,
        "total_amount": source.get("total_amount"),
        "contract_address": source.get("contract_address"),
        "chain_id": chain_id,
        "chain_name": CHAIN_NAMES.get(chain_id, "EVM"),
        "market_cap": mc_now,
        "fdv": source.get("fdv"),
        "price_snapshot": price_now,
        "value_usd": value_usd,
        "event_time": event_iso,
        "status": status,
        "phase": source.get("phase"),
        "spot_listed": bool(source.get("spot_listed")),
        "futures_listed": bool(source.get("futures_listed")),
        "completed": bool(source.get("completed")),
        "pretge": bool(source.get("pretge")),
        "source_channel": "historical",
        "raw_text": None,
        "created_at": now.isoformat(),
    }


def _is_missing(value):
    return value is None or value == "" or value == [] or value == {}


def merge_existing(existing, incoming):
    """Strict append-only policy: a matched canonical row is byte/logically preserved."""
    return deepcopy(existing)


def _dedupe_existing(rows, label):
    out = []
    seen = set()
    collisions = []

    for row in rows:
        key = event_identity(row)
        if key in seen:
            collisions.append(key)
            continue
        seen.add(key)
        out.append(deepcopy(row))

    if collisions:
        raise RuntimeError(
            f"fatal: {label} contains {len(collisions)} duplicate canonical identities"
        )
    return out, set()


def merge_catalog(existing_rows, source_events, *, require_ended=False):
    """
    Merge eligible source events into an existing canonical collection.

    all.json eligibility: completed=true.
    history.json eligibility: completed=true AND status=ended.

    Returns (merged, stats, added_rows). Existing rows are never removed.
    """
    merged, existing_collisions = _dedupe_existing(existing_rows, "existing")
    index = {event_identity(row): i for i, row in enumerate(merged)}

    added_rows = []
    matched = 0
    skipped_incomplete = 0
    source_duplicate_keys = set()
    seen_source = set()

    skipped_not_ended = 0

    for incoming in source_events:
        if not incoming.get("completed"):
            skipped_incomplete += 1
            continue
        if require_ended and incoming.get("status") != "ended":
            skipped_not_ended += 1
            continue

        key = event_identity(incoming)
        if key in seen_source:
            source_duplicate_keys.add(key)
            continue
        seen_source.add(key)

        if key in index:
            idx = index[key]
            merged[idx] = merge_existing(merged[idx], incoming)
            matched += 1
        else:
            row = deepcopy(incoming)
            merged.append(row)
            index[key] = len(merged) - 1
            added_rows.append(row)

    merged.sort(key=lambda x: x.get("event_time") or "", reverse=True)

    if len(merged) < len(existing_rows):
        raise RuntimeError("merge guard failed: canonical count decreased")

    stats = {
        "existing": len(existing_rows),
        "merged": len(merged),
        "matched": matched,
        "added": len(added_rows),
        "skipped_incomplete": skipped_incomplete,
        "skipped_not_ended": skipped_not_ended,
        "existing_identity_collisions": len(existing_collisions),
        "source_duplicate_identities": len(source_duplicate_keys),
    }
    return merged, stats, added_rows



def _contract_chain_key(event):
    contract = _norm_contract(event.get("contract_address") or event.get("contract"))
    chain = _norm_text(event.get("chain_id") or event.get("chainId") or "56")
    return (contract, chain) if contract else None


def _parse_event_dt(event):
    raw = _norm_text(event.get("event_time") or event.get("date"))
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def report_near_duplicates(label, existing_rows, added_rows, hours=36):
    """
    Read-only review aid. Same contract+chain with a nearby timestamp but a
    different exact identity is suspicious and must be reviewed before apply.
    Repeated rounds separated by days/weeks remain visible but are not flagged.
    """
    by_contract = {}
    for row in existing_rows:
        key = _contract_chain_key(row)
        if key:
            by_contract.setdefault(key, []).append(row)

    suspicious = 0
    contract_matches = 0
    for row in added_rows:
        key = _contract_chain_key(row)
        candidates = by_contract.get(key, []) if key else []
        if not candidates:
            continue
        contract_matches += 1
        new_dt = _parse_event_dt(row)
        for old in candidates:
            old_dt = _parse_event_dt(old)
            delta_h = None
            if new_dt and old_dt:
                delta_h = abs((new_dt - old_dt).total_seconds()) / 3600.0
            is_suspicious = delta_h is not None and delta_h <= hours
            if is_suspicious:
                suspicious += 1
            print(
                f"[review] {label} CONTRACT_MATCH "
                f"new={row.get('symbol') or ''}@{row.get('event_time') or ''} "
                f"old={old.get('symbol') or old.get('token') or ''}@"
                f"{old.get('event_time') or old.get('date') or ''} "
                f"delta_hours={delta_h if delta_h is not None else 'NA'} "
                f"suspicious={'YES' if is_suspicious else 'NO'}"
            )

    print(
        f"[review] {label}_contract_matches={contract_matches} "
        f"{label}_suspicious_near_duplicates={suspicious}"
    )
    return suspicious

def get_r2():
    required = (
        "R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY", "R2_BUCKET_NAME",
    )
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("missing R2 env: " + ",".join(missing))
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
    )


def load_required_list(r2, bucket, key):
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"fatal: cannot read canonical {key}: {exc}") from exc

    if not isinstance(data, list):
        raise RuntimeError(f"fatal: canonical {key} is not a JSON list")
    return data, raw


def _digest(data):
    body = json.dumps(
        data, default=str, ensure_ascii=False,
        sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def upload(r2, bucket, key, data):
    body = json.dumps(
        data, default=str, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    r2.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=60",
    )
    print(f"[R2] {key} ({len(data)} records, {len(body)//1024}KB)")


def print_added(label, rows):
    print(f"[dry-run] {label}_ADDED={len(rows)}")
    for row in sorted(rows, key=lambda x: x.get("event_time") or ""):
        print(
            "[dry-run] ADD "
            f"{label} "
            f"{row.get('event_time') or ''} "
            f"{row.get('symbol') or ''} "
            f"type={row.get('event_type') or ''} "
            f"phase={row.get('phase')} "
            f"chain={row.get('chain_id') or ''} "
            f"contract={row.get('contract_address') or ''}"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/airdrops.json")
    parser.add_argument("--no-prices", action="store_true")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write R2. Default is dry-run/read-only.",
    )
    parser.add_argument(
        "--max-add",
        type=int,
        default=100,
        help="Fail closed if either canonical object would add more than this many rows.",
    )
    args = parser.parse_args()

    with open(args.input, encoding="utf-8") as handle:
        raw = json.load(handle)
    airdrops = raw if isinstance(raw, list) else raw.get("airdrops", [])
    if not isinstance(airdrops, list):
        raise RuntimeError("input airdrops payload is not a list")
    print(f"[source] total={len(airdrops)}")

    prices = {} if args.no_prices else fetch_live_prices()
    now = datetime.now(timezone.utc)
    source_events = [map_event(item, prices, now=now) for item in airdrops]
    completed_count = sum(1 for event in source_events if event.get("completed"))
    print(f"[source] completed={completed_count} incomplete={len(source_events)-completed_count}")

    bucket = os.environ.get("R2_BUCKET_NAME") or ""
    r2 = get_r2()

    current_all, current_all_raw = load_required_list(r2, bucket, R2_ALL_KEY)
    current_history, current_history_raw = load_required_list(r2, bucket, R2_HISTORY_KEY)

    print(
        f"[current] all={len(current_all)} history={len(current_history)} "
        f"all_bytes={len(current_all_raw)} history_bytes={len(current_history_raw)}"
    )

    current_all_keys = {event_identity(row) for row in current_all}
    current_history_keys = {event_identity(row) for row in current_history}
    missing_history_keys = current_history_keys - current_all_keys
    print(f"[guard] history_identities_missing_from_all={len(missing_history_keys)}")
    if missing_history_keys:
        raise RuntimeError(
            "fatal: current history contains identities absent from all.json; "
            "repair canonical ownership before merge"
        )

    merged_all, all_stats, added_all = merge_catalog(
        current_all, source_events, require_ended=False
    )
    merged_history, history_stats, added_history = merge_catalog(
        current_history, source_events, require_ended=True
    )

    print("[plan] ALL " + " ".join(f"{k}={v}" for k, v in all_stats.items()))
    print("[plan] HISTORY " + " ".join(f"{k}={v}" for k, v in history_stats.items()))
    print_added("ALL", added_all)
    print_added("HISTORY", added_history)

    suspicious_all = report_near_duplicates("ALL", current_all, added_all)
    suspicious_history = report_near_duplicates(
        "HISTORY", current_history, added_history
    )
    if suspicious_all or suspicious_history:
        raise RuntimeError(
            f"suspicious near-duplicate guard: all={suspicious_all} "
            f"history={suspicious_history}"
        )

    if all_stats["added"] > args.max_add or history_stats["added"] > args.max_add:
        raise RuntimeError(
            f"max-add guard exceeded: all={all_stats['added']} "
            f"history={history_stats['added']} max={args.max_add}"
        )

    print(f"[digest] current_all={_digest(current_all)}")
    print(f"[digest] merged_all={_digest(merged_all)}")
    print(f"[digest] current_history={_digest(current_history)}")
    print(f"[digest] merged_history={_digest(merged_history)}")

    if not args.apply:
        print("[mode] DRY_RUN")
        print("[mutation] NONE")
        return

    # Apply ordering is intentional: downstream price enrichment reads all.json.
    # If the second write fails, existing History remains safe and the next
    # listing-price run can reconstruct it from the already-merged all.json.
    upload(r2, bucket, R2_ALL_KEY, merged_all)
    upload(r2, bucket, R2_HISTORY_KEY, merged_history)

    verify_all, _ = load_required_list(r2, bucket, R2_ALL_KEY)
    verify_history, _ = load_required_list(r2, bucket, R2_HISTORY_KEY)
    if _digest(verify_all) != _digest(merged_all):
        raise RuntimeError("postcheck failed: all.json digest mismatch")
    if _digest(verify_history) != _digest(merged_history):
        raise RuntimeError("postcheck failed: history.json digest mismatch")

    print(f"[postcheck] all={len(verify_all)} history={len(verify_history)}")
    print("[mode] APPLY")
    print("[mutation] R2_ALL_AND_HISTORY")
    print("[done] merge-preserving Alpha History import PASS")


if __name__ == "__main__":
    main()
