"""
sync_alpha_history.py
─────────────────────
Xử lý file airdrops JSON, enrich với giá từ Binance Alpha API,
rồi upload 4 file JSON lên R2.

Chạy một lần để seed dữ liệu lịch sử:
  python scripts/sync_alpha_history.py --input data/airdrops.json

Env vars cần có:
  R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from botocore.config import Config

try:
    import boto3
except ImportError:
    sys.exit("Missing boto3. Run: pip install boto3")

# ── Config ────────────────────────────────────────────────────────────
BINANCE_TOKEN_LIST = (
    "https://www.binance.com/bapi/defi/v1/public/wallet-direct/"
    "buw/wallet/cex/alpha/all/token/list"
)
CHAIN_NAMES = {
    "56": "BSC", "1": "ETH", "8453": "Base",
    "501": "SOL", "784": "SUI", "42161": "ARB",
    "146": "SONIC", "59144": "LINEA",
}


# ── Fetch live prices from Binance Alpha ──────────────────────────────
def fetch_live_prices():
    """Returns dict: symbol → {price, marketCap}"""
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
                    "price":     float(t["price"]),
                    "marketCap": float(t.get("marketCap") or 0),
                }
        print(f"[prices] Fetched {len(prices)} tokens from Binance Alpha")
    except Exception as e:
        print(f"[prices] Warning: could not fetch live prices: {e}")
    return prices


# ── Map one airdrop entry to our schema ──────────────────────────────
def map_event(e, prices):
    symbol   = (e.get("token") or "").strip()
    date_str = e.get("date", "")
    time_str = e.get("time", "00:00") or "00:00"
    # Some time values are non-standard
    if not time_str or not time_str[0].isdigit():
        time_str = "00:00"
    # Try to parse event datetime
    event_iso = None
    status = "ended"
    try:
        dt = datetime.strptime(f"{date_str}T{time_str}:00", "%Y-%m-%dT%H:%M:%S")
        dt_utc = dt.replace(tzinfo=timezone.utc)
        event_iso = dt_utc.isoformat()
        now = datetime.now(timezone.utc)
        delta = (dt_utc - now).total_seconds()
        if delta > 3600:
            status = "upcoming"
        elif delta > -3600:
            status = "live"
        else:
            status = "ended"
    except Exception:
        event_iso = date_str
        status = "ended"

    # Price enrichment
    live = prices.get(symbol, {})
    price_now = live.get("price")
    mc_now    = live.get("marketCap") or e.get("market_cap")

    amount_raw = e.get("amount")
    value_usd  = None
    if price_now and amount_raw:
        try:
            value_usd = round(float(amount_raw) * price_now, 2)
        except Exception:
            pass

    chain_id = str(e.get("chain_id") or "56")

    return {
        "project_name":     e.get("name") or symbol,
        "symbol":           symbol,
        "event_type":       (e.get("type") or "grab").lower() or "grab",
        "points_threshold": str(e.get("points") or "").strip(),
        "amount_per_user":  amount_raw,
        "total_amount":     e.get("total_amount"),
        "contract_address": e.get("contract_address"),
        "chain_id":         chain_id,
        "chain_name":       CHAIN_NAMES.get(chain_id, "EVM"),
        "market_cap":       mc_now,
        "fdv":              e.get("fdv"),
        "price_snapshot":   price_now,
        "value_usd":        value_usd,
        "event_time":       event_iso,
        "status":           status,
        "phase":            e.get("phase"),
        "spot_listed":      bool(e.get("spot_listed")),
        "futures_listed":   bool(e.get("futures_listed")),
        "completed":        bool(e.get("completed")),
        "pretge":           bool(e.get("pretge")),
        "source_channel":   "historical",
        "raw_text":         None,
        "created_at":       datetime.now(timezone.utc).isoformat(),
    }


# ── Upload to R2 ──────────────────────────────────────────────────────
def get_r2():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["R2_ENDPOINT_URL"],
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
    )


def upload(r2, bucket, key, data):
    body = json.dumps(data, default=str, ensure_ascii=False,
                      separators=(",", ":")).encode("utf-8")
    r2.put_object(
        Bucket=bucket, Key=key, Body=body,
        ContentType="application/json",
        CacheControl="public, max-age=60",
    )
    print(f"[R2] {key}  ({len(data)} records, {len(body)//1024}KB)")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/airdrops.json",
                        help="Path to airdrops JSON file")
    parser.add_argument("--no-prices", action="store_true",
                        help="Skip live price fetch")
    args = parser.parse_args()

    # Load source data
    with open(args.input, encoding="utf-8") as f:
        raw = json.load(f)
    airdrops = raw if isinstance(raw, list) else raw.get("airdrops", [])
    print(f"[data] Loaded {len(airdrops)} records from {args.input}")

    # Live prices
    prices = {} if args.no_prices else fetch_live_prices()

    # Map & sort
    events = [map_event(e, prices) for e in airdrops]
    events.sort(key=lambda x: x.get("event_time") or "", reverse=True)

    ended    = [e for e in events if e["status"] == "ended"]
    live_ev  = [e for e in events if e["status"] == "live"]
    upcoming = [e for e in events if e["status"] == "upcoming"]

    print(f"[data] ended={len(ended)}  live={len(live_ev)}  upcoming={len(upcoming)}")

    # Upload
    bucket = os.environ["R2_BUCKET_NAME"]
    r2     = get_r2()
    upload(r2, bucket, "alpha-events/history.json",  ended)
    upload(r2, bucket, "alpha-events/live.json",     live_ev)
    upload(r2, bucket, "alpha-events/upcoming.json", upcoming)
    upload(r2, bucket, "alpha-events/all.json",      events)

    print("[done] All files uploaded ✓")


if __name__ == "__main__":
    main()
