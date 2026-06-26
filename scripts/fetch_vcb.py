#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/fetch_vcb.py — V3 Smart Multi-Bank Fetcher

Mục tiêu:
- Fetch tỷ giá USD/VND từ SBV + Big 4: VCB, BID, CTG, TCB
- Lưu 2 file riêng lên R2:
    1) macro-rates.json   -> dữ liệu tổng hợp / index / SBV
    2) bank-details.json  -> dữ liệu chi tiết từng bank
- Tự refresh token vnappmob khi cần
- Dùng requests.Session + Retry để chống lỗi tạm thời
- Backfill theo chunk để không timeout / không ngốn RAM
- Incremental hằng ngày chỉ quét vùng gần nhất để bắt ngày thiếu
- Checkpoint sau mỗi chunk để resume an toàn

Env:
- R2_ACCESS_KEY_ID
- R2_SECRET_ACCESS_KEY
- R2_ENDPOINT_URL
- R2_BUCKET_NAME

Tuỳ chọn:
- START_DATE=2015-01-01
- INCREMENTAL_LOOKBACK_DAYS=14
- BACKFILL_CHUNK_DAYS=45
- MAX_WORKERS_BACKFILL=6
- MAX_WORKERS_INCREMENTAL=2
- REQUEST_TIMEOUT=20
- TOKEN_TIMEOUT=15
- TOKEN_MAX_AGE_SECONDS=1200
- CHECKPOINT_EVERY_ROWS=200
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
import requests
from botocore.exceptions import ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────

START_DATE = os.getenv("START_DATE", "2015-01-01")

API_TOKEN_URL = "https://api.vnappmob.com/api/request_api_key?scope=exchange_rate"
API_RATE_URL = "https://api.vnappmob.com/api/v2/exchange_rate/"

MACRO_KEY = "macro-rates.json"
BANK_KEY = "bank-details.json"

BANKS = ("sbv", "vcb", "bid", "ctg", "tcb")
BIG4 = ("vcb", "bid", "ctg", "tcb")

INCREMENTAL_LOOKBACK_DAYS = int(os.getenv("INCREMENTAL_LOOKBACK_DAYS", "14"))
BACKFILL_CHUNK_DAYS = int(os.getenv("BACKFILL_CHUNK_DAYS", "45"))
MAX_WORKERS_BACKFILL = int(os.getenv("MAX_WORKERS_BACKFILL", "6"))
MAX_WORKERS_INCREMENTAL = int(os.getenv("MAX_WORKERS_INCREMENTAL", "2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
TOKEN_TIMEOUT = int(os.getenv("TOKEN_TIMEOUT", "15"))
TOKEN_MAX_AGE_SECONDS = int(os.getenv("TOKEN_MAX_AGE_SECONDS", "1200"))
CHECKPOINT_EVERY_ROWS = int(os.getenv("CHECKPOINT_EVERY_ROWS", "200"))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update(HEADERS)
    return s


def safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        n = int(float(v))
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def parse_date_str(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def daterange(start: date, end: date) -> List[str]:
    if end < start:
        return []
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def chunked(seq: Sequence[str], size: int) -> Iterable[List[str]]:
    size = max(1, int(size))
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


def merge_rows(existing_rows: List[Dict[str, Any]], new_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_date: Dict[str, Dict[str, Any]] = {}
    for row in existing_rows:
        d = row.get("date")
        if d:
            by_date[d] = row
    for row in new_rows:
        d = row.get("date")
        if d:
            by_date[d] = row
    return sorted(by_date.values(), key=lambda r: r["date"])


def median_int(values: List[int]) -> Optional[int]:
    if not values:
        return None
    return int(round(statistics.median(values)))


# ─────────────────────────────────────────────────────────────────────────────
# R2
# ─────────────────────────────────────────────────────────────────────────────

def get_r2():
    key = os.getenv("R2_ACCESS_KEY_ID")
    secret = os.getenv("R2_SECRET_ACCESS_KEY")
    endpoint = os.getenv("R2_ENDPOINT_URL")
    bucket = os.getenv("R2_BUCKET_NAME")

    if not all([key, secret, endpoint, bucket]):
        raise RuntimeError("Thiếu R2 env vars: R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY / R2_ENDPOINT_URL / R2_BUCKET_NAME")

    client = boto3.client(
        "s3",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        endpoint_url=endpoint,
    )
    return client, bucket


def load_existing_rows(r2, bucket: str, key: str) -> List[Dict[str, Any]]:
    try:
        obj = r2.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read().decode("utf-8")
        data = json.loads(raw)
        rows = data.get("rows", [])
        if not isinstance(rows, list):
            raise ValueError(f"{key}: rows không phải list")
        print(f"  📦 {key}: {len(rows):,} rows")
        return rows
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound", "NoSuchBucket"):
            print(f"  📄 {key} chưa có → tạo mới")
            return []
        raise
    except Exception as e:
        print(f"  ⚠️  Load {key} lỗi: {e} → tạo mới")
        return []


def save_rows_to_r2(r2, bucket: str, key: str, rows: List[Dict[str, Any]], kind: str):
    payload = {
        "v": 3,
        "kind": kind,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(rows),
        "rows": rows,
    }
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    r2.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8",
        CacheControl="max-age=3600",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Token Manager
# ─────────────────────────────────────────────────────────────────────────────

class TokenManager:
    def __init__(self, session: requests.Session):
        self._session = session
        self._token: Optional[str] = None
        self._fetched_at: float = 0.0
        self._lock = threading.Lock()

    def _extract_token(self, js: Dict[str, Any]) -> str:
        token = js.get("results")

        if isinstance(token, list):
            token = token[0] if token else None
        if isinstance(token, dict):
            token = token.get("token") or token.get("access_token") or token.get("api_key")
        if token is None:
            token = js.get("token") or js.get("access_token") or js.get("api_key")

        token = str(token).strip() if token else ""
        if not token:
            raise RuntimeError(f"Token rỗng: {js}")
        return token

    def get(self, force_refresh: bool = False) -> str:
        with self._lock:
            age = time.time() - self._fetched_at
            if self._token and not force_refresh and age < TOKEN_MAX_AGE_SECONDS:
                return self._token

            last_err: Optional[Exception] = None
            for attempt in range(1, 4):
                try:
                    r = self._session.get(API_TOKEN_URL, timeout=TOKEN_TIMEOUT)
                    r.raise_for_status()
                    js = r.json()
                    token = self._extract_token(js)
                    self._token = token
                    self._fetched_at = time.time()
                    return token
                except Exception as e:
                    last_err = e
                    if attempt < 3:
                        time.sleep(0.8 * attempt)

            raise RuntimeError(f"Không thể lấy API Token sau 3 lần: {last_err}")


# ─────────────────────────────────────────────────────────────────────────────
# API Fetch
# ─────────────────────────────────────────────────────────────────────────────

def parse_rate_response(js: Dict[str, Any], date_str: str) -> Optional[Dict[str, Any]]:
    results = js.get("results", [])
    if not isinstance(results, list):
        return None

    usd = next((x for x in results if str(x.get("currency", "")).upper() == "USD"), None)
    if not usd:
        return None

    return {
        "date": date_str,
        "cash": safe_int(usd.get("cash")),
        "transfer": safe_int(usd.get("transfer")),
        "sell": safe_int(usd.get("sell")),
    }


def fetch_bank_day(
    session: requests.Session,
    token_mgr: TokenManager,
    bank: str,
    date_str: str,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Returns:
      row, auth_failed

    auth_failed=True khi token hết hạn / bị từ chối → caller refresh 1 lần rồi retry.
    """
    token = token_mgr.get()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    url = f"{API_RATE_URL}{bank}?date={date_str}"

    try:
        r = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

        if r.status_code in (401, 403):
            return None, True
        if r.status_code in (404, 429):
            return None, False
        if not r.ok:
            return None, False

        js = r.json()
        row = parse_rate_response(js, date_str)
        return row, False

    except requests.RequestException:
        return None, False
    except Exception:
        return None, False


def fetch_one_day(
    session: requests.Session,
    token_mgr: TokenManager,
    date_str: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    macro_row: Dict[str, Any] = {"date": date_str}
    bank_row: Dict[str, Any] = {"date": date_str}

    valid_bank_sells: List[int] = []
    bank_hits = 0

    # SBV trước
    sbv_row, auth_failed = fetch_bank_day(session, token_mgr, "sbv", date_str)
    if auth_failed:
        token_mgr.get(force_refresh=True)
        sbv_row, _ = fetch_bank_day(session, token_mgr, "sbv", date_str)

    if sbv_row:
        if sbv_row.get("cash") is not None:
            macro_row["sbv_cash"] = sbv_row["cash"]
            bank_row["sbv_cash"] = sbv_row["cash"]
        if sbv_row.get("transfer") is not None:
            macro_row["sbv_transfer"] = sbv_row["transfer"]
            bank_row["sbv_transfer"] = sbv_row["transfer"]
        if sbv_row.get("sell") is not None:
            macro_row["sbv_sell"] = sbv_row["sell"]
            bank_row["sbv_sell"] = sbv_row["sell"]

    # Big 4
    for bank in BIG4:
        row, auth_failed = fetch_bank_day(session, token_mgr, bank, date_str)
        if auth_failed:
            token_mgr.get(force_refresh=True)
            row, _ = fetch_bank_day(session, token_mgr, bank, date_str)

        if not row:
            continue

        bank_hits += 1

        if row.get("cash") is not None:
            bank_row[f"{bank}_cash"] = row["cash"]
        if row.get("transfer") is not None:
            bank_row[f"{bank}_transfer"] = row["transfer"]
        if row.get("sell") is not None:
            bank_row[f"{bank}_sell"] = row["sell"]
            valid_bank_sells.append(int(row["sell"]))

    if "sbv_sell" not in macro_row and not valid_bank_sells and bank_hits == 0:
        return None, None

    if valid_bank_sells:
        avg_sell = int(round(sum(valid_bank_sells) / len(valid_bank_sells)))
        med_sell = median_int(valid_bank_sells)
        best_sell = min(valid_bank_sells)
        worst_sell = max(valid_bank_sells)

        macro_row["vn_bank_index_sell"] = avg_sell
        macro_row["vn_bank_median_sell"] = med_sell
        macro_row["vn_bank_best_sell"] = best_sell
        macro_row["vn_bank_worst_sell"] = worst_sell
        macro_row["bank_count"] = len(valid_bank_sells)

        if macro_row.get("sbv_sell") is not None:
            macro_row["sbv_vs_index"] = int(macro_row["sbv_sell"]) - avg_sell
            macro_row["sbv_vs_best"] = int(macro_row["sbv_sell"]) - best_sell

    return macro_row, bank_row


# ─────────────────────────────────────────────────────────────────────────────
# Date plan
# ─────────────────────────────────────────────────────────────────────────────

def build_target_dates(
    existing_macro_rows: List[Dict[str, Any]],
    start_date: str,
    today: date,
    lookback_days: int,
) -> List[str]:
    known_dates = sorted({r["date"] for r in existing_macro_rows if r.get("date")})

    if not known_dates:
        return daterange(parse_date_str(start_date), today)

    latest = parse_date_str(known_dates[-1])
    window_start = max(parse_date_str(start_date), latest - timedelta(days=max(0, lookback_days - 1)))
    target = daterange(window_start, today)

    known = set(known_dates)
    return [d for d in target if d not in known]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def print_header():
    print("🏦 Multi-Bank USD/VND Fetcher — V3")
    print(f"🕒 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(
        f"⚙️  start={START_DATE} | lookback={INCREMENTAL_LOOKBACK_DAYS}d | "
        f"chunk={BACKFILL_CHUNK_DAYS}d | backfill_workers={MAX_WORKERS_BACKFILL} | "
        f"incremental_workers={MAX_WORKERS_INCREMENTAL}"
    )


def summarize_latest(macro_rows: List[Dict[str, Any]]):
    if not macro_rows:
        print("✅ Không có dữ liệu mới.")
        return

    last = macro_rows[-1]
    parts = [f"date={last.get('date')}"]
    if last.get("sbv_sell") is not None:
        parts.append(f"SBV={last.get('sbv_sell')}")
    if last.get("vn_bank_index_sell") is not None:
        parts.append(f"INDEX={last.get('vn_bank_index_sell')}")
    if last.get("bank_count") is not None:
        parts.append(f"banks={last.get('bank_count')}")
    print("✅ Latest:", " | ".join(parts))


def fetch_dates_in_chunk(
    session: requests.Session,
    token_mgr: TokenManager,
    date_chunk: List[str],
    workers: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    chunk_macro: List[Dict[str, Any]] = []
    chunk_bank: List[Dict[str, Any]] = []

    if not date_chunk:
        return chunk_macro, chunk_bank

    if workers <= 1:
        for d in date_chunk:
            m_row, b_row = fetch_one_day(session, token_mgr, d)
            if m_row:
                chunk_macro.append(m_row)
            if b_row:
                chunk_bank.append(b_row)
        return chunk_macro, chunk_bank

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_one_day, session, token_mgr, d): d for d in date_chunk}
        for fut in as_completed(futures):
            d = futures[fut]
            try:
                m_row, b_row = fut.result()
                if m_row:
                    chunk_macro.append(m_row)
                if b_row:
                    chunk_bank.append(b_row)
            except Exception as e:
                print(f"  ⚠️  {d}: {e}")

    chunk_macro.sort(key=lambda r: r["date"])
    chunk_bank.sort(key=lambda r: r["date"])
    return chunk_macro, chunk_bank


def main():
    parser = argparse.ArgumentParser(description="Fetch USD/VND rates from SBV + Big 4 and store to R2.")
    parser.add_argument("--start-date", default=START_DATE)
    parser.add_argument("--lookback-days", type=int, default=INCREMENTAL_LOOKBACK_DAYS)
    parser.add_argument("--backfill-chunk-days", type=int, default=BACKFILL_CHUNK_DAYS)
    parser.add_argument("--max-workers-backfill", type=int, default=MAX_WORKERS_BACKFILL)
    parser.add_argument("--max-workers-incremental", type=int, default=MAX_WORKERS_INCREMENTAL)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print_header()

    r2, bucket = get_r2()
    session = build_session()
    token_mgr = TokenManager(session)

    macro_existing = load_existing_rows(r2, bucket, MACRO_KEY)
    bank_existing = load_existing_rows(r2, bucket, BANK_KEY)

    today = date.today()
    target_dates = build_target_dates(
        existing_macro_rows=macro_existing,
        start_date=args.start_date,
        today=today,
        lookback_days=args.lookback_days,
    )

    if not target_dates:
        print("✅ Up-to-date. Không có ngày thiếu.")
        summarize_latest(macro_existing)
        return

    full_backfill = len(macro_existing) == 0
    mode = "BACKFILL" if full_backfill else "INCREMENTAL"
    workers = args.max_workers_backfill if full_backfill else args.max_workers_incremental
    chunk_size = args.backfill_chunk_days if full_backfill else max(1, min(14, args.lookback_days))

    print(f"📡 [{mode}] cần fetch {len(target_dates)} ngày: {target_dates[0]} → {target_dates[-1]}")
    print(f"🧱 chunk_size={chunk_size} | workers={workers}")

    pending_macro: List[Dict[str, Any]] = []
    pending_bank: List[Dict[str, Any]] = []

    processed = 0
    total = len(target_dates)
    last_checkpoint_count = 0

    for idx, date_chunk in enumerate(chunked(target_dates, chunk_size), start=1):
        print(f"\n▶ Chunk {idx}: {date_chunk[0]} → {date_chunk[-1]} ({len(date_chunk)} ngày)")

        # Một chút bảo hiểm: đầu mỗi chunk xin token mới nếu backfill lớn
        if full_backfill and idx > 1:
            token_mgr.get(force_refresh=True)

        chunk_macro, chunk_bank = fetch_dates_in_chunk(
            session=session,
            token_mgr=token_mgr,
            date_chunk=date_chunk,
            workers=workers,
        )

        if chunk_macro:
            pending_macro.extend(chunk_macro)
        if chunk_bank:
            pending_bank.extend(chunk_bank)

        processed += len(date_chunk)
        print(f"   ... processed={processed}/{total} | chunk_macro={len(chunk_macro)} | chunk_bank={len(chunk_bank)}")

        # Merge + save sau mỗi chunk
        macro_all = merge_rows(macro_existing, pending_macro)
        bank_all = merge_rows(bank_existing, pending_bank)

        should_checkpoint = (
            full_backfill
            or len(macro_all) - last_checkpoint_count >= CHECKPOINT_EVERY_ROWS
            or idx == 1
            or processed >= total
        )

        if should_checkpoint:
            if args.dry_run:
                print(f"   [DRY RUN] macro_total={len(macro_all)} bank_total={len(bank_all)}")
            else:
                save_rows_to_r2(r2, bucket, MACRO_KEY, macro_all, kind="macro-rates")
                save_rows_to_r2(r2, bucket, BANK_KEY, bank_all, kind="bank-details")
                print(f"   💾 Saved → macro={len(macro_all):,} rows | bank={len(bank_all):,} rows")
            last_checkpoint_count = len(macro_all)

            # Clear pending vì đã được merge vào snapshot hiện tại
            macro_existing = macro_all
            bank_existing = bank_all
            pending_macro = []
            pending_bank = []

    # Final save trong trường hợp chunk cuối chưa đủ checkpoint
    final_macro = merge_rows(macro_existing, pending_macro)
    final_bank = merge_rows(bank_existing, pending_bank)

    if final_macro != macro_existing or final_bank != bank_existing:
        if args.dry_run:
            print(f"   [DRY RUN final] macro_total={len(final_macro)} bank_total={len(final_bank)}")
        else:
            save_rows_to_r2(r2, bucket, MACRO_KEY, final_macro, kind="macro-rates")
            save_rows_to_r2(r2, bucket, BANK_KEY, final_bank, kind="bank-details")
            print(f"   💾 Final save → macro={len(final_macro):,} rows | bank={len(final_bank):,} rows")

    print("\n🎉 Hoàn tất!")
    summarize_latest(final_macro if final_macro else macro_existing)


if __name__ == "__main__":
    main()
