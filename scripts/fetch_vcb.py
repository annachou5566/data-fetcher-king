#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/fetch_vcb.py — Smart Multi-Bank Fetcher (V4)

Mục tiêu:
- Fetch tỷ giá USD/VND từ VNAppMob cho SBV + VCB + BID + CTG + TCB + STB
- Lưu 2 file riêng lên R2:
    1) macro-rates.json  -> dữ liệu tổng hợp / index / SBV
    2) bank-details.json  -> dữ liệu chi tiết từng bank
- Tự refresh token khi 401/403 (Đã chặn thundering herd)
- requests.Session + Retry
- Backfill theo chunk để không timeout
- Incremental quét vùng gần nhất để bắt ngày thiếu
- Checkpoint an toàn lên R2

Lưu ý theo doc VNAppMob:
- SBV: GET /api/v2/exchange_rate/sbv -> fields: buy, sell
- VCB/CTG/TCB/BID/STB: fields: buy_cash, buy_transfer, sell
- api_key scope exchange_rate hết hạn mặc định sau 15 ngày
- VCB và TCB hỗ trợ query date. Các bank khác KHÔNG hỗ trợ query ngày quá khứ.
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

START_DATE = os.getenv("START_DATE", "2020-01-01")

API_TOKEN_URL = "https://api.vnappmob.com/api/request_api_key?scope=exchange_rate"
API_BASE = "https://api.vnappmob.com/api/v2/exchange_rate"

MACRO_KEY = "macro-rates.json"
BANK_KEY = "bank-details.json"

# SBI = SBV
BANKS = ("sbv", "vcb", "bid", "ctg", "tcb", "stb")
DATE_SUPPORTED = {"vcb", "tcb"}

# Nếu muốn chạy nhẹ hơn, đổi bằng env
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
# Small utils
# ─────────────────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(msg, flush=True)


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(pool_connections=16, pool_maxsize=16, max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


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
        yield list(seq[i:i + size])


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
        log(f"  📦 {key}: {len(rows):,} rows")
        return rows
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound", "NoSuchBucket"):
            log(f"  📄 {key} chưa có → tạo mới")
            return []
        raise
    except Exception as e:
        log(f"  ⚠️  Load {key} lỗi: {e} → tạo mới")
        return []


def save_rows_to_r2(r2, bucket: str, key: str, rows: List[Dict[str, Any]], kind: str):
    payload = {
        "v": 4,
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
# Token manager
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

            # CHẶN THUNDERING HERD: Nếu bị ép refresh (do 401) NHƯNG token 
            # vừa được luồng khác lấy mới trong 10s qua -> Dùng luôn token đó
            if force_refresh and age < 10 and self._token:
                return self._token

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
# API parsing
# ─────────────────────────────────────────────────────────────────────────────

def parse_sbv_row(js: Dict[str, Any], snapshot_date: str) -> Optional[Dict[str, Any]]:
    results = js.get("results", [])
    if not isinstance(results, list):
        return None

    usd = next((x for x in results if str(x.get("currency", "")).upper() == "USD"), None)
    if not usd:
        return None

    return {
        "date": snapshot_date,
        "buy": safe_int(usd.get("buy")),
        "sell": safe_int(usd.get("sell")),
    }


def parse_bank_row(js: Dict[str, Any], snapshot_date: str) -> Optional[Dict[str, Any]]:
    results = js.get("results", [])
    if not isinstance(results, list):
        return None

    usd = next((x for x in results if str(x.get("currency", "")).upper() == "USD"), None)
    if not usd:
        return None

    return {
        "date": snapshot_date,
        "buy_cash": safe_int(usd.get("buy_cash")),
        "buy_transfer": safe_int(usd.get("buy_transfer")),
        "sell": safe_int(usd.get("sell")),
    }


def fetch_endpoint(
    session: requests.Session,
    token_mgr: TokenManager,
    bank: str,
    snapshot_date: str,
) -> Optional[Dict[str, Any]]:
    """
    SBV: latest only, no date query
    VCB/TCB: can send date
    BID/CTG/STB: latest only
    """
    # CHẶN LỖI XUYÊN KHÔNG:
    # Nếu ngân hàng không hỗ trợ truy vấn quá khứ VÀ ngày cần lấy không phải hôm nay -> Bỏ qua ngay
    if bank not in DATE_SUPPORTED and snapshot_date != date.today().isoformat():
        return None

    token = token_mgr.get()
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    url = f"{API_BASE}/{bank}"
    params = {"date": snapshot_date} if bank in DATE_SUPPORTED else None

    try:
        r = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

        if r.status_code in (401, 403):
            token = token_mgr.get(force_refresh=True)
            headers["Authorization"] = f"Bearer {token}"
            r = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

        if not r.ok:
            return None

        js = r.json()

        if bank == "sbv":
            return parse_sbv_row(js, snapshot_date)

        return parse_bank_row(js, snapshot_date)

    except requests.RequestException:
        return None
    except Exception:
        return None


def fetch_one_snapshot(
    session: requests.Session,
    token_mgr: TokenManager,
    snapshot_date: str,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Trả về: macro_row, bank_row
    bank_row chứa chi tiết từng bank theo cùng snapshot_date.
    """
    macro_row: Dict[str, Any] = {"date": snapshot_date}
    bank_row: Dict[str, Any] = {"date": snapshot_date}

    valid_sells: List[int] = []
    bank_hits = 0

    for bank in BANKS:
        row = fetch_endpoint(session, token_mgr, bank, snapshot_date)
        if not row:
            continue

        bank_hits += 1

        if bank == "sbv":
            if row.get("buy") is not None:
                macro_row["sbv_buy"] = row["buy"]
                bank_row["sbv_buy"] = row["buy"]
            if row.get("sell") is not None:
                macro_row["sbv_sell"] = row["sell"]
                bank_row["sbv_sell"] = row["sell"]
        else:
            if row.get("buy_cash") is not None:
                bank_row[f"{bank}_buy_cash"] = row["buy_cash"]
            if row.get("buy_transfer") is not None:
                bank_row[f"{bank}_buy_transfer"] = row["buy_transfer"]
            if row.get("sell") is not None:
                bank_row[f"{bank}_sell"] = row["sell"]
                valid_sells.append(int(row["sell"]))

    if "sbv_sell" not in macro_row and not valid_sells and bank_hits == 0:
        return None, None

    if valid_sells:
        avg_sell = int(round(sum(valid_sells) / len(valid_sells)))
        med_sell = median_int(valid_sells)
        best_sell = min(valid_sells)
        worst_sell = max(valid_sells)

        macro_row["vn_bank_index_sell"] = avg_sell
        macro_row["vn_bank_median_sell"] = med_sell
        macro_row["vn_bank_best_sell"] = best_sell
        macro_row["vn_bank_worst_sell"] = worst_sell
        macro_row["bank_count"] = len(valid_sells)

        bank_row["vn_bank_index_sell"] = avg_sell
        bank_row["vn_bank_median_sell"] = med_sell
        bank_row["vn_bank_best_sell"] = best_sell
        bank_row["vn_bank_worst_sell"] = worst_sell
        bank_row["bank_count"] = len(valid_sells)

        if macro_row.get("sbv_sell") is not None:
            macro_row["sbv_vs_index"] = int(macro_row["sbv_sell"]) - avg_sell
            macro_row["sbv_vs_best"] = int(macro_row["sbv_sell"]) - best_sell

    return macro_row, bank_row


# ─────────────────────────────────────────────────────────────────────────────
# Planning
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

    # Quét lùi một đoạn ngắn để bắt ngày thiếu nếu job trước bị lỗi
    window_start = max(parse_date_str(start_date), latest - timedelta(days=max(0, lookback_days - 1)))
    target = daterange(window_start, today)

    known = set(known_dates)
    return [d for d in target if d not in known]


# ─────────────────────────────────────────────────────────────────────────────
# Fetch engine
# ─────────────────────────────────────────────────────────────────────────────

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
            m_row, b_row = fetch_one_snapshot(session, token_mgr, d)
            if m_row:
                chunk_macro.append(m_row)
            if b_row:
                chunk_bank.append(b_row)
        return chunk_macro, chunk_bank

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_one_snapshot, session, token_mgr, d): d for d in date_chunk}
        for fut in as_completed(futures):
            d = futures[fut]
            try:
                m_row, b_row = fut.result()
                if m_row:
                    chunk_macro.append(m_row)
                if b_row:
                    chunk_bank.append(b_row)
            except Exception as e:
                log(f"  ⚠️  {d}: {e}")

    chunk_macro.sort(key=lambda r: r["date"])
    chunk_bank.sort(key=lambda r: r["date"])
    return chunk_macro, chunk_bank


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def print_header() -> None:
    log("🏦 Multi-Bank USD/VND Fetcher — V4 (Safe Backfill & Anti-Spam)")
    log(f"🕒 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log(
        f"⚙️  start={START_DATE} | lookback={INCREMENTAL_LOOKBACK_DAYS}d | "
        f"chunk={BACKFILL_CHUNK_DAYS}d | backfill_workers={MAX_WORKERS_BACKFILL} | "
        f"incremental_workers={MAX_WORKERS_INCREMENTAL}"
    )


def summarize_latest(macro_rows: List[Dict[str, Any]]) -> None:
    if not macro_rows:
        log("✅ Không có dữ liệu mới.")
        return

    last = macro_rows[-1]
    parts = [f"date={last.get('date')}"]
    if last.get("sbv_sell") is not None:
        parts.append(f"SBV={last.get('sbv_sell')}")
    if last.get("vn_bank_index_sell") is not None:
        parts.append(f"INDEX={last.get('vn_bank_index_sell')}")
    if last.get("bank_count") is not None:
        parts.append(f"banks={last.get('bank_count')}")
    log("✅ Latest: " + " | ".join(parts))


def main():
    parser = argparse.ArgumentParser(description="Fetch USD/VND rates from VNAppMob and store to R2.")
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
        log("✅ Up-to-date. Không có ngày thiếu.")
        summarize_latest(macro_existing)
        return

    full_backfill = len(macro_existing) == 0
    mode = "BACKFILL" if full_backfill else "INCREMENTAL"
    workers = args.max_workers_backfill if full_backfill else args.max_workers_incremental
    chunk_size = args.backfill_chunk_days if full_backfill else max(1, min(14, args.lookback_days))

    log(f"📡 [{mode}] cần fetch {len(target_dates)} ngày: {target_dates[0]} → {target_dates[-1]}")
    log(f"🧱 chunk_size={chunk_size} | workers={workers}")

    pending_macro: List[Dict[str, Any]] = []
    pending_bank: List[Dict[str, Any]] = []

    processed = 0
    total = len(target_dates)
    last_checkpoint_count = len(macro_existing)

    for idx, date_chunk in enumerate(chunked(target_dates, chunk_size), start=1):
        log(f"\n▶ Chunk {idx}: {date_chunk[0]} → {date_chunk[-1]} ({len(date_chunk)} ngày)")

        if full_backfill and idx > 1:
            # Token mới đầu mỗi chunk khi backfill lớn để tránh hết hạn giữa chừng
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
        log(f"   ... processed={processed}/{total} | chunk_macro={len(chunk_macro)} | chunk_bank={len(chunk_bank)}")

        macro_all = merge_rows(macro_existing, pending_macro)
        bank_all = merge_rows(bank_existing, pending_bank)

        should_checkpoint = (
            full_backfill
            or (len(macro_all) - last_checkpoint_count) >= CHECKPOINT_EVERY_ROWS
            or idx == 1
            or processed >= total
        )

        if should_checkpoint:
            if args.dry_run:
                log(f"   [DRY RUN] macro_total={len(macro_all)} bank_total={len(bank_all)}")
            else:
                save_rows_to_r2(r2, bucket, MACRO_KEY, macro_all, kind="macro-rates")
                save_rows_to_r2(r2, bucket, BANK_KEY, bank_all, kind="bank-details")
                log(f"   💾 Saved → macro={len(macro_all):,} rows | bank={len(bank_all):,} rows")

            last_checkpoint_count = len(macro_all)
            macro_existing = macro_all
            bank_existing = bank_all
            pending_macro = []
            pending_bank = []

    # Final save nếu còn pending
    final_macro = merge_rows(macro_existing, pending_macro)
    final_bank = merge_rows(bank_existing, pending_bank)

    if final_macro != macro_existing or final_bank != bank_existing:
        if args.dry_run:
            log(f"   [DRY RUN final] macro_total={len(final_macro)} bank_total={len(final_bank)}")
        else:
            save_rows_to_r2(r2, bucket, MACRO_KEY, final_macro, kind="macro-rates")
            save_rows_to_r2(r2, bucket, BANK_KEY, final_bank, kind="bank-details")
            log(f"   💾 Final save → macro={len(final_macro):,} rows | bank={len(final_bank):,} rows")

    log("\n🎉 Hoàn tất!")
    summarize_latest(final_macro if final_macro else macro_existing)


if __name__ == "__main__":
    main()
