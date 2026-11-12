"""PNJ historical quotes: bounded, opt-in GitHub Actions backfill.

One official UI-backed endpoint call per calendar date, serial and slow.
This is NOT an official bulk API, a publication license, or an active collector.
All PNJ products/regions/timestamps are retained as source facts, not OHLC.
R2 writes require separate, explicit authorization; never use a new bucket.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

START_DATE = date(2016, 1, 1)  # Earliest YEAR offered in PNJ public UI, not proof every day exists.
SOURCE = 'https://giavang.pnj.com.vn/'
ENDPOINT = 'https://edge-cf-api.pnj.io/ecom-frontend/v1/get-gold-price-history'
PREFIX = 'metal/pnj/history-v1/'
STATE_KEY = PREFIX + '_state.json'
YEAR_SCHEMA = 'wa-pnj-year-progress-v1'
MAX_DAYS_HARD = 24
MAX_BYTES = 2_000_000
UTC7 = ZoneInfo('Asia/Ho_Chi_Minh')
UA = 'WaveAlpha-MetalMarket-HistoryCollector/0.1 (single-threaded, fail-closed)'


class CollectionHold(RuntimeError):
    pass


def today_vn() -> date:
    return datetime.now(UTC7).date()


def exact_date(s: str) -> date:
    d = date.fromisoformat(s)
    if d.isoformat() != s:
        raise ValueError('NON_CANONICAL_DATE')
    return d


def key_for(day: date) -> str:
    return f'{PREFIX}days/{day:%Y/%m}/{day:%Y-%m-%d}.json'


def year_key(year: int) -> str:
    if year < START_DATE.year or year > today_vn().year:
        raise ValueError('YEAR_OUTSIDE_SOURCE_RANGE')
    return f'{PREFIX}years/{year}/_progress.json'


def new_year_progress(day: date) -> dict:
    return {
        'schema': YEAR_SCHEMA, 'year': day.year,
        'next_date': day.isoformat(),
        'started_from_january_1': (day.month, day.day) == (1, 1),
        'days_committed': 0, 'days_observed': 0,
        'empty_unverified_days': 0, 'quarantined_rows': 0,
        'scan_status': 'IN_PROGRESS',
    }


def commit_year_progress(storage, day: date, summary: dict) -> str:
    """Idempotently advance per-year receipt; each day was already saved to R2.

    If a prior attempt wrote this receipt but the global cursor failed,
    only acknowledge the receipt, never double-increment the counters.
    """
    key = year_key(day.year)
    progress = storage.read(key)
    if progress is None:
        progress = new_year_progress(day)
    if not isinstance(progress, dict) or progress.get('schema') != YEAR_SCHEMA or progress.get('year') != day.year:
        raise CollectionHold('YEAR_PROGRESS_SCHEMA_MISMATCH')
    if summary.get('coverage') not in ('OBSERVED', 'EMPTY_UNVERIFIED'):
        raise CollectionHold('DAY_COVERAGE_UNQUALIFIED')
    nxt = exact_date(progress['next_date'])
    after = day + timedelta(days=1)
    if nxt == after:
        return progress['scan_status']  # Previous attempt already committed.
    if nxt != day or progress.get('scan_status') != 'IN_PROGRESS':
        raise CollectionHold('YEAR_PROGRESS_CURSOR_CONFLICT')
    progress['next_date'] = after.isoformat()
    progress['days_committed'] += 1
    progress['days_observed'] += (summary['coverage'] == 'OBSERVED')
    progress['empty_unverified_days'] += (summary['coverage'] != 'OBSERVED')
    progress['quarantined_rows'] += summary['quarantined_row_count']
    if after.year != day.year:
        if not progress['started_from_january_1']:
            progress['scan_status'] = 'PARTIAL_YEAR_PRIOR_DAYS_UNVERIFIED'
        elif progress['empty_unverified_days']:
            progress['scan_status'] = 'SCANNED_YEAR_HAS_EMPTY_UNVERIFIED_DAYS'
        else:
            progress['scan_status'] = 'SCANNED_YEAR_DAYS_OBSERVED'
    storage.write(key, progress)
    return progress['scan_status']


def limit_days(max_days: int) -> int:
    if not 1 <= max_days <= MAX_DAYS_HARD:
        raise ValueError('DAY_LIMIT_OUT_OF_BOUNDS')
    return max_days


def inspect_payload(payload: object, day: date) -> dict:
    if not isinstance(payload, dict) or not isinstance(payload.get('locations'), list):
        raise CollectionHold('SCHEMA_UNKNOWN')
    products, prices, anomalies = 0, 0, 0
    sjc_rows = ring_rows = invalid_price_rows = inverted_spread_rows = 0
    integer_price = re.compile(r'(?:[0-9]{1,3}(?:\.[0-9]{3})+|[0-9]+)')

    def parse_thousand_vnd(value):
        if value is None or value == '':
            return None
        if not isinstance(value, str) or not integer_price.fullmatch(value):
            raise ValueError('INVALID_PRICE_ENCODING')
        amount = int(value.replace('.', ''))
        if not 1_000 <= amount <= 1_000_000:
            raise ValueError('PRICE_OUTSIDE_SAFETY_RANGE')
        return amount
    for loc in payload['locations']:
        if not isinstance(loc, dict) or not isinstance(loc.get('gold_type'), list):
            raise CollectionHold('SCHEMA_LOCATION_CHANGED')
        region = loc.get('name')
        if not isinstance(region, str) or not region.strip():
            raise CollectionHold('SCHEMA_REGION_NAME_CHANGED')
        for product in loc['gold_type']:
            if not isinstance(product, dict) or not isinstance(product.get('data'), list):
                raise CollectionHold('SCHEMA_PRODUCT_CHANGED')
            name = product.get('name')
            if not isinstance(name, str) or not name.strip():
                raise CollectionHold('SCHEMA_PRODUCT_NAME_CHANGED')
            products += 1
            for row in product['data']:
                if not isinstance(row, dict):
                    anomalies += 1
                    continue
                ts = row.get('updated_at')
                if not isinstance(ts, str):
                    anomalies += 1
                    continue
                try:
                    stamp = datetime.strptime(ts, '%d/%m/%Y %H:%M:%S')
                except ValueError:
                    anomalies += 1
                    continue
                if stamp.date() != day:
                    anomalies += 1
                    continue
                try:
                    buy = parse_thousand_vnd(row.get('gia_mua'))
                    sell = parse_thousand_vnd(row.get('gia_ban'))
                    if buy is None and sell is None:
                        raise ValueError('NO_QUOTE')
                except ValueError:
                    anomalies += 1
                    invalid_price_rows += 1
                    continue
                if buy is not None and sell is not None and buy > sell:
                    anomalies += 1
                    inverted_spread_rows += 1
                    continue
                prices += 1
                if region in ('TPHCM', 'Hà Nội', 'Đà Nẵng', 'Miền Tây', 'Tây Nguyên', 'Đông Nam Bộ') and name == 'SJC':
                    sjc_rows += 1
                if name == 'Nhẫn Trơn PNJ 999.9':
                    ring_rows += 1
    targets = 'BOTH' if sjc_rows and ring_rows else 'SJC_ONLY' if sjc_rows else 'RING_ONLY' if ring_rows else 'NEITHER'
    return {'locations': len(payload['locations']), 'products': products,
            'updates_with_valid_date': prices, 'quarantined_row_count': anomalies,
            'invalid_price_rows': invalid_price_rows,
            'inverted_spread_rows': inverted_spread_rows,
            'sjc_valid_rows': sjc_rows, 'pnj_ring_valid_rows': ring_rows,
            'target_coverage': targets,
            'coverage': 'OBSERVED' if prices else 'EMPTY_UNVERIFIED'}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def fetch_day(day: date, *, opener=None):
    opener = opener or urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    url = ENDPOINT + '?date=' + day.strftime('%Y%m%d')
    req = urllib.request.Request(url, headers={'Accept': 'application/json', 'User-Agent': UA})
    try:
        with opener.open(req, timeout=18) as resp:
            if resp.status != 200:
                raise CollectionHold('UPSTREAM_NON_200')
            content_type = resp.headers.get('Content-Type', '').lower()
            if 'json' not in content_type:
                raise CollectionHold('UPSTREAM_NOT_JSON')
            raw = resp.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise CollectionHold('UPSTREAM_RESPONSE_TOO_LARGE')
    except urllib.error.HTTPError as exc:
        # No bypass, proxies, cookie impersonation, retries on access denial or rate limits.
        raise CollectionHold('UPSTREAM_HTTP_' + str(exc.code)) from None
    except (urllib.error.URLError, TimeoutError):
        raise CollectionHold('UPSTREAM_NETWORK_ERROR') from None
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise CollectionHold('UPSTREAM_INVALID_JSON') from None
    summary = inspect_payload(payload, day)
    return payload, summary


def snapshot(day: date, payload: dict, summary: dict, observed_at: str) -> dict:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    return {'schema': 'wa-pnj-history-raw-v1', 'date': day.isoformat(),
            'publisher': 'PNJ', 'source_page': SOURCE,
            'source_endpoint_path': '/ecom-frontend/v1/get-gold-price-history',
            'collection_method': 'OFFICIAL_PUBLIC_HISTORY_PAGE_BACKEND_UNDOCUMENTED',
            'publication_status': 'INTERNAL_REVIEW_ONLY',
            'observed_at': observed_at, 'unit': 'thousand_VND_per_luong',
            'response_sha256': hashlib.sha256(canonical).hexdigest(),
            'summary': summary, 'raw': payload}


def checked_json(blob: bytes) -> dict:
    if len(blob) > MAX_BYTES * 2:
        raise CollectionHold('R2_OBJECT_TOO_LARGE')
    try:
        doc = json.loads(blob)
    except (ValueError, UnicodeError):
        raise CollectionHold('R2_INVALID_JSON') from None
    if not isinstance(doc, dict):
        raise CollectionHold('R2_INVALID_SCHEMA')
    return doc


class R2:
    def __init__(self):
        required = ['R2_ENDPOINT_URL', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET_NAME']
        if any(not os.getenv(x) for x in required):
            raise CollectionHold('R2_CREDENTIAL_OR_BUCKET_NOT_CONFIGURED')
        import boto3
        self.bucket = os.environ['R2_BUCKET_NAME']
        self.client = boto3.client('s3', endpoint_url=os.environ['R2_ENDPOINT_URL'],
            aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'])
        self.client.head_bucket(Bucket=self.bucket)

    def read(self, key: str):
        from botocore.exceptions import ClientError
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as err:
            code = str(err.response.get('Error', {}).get('Code', ''))
            if code in ('NoSuchKey', '404'):
                return None
            raise CollectionHold('R2_READ_FAILED') from None
        return checked_json(obj['Body'].read(MAX_BYTES * 2 + 1))

    def write(self, key: str, doc: dict):
        body = json.dumps(doc, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
        if len(body) > MAX_BYTES * 2:
            raise CollectionHold('R2_WRITE_TOO_LARGE')
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body,
            ContentType='application/json', CacheControl='private, no-store')


def check_approvals():
    if os.getenv('PNJ_HISTORY_COLLECT_APPROVED') != '1':
        raise CollectionHold('COLLECTION_APPROVAL_NOT_ENABLED')
    if os.getenv('PNJ_HISTORY_R2_WRITE_APPROVED') != '1':
        raise CollectionHold('R2_WRITE_APPROVAL_NOT_ENABLED')


def backfill(storage, *, first: date = START_DATE, last: date | None = None,
             max_days: int = 18, delay: float = 10.0, fetcher=fetch_day, sleeper=time.sleep,
             now=None) -> dict:
    max_days = limit_days(max_days)
    if delay < 8:
        raise ValueError('MIN_DELAY_8_SECONDS')
    last = last or (today_vn() - timedelta(days=1))
    if last > today_vn() - timedelta(days=1):
        raise ValueError('CURRENT_OR_FUTURE_DAY_NOT_ALLOWED')
    if first < START_DATE:
        raise ValueError('DATE_BEFORE_PNJ_UI_YEAR_RANGE')
    state = storage.read(STATE_KEY)
    if state is not None:
        if state.get('schema') != 'wa-pnj-history-backfill-state-v1':
            raise CollectionHold('STATE_SCHEMA_MISMATCH')
        cursor = exact_date(state['next_date'])
        if cursor < first or cursor > today_vn() + timedelta(days=1):
            raise CollectionHold('STATE_CURSOR_OUTSIDE_RANGE')
        if state.get('paused_reason'):
            raise CollectionHold('STATE_PAUSED_' + str(state['paused_reason']))
    else:
        cursor = first
        state = {'schema': 'wa-pnj-history-backfill-state-v1', 'next_date': cursor.isoformat(),
                 'days_processed': 0, 'days_with_updates': 0, 'empty_unverified_days': 0,
                 'quarantined_rows': 0}
    done = fetched = reused = 0
    active_year = cursor.year  # One run handles one year only, never crosses rollover.
    year_scan_status = 'IN_PROGRESS'
    while cursor <= last and done < max_days and cursor.year == active_year:
        key = key_for(cursor)
        existing = storage.read(key)
        if existing is not None:
            if existing.get('schema') != 'wa-pnj-history-raw-v1' or existing.get('date') != cursor.isoformat():
                raise CollectionHold('EXISTING_DAY_OBJECT_MISMATCH')
            summary = existing.get('summary')
            if not isinstance(summary, dict) or 'coverage' not in summary:
                raise CollectionHold('EXISTING_DAY_METADATA_MISMATCH')
            reused += 1
        else:
            try:
                payload, summary = fetcher(cursor)
            except CollectionHold as exc:
                if str(exc) in ('UPSTREAM_HTTP_301', 'UPSTREAM_HTTP_302', 'UPSTREAM_HTTP_307', 'UPSTREAM_HTTP_308', 'UPSTREAM_HTTP_401', 'UPSTREAM_HTTP_403', 'UPSTREAM_HTTP_418', 'UPSTREAM_HTTP_429', 'UPSTREAM_HTTP_451'):
                    state['paused_reason'] = str(exc)
                    state['paused_date'] = cursor.isoformat()
                    storage.write(STATE_KEY, state)
                raise
            observation = (now or (lambda: datetime.now(timezone.utc)))().isoformat()
            doc = snapshot(cursor, payload, summary, observation)
            storage.write(key, doc)  # Save exact day BEFORE advancing cursor.
            fetched += 1
        year_scan_status = commit_year_progress(storage, cursor, summary)
        # Only advance the global cursor AFTER the day object and year receipt
        # have both been durably persisted. Retry can safely reconcile a crash.
        state['days_processed'] += 1
        state['days_with_updates'] += summary['coverage'] == 'OBSERVED'
        state['empty_unverified_days'] += summary['coverage'] != 'OBSERVED'
        state['quarantined_rows'] += summary['quarantined_row_count']
        cursor += timedelta(days=1)
        state['next_date'] = cursor.isoformat()
        storage.write(STATE_KEY, state)
        done += 1
        print('DAY_OK date=' + (cursor - timedelta(days=1)).isoformat() +
              ' source=' + ('R2_EXISTING' if existing is not None else 'PNJ') +
              ' coverage=' + summary['coverage'], flush=True)
        if cursor <= last and done < max_days:
            sleeper(delay)
    return {'processed': done, 'fetched': fetched, 'reused': reused,
            'year': active_year, 'year_state_key': year_key(active_year),
            'year_scan_status': year_scan_status,
            'year_boundary_reached': cursor.year != active_year,
            'next_date': state['next_date'], 'historical_through': last.isoformat(),
            'complete_through_target': cursor > last}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run', action='store_true', help='Requires two explicit approval flags')
    p.add_argument('--max-days', type=int, default=18)
    p.add_argument('--delay-seconds', type=float, default=10.0)
    args = p.parse_args(argv)
    try:
        limit_days(args.max_days)
        if args.delay_seconds < 8: raise ValueError('MIN_DELAY_8_SECONDS')
        if not args.run:
            print('PNJ_HISTORY_PLAN_BEGIN')
            print('MODE=OFFLINE_NO_EGRESS_NO_R2')
            print('DAY_RANGE=2016-01-01..YESTERDAY_ASIA_HO_CHI_MINH')
            print('MODE_STRATEGY=ONE_YEAR_PER_RUN_DAY_OBJECT_FIRST_YEAR_RECEIPT_SECOND_CURSOR_LAST')
            print('YEAR_PROGRESS=metal/pnj/history-v1/years/YYYY/_progress.json')
            print('MAX_DAYS_PER_RUN=' + str(args.max_days))
            print('SLEEP_SECONDS=' + str(args.delay_seconds))
            print('PNJ_HISTORY_PLAN_END')
            return 0
        check_approvals()
        print('PNJ_HISTORY_BACKFILL_BEGIN')
        print('BACKFILL=' + json.dumps(backfill(R2(), max_days=args.max_days, delay=args.delay_seconds), sort_keys=True))
        print('PNJ_HISTORY_BACKFILL_END')
        return 0
    except (CollectionHold, ValueError, KeyError) as e:
        print('STATUS=HOLD reason=' + str(e), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
