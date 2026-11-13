"""PNJ clean-v1: immutable snapshots + a mutable derived daily view.

Source of truth remains metal/pnj/history-v1/days/**. Never edit original
PNJ objects. Clean quotes are historical retail bid/ask, NOT traded OHLC.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pnj_history_backfill import (
    START_DATE, CollectionHold, R2, exact_date, key_for, today_vn, UTC7,
)

PREFIX = 'metal/pnj/clean-v1/'
STATE_KEY = PREFIX + '_state.json'
PARSER = 'pnj-retail-bidask-clean-1.0'
MAX_BATCH = 4000
TIME_BUDGET_SECONDS = 135 * 60
SJC_REGIONS = frozenset(('TPHCM', 'Hà Nội', 'Đà Nẵng', 'Miền Tây', 'Tây Nguyên', 'Đông Nam Bộ'))
PRICE = re.compile(r'(?:[0-9]{1,3}(?:\.[0-9]{3})+|[0-9]+)\Z')
SOURCE_SCHEMA = 'wa-pnj-history-raw-v1'


def canonical_hash(payload: object) -> str:
    data = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(data).hexdigest()


def day_key(day: date) -> str:
    return PREFIX + f'days/{day:%Y/%m}/{day:%Y-%m-%d}.json'


def snapshot_key(day: date, raw_hash: str) -> str:
    if not re.fullmatch(r'[a-f0-9]{64}', raw_hash):
        raise CollectionHold('BAD_RAW_HASH')
    return PREFIX + f'snapshots/{day:%Y/%m}/{day:%Y-%m-%d}/{raw_hash}.json'


def revision_pointer_key(day: date) -> str:
    return f'metal/pnj/history-v1/latest/{day:%Y/%m}/{day:%Y-%m-%d}.json'


def source_for_day(storage, day: date):
    baseline_key = key_for(day)
    pointer = storage.read(revision_pointer_key(day))
    if pointer is None:
        key = baseline_key
    else:
        if (pointer.get('schema') != 'wa-pnj-source-latest-v1' or
            pointer.get('date') != day.isoformat() or
            not isinstance(pointer.get('object_key'), str) or
            not (pointer['object_key'] == baseline_key or
                 pointer['object_key'].startswith(
                     f'metal/pnj/history-v1/revisions/{day:%Y/%m}/{day:%Y-%m-%d}/'))):
            raise CollectionHold('SOURCE_POINTER_INVALID')
        key = pointer['object_key']
    doc = storage.read(key)
    if doc is None:
        raise CollectionHold('SOURCE_DAY_MISSING_' + day.isoformat())
    validate_source(doc, day)
    if pointer is not None and pointer.get('raw_sha256') != doc['response_sha256']:
        raise CollectionHold('SOURCE_POINTER_HASH_MISMATCH')
    return doc, key


def validate_source(doc: dict, day: date):
    if (not isinstance(doc, dict) or doc.get('schema') != SOURCE_SCHEMA or
        doc.get('date') != day.isoformat() or
        doc.get('unit') != 'thousand_VND_per_luong' or
        not isinstance(doc.get('raw'), dict)):
        raise CollectionHold('SOURCE_DAY_CONTRACT_MISMATCH')
    actual = canonical_hash(doc['raw'])
    if doc.get('response_sha256') != actual:
        raise CollectionHold('SOURCE_DAY_RAW_SHA256_MISMATCH')
    if not isinstance(doc['raw'].get('locations'), list):
        raise CollectionHold('SOURCE_LOCATIONS_SCHEMA_MISMATCH')


def price_vnd(value):
    if not isinstance(value, str) or not PRICE.fullmatch(value):
        raise ValueError('NON_NUMERIC_OR_ABSENT_PRICE')
    n = int(value.replace('.', ''))
    if not 1_000 <= n <= 1_000_000:
        raise ValueError('PRICE_OUTSIDE_SAFETY_RANGE')
    return n * 1000


def normalize(day: date, source_doc: dict, source_key: str) -> dict:
    validate_source(source_doc, day)
    rows, rejected = {}, []
    duplicate_identical = 0
    for loc in source_doc['raw']['locations']:
        if not isinstance(loc, dict) or not isinstance(loc.get('gold_type'), list):
            raise CollectionHold('SOURCE_LOCATION_SCHEMA_DRIFT')
        region = loc.get('name')
        if not isinstance(region, str):
            raise CollectionHold('SOURCE_REGION_SCHEMA_DRIFT')
        for product in loc['gold_type']:
            if not isinstance(product, dict) or not isinstance(product.get('data'), list):
                raise CollectionHold('SOURCE_PRODUCT_SCHEMA_DRIFT')
            name = product.get('name')
            if not isinstance(name, str):
                raise CollectionHold('SOURCE_PRODUCT_NAME_SCHEMA_DRIFT')
            symbol = ('PNJ_SJC_QUOTE' if name == 'SJC' and region in SJC_REGIONS
                      else 'PNJ_RING_9999' if name == 'Nhẫn Trơn PNJ 999.9'
                      and region == 'Giá vàng nữ trang' else None)
            if symbol is None:
                continue  # Other PNJ products are neither SJC nor rings.
            for r in product['data']:
                if not isinstance(r, dict):
                    rejected.append({'symbol': symbol, 'region': region,
                                     'reason': 'ROW_NOT_OBJECT'})
                    continue
                ts = r.get('updated_at')
                try:
                    stamp = datetime.strptime(ts, '%d/%m/%Y %H:%M:%S')
                    if stamp.date() != day:
                        raise ValueError('WRONG_SOURCE_DAY')
                    buy, sell = price_vnd(r.get('gia_mua')), price_vnd(r.get('gia_ban'))
                    if buy > sell:
                        raise ValueError('INVERTED_SPREAD')
                except (TypeError, ValueError) as e:
                    rejected.append({'symbol': symbol, 'region': region,
                                     'updated_at': ts if isinstance(ts, str) else None,
                                     'reason': str(e), 'source_buy': r.get('gia_mua'),
                                     'source_sell': r.get('gia_ban')})
                    continue
                key = (symbol, region, stamp.isoformat())
                quote = {'symbol': symbol, 'region': region,
                         'time_local': stamp.isoformat(),
                         'buy_vnd_per_luong': buy, 'sell_vnd_per_luong': sell}
                if key in rows:
                    if rows[key] is None:
                        rejected.append({'symbol': symbol, 'region': region,
                                         'updated_at': ts,
                                         'reason': 'CONFLICTING_SAME_TIMESTAMP'})
                    elif rows[key] != quote:
                        # Quarantine both sides: a source conflict must never
                        # arbitrarily choose a winner as an accepted quote.
                        rejected.append({'symbol': symbol, 'region': region,
                                         'updated_at': ts,
                                         'reason': 'CONFLICTING_SAME_TIMESTAMP',
                                         'source_buy_vnd': rows[key]['buy_vnd_per_luong'],
                                         'source_sell_vnd': rows[key]['sell_vnd_per_luong']})
                        rejected.append({'symbol': symbol, 'region': region,
                                         'updated_at': ts,
                                         'reason': 'CONFLICTING_SAME_TIMESTAMP',
                                         'source_buy_vnd': buy,
                                         'source_sell_vnd': sell})
                        rows[key] = None
                    else:
                        duplicate_identical += 1
                else:
                    rows[key] = quote
    valid = [q for q in rows.values() if q is not None]
    valid.sort(key=lambda q: (q['symbol'], q['region'], q['time_local']))
    symbols = {s: sum(q['symbol'] == s for q in valid)
               for s in ('PNJ_SJC_QUOTE', 'PNJ_RING_9999')}
    status = ('BOTH_TARGETS_OBSERVED' if all(symbols.values()) else
              'NO_TARGET_QUOTES' if not any(symbols.values()) else
              'PARTIAL_TARGET_COVERAGE')
    return {'schema': 'wa-pnj-clean-day-v1', 'parser': PARSER,
            'date': day.isoformat(), 'publisher': 'PNJ',
            'publication_status': 'INTERNAL_REVIEW_ONLY',
            'unit': 'VND_per_luong', 'quote_type': 'RETAIL_BID_ASK_NOT_OHLC',
            'source_key': source_key, 'source_sha256': source_doc['response_sha256'],
            'source_observed_at': source_doc.get('observed_at'),
            'summary': {'target_status': status, 'valid_rows': len(valid),
                        'sjc_rows': symbols['PNJ_SJC_QUOTE'],
                        'pnj_ring_rows': symbols['PNJ_RING_9999'],
                        'target_quarantined_rows': len(rejected),
                        'identical_duplicates_dropped': duplicate_identical,
                        'raw_source_quarantined_rows': source_doc.get('summary', {}).get(
                            'quarantined_row_count', 0),
                        'source_day_status': source_doc.get('summary', {}).get(
                            'coverage', 'UNKNOWN')},
            'quotes': valid, 'quarantine': rejected}


def materialize_day(storage, day: date):
    source_doc, source_key = source_for_day(storage, day)
    clean = normalize(day, source_doc, source_key)
    snapkey = snapshot_key(day, source_doc['response_sha256'])
    old_snap = storage.read(snapkey)
    if old_snap is not None and old_snap != clean:
        raise CollectionHold('IMMUTABLE_CLEAN_SNAPSHOT_CONFLICT')
    if old_snap is None:
        storage.write(snapkey, clean)
    # Mutable daily view is permitted ONLY in the clean-v1 namespace; every
    # version is preserved in snapshots. Original source day is never modified.
    current_key = day_key(day)
    current = storage.read(current_key)
    if current is not None and (current.get('schema') != 'wa-pnj-clean-day-v1' or
                                current.get('date') != day.isoformat()):
        raise CollectionHold('CLEAN_CURRENT_SCHEMA_CONFLICT')
    if current != clean:
        storage.write(current_key, clean)
    return clean['summary'], (old_snap is None or current != clean)


def build_history(storage, *, first=START_DATE, last=None, max_days=MAX_BATCH,
                  monotonic=time.monotonic):
    if not 1 <= max_days <= MAX_BATCH:
        raise ValueError('CLEAN_DAY_LIMIT_OUT_OF_BOUNDS')
    last = last or (today_vn() - timedelta(days=1))
    if last > today_vn() - timedelta(days=1) or first < START_DATE:
        raise CollectionHold('HISTORY_CLEAN_DATE_RANGE')
    state = storage.read(STATE_KEY)
    if state is None:
        state = {'schema': 'wa-pnj-clean-progress-v1',
                 'next_date': first.isoformat(), 'days_done': 0,
                 'days_with_both_targets': 0, 'days_without_target': 0}
    if state.get('schema') != 'wa-pnj-clean-progress-v1':
        raise CollectionHold('CLEAN_PROGRESS_SCHEMA')
    day = exact_date(state['next_date'])
    if not first <= day <= today_vn() + timedelta(days=1):
        raise CollectionHold('CLEAN_PROGRESS_DATE_OUT_OF_RANGE')
    began=monotonic()
    processed=changed=0
    while day <= last and processed < max_days:
        if monotonic()-began >= TIME_BUDGET_SECONDS:
            break
        summary, did_change = materialize_day(storage, day)
        state['days_done'] += 1
        state['days_with_both_targets'] += summary['target_status'] == 'BOTH_TARGETS_OBSERVED'
        state['days_without_target'] += summary['target_status'] == 'NO_TARGET_QUOTES'
        day += timedelta(days=1)
        state['next_date'] = day.isoformat()
        storage.write(STATE_KEY, state)
        processed += 1
        changed += did_change
        if processed % 100 == 0 or day > last:
            print(f'CLEAN_PROGRESS days={processed} next_date={day.isoformat()}', flush=True)
    return {'processed': processed, 'materialized': changed,
            'next_date': state['next_date'], 'target_through': last.isoformat(),
            'caught_up': day > last}


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--run', action='store_true')
    p.add_argument('--max-days', type=int, default=MAX_BATCH)
    args=p.parse_args(argv)
    if not args.run:
        print('PNJ_CLEAN_PLAN MODE=OFFLINE_NO_EGRESS max_days='+str(args.max_days))
        return 0
    if os.environ.get('PNJ_CLEAN_R2_WRITE_APPROVED') != '1':
        print('STATUS=HOLD reason=CLEAN_WRITE_NOT_APPROVED', file=sys.stderr)
        return 2
    try:
        print('PNJ_CLEAN_BEGIN')
        print('PNJ_CLEAN='+json.dumps(build_history(R2(), max_days=args.max_days),
                                     sort_keys=True))
        print('PNJ_CLEAN_END')
        return 0
    except (CollectionHold, ValueError, KeyError) as exc:
        print('STATUS=HOLD reason='+str(exc),file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
