"""PNJ source daily catch-up + last three closed-day source revision audit.

Original R2 days are immutable. Changes go into content-addressed revision
objects and an audited latest-pointer. Do not create trading OHLC or invent
prices on market holidays.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pnj_history_backfill import (
    CollectionHold, R2, backfill, check_approvals, exact_date, fetch_day,
    key_for, snapshot, today_vn
)
from pnj_clean_v1 import (
    canonical_hash, materialize_day, revision_pointer_key,
    source_for_day, validate_source
)

RECHECK_DAYS = 3
REVISION_PREFIX = 'metal/pnj/history-v1/revisions/'
MAX_CATCHUP = 7


def revision_key(day, digest):
    if len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
        raise CollectionHold('REVISION_SHA_INVALID')
    return REVISION_PREFIX + f'{day:%Y/%m}/{day:%Y-%m-%d}/{digest}.json'


def persist_recheck(storage, day, payload, summary, *, now=None):
    # A new source version never overwrites initial historical days or an
    # earlier revision. Clean view is re-materialized from the latest pointer.
    current, current_key = source_for_day(storage, day)
    fresh_sha = canonical_hash(payload)
    current_sha = current['response_sha256']
    if fresh_sha == current_sha:
        clean, updated = materialize_day(storage, day)
        return {'date':day.isoformat(), 'status':'UNCHANGED',
                'clean_updated':bool(updated), 'coverage':clean['target_status']}
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    candidate = snapshot(day, payload, summary, stamp)
    baseline_key = key_for(day)
    baseline = storage.read(baseline_key)
    if baseline is None:
        raise CollectionHold('IMMUTABLE_BASELINE_MISSING')
    if baseline.get('response_sha256') == fresh_sha:
        # PNJ returned to the original source version: use its existing
        # immutable object instead of writing a duplicate revision.
        rev_key = baseline_key
    else:
        rev_key = revision_key(day, fresh_sha)
        old = storage.read(rev_key)
        if old is not None:
            validate_source(old, day)
            if old.get('response_sha256') != fresh_sha:
                raise CollectionHold('IMMUTABLE_SOURCE_REVISION_CONFLICT')
        else:
            storage.write(rev_key, candidate)
    previous = current.get('summary') or {}
    # A transient PNJ partial/empty upstream response must not silently erase
    # an already validated historical quote series. Preserve it for review,
    # without moving the source pointer or clean daily view.
    regressed = ((previous.get('coverage') == 'OBSERVED' and
                  summary.get('coverage') != 'OBSERVED') or
                 (previous.get('target_coverage') == 'BOTH' and
                  summary.get('target_coverage') != 'BOTH'))
    if regressed:
        review = {'schema': 'wa-pnj-source-revision-review-v1',
                  'date': day.isoformat(), 'candidate_key': rev_key,
                  'candidate_sha256': fresh_sha, 'previous_key': current_key,
                  'previous_sha256': current_sha,
                  'reason': 'SOURCE_TARGET_COVERAGE_REGRESSION',
                  'publication_status': 'INTERNAL_REVIEW_ONLY'}
        review_key = ('metal/pnj/history-v1/review/' +
                      f'{day:%Y/%m}/{day:%Y-%m-%d}/{fresh_sha}.json')
        existing_review = storage.read(review_key)
        if existing_review is not None and existing_review != review:
            raise CollectionHold('SOURCE_REVIEW_RECEIPT_CONFLICT')
        if existing_review is None:
            storage.write(review_key, review)
        return {'date': day.isoformat(), 'status': 'SOURCE_REVISION_HELD_FOR_REVIEW',
                'clean_updated': False, 'coverage': previous.get('target_coverage')}
    latest_key = revision_pointer_key(day)
    pointer = {'schema':'wa-pnj-source-latest-v1', 'date':day.isoformat(),
               'object_key':rev_key, 'raw_sha256':fresh_sha,
               'previous_key':current_key, 'previous_sha256':current_sha,
               'changed_at':stamp, 'publication_status':'INTERNAL_REVIEW_ONLY'}
    storage.write(latest_key, pointer)
    clean, _ = materialize_day(storage, day)
    return {'date':day.isoformat(), 'status':'SOURCE_REVISION_SAVED',
            'clean_updated':True, 'coverage':clean['target_status']}


def run_daily(storage, *, max_catchup=MAX_CATCHUP, days_to_recheck=RECHECK_DAYS,
              fetcher=fetch_day, sleeper=time.sleep, date_now=today_vn):
    if not 1 <= max_catchup <= MAX_CATCHUP or not 1 <= days_to_recheck <= 3:
        raise ValueError('DAILY_BOUNDS')
    yesterday = date_now() - timedelta(days=1)
    # The original history backfill owns the source cursor and year receipts.
    # It is safe to invoke after completion: no source traffic in that case.
    caught = backfill(storage, max_days=max_catchup, last=yesterday,
                      delay=10.0, fetcher=fetcher, sleeper=sleeper)
    state = storage.read('metal/pnj/history-v1/_state.json')
    if state is None:
        raise CollectionHold('HISTORY_STATE_MISSING')
    if exact_date(state['next_date']) <= yesterday:
        return {'status':'SOURCE_BACKLOG_PENDING', 'catchup':caught,
                'next_date':state['next_date'], 'rechecks':[]}
    rechecks=[]
    # Rechecking multiple days remains serial with 10-sec spacing, same host.
    # A first 10-second gap separates source catch-up and source recheck.
    if caught['fetched']:
        sleeper(10.0)
    for age in range(days_to_recheck - 1, -1, -1):
        day = yesterday - timedelta(days=age)
        # Only recheck days within the historical source range.
        if day < exact_date('2016-01-01'):
            continue
        # Confirm source already persisted before any fresh HTTP query.
        source_for_day(storage, day)
        try:
            payload, summary = fetcher(day)
        except CollectionHold as exc:
            # Source access denial applies to rechecks too, not only new days.
            # Persist the same fail-closed pause as the historical backfiller.
            if str(exc) in (
                'UPSTREAM_HTTP_301', 'UPSTREAM_HTTP_302',
                'UPSTREAM_HTTP_307', 'UPSTREAM_HTTP_308',
                'UPSTREAM_HTTP_401', 'UPSTREAM_HTTP_403',
                'UPSTREAM_HTTP_418', 'UPSTREAM_HTTP_429',
                'UPSTREAM_HTTP_451',
            ):
                state['paused_reason'] = str(exc)
                state['paused_date'] = day.isoformat()
                storage.write('metal/pnj/history-v1/_state.json', state)
            raise
        rechecks.append(persist_recheck(storage, day, payload, summary))
        if age > 0:
            sleeper(10.0)
    return {'status':'DAILY_OK', 'catchup':caught,
            'next_date':state['next_date'], 'rechecks':rechecks}


def main(argv=None):
    p=argparse.ArgumentParser()
    p.add_argument('--run', action='store_true')
    p.add_argument('--max-catchup', type=int, default=MAX_CATCHUP)
    args=p.parse_args(argv)
    if not args.run:
        print('PNJ_DAILY_PLAN MODE=OFFLINE_NO_EGRESS '
              'yesterday_only=1 recheck_previous_closed_days=3 '
              'max_catchup='+str(args.max_catchup))
        return 0
    if os.environ.get('PNJ_DAILY_APPROVED') != '1':
        print('STATUS=HOLD reason=DAILY_NOT_APPROVED',file=sys.stderr)
        return 2
    try:
        check_approvals()
        print('PNJ_DAILY_BEGIN')
        print('PNJ_DAILY='+json.dumps(run_daily(R2(),max_catchup=args.max_catchup),
                                     sort_keys=True))
        print('PNJ_DAILY_END')
        return 0
    except (CollectionHold, ValueError, KeyError) as exc:
        print('STATUS=HOLD reason='+str(exc),file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
