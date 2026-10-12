"""Source-only Alpha Events +24h handoff IN EXISTING listing-price writer.
No scheduler, new R2 key, queue, or second writer. Disabled at call-site by default.
Do not activate without owner-approved R2, Supabase, Oracle and manual seed fencing.
"""
import copy
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone

LIVE_MS = 86_400_000
MAX_BYTES = 2_000_000
MAX_ROWS = 20_000
CANONICAL = re.compile(r'^ae:[0-9a-f]{24}$')
UTC_STAMP = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$')
ALL_KEY = 'alpha-events/all.json'
HISTORY_KEY = 'alpha-events/history.json'
LIVE_KEY = 'alpha-events/live.json'

class CutoverHold(RuntimeError):
    pass

def _hold(reason):
    raise CutoverHold('WA_AE_HISTORY24H_HOLD_' + reason)

def _stamp(value):
    if not isinstance(value, str) or not UTC_STAMP.fullmatch(value):
        _hold('CLOCK_FORMAT')
    try:
        d = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    except ValueError:
        _hold('CLOCK_INVALID')
    if d.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z' != value:
        _hold('CLOCK_ROUNDTRIP')
    return d

def _rows(rows, name):
    if not isinstance(rows, list) or len(rows) > MAX_ROWS:
        _hold(name + '_BOUNDED_ROWS')
    if any(not isinstance(x, dict) for x in rows):
        _hold(name + '_NONOBJECT')
    return rows

def _fingerprint(row):
    return json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(',', ':'))

def _indexes(rows, source):
    result = {}
    for i, row in enumerate(rows):
        key = row.get('id')
        if isinstance(key, str) and CANONICAL.fullmatch(key):
            if key in result:
                _hold(source + '_CANONICAL_DUPLICATE')
            result[key] = i
        elif row.get('firstLiveAt') is not None:
            _hold(source + '_STAMP_ID_INVALID')
    return result

def _compatible(old, new):
    for field in ('symbol', 'event_type', 'contract_address'):
        a, b = old.get(field), new.get(field)
        if a is not None and b is not None and str(a).lower() != str(b).lower():
            _hold('IDENTITY_CONFLICT')
    if old.get('firstLiveAt') is not None and old['firstLiveAt'] != new['firstLiveAt']:
        _hold('IMMUTABLE_CLOCK_CONFLICT')

def _overlay_without_erasing(prior, addition):
    result = dict(prior)
    for key, value in addition.items():
        if value is not None or key not in result:
            result[key] = copy.deepcopy(value)
    return result

def reconcile_price_enrichment(pre_enrichment, enriched_all, prior_history):
    """Keep the existing listing-price writer as owner of legacy price updates.

    Historical rows must be matched to their own pre-enrichment ALL rows, not
    fuzzy-matched on symbol/date (multi-round Alpha events may share symbols).
    A run can enrich rows, but must not silently erase old History prices.
    """
    before = _rows(pre_enrichment, 'PRE_ENRICHMENT')
    after = _rows(enriched_all, 'POST_ENRICHMENT')
    old_history = _rows(prior_history, 'PRIOR_HISTORY')
    if len(before) != len(after):
        _hold('ENRICHMENT_REORDER_OR_CARDINALITY')
    for a, b in zip(before, after):
        for field in ('id', 'source_msg_id', 'event_time'):
            if a.get(field) != b.get(field):
                _hold('ENRICHMENT_IDENTITY_MOVED')
    # An old historical row must be present verbatim in old ALL before this
    # invocation; no guessed fallback match is allowed for ID-less rows.
    positions = {}
    for i, row in enumerate(before):
        positions.setdefault(_fingerprint(row), []).append(i)
    result_all = copy.deepcopy(after)
    for old in old_history:
        matches = positions.get(_fingerprint(old), [])
        if not matches:
            _hold('HISTORY_PREIMAGE_NOT_IN_ALL')
        index = matches.pop(0)
        newer = result_all[index]
        if newer.get('status') in ('live', 'upcoming'):
            _hold('HISTORY_STATUS_REGRESSION')
        for price_key in ('listing_price', 'spot_listing_price'):
            if old.get(price_key) is not None and newer.get(price_key) is None:
                _hold('HISTORY_PRICE_ERASURE')
        # Old optional fields (including imported enrichments) are retained
        # when the price updater did not touch them.
        result_all[index] = _overlay_without_erasing(old, newer)
    result_history = [copy.deepcopy(x) for x in result_all
                      if x.get('status') not in ('live', 'upcoming')]
    # Historical multiset containment remains mandatory after enrichment.
    if len(result_history) < len(old_history):
        _hold('HISTORY_CARDINALITY_REGRESSION')
    return result_all, result_history

def plan(all_rows, history_rows, live_rows, now):
    """Pure deterministic plan; never returns permission to retire physical Live."""
    old_all = _rows(all_rows, 'ALL')
    old_history = _rows(history_rows, 'HISTORY')
    old_live = _rows(live_rows, 'LIVE')
    if now.tzinfo is None or now.utcoffset() != timedelta(0):
        _hold('NOW_NOT_UTC')
    # Whole-row multiset containment: legacy records with missing IDs survive.
    counts = Counter(map(_fingerprint, old_all))
    for row in old_history:
        key = _fingerprint(row)
        if counts[key] == 0:
            _hold('EXISTING_HISTORY_NOT_IN_ALL')
        counts[key] -= 1
    new_all = copy.deepcopy(old_all)
    new_history = copy.deepcopy(old_history)
    all_ids, hist_ids = _indexes(new_all, 'ALL'), _indexes(new_history, 'HISTORY')
    _indexes(old_live, 'LIVE')
    complete = []
    for candidate in old_live:
        cid = candidate.get('id')
        clock = candidate.get('firstLiveAt')
        if clock is None:
            continue  # Unstamped legacy is not guessed from event_time.
        if not isinstance(cid, str) or not CANONICAL.fullmatch(cid):
            _hold('LIVE_CANONICAL_ID')
        first = _stamp(clock)
        if now < first + timedelta(milliseconds=LIVE_MS):
            if cid in hist_ids:
                _hold('EARLY_HISTORY_REENTRY')
            continue
        old_a = new_all[all_ids[cid]] if cid in all_ids else {}
        old_h = new_history[hist_ids[cid]] if cid in hist_ids else {}
        _compatible(old_a, candidate)
        _compatible(old_h, candidate)
        data = copy.deepcopy(candidate)
        data = _overlay_without_erasing(data, old_a)
        data = _overlay_without_erasing(data, old_h)
        data.update({'id': cid, 'firstLiveAt': clock,
                     'status': 'ended', 'lifecycle_state': 'history'})
        if cid in all_ids:
            new_all[all_ids[cid]] = copy.deepcopy(data)
        else:
            all_ids[cid] = len(new_all)
            new_all.append(copy.deepcopy(data))
        if cid in hist_ids:
            new_history[hist_ids[cid]] = copy.deepcopy(data)
        else:
            hist_ids[cid] = len(new_history)
            new_history.append(copy.deepcopy(data))
        complete.append(cid)
    # Verify output contains exact current History rows except the ones being
    # promoted (which can only have fields added and terminal status forced).
    for i, row in enumerate(old_history):
        if i >= len(new_history):
            _hold('HISTORY_LOST_ROW')
        if row.get('id') not in complete and new_history[i] != row:
            _hold('LEGACY_HISTORY_CHANGED')
    _rows(new_all, 'ALL_OUT')
    _rows(new_history, 'HISTORY_OUT')
    return {'all': new_all, 'history': new_history,
            'promoted': complete, 'retire_live': False,
            'independent_writer': False}

def snapshot(r2, bucket, key):
    """Bounded, strict, versioned R2 GET. No permissive [] on failure."""
    obj = r2.get_object(Bucket=bucket, Key=key)
    etag = obj.get('ETag')
    if not isinstance(etag, str) or not etag.strip():
        _hold('R2_ETAG_MISSING')
    length = obj.get('ContentLength')
    if isinstance(length, int) and length > MAX_BYTES:
        _hold('R2_OBJECT_TOO_LARGE')
    body = obj['Body'].read(MAX_BYTES + 1)
    if len(body) > MAX_BYTES:
        _hold('R2_OBJECT_TOO_LARGE')
    try:
        values = json.loads(body.decode('utf-8'))
    except (ValueError, UnicodeError):
        _hold('R2_INVALID_JSON')
    _rows(values, key.upper())
    return values, etag

def _json_body(rows):
    raw = json.dumps(rows, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    if len(raw) > MAX_BYTES:
        _hold('R2_WRITE_TOO_LARGE')
    return raw

def publish_pair(r2, bucket, plan_value, old_all_etag, old_hist_etag):
    """CAS All -> readback -> CAS History -> independent paired readback.
    On lost ACK/partial error: HOLD with Live untouched; no blind retries.
    """
    if plan_value.get('retire_live') is not False or plan_value.get('independent_writer') is not False:
        _hold('UNQUALIFIED_PLAN')
    values = [plan_value['all'], plan_value['history']]
    bodies = list(map(_json_body, values))
    for key, etag, body, value in ((ALL_KEY, old_all_etag, bodies[0], values[0]),
                                   (HISTORY_KEY, old_hist_etag, bodies[1], values[1])):
        r2.put_object(Bucket=bucket, Key=key, Body=body, IfMatch=etag,
                      ContentType='application/json', CacheControl='public, max-age=60')
        check, _ = snapshot(r2, bucket, key)
        if check != value:
            _hold('R2_POSTWRITE_MISMATCH')
    all_check, _ = snapshot(r2, bucket, ALL_KEY)
    hist_check, _ = snapshot(r2, bucket, HISTORY_KEY)
    if all_check != values[0] or hist_check != values[1]:
        _hold('R2_PAIR_MISMATCH')
    return {'pair_verified': True, 'promoted': plan_value['promoted'],
            'retire_live': False}