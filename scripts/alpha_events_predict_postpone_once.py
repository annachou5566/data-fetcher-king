"""Pure exact-ID operator-approved PREDICT schedule correction for the existing History writer.

Never synthesizes a receipt/revision link or claims completed TGE. Preserves
archived row inside the existing All object, and removes only its History copy.
The caller owns all CAS/readback and the existing pair lock; no client or I/O.
"""
import copy
import hashlib
import json

TARGET = 'ae:d8d1216862af5c5806bda532'
PRIME = 'ae:269fa9d99485b133fef05f5f'
OLD_TIME = '2026-11-18T09:00:00.000Z'
OFFICIAL_EVIDENCE = 'https://www.binance.com/en/square/post/11-17-2026-predict-fun-delays-predict-tge-and-season-1-airdrop-claims-389137807309625'
MARKER = '_wa_predict_manual_postponement'
PRICE_FIELDS = frozenset({
    'price_snapshot', 'market_cap', 'listing_price', 'spot_listing_price',
    'ath_since_listing_price', 'ath_since_listing_date', 'spot_ath_price',
    'spot_ath_date', 'spot_listed', 'futures_listed', 'alpha_delisted',
    'spot_listed_at', 'is_spot_listed', 'listing_price_unavailable',
    '_tge_date_checked', '_vwap_daybound_recompute', '_multiround_checked',
    '_vwap_daybound_checked',
})

class PredictPostponeHold(RuntimeError):
    pass

def _hold(code):
    raise PredictPostponeHold('WA_PREDICT_POSTPONE_HOLD_' + code)

def _sha(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':'), allow_nan=False).encode()).hexdigest()

def _check(rows, label):
    if not isinstance(rows, list) or len(rows) > 20_000 or any(not isinstance(r, dict) for r in rows):
        _hold(label + '_ROWS_INVALID')
    return rows

def _one(rows, label):
    matches = [(i,r) for i,r in enumerate(rows) if r.get('id') == TARGET]
    if len(matches) != 1:
        _hold(label + '_TARGET_CARDINALITY')
    return matches[0]

def _not_terminal(row):
    return not (row.get('completion_verified') is True or
                row.get('official_action_verified') is True or
                row.get('officialConfirmation') not in (None, '') or
                row.get('official_confirmation') not in (None, ''))

def _identity(row, time=OLD_TIME):
    return (row.get('id') == TARGET and row.get('symbol') == 'PREDICT' and
            row.get('event_time') == time and
            str(row.get('distribution_type', '')).upper() == 'TGE' and
            str(row.get('event_type') or '').lower() in ('', 'tge') and
            _not_terminal(row))

def _identity_hash(row):
    return _sha({k:v for k,v in row.items() if k not in PRICE_FIELDS})

def _price_safe(original, enriched):
    for key in PRICE_FIELDS:
        if original.get(key) is None:
            continue
        newer = enriched.get(key)
        if newer is None: return False
        if isinstance(original[key], dict):
            if not isinstance(newer, dict): return False
            for field, value in original[key].items():
                if value is not None and newer.get(field) is None: return False
    return True

def is_postponed_all(row):
    return (isinstance(row, dict) and row.get('id') == TARGET and
            row.get('status') == 'postponed_unverified' and
            row.get('notice_action') == 'postponed' and
            row.get('event_time') is None and isinstance(row.get(MARKER), dict) and
            row[MARKER].get('evidence_url') == OFFICIAL_EVIDENCE and
            row[MARKER].get('original_event_time') == OLD_TIME and
            row[MARKER].get('previous_identity_sha256') is not None)

def keep_in_history(row):
    """Filter the corrected exact ID only, never filter other PREDICT rounds."""
    return not is_postponed_all(row)

def repair_pair(all_rows, history_rows):
    """Return (All, History, corrected?, phase), no writes/no guessed joins.

    The existing writer may be recovering an All-first/History-second CAS.
    Existing unrelated History entries are preserved in their order.
    """
    all_rows = _check(all_rows, 'ALL')
    history_rows = _check(history_rows, 'HISTORY')
    if not any(isinstance(r, dict) and r.get('id') == TARGET for r in all_rows):
        if any(r.get('id') == TARGET for r in history_rows):
            _hold('HISTORY_WITHOUT_ALL')
        return all_rows, history_rows, False, 'NOT_PRESENT'
    new_all = copy.deepcopy(all_rows)
    index, row = _one(new_all, 'ALL')
    in_history = [(i,r) for i,r in enumerate(history_rows) if r.get('id') == TARGET]
    if len(in_history) > 1:
        _hold('HISTORY_DUPLICATE_TARGET')

    if is_postponed_all(row):
        marker = row[MARKER]
        if not isinstance(marker.get('previous_identity_sha256'), str) or len(marker['previous_identity_sha256']) != 64:
            _hold('INVALID_CORRECTION_MARKER')
        if in_history:
            _, previous = in_history[0]
            if not _identity(previous) or _identity_hash(previous) != marker['previous_identity_sha256']:
                _hold('UNEXPECTED_HISTORY_WHILE_RECOVERING')
        return new_all, [copy.deepcopy(r) for r in history_rows if r.get('id') != TARGET], bool(in_history), 'RECOVER_OR_DONE'

    if not in_history:
        _hold('MISSING_HISTORY_PREIMAGE')
    _, previous = in_history[0]
    if not _identity(row) or not _identity(previous):
        _hold('NOT_EXACT_18NOV_UNVERIFIED_TGE')
    if row.get('status') != 'expired_unverified' or previous.get('status') != 'expired_unverified':
        _hold('NOT_UNVERIFIED_HISTORY')
    if row.get('archive_reason') != 'scheduled_time_elapsed_unverified' or previous.get('archive_reason') != 'scheduled_time_elapsed_unverified':
        _hold('NOT_SCHEDULE_EXPIRED')
    if _identity_hash(row) != _identity_hash(previous):
        _hold('TARGET_PREIMAGE_DIVERGENCE')
    if not _price_safe(previous, row):
        _hold('PRICE_WOULD_BE_LOST')
    if any(r.get('id') == PRIME for r in history_rows) and not any(r.get('id') == PRIME for r in all_rows):
        _hold('PRIME_NOT_IN_ALL')
    # Keep all original identity and price fields available under a marker,
    # without inventing a Telegram link, new schedule, or TGE completion.
    record = copy.deepcopy(row)
    record['status'] = 'postponed_unverified'
    record['lifecycle_state'] = 'PREDICTED'
    record['event_time'] = None
    record['notice_action'] = 'postponed'
    record['completion_verified'] = False
    record[MARKER] = {
        'schema': 1,
        'owner_decision': 'manual_backend_correction_2026-11-19',
        'evidence_url': OFFICIAL_EVIDENCE,
        'original_event_time': OLD_TIME,
        'previous_status': previous.get('status'),
        'previous_lifecycle_state': previous.get('lifecycle_state'),
        'previous_archive_reason': previous.get('archive_reason'),
        'previous_identity_sha256': _identity_hash(previous),
    }
    new_all[index] = record
    new_hist = [copy.deepcopy(r) for r in history_rows if r.get('id') != TARGET]
    if len(new_all) != len(all_rows) or len(new_hist) != len(history_rows)-1:
        _hold('PAIR_COUNT_DRIFT')
    for before, after in zip([r for r in history_rows if r.get('id') != TARGET], new_hist):
        if before != after:
            _hold('UNRELATED_HISTORY_CHANGED')
    return new_all, new_hist, True, 'APPLY_ONCE'
