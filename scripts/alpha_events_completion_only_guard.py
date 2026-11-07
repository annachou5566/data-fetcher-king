"""Preserve legacy History and price enrichment.

Default price enrichment cannot silently admit events. The dedicated Lite
schedule planner admits only exact canonical Active events whose event_time
elapsed, marked expired_unverified (never completed). The existing writer
CAS-publishes both All/History. Signed evidence is required only when claiming
real completion, not when moving a missed schedule to History.
"""
import copy
import json
from collections import Counter, defaultdict

_MUTABLE_ENRICHMENT = frozenset({
    'price_snapshot', 'market_cap', 'listing_price', 'spot_listing_price',
    'ath_since_listing_price', 'ath_since_listing_date', 'spot_ath_price',
    'spot_ath_date', 'spot_listed', 'futures_listed', 'alpha_delisted',
    'spot_listed_at', 'is_spot_listed', 'listing_price_unavailable',
    '_tge_date_checked', '_vwap_daybound_recompute',
    '_multiround_checked', '_vwap_daybound_checked',
})

class CompletionOnlyHold(RuntimeError):
    pass

def _hold(code):
    raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_' + code)

def _checked(rows, label):
    if not isinstance(rows, list):
        _hold(label + '_NOT_LIST')
    for row in rows:
        if not isinstance(row, dict):
            _hold(label + '_ROW_INVALID')
    return rows

def _identity(row):
    # Never use naive ID: historical archive contains missing/duplicated IDs.
    immutable = {k: v for k, v in row.items() if k not in _MUTABLE_ENRICHMENT}
    try:
        return json.dumps(immutable, ensure_ascii=False, sort_keys=True,
                          separators=(',', ':'), allow_nan=False)
    except (TypeError, ValueError):
        _hold('UNSERIALIZABLE_ARCHIVE_IDENTITY')

def _no_price_erasure(old, new):
    for key in _MUTABLE_ENRICHMENT:
        if key not in old or old[key] is None:
            continue
        if key not in new or new[key] is None:
            return False
        # An established positive listing/peak price cannot silently become zero.
        # A market-cap or market-status transition may legitimately reach zero.
        if key in {'price_snapshot', 'listing_price', 'spot_listing_price',
                   'ath_since_listing_price', 'spot_ath_price'}:
            if (isinstance(old[key], (int, float)) and not isinstance(old[key], bool)
                    and old[key] > 0 and isinstance(new[key], (int, float))
                    and not isinstance(new[key], bool) and new[key] == 0):
                return False
        if isinstance(old[key], dict):
            if not isinstance(new[key], dict):
                return False
            for inner_key, value in old[key].items():
                if value is not None and (inner_key not in new[key] or
                                          new[key][inner_key] is None):
                    return False
                if (key in {'price_snapshot', 'listing_price', 'spot_listing_price',
                            'ath_since_listing_price', 'spot_ath_price'}
                        and isinstance(value, (int, float)) and not isinstance(value, bool)
                        and value > 0 and isinstance(new[key].get(inner_key), (int, float))
                        and not isinstance(new[key].get(inner_key), bool)
                        and new[key][inner_key] == 0):
                    return False
    return True

def enforce_no_unverified_history_admission(previous_history, proposed_history):
    """Cutover/CAS mode: reject new, missing, forged or ambiguous History rows."""
    old = _checked(previous_history, 'PREVIOUS_HISTORY')
    new = _checked(proposed_history, 'PROPOSED_HISTORY')
    if len(old) != len(new):
        _hold('HISTORY_CARDINALITY_CHANGED')
    prev, after = defaultdict(list), defaultdict(list)
    for row in old:
        prev[_identity(row)].append(row)
    for row in new:
        after[_identity(row)].append(row)
    if set(prev) != set(after):
        _hold('UNVERIFIED_HISTORY_IDENTITY_CHANGE')
    for key, originals in prev.items():
        candidates = after[key]
        if len(originals) != len(candidates):
            _hold('UNVERIFIED_HISTORY_MULTISET_CHANGE')
        if len(originals) > 1:
            # Ambiguous pre-canonical duplicates may not silently swap price.
            digest = lambda rs: Counter(json.dumps(x, sort_keys=True,
                ensure_ascii=False, separators=(',', ':')) for x in rs)
            if digest(originals) != digest(candidates):
                _hold('AMBIGUOUS_LEGACY_PRICE_CHANGE')
        elif not _no_price_erasure(originals[0], candidates[0]):
            _hold('ARCHIVED_PRICE_ERASURE')
    return len(old)

def ensure_all_contains_existing_history(all_events, history_rows):
    """Before any CAS, prove All still represents every retained History row.

    Preserve legacy ID-less/duplicate entries without token/date fuzzy matches.
    Prices may be enriched, never erased. Ambiguity holds rather than guessing.
    """
    all_rows = _checked(all_events, 'ALL')
    hist_rows = _checked(history_rows, 'HISTORY')
    by_all, by_hist = defaultdict(list), defaultdict(list)
    for row in all_rows:
        by_all[_identity(row)].append(row)
    for row in hist_rows:
        by_hist[_identity(row)].append(row)
    for key, archived in by_hist.items():
        counterparts = by_all.get(key, [])
        if len(counterparts) < len(archived):
            _hold('HISTORY_NOT_REPRESENTED_IN_ALL')
        if len(archived) != 1 or len(counterparts) != 1:
            # Duplicate immutable identities cannot be safely paired to a
            # particular price update. Permit only exact legacy copies.
            counts = Counter(json.dumps(r, sort_keys=True, ensure_ascii=False,
                separators=(',', ':'), allow_nan=False) for r in counterparts)
            for row in archived:
                marker = json.dumps(row, sort_keys=True, ensure_ascii=False,
                    separators=(',', ':'), allow_nan=False)
                if not counts[marker]:
                    _hold('AMBIGUOUS_ALL_HISTORY_PRICE_PAIR')
                counts[marker] -= 1
        elif not _no_price_erasure(archived[0], counterparts[0]):
            _hold('ALL_ARCHIVED_PRICE_ERASURE')
    return len(hist_rows)

def keep_existing_history_without_unverified_admission(previous_history, proposed_history):
    """Normal enrichment: retain all original History in original order."""
    old = _checked(previous_history, 'PREVIOUS_HISTORY')
    new = _checked(proposed_history, 'PROPOSED_HISTORY')
    prior, proposed = defaultdict(list), defaultdict(list)
    for index, row in enumerate(old):
        prior[_identity(row)].append((index, row))
    for row in new:
        proposed[_identity(row)].append(row)
    safe = copy.deepcopy(old)
    held = 0
    for key, candidates in proposed.items():
        originals = prior.get(key)
        if not originals:
            held += len(candidates)
            continue
        if len(originals) != len(candidates):
            held += max(0, len(candidates) - len(originals))
            continue
        if len(originals) > 1:
            # No unique row-to-price mapping: keep original duplicates.
            for candidate in candidates:
                if candidate not in [r for _, r in originals]:
                    held += 1
            continue
        index, previous = originals[0]
        candidate = candidates[0]
        if _no_price_erasure(previous, candidate):
            safe[index] = copy.deepcopy(candidate)
        else:
            held += 1
    enforce_no_unverified_history_admission(old, safe)
    return safe, held

def enforce_lite_no_timed_history(lite_enabled, *, history24h=False, legacy24h=False, pending24h=False, official1h=False):
    """Block legacy 1h/24h inference when Lite is selected.

    This does NOT block the separate event_time-based schedule archive,
    which is always labeled expired_unverified, not completed.
    """
    if lite_enabled and (history24h or legacy24h or pending24h or official1h):
        _hold('LITE_TIME_BASED_HISTORY_FORBIDDEN')
    return True
