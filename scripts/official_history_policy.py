"""Pure, source-only Official1h History reconciliation contract.

Requires a trusted, upstream-authenticated Binance notice classifier and the
existing official_onehour.qualify_confirmation() validation. Does NOT read
Telegram, sign anything, dispatch work, mutate R2, or retire Active.

Do not deploy this module by itself. Attach to the one existing All/History
writer only after the Oracle source, receipt identity and signed provenance
are independently qualified. NO new writer, scheduler, workflow or branch.
"""
from __future__ import annotations

import copy
import re
from collections import Counter
from datetime import datetime, timedelta, timezone

CID = re.compile(r'^ae:[0-9a-f]{24}$')
MAX_ROWS = 20_000
TERMS = ('amount_per_user', 'points_threshold', 'points_cost')
IDENTITY = ('id', 'symbol', 'event_time', 'chain_id', 'contract_address',
            'event_type', 'project_name')

# Preserve exact round identity while tolerating .000Z vs Z timestamps.
ROUND_UTC = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?ZENRICHMENT = frozenset({
    'price_snapshot','market_cap','listing_price','spot_listing_price',
    'ath_since_listing_price','ath_since_listing_date','spot_ath_price',
    'spot_ath_date','spot_listed','futures_listed','alpha_delisted',
    'spot_listed_at','is_spot_listed','listing_price_unavailable',
    '_tge_date_checked','_vwap_daybound_recompute','_multiround_checked',
    '_vwap_daybound_checked',
})
ALLOWED_CHANGE = frozenset(TERMS) | frozenset({
    'status','lifecycle_state','archive_reason','completion_verified',
    'officialConfirmation','official_terms_authority','official_action_verified',
}) | ENRICHMENT


class OfficialHistoryHold(RuntimeError):
    pass


def hold(code):
    raise OfficialHistoryHold('WA_AE_OFFICIAL_HISTORY_HOLD_' + code)


def _rows(rows, name):
    if not isinstance(rows, list) or len(rows) > MAX_ROWS or any(not isinstance(x, dict) for x in rows):
        hold(name + '_INVALID')
    return rows


def _index(rows, name):
    by_id = {}
    for row in _rows(rows, name):
        cid = row.get('id')
        if isinstance(cid, str) and CID.fullmatch(cid):
            if cid in by_id:
                hold(name + '_DUPLICATE_CANONICAL')
            by_id[cid] = row
        elif cid is not None and str(cid).startswith('ae:'):
            hold(name + '_BAD_CANONICAL_ID')
    return by_id


def _identity(previous, received):
    if previous is None:
        return
    for field in IDENTITY:
        a, b = previous.get(field), received.get(field)
        if a in (None, '') or b in (None, ''):
            continue
        if field == 'contract_address':
            a, b = str(a).lower(), str(b).lower()
        if field == 'event_type':
            a, b = str(a).upper(), str(b).upper()
        if field == 'event_time':
            a, b = _round_utc(a), _round_utc(b)
        if a != b:
            hold('IMMUTABLE_IDENTITY_CONFLICT')
    for field in ('firstActiveAt', 'firstLiveAt'):
        a, b = previous.get(field), received.get(field)
        if a not in (None, '') and b not in (None, '') and a != b:
            hold('IMMUTABLE_ACTIVE_CLOCK_CONFLICT')


def _safe_no_price_erasure(before, after):
    for k in ENRICHMENT:
        if before.get(k) is None:
            continue
        if after.get(k) is None:
            hold('ENRICHMENT_ERASURE')
        if isinstance(before[k], (int, float)) and not isinstance(before[k], bool) and before[k] > 0:
            if isinstance(after[k], (int, float)) and not isinstance(after[k], bool) and after[k] == 0:
                hold('ENRICHMENT_ZEROED')
        if isinstance(before[k], dict):
            if not isinstance(after[k], dict):
                hold('ENRICHMENT_TYPE_ERASURE')
            for child, val in before[k].items():
                if val is not None and after[k].get(child) is None:
                    hold('ENRICHMENT_SUBFIELD_ERASURE')
    return True


def _terms(proof, action):
    """Official terms must come from authenticated matching source text.

    For Binance airdrops require all three source-qualified numbers BEFORE
    replacing external fields / representing archived details as official.
    If the notice omits a value, do not silently certify an external guess.
    """
    subject = proof.get('verifiedSubject') or {}
    values = subject.get('officialTerms')
    if action != 'AIRDROP' and values is None:
        return {}
    if not isinstance(values, dict):
        hold('OFFICIAL_TERMS_NOT_NORMALIZED')
    if set(values) - set(TERMS):
        hold('UNEXPECTED_OFFICIAL_TERMS')
    if action == 'AIRDROP' and set(values) != set(TERMS):
        hold('INCOMPLETE_OFFICIAL_AIRDROP_TERMS')
    for key, value in values.items():
        if not isinstance(value, int) or isinstance(value, bool):
            hold('OFFICIAL_TERM_NOT_INTEGER')
        if value < (0 if key == 'points_cost' else 1) or value > 10_000_000_000:
            hold('OFFICIAL_TERM_OUT_OF_BOUNDS')
    return values


def _same_qualified_proof(a, b):
    return a.get('officialConfirmation') == b.get('officialConfirmation')


def reconcile(plan, active_rows, now, qualify_confirmation, *, signed_canonical_rows=None):
    """Upgrade an event ALREADY in History, or admit an exact verified Active event.

    All / History are the only targets. Immutable original receipts remain in
    Supabase. The caller must still run CAS/readback on the existing writer and
    independently retire Active only after a verified pair commit.
    """
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
        hold('CLOCK_NOT_UTC')
    if not isinstance(plan, dict) or plan.get('independent_writer') is not False or plan.get('retire_live') is not False:
        hold('WRITER_OWNERSHIP')
    all_rows = copy.deepcopy(_rows(plan.get('all'), 'ALL'))
    history_rows = copy.deepcopy(_rows(plan.get('history'), 'HISTORY'))
    active_rows = copy.deepcopy(_rows(active_rows, 'ACTIVE'))
    # Optional *trusted upstream output*, not raw user data or external notices.
    # The existing Oracle owner must supply these rows from an authenticated
    # Canonical V2 RPC with its original receipt custody already checked.
    # This permits a verified message to repair an event AFTER Active retired.
    canonical_rows = copy.deepcopy(_rows(
        [] if signed_canonical_rows is None else signed_canonical_rows,
        'SIGNED_CANONICAL'))
    for row in canonical_rows:
        if row.get('officialConfirmation') is None:
            hold('CANONICAL_FEED_UNVERIFIED')
    idx_canonical = _index(canonical_rows, 'SIGNED_CANONICAL')
    idx_all = _index(all_rows, 'ALL')
    idx_hist = _index(history_rows, 'HISTORY')
    idx_active = _index(active_rows, 'ACTIVE')
    # All is the canonical source of price enrichments and existing History.
    for cid, old_hist in idx_hist.items():
        if cid not in idx_all:
            hold('HISTORY_MISSING_FROM_ALL')
        _identity(old_hist, idx_all[cid])
    ids = sorted(set(idx_all) | set(idx_hist) | set(idx_active) | set(idx_canonical))
    qualified = []
    overdue = []
    verified_proofs = {}
    for cid in ids:
        source_rows = [r for idx in (idx_canonical, idx_active, idx_all, idx_hist) if (r := idx.get(cid)) and r.get('officialConfirmation') is not None]
        if not source_rows:
            continue
        # A new signed confirmation can be forwarded by Active/Oracle and is
        # not allowed to silently disagree across read replicas.
        if any(not _same_qualified_proof(source_rows[0], r) for r in source_rows[1:]):
            hold('MULTIPLE_OFFICIAL_PROOFS_CONFLICT')
        source = source_rows[0]
        confirmation = qualify_confirmation(source, now)
        if confirmation is None:
            hold('PROOF_NOT_QUALIFIED')
        if confirmation.get('terminal') is not True:
            # Signed claim-open receipts are valid evidence for term enrichment,
            # never an event-ended History admission.
            continue
        action = confirmation.get('action')
        proof = source['officialConfirmation']
        terms = _terms(proof, action)
        for previous in (idx_all.get(cid), idx_hist.get(cid), idx_active.get(cid), idx_canonical.get(cid)):
            _identity(previous, source)
        result = copy.deepcopy(idx_all.get(cid) or idx_hist.get(cid) or source)
        # Never throw away existing price enrichment when upstream Active is
        # behind the already archived market-data writer.
        for other in (idx_hist.get(cid), idx_active.get(cid), idx_canonical.get(cid)):
            if other is None:
                continue
            for field, value in other.items():
                if field not in result or result[field] is None:
                    result[field] = copy.deepcopy(value)
        result.update(copy.deepcopy(terms))
        result['officialConfirmation'] = copy.deepcopy(proof)
        result['official_terms_authority'] = ({
            'sourceIdentity': proof['binding']['sourceIdentity'],
            'sourceDigest': proof['binding']['sourceDigest'],
            'receiptId': proof['receiptId'],
            'fields': sorted(terms),
        })
        result['status'] = 'ended'
        result['lifecycle_state'] = 'history'
        result['archive_reason'] = 'binance_official_confirmed'
        result['official_action_verified'] = True
        # Verified occurrence/action, NOT a claim that every user redeemed.
        result['completion_verified'] = True
        for previous in (idx_all.get(cid), idx_hist.get(cid)):
            if previous is not None:
                _safe_no_price_erasure(previous, result)
        idx_all[cid] = copy.deepcopy(result)
        idx_hist[cid] = copy.deepcopy(result)
        qualified.append(cid)
        if now > confirmation['deadline']:
            overdue.append(cid)
    # Rebuild by replacing in-place and only appending newly-qualified events.
    old_all = {r['id']: i for i, r in enumerate(all_rows) if r.get('id') in idx_all}
    old_hist = {r['id']: i for i, r in enumerate(history_rows) if r.get('id') in idx_hist}
    for cid in qualified:
        for rows, pos, index in ((all_rows, old_all, idx_all), (history_rows, old_hist, idx_hist)):
            if cid in pos:
                rows[pos[cid]] = index[cid]
            else:
                rows.append(index[cid])
    _rows(all_rows, 'ALL_OUT')
    _rows(history_rows, 'HISTORY_OUT')
    return {**plan, 'all': all_rows, 'history': history_rows,
            'official_promoted': sorted(qualified),
            'official_overdue': sorted(overdue),
            'official_qualified_proofs': {cid: idx_all[cid]['officialConfirmation'] for cid in qualified},
            'retire_live': False, 'retire_pending': False,
            'retire_public_active': False, 'independent_writer': False}


def enforce_authenticated_history_guard(previous, proposed, qualified_proofs, qualify_confirmation, now, existing_legacy_guard):
    """Allow only authenticated official changes; keep previous guard for all others.

    The caller must pass proofs directly from reconcile(), never a user-controlled
    manifest, and call this before the existing pair CAS.
    """
    _rows(previous, 'HISTORY_PREVIOUS')
    _rows(proposed, 'HISTORY_PROPOSED')
    old_by = _index(previous, 'HISTORY_OLD')
    new_by = _index(proposed, 'HISTORY_NEW')
    if not isinstance(qualified_proofs, dict):
        hold('QUALIFIED_PROOFS_INVALID')
    for cid, proof in qualified_proofs.items():
        candidate = new_by.get(cid)
        if candidate is None or candidate.get('officialConfirmation') != proof:
            hold('QUALIFIED_PROOF_NOT_IN_FINAL_HISTORY')
        qualified = qualify_confirmation(candidate, now)
        if qualified is None or qualified.get('terminal') is not True:
            hold('QUALIFIED_PROOF_RECHECK_FAILED')
    # Nonqualified rows must still pass the original unverified-history guard,
    # even when they were already in History with no canonical ID.
    before_ordinary = [r for r in previous if r.get('id') not in qualified_proofs]
    after_ordinary = [r for r in proposed if r.get('id') not in qualified_proofs]
    existing_legacy_guard(before_ordinary, after_ordinary)
    for cid in qualified_proofs:
        newer = new_by[cid]
        older = old_by.get(cid)
        if older is not None:
            _identity(older, newer)
            changed = {k for k in set(older) | set(newer) if older.get(k) != newer.get(k)}
            if changed - ALLOWED_CHANGE:
                hold('OFFICIAL_CHANGE_OUTSIDE_ALLOWLIST')
            _safe_no_price_erasure(older, newer)
        if newer.get('status') != 'ended' or newer.get('lifecycle_state') != 'history':
            hold('OFFICIAL_HISTORY_STATUS')
        actual_terms = _terms(qualified_proofs[cid], newer['officialConfirmation']['verifiedSubject']['confirmedAction'])
        if any(newer.get(k) != v for k, v in actual_terms.items()):
            hold('OFFICIAL_TERMS_CHANGED_AFTER_PROOF')
    # Caller ensures no post-guard nonqualifying R2 mutations before CAS.
    return len(qualified_proofs)


def reconcile_authenticated(plan, active_rows, now, qualify_confirmation, *, durable_canonical_rows):
    """Production entrypoint: rejects Active/R2-only official authority.

    durable_canonical_rows MUST originate from an authenticated post-CAS
    Canonical V2 readback, never from public R2 or a user manifest.
    """
    canonical = _rows(durable_canonical_rows, 'DURABLE_CANONICAL')
    canonical_index = _index(canonical, 'DURABLE_CANONICAL')
    for name, rows in (('ALL',plan.get('all')),('HISTORY',plan.get('history')),('ACTIVE',active_rows)):
        for item in _rows(rows, name):
            proof = item.get('officialConfirmation')
            if proof is None: continue
            cid = item.get('id')
            authority = canonical_index.get(cid)
            if authority is None or authority.get('officialConfirmation') != proof:
                hold('DURABLE_CANONICAL_REQUIRED')
    return reconcile(plan,active_rows,now,qualify_confirmation,
                     signed_canonical_rows=canonical)
)


def _round_utc(value):
    if not isinstance(value, str) or not ROUND_UTC.fullmatch(value):
        hold('ROUND_NOT_CANONICAL_UTC')
    try:
        dt = datetime.fromisoformat(value[:-1] + '+00:00')
    except (ValueError, OverflowError):
        hold('ROUND_INVALID')
    if dt.utcoffset() != timedelta(0):
        hold('ROUND_NON_UTC')
    return dt

ENRICHMENT = frozenset({
    'price_snapshot','market_cap','listing_price','spot_listing_price',
    'ath_since_listing_price','ath_since_listing_date','spot_ath_price',
    'spot_ath_date','spot_listed','futures_listed','alpha_delisted',
    'spot_listed_at','is_spot_listed','listing_price_unavailable',
    '_tge_date_checked','_vwap_daybound_recompute','_multiround_checked',
    '_vwap_daybound_checked',
})
ALLOWED_CHANGE = frozenset(TERMS) | frozenset({
    'status','lifecycle_state','archive_reason','completion_verified',
    'officialConfirmation','official_terms_authority','official_action_verified',
}) | ENRICHMENT


class OfficialHistoryHold(RuntimeError):
    pass


def hold(code):
    raise OfficialHistoryHold('WA_AE_OFFICIAL_HISTORY_HOLD_' + code)


def _rows(rows, name):
    if not isinstance(rows, list) or len(rows) > MAX_ROWS or any(not isinstance(x, dict) for x in rows):
        hold(name + '_INVALID')
    return rows


def _index(rows, name):
    by_id = {}
    for row in _rows(rows, name):
        cid = row.get('id')
        if isinstance(cid, str) and CID.fullmatch(cid):
            if cid in by_id:
                hold(name + '_DUPLICATE_CANONICAL')
            by_id[cid] = row
        elif cid is not None and str(cid).startswith('ae:'):
            hold(name + '_BAD_CANONICAL_ID')
    return by_id


def _identity(previous, received):
    if previous is None:
        return
    for field in IDENTITY:
        a, b = previous.get(field), received.get(field)
        if a in (None, '') or b in (None, ''):
            continue
        if field == 'contract_address':
            a, b = str(a).lower(), str(b).lower()
        if field == 'event_type':
            a, b = str(a).upper(), str(b).upper()
        if a != b:
            hold('IMMUTABLE_IDENTITY_CONFLICT')
    for field in ('firstActiveAt', 'firstLiveAt'):
        a, b = previous.get(field), received.get(field)
        if a not in (None, '') and b not in (None, '') and a != b:
            hold('IMMUTABLE_ACTIVE_CLOCK_CONFLICT')


def _safe_no_price_erasure(before, after):
    for k in ENRICHMENT:
        if before.get(k) is None:
            continue
        if after.get(k) is None:
            hold('ENRICHMENT_ERASURE')
        if isinstance(before[k], (int, float)) and not isinstance(before[k], bool) and before[k] > 0:
            if isinstance(after[k], (int, float)) and not isinstance(after[k], bool) and after[k] == 0:
                hold('ENRICHMENT_ZEROED')
        if isinstance(before[k], dict):
            if not isinstance(after[k], dict):
                hold('ENRICHMENT_TYPE_ERASURE')
            for child, val in before[k].items():
                if val is not None and after[k].get(child) is None:
                    hold('ENRICHMENT_SUBFIELD_ERASURE')
    return True


def _terms(proof, action):
    """Official terms must come from authenticated matching source text.

    For Binance airdrops require all three source-qualified numbers BEFORE
    replacing external fields / representing archived details as official.
    If the notice omits a value, do not silently certify an external guess.
    """
    subject = proof.get('verifiedSubject') or {}
    values = subject.get('officialTerms')
    if action != 'AIRDROP' and values is None:
        return {}
    if not isinstance(values, dict):
        hold('OFFICIAL_TERMS_NOT_NORMALIZED')
    if set(values) - set(TERMS):
        hold('UNEXPECTED_OFFICIAL_TERMS')
    if action == 'AIRDROP' and set(values) != set(TERMS):
        hold('INCOMPLETE_OFFICIAL_AIRDROP_TERMS')
    for key, value in values.items():
        if not isinstance(value, int) or isinstance(value, bool):
            hold('OFFICIAL_TERM_NOT_INTEGER')
        if value < (0 if key == 'points_cost' else 1) or value > 10_000_000_000:
            hold('OFFICIAL_TERM_OUT_OF_BOUNDS')
    return values


def _same_qualified_proof(a, b):
    return a.get('officialConfirmation') == b.get('officialConfirmation')


def reconcile(plan, active_rows, now, qualify_confirmation, *, signed_canonical_rows=None):
    """Upgrade an event ALREADY in History, or admit an exact verified Active event.

    All / History are the only targets. Immutable original receipts remain in
    Supabase. The caller must still run CAS/readback on the existing writer and
    independently retire Active only after a verified pair commit.
    """
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
        hold('CLOCK_NOT_UTC')
    if not isinstance(plan, dict) or plan.get('independent_writer') is not False or plan.get('retire_live') is not False:
        hold('WRITER_OWNERSHIP')
    all_rows = copy.deepcopy(_rows(plan.get('all'), 'ALL'))
    history_rows = copy.deepcopy(_rows(plan.get('history'), 'HISTORY'))
    active_rows = copy.deepcopy(_rows(active_rows, 'ACTIVE'))
    # Optional *trusted upstream output*, not raw user data or external notices.
    # The existing Oracle owner must supply these rows from an authenticated
    # Canonical V2 RPC with its original receipt custody already checked.
    # This permits a verified message to repair an event AFTER Active retired.
    canonical_rows = copy.deepcopy(_rows(
        [] if signed_canonical_rows is None else signed_canonical_rows,
        'SIGNED_CANONICAL'))
    for row in canonical_rows:
        if row.get('officialConfirmation') is None:
            hold('CANONICAL_FEED_UNVERIFIED')
    idx_canonical = _index(canonical_rows, 'SIGNED_CANONICAL')
    idx_all = _index(all_rows, 'ALL')
    idx_hist = _index(history_rows, 'HISTORY')
    idx_active = _index(active_rows, 'ACTIVE')
    # All is the canonical source of price enrichments and existing History.
    for cid, old_hist in idx_hist.items():
        if cid not in idx_all:
            hold('HISTORY_MISSING_FROM_ALL')
        _identity(old_hist, idx_all[cid])
    ids = sorted(set(idx_all) | set(idx_hist) | set(idx_active) | set(idx_canonical))
    qualified = []
    overdue = []
    verified_proofs = {}
    for cid in ids:
        source_rows = [r for idx in (idx_canonical, idx_active, idx_all, idx_hist) if (r := idx.get(cid)) and r.get('officialConfirmation') is not None]
        if not source_rows:
            continue
        # A new signed confirmation can be forwarded by Active/Oracle and is
        # not allowed to silently disagree across read replicas.
        if any(not _same_qualified_proof(source_rows[0], r) for r in source_rows[1:]):
            hold('MULTIPLE_OFFICIAL_PROOFS_CONFLICT')
        source = source_rows[0]
        confirmation = qualify_confirmation(source, now)
        if confirmation is None:
            hold('PROOF_NOT_QUALIFIED')
        action = confirmation.get('action')
        proof = source['officialConfirmation']
        terms = _terms(proof, action)
        for previous in (idx_all.get(cid), idx_hist.get(cid), idx_active.get(cid), idx_canonical.get(cid)):
            _identity(previous, source)
        result = copy.deepcopy(idx_all.get(cid) or idx_hist.get(cid) or source)
        # Never throw away existing price enrichment when upstream Active is
        # behind the already archived market-data writer.
        for other in (idx_hist.get(cid), idx_active.get(cid), idx_canonical.get(cid)):
            if other is None:
                continue
            for field, value in other.items():
                if field not in result or result[field] is None:
                    result[field] = copy.deepcopy(value)
        result.update(copy.deepcopy(terms))
        result['officialConfirmation'] = copy.deepcopy(proof)
        result['official_terms_authority'] = ({
            'sourceIdentity': proof['binding']['sourceIdentity'],
            'sourceDigest': proof['binding']['sourceDigest'],
            'receiptId': proof['receiptId'],
            'fields': sorted(terms),
        })
        result['status'] = 'ended'
        result['lifecycle_state'] = 'history'
        result['archive_reason'] = 'binance_official_confirmed'
        result['official_action_verified'] = True
        # Verified occurrence/action, NOT a claim that every user redeemed.
        result['completion_verified'] = True
        for previous in (idx_all.get(cid), idx_hist.get(cid)):
            if previous is not None:
                _safe_no_price_erasure(previous, result)
        idx_all[cid] = copy.deepcopy(result)
        idx_hist[cid] = copy.deepcopy(result)
        qualified.append(cid)
        if now > confirmation['deadline']:
            overdue.append(cid)
    # Rebuild by replacing in-place and only appending newly-qualified events.
    old_all = {r['id']: i for i, r in enumerate(all_rows) if r.get('id') in idx_all}
    old_hist = {r['id']: i for i, r in enumerate(history_rows) if r.get('id') in idx_hist}
    for cid in qualified:
        for rows, pos, index in ((all_rows, old_all, idx_all), (history_rows, old_hist, idx_hist)):
            if cid in pos:
                rows[pos[cid]] = index[cid]
            else:
                rows.append(index[cid])
    _rows(all_rows, 'ALL_OUT')
    _rows(history_rows, 'HISTORY_OUT')
    return {**plan, 'all': all_rows, 'history': history_rows,
            'official_promoted': sorted(qualified),
            'official_overdue': sorted(overdue),
            'official_qualified_proofs': {cid: idx_all[cid]['officialConfirmation'] for cid in qualified},
            'retire_live': False, 'retire_pending': False,
            'retire_public_active': False, 'independent_writer': False}


def enforce_authenticated_history_guard(previous, proposed, qualified_proofs, qualify_confirmation, now, existing_legacy_guard):
    """Allow only authenticated official changes; keep previous guard for all others.

    The caller must pass proofs directly from reconcile(), never a user-controlled
    manifest, and call this before the existing pair CAS.
    """
    _rows(previous, 'HISTORY_PREVIOUS')
    _rows(proposed, 'HISTORY_PROPOSED')
    old_by = _index(previous, 'HISTORY_OLD')
    new_by = _index(proposed, 'HISTORY_NEW')
    if not isinstance(qualified_proofs, dict):
        hold('QUALIFIED_PROOFS_INVALID')
    for cid, proof in qualified_proofs.items():
        candidate = new_by.get(cid)
        if candidate is None or candidate.get('officialConfirmation') != proof:
            hold('QUALIFIED_PROOF_NOT_IN_FINAL_HISTORY')
        qualified = qualify_confirmation(candidate, now)
        if qualified is None:
            hold('QUALIFIED_PROOF_RECHECK_FAILED')
    # Nonqualified rows must still pass the original unverified-history guard,
    # even when they were already in History with no canonical ID.
    before_ordinary = [r for r in previous if r.get('id') not in qualified_proofs]
    after_ordinary = [r for r in proposed if r.get('id') not in qualified_proofs]
    existing_legacy_guard(before_ordinary, after_ordinary)
    for cid in qualified_proofs:
        newer = new_by[cid]
        older = old_by.get(cid)
        if older is not None:
            _identity(older, newer)
            changed = {k for k in set(older) | set(newer) if older.get(k) != newer.get(k)}
            if changed - ALLOWED_CHANGE:
                hold('OFFICIAL_CHANGE_OUTSIDE_ALLOWLIST')
            _safe_no_price_erasure(older, newer)
        if newer.get('status') != 'ended' or newer.get('lifecycle_state') != 'history':
            hold('OFFICIAL_HISTORY_STATUS')
        actual_terms = _terms(qualified_proofs[cid], newer['officialConfirmation']['verifiedSubject']['confirmedAction'])
        if any(newer.get(k) != v for k, v in actual_terms.items()):
            hold('OFFICIAL_TERMS_CHANGED_AFTER_PROOF')
    # Caller ensures no post-guard nonqualifying R2 mutations before CAS.
    return len(qualified_proofs)


def reconcile_authenticated(plan, active_rows, now, qualify_confirmation, *, durable_canonical_rows):
    """Production entrypoint: rejects Active/R2-only official authority.

    durable_canonical_rows MUST originate from an authenticated post-CAS
    Canonical V2 readback, never from public R2 or a user manifest.
    """
    canonical = _rows(durable_canonical_rows, 'DURABLE_CANONICAL')
    canonical_index = _index(canonical, 'DURABLE_CANONICAL')
    for name, rows in (('ALL',plan.get('all')),('HISTORY',plan.get('history')),('ACTIVE',active_rows)):
        for item in _rows(rows, name):
            proof = item.get('officialConfirmation')
            if proof is None: continue
            cid = item.get('id')
            authority = canonical_index.get(cid)
            if authority is None or authority.get('officialConfirmation') != proof:
                hold('DURABLE_CANONICAL_REQUIRED')
    return reconcile(plan,active_rows,now,qualify_confirmation,
                     signed_canonical_rows=canonical)
