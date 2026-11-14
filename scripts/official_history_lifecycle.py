"""SOURCE-ONLY local candidate: nonterminal Binance claim-open reconciliation.

Only the existing All/History writer may apply the returned plan via existing
ETag CAS/readback. No R2/network access, no new writer or external proof mint.
A precondition is an authenticated Oracle Canonical readback/handoff; rows
coming from public R2 are NOT equivalent. Production call-site not wired.
"""
from __future__ import annotations
import copy
import re
from datetime import timedelta, datetime, timezone
from official_history_policy import (
    _rows, _index, _identity, _terms, _safe_no_price_erasure,
    hold, ENRICHMENT, TERMS,
)

# Only terminal result messages (not a RAVE-style claim-open notice) may
# separately promote History. Terminal action classifier is not yet qualified.
CLAIM_OPEN = 'CLAIM_OPEN'
ALLOWED_OPEN_FIELDS = frozenset(TERMS) | frozenset({
    'officialConfirmation', 'official_terms_authority', 'official_action_verified',
}) | ENRICHMENT

UTC_INSTANT = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?Z\Z')

def _utc_instant(value):
    """Exact UTC instant equality; normalize only seconds vs .000 seconds."""
    if not isinstance(value, str) or not UTC_INSTANT.fullmatch(value):
        return None
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return dt if dt.utcoffset() == timedelta(0) else None
    except (ValueError, OverflowError):
        return None

def _exact_identity(previous, received):
    if previous is None:
        return
    # The existing writer and Node Canonical can serialize the *same* instant
    # as `...:00Z` vs `...:00.000Z`; never loosen the actual round match.
    a, b = previous.get('event_time'), received.get('event_time')
    if a != b and _utc_instant(a) is not None and _utc_instant(a) == _utc_instant(b):
        received = dict(received, event_time=a)
    _identity(previous, received)


def reconcile_authenticated_claim_open(plan, active_rows, now, qualify_confirmation, *, durable_canonical_rows):
    if (not isinstance(now, datetime) or now.tzinfo is None or
            now.utcoffset() != timedelta(0)):
        hold('NOW_NOT_UTC')
    if (not isinstance(plan, dict) or plan.get('independent_writer') is not False
            or plan.get('retire_live') is not False):
        hold('SINGLE_WRITER_REQUIRED')
    before_all = copy.deepcopy(_rows(plan.get('all'), 'ALL'))
    before_history = copy.deepcopy(_rows(plan.get('history'), 'HISTORY'))
    active = copy.deepcopy(_rows(active_rows, 'ACTIVE'))
    canon = copy.deepcopy(_rows(durable_canonical_rows, 'AUTHENTICATED_CANONICAL'))
    by_all, by_hist, by_active, by_canon = (
        _index(before_all, 'ALL'), _index(before_history, 'HISTORY'),
        _index(active, 'ACTIVE'), _index(canon, 'AUTHENTICATED_CANONICAL'),
    )
    out_all, out_hist = copy.deepcopy(before_all), copy.deepcopy(before_history)
    positions_all = {row['id']: i for i, row in enumerate(out_all) if row.get('id') in by_all}
    positions_hist = {row['id']: i for i, row in enumerate(out_hist) if row.get('id') in by_hist}
    enriched = []
    for cid, row in by_canon.items():
        proof = row.get('officialConfirmation')
        if not isinstance(proof, dict):
            hold('DURABLE_PROOF_REQUIRED')
        subject = proof.get('verifiedSubject') or {}
        if (subject.get('confirmedAction') != 'AIRDROP' or
                subject.get('lifecycleAction') != CLAIM_OPEN or
                subject.get('noticeStage') != 'CONFIRMED' or
                subject.get('preAnnouncement') is not False):
            hold('NOT_EXACT_CLAIM_OPEN')
        if cid not in by_all:
            # No new All/History admission without the existing event anchor.
            hold('CANONICAL_WITHOUT_EXISTING_ALL')
        if row.get('id') != subject.get('canonicalId'):
            hold('CANONICAL_SUBJECT_ID')
        qualified = qualify_confirmation(row, now)
        if qualified is None or qualified.get('action') != 'AIRDROP':
            hold('BINANCE_QUALIFIER_FAILED')
        terms = _terms(proof, 'AIRDROP')
        for prior in (by_all.get(cid), by_hist.get(cid), by_active.get(cid)):
            _exact_identity(prior, row)
            if prior is not None and prior.get('officialConfirmation') not in (None, proof):
                hold('SOURCE_PROOF_CONFLICT')
        old_all = by_all[cid]
        result = copy.deepcopy(old_all)
        result.update(copy.deepcopy(terms))
        result['officialConfirmation'] = copy.deepcopy(proof)
        result['official_action_verified'] = True
        result['official_terms_authority'] = {
            'sourceIdentity': proof['binding']['sourceIdentity'],
            'sourceDigest': proof['binding']['sourceDigest'],
            'receiptId': proof['receiptId'], 'fields': sorted(terms),
        }
        # Preserve status/clock/archive_reason/completion_verified exactly.
        # Claim OPEN does not prove event ENDED or user CLAIMED.
        _safe_no_price_erasure(old_all, result)
        out_all[positions_all[cid]] = result
        if cid in by_hist:
            prior_hist = by_hist[cid]
            if prior_hist.get('completion_verified') is True:
                hold('EXISTING_COMPLETION_MUST_NOT_BE_DOWNGRADED')
            merged = copy.deepcopy(prior_hist)
            merged.update(copy.deepcopy(terms))
            for field in ('officialConfirmation', 'official_action_verified', 'official_terms_authority'):
                merged[field] = copy.deepcopy(result[field])
            _safe_no_price_erasure(prior_hist, merged)
            out_hist[positions_hist[cid]] = merged
        enriched.append(cid)
    # Existing History cannot lose any row; no new rows may be admitted.
    if len(out_hist) != len(before_history):
        hold('HISTORY_LENGTH_CHANGED')
    for previous, current in zip(before_history, out_hist):
        _exact_identity(previous, current)
        changes = {k for k in set(previous)|set(current) if previous.get(k)!=current.get(k)}
        if changes - ALLOWED_OPEN_FIELDS:
            hold('HISTORY_NONTERMINAL_MUTATION')
        _safe_no_price_erasure(previous, current)
    # All source changes must be exactly within authenticated row allowlist.
    if len(out_all) != len(before_all):
        hold('ALL_LENGTH_CHANGED')
    for previous, current in zip(before_all, out_all):
        _exact_identity(previous, current)
        changes = {k for k in set(previous)|set(current) if previous.get(k)!=current.get(k)}
        if changes - ALLOWED_OPEN_FIELDS:
            hold('ALL_NONTERMINAL_MUTATION')
        _safe_no_price_erasure(previous, current)
    return {**plan, 'all': out_all, 'history': out_hist,
            'official_claim_open_enriched': sorted(enriched),
            'official_promoted': [], 'retire_live': False,
            'retire_pending': False, 'retire_public_active': False,
            'independent_writer': False}
