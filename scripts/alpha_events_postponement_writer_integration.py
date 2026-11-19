"""R24 source-only integration boundary for the EXISTING Alpha Events History writer.

No client creation, credentials, network, env flags, cron, or production entrypoint.
The existing owner must provide authenticated callbacks after an independent
release approval. Calling without `compare_and_swap` only returns a read-only
plan. All tests inject entirely in-memory callbacks; no real R2/DB access.

Publish acknowledgement is NOT a cryptographic attestation by itself.
`authenticated_publisher_receipt` must be implemented by a separately
qualified, authorized owner interface. A plain environment JSON flag or a
caller-supplied PASS object MUST NOT be wired to that callback.
"""
import copy
from alpha_events_postponement_reconcile import (
    ALL_KEY, HISTORY_KEY, ACTIVE_KEYS, _snapshot, _sha,
    PostponementHold, hold, plan_postponement_retraction, reconcile_unknown_ack,
)

PUBLIC_KEY = 'alpha-events/public/active.json'
READ_KEYS = ACTIVE_KEYS + (PUBLIC_KEY, ALL_KEY, HISTORY_KEY)


def _read_every(read_snapshot):
    if not callable(read_snapshot): hold('INDEPENDENT_R2_READBACK_REQUIRED')
    result = {}
    for key in READ_KEYS:
        obj = read_snapshot(key)
        rows = _snapshot(obj, key)
        result[key] = {'key': key, 'etag': obj['etag'], 'rows': rows}
    return result


def _source_inputs(target_id, read_authenticated_canonical, authenticated_publisher_receipt):
    if not callable(read_authenticated_canonical): hold('AUTHENTICATED_CANONICAL_READER_REQUIRED')
    if not callable(authenticated_publisher_receipt): hold('QUALIFIED_PUBLISHER_RECEIPT_READER_REQUIRED')
    # These callbacks must authenticate the *source*, not merely parse a JSON
    # payload. Planner independently cross-checks identity, revision and refs.
    canonical = read_authenticated_canonical(target_id)
    receipt = authenticated_publisher_receipt(target_id)
    return canonical, receipt


def plan_one_stage(*, target_id, read_snapshot, read_authenticated_canonical,
                   authenticated_publisher_receipt):
    """One deterministic, read-only stage with exact source preimages."""
    canonical, publisher = _source_inputs(
        target_id, read_authenticated_canonical, authenticated_publisher_receipt)
    snapshots = _read_every(read_snapshot)
    active = [snapshots[k] for k in ACTIVE_KEYS]
    planned = plan_postponement_retraction(
        all_snapshot=snapshots[ALL_KEY],
        history_snapshot=snapshots[HISTORY_KEY], active_versions=active,
        public_rows=snapshots[PUBLIC_KEY]['rows'], publish_ack=publisher,
        target_id=target_id, canonical_row=canonical)
    return planned, snapshots, canonical, publisher


def execute_one_stage(*, target_id, read_snapshot, read_authenticated_canonical,
                      authenticated_publisher_receipt, compare_and_swap=None):
    """One CAS at most; never auto-retry and never cross the stage boundary.

    `compare_and_swap` is supplied ONLY by the existing single History writer,
    when separately authorized. This module contains no real storage adapter.
    """
    stage, pinned, canonical, publisher = plan_one_stage(
        target_id=target_id, read_snapshot=read_snapshot,
        read_authenticated_canonical=read_authenticated_canonical,
        authenticated_publisher_receipt=authenticated_publisher_receipt)
    op = stage.get('operation')
    if op is None:
        return {'stage': 'COMPLETE', 'changed': False}
    if compare_and_swap is None:
        return {'stage': stage['stage'], 'changed': False, 'operation': copy.deepcopy(op)}
    if not callable(compare_and_swap): hold('CAS_WRITER_INVALID')

    # Never write when an authenticated canonical/publisher version moved,
    # or when any Active/History/All key moved since the first read.
    again_canonical, again_publisher = _source_inputs(
        target_id, read_authenticated_canonical, authenticated_publisher_receipt)
    if again_canonical != canonical or again_publisher != publisher:
        hold('AUTHENTICATED_SOURCE_CHANGED_BEFORE_CAS')
    before = _read_every(read_snapshot)
    for key in READ_KEYS:
        if before[key] != pinned[key]: hold('R2_PREIMAGE_MOVED_BEFORE_CAS')

    key = op['key']
    try:
        # Compare-and-swap must enforce IfMatch=op.expectedEtag in the trusted
        # History writer; no unconditional PUT is allowed at this boundary.
        compare_and_swap(op)
    except Exception:
        # ACK uncertainty: never retry blindly. Re-read exact object and
        # classify changed / unchanged / unexpected. Do not leak exception text.
        observed = read_snapshot(key)
        outcome = reconcile_unknown_ack(op, observed, pinned[key])
        if outcome == 'COMMITTED_READBACK':
            if observed.get('etag') == pinned[key]['etag']:
                hold('UNKNOWN_ACK_NO_NEW_VERSION')
            changed = True
        else:
            return {'stage': stage['stage'], 'changed': False,
                    'status': 'HOLD_NOT_COMMITTED_RECHECK_CAS'}
    else:
        observed = read_snapshot(key)
        if _snapshot(observed, key) != op['rows'] or _sha(observed['rows']) != op['rowsSha256']:
            hold('CAS_POSTWRITE_READBACK_MISMATCH')
        if observed.get('etag') == pinned[key]['etag']:
            hold('CAS_POSTWRITE_ETAG_UNCHANGED')
        changed = True

    after = _read_every(read_snapshot)
    for other in READ_KEYS:
        if other == key:
            if after[other]['rows'] != op['rows'] or _sha(after[other]['rows']) != op['rowsSha256']:
                hold('CAS_POSTWRITE_DRIFT')
        elif after[other] != pinned[other]:
            hold('OTHER_GENERATION_CHANGED_AFTER_CAS')
    end_canonical, end_publisher = _source_inputs(
        target_id, read_authenticated_canonical, authenticated_publisher_receipt)
    if end_canonical != canonical or end_publisher != publisher:
        hold('AUTHENTICATED_SOURCE_CHANGED_AFTER_CAS')
    return {'stage': stage['stage'], 'changed': changed,
            'status': 'ONE_CAS_VERIFIED', 'rowsSha256': op['rowsSha256']}
