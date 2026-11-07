"""Alpha Events Lite History admission fence for the EXISTING price writer.

No timer, status=ended, or row-supplied proof may create a new canonical
History entry. Normal price-enrichment runs omit unverified additions while
continuing to update existing History. Special CAS/cutover runs HOLD.
"""
import re

_CANONICAL_ID = re.compile(r'^ae:[0-9a-f]{24}$')


class CompletionOnlyHold(RuntimeError):
    pass


def _canonical_ids(rows, name):
    if not isinstance(rows, list):
        raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_' + name + '_NOT_LIST')
    ids = set()
    for row in rows:
        if not isinstance(row, dict):
            raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_' + name + '_ROW_INVALID')
        cid = row.get('id')
        if isinstance(cid, str) and _CANONICAL_ID.fullmatch(cid):
            if cid in ids:
                raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_' + name + '_DUPLICATE_ID')
            ids.add(cid)
    return ids


def enforce_no_unverified_history_admission(previous_history, proposed_history):
    """Before conditional All/History CAS: abort on new canonical IDs."""
    before = _canonical_ids(previous_history, 'PREVIOUS_HISTORY')
    after = _canonical_ids(proposed_history, 'PROPOSED_HISTORY')
    if after - before:
        raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_NEW_CANONICAL_HISTORY_UNVERIFIED')
    if before - after:
        raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_ARCHIVED_CANONICAL_LOST')
    if len(proposed_history) < len(previous_history):
        raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_HISTORY_SHRANK')
    return len(after)


def keep_existing_history_without_unverified_admission(previous_history, proposed_history):
    """Normal enrichment: skip ONLY unverified new canonical rows, keep prices.

    No input for self-declared completed/proof/ended is accepted. The existing
    price writer retains ownership of both R2 keys.
    """
    before = _canonical_ids(previous_history, 'PREVIOUS_HISTORY')
    _canonical_ids(proposed_history, 'PROPOSED_HISTORY')
    safe = []
    held = 0
    for row in proposed_history:
        cid = row.get('id')
        if isinstance(cid, str) and _CANONICAL_ID.fullmatch(cid) and cid not in before:
            held += 1
        else:
            safe.append(row)
    enforce_no_unverified_history_admission(previous_history, safe)
    return safe, held
