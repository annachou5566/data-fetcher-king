"""Reject NEW canonical Alpha Events History rows without trusted completion.

Called by the EXISTING price/History writer immediately before either its
normal R2 upload or CAS pair. Scheduled timestamps, ended status, completed
flags, and elapsed 1h/24h are never proof. Legacy rows (including ID-less
records) are outside this admission guard and retain their existing owner.
No new writer, timer, network request or signing secret is introduced.
"""
import re

_CANONICAL_ID = re.compile(r'^ae:[0-9a-f]{24}$')

class CompletionOnlyHold(RuntimeError):
    pass

def _canonical_ids(rows, name):
    if not isinstance(rows, list):
        raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_'+name+'_NOT_LIST')
    ids=set()
    for row in rows:
        if not isinstance(row,dict):
            raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_'+name+'_ROW_INVALID')
        cid=row.get('id')
        if isinstance(cid,str) and _CANONICAL_ID.fullmatch(cid):
            if cid in ids:
                raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_'+name+'_DUPLICATE_ID')
            ids.add(cid)
    return ids

def enforce_no_unverified_history_admission(previous_history, proposed_history):
    """Fail before R2 writes if a new canonical identity enters History.

    A future signed source-action + existing-writer CAS/ACK flow must replace
    this gate. Self-declared row fields never authorize history admission.
    """
    before=_canonical_ids(previous_history,'PREVIOUS_HISTORY')
    after=_canonical_ids(proposed_history,'PROPOSED_HISTORY')
    if after-before:
        raise CompletionOnlyHold('WA_AE_COMPLETION_ONLY_HOLD_NEW_CANONICAL_HISTORY_UNVERIFIED')
    return len(after)
