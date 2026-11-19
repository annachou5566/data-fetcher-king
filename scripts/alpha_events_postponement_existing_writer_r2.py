"""R25: R2 primitives for the EXISTING, sole All/History writer.

This is an inert adapter: constructing it never opens a connection. No
credential, service, scheduler, flag or production entrypoint is supplied.
The caller must separately provide authenticated canonical + verified
publisher proof. The adapter CANNOT establish that proof.
"""
import json
import re
from alpha_events_postponement_reconcile import (
    ACTIVE_KEYS, ALL_KEY, HISTORY_KEY, _sha, _rows, hold,
)

PUBLIC_KEY = 'alpha-events/public/active.json'
READABLE = frozenset((*ACTIVE_KEYS, PUBLIC_KEY, ALL_KEY, HISTORY_KEY))
WRITABLE = frozenset((ALL_KEY, HISTORY_KEY))
ETAG = re.compile(r'"[^"\r\n]{1,180}"\Z')
MAX_BYTES = 2_000_000  # pinned to existing history24h_cutover contract


def bind_existing_writer_r2(*, r2, bucket, snapshot_fn=None):
    """Return read_snapshot and IfMatch-only CAS callbacks; no eager I/O.

    In live source, snapshot_fn is the existing history24h_cutover.snapshot.
    Tests may inject an in-memory implementation with the same signature.
    """
    if r2 is None or not isinstance(bucket, str) or not bucket.strip():
        hold('EXISTING_OWNER_CLIENT_REQUIRED')
    if snapshot_fn is None:
        from history24h_cutover import snapshot as snapshot_fn
    if not callable(snapshot_fn):
        hold('EXISTING_OWNER_SNAPSHOT_REQUIRED')

    def read_snapshot(key):
        if key not in READABLE:
            hold('R2_UNRECOGNIZED_READ_KEY')
        rows, etag = snapshot_fn(r2, bucket, key)
        if not isinstance(etag, str) or not ETAG.fullmatch(etag):
            hold('R2_ETAG_INVALID')
        return {'key': key, 'rows': _rows(rows, key), 'etag': etag}

    def compare_and_swap(operation):
        if not isinstance(operation, dict) or operation.get('key') not in WRITABLE:
            hold('R2_WRITE_KEY_NOT_ALLOWED')
        key = operation['key']
        etag = operation.get('expectedEtag')
        if not isinstance(etag, str) or not ETAG.fullmatch(etag):
            hold('R2_EXPECTED_ETAG_INVALID')
        rows = _rows(operation.get('rows'), key)
        if not isinstance(operation.get('rowsSha256'), str) or operation['rowsSha256'] != _sha(rows):
            hold('R2_CAS_PAYLOAD_HASH_MISMATCH')
        try:
            body = json.dumps(rows, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
        except (ValueError, TypeError, OverflowError):
            hold('R2_CAS_PAYLOAD_INVALID')
        if len(body) > MAX_BYTES:
            hold('R2_CAS_BYTES_LIMIT')
        # S3's conditional write is mandatory. No plain PutObject or retry.
        # The integration caller owns independent readback and stage fencing.
        return r2.put_object(Bucket=bucket, Key=key, Body=body, IfMatch=etag,
                             ContentType='application/json', CacheControl='public, max-age=60')

    return read_snapshot, compare_and_swap
