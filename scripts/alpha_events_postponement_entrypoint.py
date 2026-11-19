"""Backend-only PREDICT postponement gate in EXISTING price writer.

Always OFF at production callsite, no secret/client/scheduler. Authenticated
callbacks must be provided from separately qualified owner code.
"""
def _hold(code):
    raise RuntimeError('WA_AE_POSTPONE_WRITER_HOLD_'+code)


def run_existing_writer_postponement(*, mode, r2_factory, bucket,
                                      target_id=None, canonical_reader=None,
                                      publisher_reader=None, snapshot_fn=None,
                                      writer_approved=False,
                                      independent_provenance_verified=False):
    if mode == 'OFF':
        return {'status': 'OFF', 'wrote': False}
    if mode not in ('PLAN_ONLY', 'EXECUTE_ONE_CAS'):
        _hold('EXCLUSIVE_MODE_INVALID')
    import re
    if not isinstance(target_id, str) or not re.fullmatch(r'ae:[0-9a-f]{24}', target_id):
        _hold('EXACT_TARGET_REQUIRED')
    # Before any R2 client, network, ordinary price run, or data mutation.
    if not callable(canonical_reader):
        _hold('AUTHENTICATED_CANONICAL_READER_REQUIRED')
    if not callable(publisher_reader):
        _hold('INDEPENDENT_PUBLISHER_PROOF_READER_REQUIRED')
    if mode == 'EXECUTE_ONE_CAS' and (
            writer_approved is not True or independent_provenance_verified is not True):
        _hold('EXECUTION_NOT_AUTHORIZED')
    if not callable(r2_factory):
        _hold('EXISTING_R2_FACTORY_REQUIRED')
    from alpha_events_postponement_existing_writer_r2 import bind_existing_writer_r2
    from alpha_events_postponement_writer_integration import execute_one_stage
    r2 = r2_factory()
    reader, cas = bind_existing_writer_r2(r2=r2, bucket=bucket, snapshot_fn=snapshot_fn)
    result = execute_one_stage(
        target_id=target_id, read_snapshot=reader,
        read_authenticated_canonical=canonical_reader,
        authenticated_publisher_receipt=publisher_reader,
        compare_and_swap=cas if mode == 'EXECUTE_ONE_CAS' else None)
    return {'status': result['stage'], 'wrote': bool(result.get('changed')),
            'result': result}
