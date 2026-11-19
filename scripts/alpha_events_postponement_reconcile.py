"""R23 pure, source-only planner for EXISTING single History writer.

NO R2 requests, DB access, mutation or scheduler here. Integration must supply
trusted, independently read-back Active objects + authenticated publisher ACK,
then perform one expected-ETag CAS and readback per invocation.
"""
import copy
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import re

CID = re.compile(r'ae:[0-9a-f]{24}\Z')
TARGET_KIND = frozenset(('tge', 'token-generation-event'))
ACTIVE_KEYS = ('alpha-events/pending.json', 'alpha-events/upcoming.json', 'alpha-events/live.json')
ALL_KEY = 'alpha-events/all.json'
HISTORY_KEY = 'alpha-events/history.json'
MARK = '_ae_postponement_reconcile'

class PostponementHold(RuntimeError):
    pass

def hold(reason):
    raise PostponementHold('WA_AE_POSTPONE_WRITER_HOLD_' + reason)

def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)

def _sha(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()

def _rows(value, name):
    if not isinstance(value, list) or len(value) > 20000 or any(not isinstance(x, dict) for x in value):
        hold(name + '_INVALID')
    return copy.deepcopy(value)

def _ids(rows, name):
    # Real legacy All/History may contain older ID-less records. Preserve those
    # byte-for-byte; they cannot be the retract target or prove its identity.
    ids=[]
    for x in rows:
        cid=x.get('id')
        if cid is None:
            continue
        if not isinstance(cid,str) or not CID.fullmatch(cid): hold(name + '_ID_INVALID')
        ids.append(cid)
    if len(set(ids)) != len(ids): hold(name + '_DUPLICATE_ID')
    return ids


def _is_tge(row):
    # Canonical external-event summaries often have event_type=None. Exact
    # source evidence is TGE; the R2 projection retains distribution_type.
    def norm(v):
        return re.sub(r'[\s_-]+', '-', str(v or '').strip().lower())
    event, distribution = norm(row.get('event_type')), norm(row.get('distribution_type'))
    if event and event not in TARGET_KIND: return False
    if distribution and distribution not in TARGET_KIND: return False
    return event in TARGET_KIND or distribution in TARGET_KIND


def _legacy_other_pair(all_other, hist_other):
    # History must be a FULL exact multiset subset of All. All can legitimately
    # have additional Active/price-only rows; do not demand All==History.
    have=Counter(_json(x) for x in all_other)
    need=Counter(_json(x) for x in hist_other)
    if need-have:
        hold('LEGACY_OTHERS_PAIR_DIVERGENCE')


def _iso(value):
    if not isinstance(value,str) or not value.endswith('Z'): return None
    try:
        dt=datetime.fromisoformat(value.replace('Z','+00:00'))
        return dt if dt.utcoffset().total_seconds()==0 else None
    except (ValueError,TypeError,OverflowError,AttributeError):
        return None


def _canonical_proof(row, target_id, ack):
    # Defense in depth on an authenticated machine-RPC READBACK, not on a
    # browser/R2-provided marker. Caller must obtain row via authorized RPC.
    dig=re.compile(r'[0-9a-f]{64}\Z')
    rawref=re.compile(r'alpha-forecast-receipt:([1-9][0-9]*)\Z')
    source=re.compile(r'alphaoutside:([1-9][0-9]*)\Z')
    if not isinstance(row,dict) or row.get('canonicalId')!=target_id or not isinstance(row.get('stateDigest'),str) or not dig.fullmatch(row['stateDigest']):
        hold('AUTHENTICATED_CANONICAL_REQUIRED')
    rev=row.get('revision')
    # Per-event canonical revision and publisher global stateRevision are
    # distinct counters. NEVER require their numeric values to match.
    if type(rev) is not int or rev < 1:
        hold('CANONICAL_REVISION_INVALID')
    event=row.get('canonicalEvent')
    refs=row.get('evidenceRefs')
    if not isinstance(event,dict) or not isinstance(refs,list) or len(refs)<2 or len(refs)>1000 or event.get('canonicalId')!=target_id:
        hold('CANONICAL_CUSTODY_INVALID')
    if (event.get('eventType') not in (None,'TGE') or event.get('lifecycleState')!='PREDICTED' or
        event.get('eventTime') is not None or event.get('eventEndTime') is not None or
        event.get('officialConfirmation') is not None):
        hold('CANONICAL_NOT_POSTPONED_TGE')
    evidence=event.get('evidence')
    if not isinstance(evidence,list) or len(evidence)<2 or len(evidence)>1000:
        hold('CANONICAL_CUSTODY_INVALID')
    markers=[e for e in evidence if isinstance(e,dict) and re.fullmatch(r'TGE_POSTPONED_OF_RECEIPT_([1-9][0-9]*)',str(e.get('eventType','')))]
    if len(markers)!=1: hold('POSTPONE_MARKER_INVALID')
    n=markers[0]
    origin_ref=n['eventType'].removeprefix('TGE_POSTPONED_OF_RECEIPT_')
    origin=next((e for e in evidence if isinstance(e,dict) and e.get('rawRef')==f'alpha-forecast-receipt:{origin_ref}'),None)
    if origin is None or origin is n or origin.get('kind')!='ALPHAOUTSIDE' or n.get('kind')!='ALPHAOUTSIDE':
        hold('POSTPONE_ORIGIN_MISSING')
    origin_src=source.fullmatch(str(origin.get('sourceIdentity') or ''))
    notice_src=source.fullmatch(str(n.get('sourceIdentity') or ''))
    n_ref=rawref.fullmatch(str(n.get('rawRef') or ''))
    o_ref=rawref.fullmatch(str(origin.get('rawRef') or ''))
    if (not origin_src or not notice_src or not n_ref or not o_ref or
        origin_src.group(1)==notice_src.group(1) or
        int(n_ref.group(1))<=int(o_ref.group(1)) or
        int(n_ref.group(1))>ack.get('afterReceiptId',0)):
        hold('POSTPONE_NOTICE_IDENTITY_INVALID')
    if (not _iso(origin.get('eventTime')) or n.get('eventTime') is not None or
        origin.get('distributionType')!='TGE' or
        origin.get('symbol')!=event.get('symbol') or
        not all(isinstance(e.get('sourceDigest'),str) and dig.fullmatch(e['sourceDigest']) for e in (origin,n)) or
        not _iso(n.get('observedAt')) or not _iso(origin.get('observedAt')) or
        _iso(n['observedAt']) < _iso(origin['observedAt'])):
        hold('POSTPONE_ORIGIN_EVIDENCE_INVALID')
    seen=[]
    for evidence_row,receipt_id in ((origin,int(o_ref.group(1))),(n,int(n_ref.group(1)))):
        found=[r for r in refs if isinstance(r,dict) and
          r.get('sourceIdentity')==evidence_row['sourceIdentity'] and
          r.get('receiptId')==receipt_id and
          r.get('immutableRecordId')==evidence_row['rawRef'] and
          r.get('evidenceKind')=='ALPHAOUTSIDE' and
          r.get('receiptKind')=='EXTERNAL_ADVISORY_OBSERVATION' and
          isinstance(r.get('subjectDigest'),str) and dig.fullmatch(r['subjectDigest'])]
        if len(found)!=1: hold('POSTPONE_RECEIPT_CUSTODY_INVALID')
        seen.append(receipt_id)
    if seen[0]==seen[1]: hold('POSTPONE_RECEIPT_CUSTODY_INVALID')
    return event, origin


def _snapshot(obj, expected_key):
    if not isinstance(obj,dict) or obj.get('key')!=expected_key or not isinstance(obj.get('etag'),str) or not obj['etag']:
        hold('SNAPSHOT_ETAG_REQUIRED')
    return _rows(obj.get('rows'), expected_key)

def _active_proof(active_versions, public_rows, publish_ack, target_id, canonical_row):
    if not isinstance(active_versions,list) or len(active_versions)!=3: hold('ACTIVE_COVERAGE')
    if [v.get('key') if isinstance(v,dict) else None for v in active_versions] != list(ACTIVE_KEYS):
        hold('ACTIVE_COVERAGE')
    if not isinstance(publish_ack,dict) or publish_ack.get('status')!='ALPHA_EVENTS_V2_R2_PUBLISH_PASS':
        hold('PUBLISH_ACK_REQUIRED')
    rev=publish_ack.get('revision')
    after=publish_ack.get('afterReceiptId')
    if type(rev) is not int or rev < 0 or type(after) is not int or after < 1:
        hold('SIGNED_ACK_CURSOR_REQUIRED')
    canonical, origin=_canonical_proof(canonical_row,target_id,publish_ack)
    active=[]
    for k,obj in zip(ACTIVE_KEYS, active_versions):
        active.extend(_snapshot(obj,k))
    _ids(active,'ACTIVE')
    public=_rows(public_rows,'PUBLIC_ACTIVE')
    _ids(public,'PUBLIC_ACTIVE')
    if {r['id']:r for r in active} != {r['id']:r for r in public}:
        hold('INDEPENDENT_PUBLIC_READBACK_MISMATCH')
    found=[r for r in active if r['id']==target_id]
    if len(found)!=1: hold('TARGET_NOT_SIGNED_ACTIVE')
    row=found[0]
    if (row.get('symbol')!=canonical.get('symbol') or
        row.get('event_time') is not None or row.get('status')!='PREDICTED' or
        row.get('notice_action')!='postponed' or row.get('official_action_verified') is True or
        row.get('completion_verified') is True or not _is_tge(row)):
        hold('ACTIVE_REVISION_NOT_POSTPONED')
    return {'revision':rev,'afterReceiptId':after,'activeSha256':_sha(active),
            'originalEventTime':origin['eventTime'],
            'originalSymbol':origin['symbol'],
            'canonicalRevision':canonical_row['revision'],
            'canonicalDigest':canonical_row['stateDigest'],
            'targetActive':copy.deepcopy(row)}

def _target_valid(target, cid):
    return (target.get('id')==cid and target.get('status')=='expired_unverified' and
            target.get('event_time') is not None and _is_tge(target) and
            target.get('completion_verified') is not True and
            target.get('lifecycle_state') in ('history','HISTORY'))

def plan_postponement_retraction(*, all_snapshot, history_snapshot, active_versions, public_rows,
                                publish_ack, target_id, canonical_row=None):
    """Plan precisely ONE CAS. Caller MUST readback before calling again.

    Safe repair sequence: mark All (keep target) -> remove History -> remove All.
    All always contains History during crash/unknown ACK windows.
    """
    if not isinstance(target_id,str) or not CID.fullmatch(target_id): hold('TARGET_ID_INVALID')
    proof=_active_proof(active_versions,public_rows,publish_ack,target_id,canonical_row)
    all_rows=_snapshot(all_snapshot,ALL_KEY)
    hist_rows=_snapshot(history_snapshot,HISTORY_KEY)
    _ids(all_rows,'ALL');_ids(hist_rows,'HISTORY')
    all_index={r['id']:r for r in all_rows if isinstance(r.get('id'),str)}
    hist_index={r['id']:r for r in hist_rows if isinstance(r.get('id'),str)}
    target_a=all_index.get(target_id)
    target_h=hist_index.get(target_id)
    # A record of the same token (or even the same canonical ID) with a
    # different round cannot be silently removed via a postponed notice.
    for target in (target_a,target_h):
        if target is not None and (_iso(target.get('event_time'))!=_iso(proof['originalEventTime']) or
                                   target.get('symbol')!=proof['originalSymbol']):
            hold('TARGET_ORIGINAL_ROUND_MISMATCH')
    rest_a=[r for r in all_rows if r.get('id')!=target_id]
    rest_h=[r for r in hist_rows if r.get('id')!=target_id]
    _legacy_other_pair(rest_a,rest_h)
    if target_a is None:
        if target_h is not None: hold('HISTORY_WITHOUT_ALL')
        return {'stage':'COMPLETE','operation':None,'legacyCount':len(rest_a),'legacySha256':_sha(rest_a)}
    # Never discard a target's real price/claim/peak enrichment. R2 Active
    # projector currently does not copy these fields; missing is not zero.
    protected_prices=('claim_price','peak_price','listing_price','price_snapshot',
                      'spot_listing_price','listingPrice','market_cap')
    for price_field in protected_prices:
        prior=target_a.get(price_field)
        if prior not in (None, '', [], {}):
            if proof['targetActive'].get(price_field)!=prior:
                hold('TARGET_PRICE_ENRICHMENT_NOT_PRESERVED')
    marker=target_a.get(MARK)
    if marker is None:
        if target_h is None or target_a!=target_h or not _target_valid(target_a,target_id):
            hold('TARGET_PREIMAGE_MISMATCH')
        marker={'schema':1,'target':target_id,
                **{k:proof[k] for k in ('revision','afterReceiptId','activeSha256',
                                           'canonicalRevision','canonicalDigest',
                                           'originalEventTime','originalSymbol')},
                'legacyAllCount':len(rest_a),'legacyAllSha256':_sha(rest_a),
                'legacyHistoryCount':len(rest_h),'legacyHistorySha256':_sha(rest_h),
                'targetSha256':_sha(target_a)}
        next_all=copy.deepcopy(all_rows)
        next_all[[r.get('id') for r in all_rows].index(target_id)][MARK]=marker
        return _plan('MARK_ALL',ALL_KEY,all_snapshot['etag'],next_all,len(rest_a))
    if not isinstance(marker,dict) or marker.get('schema')!=1 or marker.get('target')!=target_id:
        hold('UNKNOWN_RECOVERY_MARKER')
    # Authenticated publisher revision must remain EXACT during this 3-step
    # repair; if changed, pause for an explicit rebase, never delete broadly.
    if any(marker.get(k)!=proof[k] for k in ('revision','afterReceiptId','activeSha256',
                                              'canonicalRevision','canonicalDigest',
                                              'originalEventTime','originalSymbol')):
        hold('ACTIVE_REVISION_CHANGED')
    if (marker.get('legacyAllCount')!=len(rest_a) or marker.get('legacyAllSha256')!=_sha(rest_a) or
        marker.get('legacyHistoryCount')!=len(rest_h) or marker.get('legacyHistorySha256')!=_sha(rest_h)):
        hold('LEGACY_CHANGED_DURING_RECONCILIATION')
    preimage=copy.deepcopy(target_a);preimage.pop(MARK,None)
    if _sha(preimage)!=marker.get('targetSha256') or not _target_valid(preimage,target_id):
        hold('TARGET_MUTATED_DURING_RECONCILIATION')
    if target_h is not None:
        if target_h!=preimage: hold('HISTORY_TARGET_CHANGED')
        return _plan('REMOVE_HISTORY',HISTORY_KEY,history_snapshot['etag'],rest_h,len(rest_a))
    return _plan('REMOVE_ALL',ALL_KEY,all_snapshot['etag'],rest_a,len(rest_a))

def _plan(stage,key,etag,rows,legacy_count):
    return {'stage':stage,'operation':{'key':key,'expectedEtag':etag,
            'rows':rows,'rowsSha256':_sha(rows)},'legacyCount':legacy_count}

def reconcile_unknown_ack(proposed_operation, observed_snapshot, old_snapshot):
    """Read-only unknown-ACK resolution. NEVER retry a write blindly."""
    if not isinstance(proposed_operation,dict): hold('UNKNOWN_ACK_PLAN_REQUIRED')
    key=proposed_operation.get('key')
    if key not in (ALL_KEY,HISTORY_KEY): hold('UNKNOWN_ACK_KEY')
    observed=_snapshot(observed_snapshot,key)
    old=_snapshot(old_snapshot,key)
    if _sha(observed)==proposed_operation.get('rowsSha256') and observed==proposed_operation.get('rows'):
        return 'COMMITTED_READBACK'
    if observed==old and observed_snapshot.get('etag')==old_snapshot.get('etag'):
        return 'NOT_COMMITTED_RECHECK_CAS'
    hold('UNKNOWN_ACK_CONCURRENT_CHANGE')
