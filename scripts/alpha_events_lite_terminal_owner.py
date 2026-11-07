"""R18 source-only terminal admission for the EXISTING History/price owner.

No independent writer, scheduler, network request, database read, or Active
retirement. The existing writer must own the exclusive fence, execute R2 CAS
and obtain an independently verified signed ACK before a publisher can retire.
"""
import base64
import copy
import hashlib
import json
import re
from datetime import datetime, timezone
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from alpha_events_completion_only_guard import ensure_all_contains_existing_history

SOURCE_FIELDS = ('schema', 'signerKeyId', 'canonicalId', 'eventType', 'action',
                 'actionCode', 'actualAt', 'sourcePublishedAt', 'sourceDigest',
                 'receiptRef', 'receiptSubjectDigest', 'sourceParserVersion',
                 'observedAt', 'noticeDigest')
SOURCE_DOMAIN = 'WAVEALPHA_ALPHA_EVENTS_R18_SOURCE_PROOF_V1\n'
CID = re.compile(r'ae:[0-9a-f]{24}\Z')
REF = re.compile(r'alpha-forecast-receipt:[1-9][0-9]*\Z')
HEX = re.compile(r'[0-9a-f]{64}\Z')
STAMP = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z\Z')
ACTIVE = ('alpha-events/pending.json','alpha-events/upcoming.json','alpha-events/live.json')

class LiteTerminalHold(RuntimeError):
    pass

def hold(reason):
    raise LiteTerminalHold('WA_AE_LITE_TERMINAL_HOLD_' + reason)

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def jbytes(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')

def stamp(value):
    if not isinstance(value,str) or not STAMP.fullmatch(value): hold('TIMESTAMP_FORMAT')
    try:
        dt=datetime.strptime(value,'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    except ValueError: hold('TIMESTAMP_INVALID')
    if dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'!=value: hold('TIMESTAMP_ROUNDTRIP')
    return dt

def rows(value,label):
    if not isinstance(value,list) or len(value)>20000 or any(not isinstance(r,dict) for r in value):
        hold(label+'_ROWS')
    return value

def verify_source_completion(*,attestation,original_notice,receipt,trusted_keys,
                             approved_actions,approved_versions,parse_original,catalog_event):
    """Verify real signed collector attestation + immutable receipt + raw bytes.

    parse_original must be the release-pinned original-action parser, not a
    self-reported field from the request. Catalog comes from trusted canonical DB.
    """
    if not isinstance(attestation,dict) or set(attestation)!=set(SOURCE_FIELDS)|{'signature'}:
        hold('ATTESTATION_SHAPE')
    f={k:attestation[k] for k in SOURCE_FIELDS}
    if (f['schema']!=1 or not isinstance(f['canonicalId'],str) or not CID.fullmatch(f['canonicalId'])
        or not isinstance(f['receiptRef'],str) or not REF.fullmatch(f['receiptRef'])
        or f['action']!='COMPLETED' or not isinstance(f['eventType'],str)
        or not isinstance(f['actionCode'],str) or not re.fullmatch(r'[A-Z][A-Z0-9_]{2,63}',f['actionCode'])
        or any(not isinstance(f[k],str) or not HEX.fullmatch(f[k]) for k in
               ('sourceDigest','noticeDigest','receiptSubjectDigest'))):
        hold('ATTESTATION_VALUES')
    actual,published,observed=(stamp(f[k]) for k in ('actualAt','sourcePublishedAt','observedAt'))
    if actual>published or published>observed: hold('SOURCE_CHRONOLOGY')
    if (not isinstance(approved_actions,dict) or not isinstance(approved_actions.get(f['eventType']),list)
        or f['actionCode'] not in approved_actions[f['eventType']]
        or not isinstance(approved_versions,dict) or approved_versions.get(f['eventType'])!=f['sourceParserVersion']):
        hold('ACTION_OR_PARSER_NOT_APPROVED')
    if not isinstance(trusted_keys,dict) or f['signerKeyId'] not in trusted_keys:
        hold('SOURCE_KEY_NOT_PINNED')
    sig=attestation['signature']
    if not isinstance(sig,str) or not re.fullmatch(r'[A-Za-z0-9_-]{86}',sig): hold('SOURCE_SIGNATURE_FORMAT')
    try:
        key=load_pem_public_key(trusted_keys[f['signerKeyId']].encode('ascii'))
        if not isinstance(key,Ed25519PublicKey): hold('SOURCE_KEY_TYPE')
        raw_sig=base64.urlsafe_b64decode(sig+'==')
        if len(raw_sig)!=64 or base64.urlsafe_b64encode(raw_sig).decode().rstrip('=')!=sig:
            hold('SOURCE_SIGNATURE_FORMAT')
        key.verify(raw_sig,SOURCE_DOMAIN.encode()+jbytes([f[k] for k in SOURCE_FIELDS]))
    except (InvalidSignature,ValueError,TypeError,AttributeError): hold('SOURCE_SIGNATURE')
    if not isinstance(original_notice,bytes) or not 1<=len(original_notice)<=262144:
        hold('ORIGINAL_BYTES')
    if not isinstance(receipt,dict) or not isinstance(receipt.get('binding'),dict):
        hold('RECEIPT_MISSING')
    b=receipt['binding']
    if (type(receipt.get('receiptId')) is not int or receipt['receiptId']<1
        or receipt.get('receiptKind')!='BINANCE_OFFICIAL_OBSERVATION'
        or receipt.get('sourceClass')!='APPROVED_BINANCE_OFFICIAL_RECEIPT'
        or receipt.get('immutableRecordId')!=f['receiptRef']
        or receipt.get('immutableRecordId')!=f'alpha-forecast-receipt:{receipt["receiptId"]}'
        or receipt.get('subjectDigest')!=f['receiptSubjectDigest']
        or receipt.get('serverRecordedAt')!=f['observedAt']
        or not isinstance(b.get('sourceIdentity'),str) or not b['sourceIdentity'].strip()
        or b.get('acquisitionMode')!='PROSPECTIVE_POLL' or b.get('relevant') is not True
        or b.get('sourcePublishedAt')!=f['sourcePublishedAt']
        or b.get('sourceDigest')!=f['sourceDigest']
        or not isinstance(b.get('normalizedLifecycle'),dict)
        or str(b['normalizedLifecycle'].get('eventType','')).upper()!=f['eventType']):
        hold('SOURCE_RECEIPT_BINDING')
    try: text=original_notice.decode('utf-8')
    except UnicodeDecodeError: hold('ORIGINAL_UTF8')
    envelope=jbytes({'sourceIdentity':b.get('sourceIdentity'),
                     'publishedAt':b['sourcePublishedAt'],'text':text})
    if digest(envelope)!=f['sourceDigest'] or digest(original_notice)!=f['noticeDigest']:
        hold('SOURCE_BYTES_DIGEST')
    if not callable(parse_original): hold('ORIGINAL_PARSER_ABSENT')
    parsed=parse_original(original_notice,f['sourceParserVersion'])
    if (not isinstance(parsed,dict) or parsed.get('status')!='EXACT_COMPLETION_ACTION'
        or parsed.get('eventType')!=f['eventType'] or parsed.get('actionCode')!=f['actionCode']
        or parsed.get('actualAt')!=f['actualAt'] or parsed.get('sourceDigest')!=f['noticeDigest']
        or parsed.get('canonicalId')!=f['canonicalId']):
        hold('ORIGINAL_ACTION_NOT_COMPLETED')
    if (not isinstance(catalog_event,dict) or catalog_event.get('id')!=f['canonicalId']
        or str(catalog_event.get('event_type','')).upper()!=f['eventType']):
        hold('CATALOG_IDENTITY')
    return {'canonicalId':f['canonicalId'],'eventType':f['eventType'],
            'actualAt':f['actualAt'],'receiptRef':f['receiptRef'],
            'sourceDigest':f['sourceDigest'],'sourceSignerKeyId':f['signerKeyId']}

def plan_verified_terminal_pair(*,attestation,original_notice,receipt,trusted_keys,
                                approved_actions,approved_versions,parse_original,
                                catalog_event,active_buckets,all_before,history_before):
    """Pure plan only. No R2 writes, no ACK, no Active retire permission."""
    proof=verify_source_completion(attestation=attestation,original_notice=original_notice,
         receipt=receipt,trusted_keys=trusted_keys,approved_actions=approved_actions,
         approved_versions=approved_versions,parse_original=parse_original,catalog_event=catalog_event)
    if not isinstance(active_buckets,list) or len(active_buckets)!=3: hold('ACTIVE_COVERAGE')
    active=[]
    for key in ACTIVE:
        matched=[s for s in active_buckets if isinstance(s,dict) and s.get('key')==key]
        if len(matched)!=1: hold('ACTIVE_BUCKETS_NOT_UNIQUE')
        active.extend(rows(matched[0].get('rows'),'ACTIVE'))
    candidates=[r for r in active if r.get('id')==proof['canonicalId']]
    if len(candidates)!=1: hold('EXACT_ACTIVE_MEMBERSHIP')
    candidate=candidates[0]
    for k in ('id','symbol','event_time','event_type','contract_address','chain_id'):
        value=catalog_event.get(k)
        if value is not None and candidate.get(k)!=value: hold('ACTIVE_CATALOG_CONFLICT')
    if str(candidate.get('event_type','')).upper()!=proof['eventType']:
        hold('ACTIVE_EVENT_TYPE')
    a=copy.deepcopy(rows(all_before,'ALL'))
    h=copy.deepcopy(rows(history_before,'HISTORY'))
    ensure_all_contains_existing_history(a,h)
    old_a=[r for r in a if r.get('id')==proof['canonicalId']]
    old_h=[r for r in h if r.get('id')==proof['canonicalId']]
    if len(old_a)>1 or len(old_h)>1 or (old_h and not old_a): hold('DUPLICATE_OR_ORPHAN_TERMINAL')
    terminal={**copy.deepcopy(candidate),'status':'ended','completed':True,
              'event_type':proof['eventType'],'actual_at':proof['actualAt'],
              'terminal_proof_ref':proof['receiptRef'],'lifecycle_state':'history'}
    if old_a:
        existing=old_a[0]
        for k,v in terminal.items():
            if k not in ('price_snapshot','market_cap','listing_price','spot_listing_price',
                         'ath_since_listing_price','ath_since_listing_date','spot_ath_price','spot_ath_date'):
                if existing.get(k)!=v: hold('PARTIAL_TERMINAL_CONFLICT')
        terminal=copy.deepcopy(existing)  # preserve any price enrichment in All
    else: a.append(copy.deepcopy(terminal))
    if old_h:
        if old_h[0]!=terminal: hold('TERMINAL_PAIR_CONFLICT')
    else: h.append(copy.deepcopy(terminal))
    if len(a)!=len(all_before)+(0 if old_a else 1) or len(h)!=len(history_before)+(0 if old_h else 1):
        hold('PAIR_CARDINALITY')
    if a[:len(all_before)]!=all_before or h[:len(history_before)]!=history_before:
        hold('ARCHIVE_CHANGED')
    return {'all':a,'history':h,'promoted':[proof['canonicalId']],
            'retire_live':False,'independent_writer':False,
            'source_proof_verified':True,'requires_existing_writer_CAS_ACK':True}
