"""Source-only trusted Canonical reader for existing data-fetcher-king writer.

Reads ONLY existing `wave_alpha_events_v2_sync_read`; no Supabase/R2 writes,
DDL or new RPC. Deployment requires separately approved, already-existing,
read-only RPC credential availability in the writer runtime. Never consume
public R2 `officialConfirmation` as its own authenticity proof.
"""
from __future__ import annotations
import hashlib
import json
import re
from datetime import datetime, timezone
import ssl
from urllib.error import HTTPError, URLError
from urllib.request import (
    HTTPRedirectHandler, HTTPSHandler, ProxyHandler, Request, build_opener,
)

class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


HOST = 'akbcpryqjigndzpuoany.supabase.co'
PATH = '/rest/v1/rpc/wave_alpha_events_v2_sync_read'
CID = re.compile(r'ae:[0-9a-f]{24}\Z')
SHA = re.compile(r'[0-9a-f]{64}\Z')
EVM = re.compile(r'0x[0-9a-f]{40}\Z')
CHANNELS = {'BINANCE_WALLET':'binance_wallet_announcements',
            'BINANCE_ANNOUNCEMENTS':'binance_announcements'}
# Original Binance messages must explicitly certify a terminal transition;
# claim opening alone remains nonterminal. The existing R26R release only
# emits CLAIM_OPEN; no terminal will be manufactured by this reader.
TERMINAL_LIFECYCLE = {'AIRDROP':'AIRDROP_DISTRIBUTION_COMPLETED',
                      'LISTING':'LISTING_LIVE','TGE':'TGE_COMPLETED'}
MAX_BYTES = 1024 * 1024

class CanonicalReadHold(RuntimeError):
    pass

def hold(code):
    raise CanonicalReadHold('WA_CANONICAL_RPC_READ_HOLD_' + code)

def _iso(s):
    if not isinstance(s,str) or not s.endswith('Z'):
        hold('TIME_FORMAT')
    try:
        dt=datetime.fromisoformat(s.replace('Z','+00:00'))
        if dt.tzinfo is None or dt.utcoffset().total_seconds()!=0:
            hold('TIME_UTC')
        return dt
    except (TypeError,ValueError,OverflowError,AttributeError):
        hold('TIME_INVALID')

def _check_provenance(row):
    if not isinstance(row,dict) or not CID.fullmatch(str(row.get('canonicalId') or '')):
        hold('ROW_CANONICAL_ID')
    c=row.get('canonicalEvent')
    refs=row.get('evidenceRefs')
    if (not isinstance(c,dict) or c.get('canonicalId') != row.get('canonicalId') or
        not isinstance(row.get('revision'),int) or isinstance(row.get('revision'),bool) or row['revision']<1 or
        not isinstance(row.get('stateDigest'),str) or not SHA.fullmatch(row['stateDigest']) or
        not isinstance(refs,list) or len(refs)>1000 or not isinstance(c.get('evidence'),list)):
        hold('CANONICAL_PERSISTENCE_CONTRACT')
    p=c.get('officialConfirmation')
    if p is None:
        return None
    b=p.get('binding') if isinstance(p,dict) else None
    s=p.get('verifiedSubject') if isinstance(p,dict) else None
    if not isinstance(b,dict) or not isinstance(s,dict):
        hold('OFFICIAL_PROOF_OBJECT')
    channel=CHANNELS.get(b.get('sourceKind'))
    msg=b.get('sourceMessageId')
    rid=p.get('receiptId')
    if (not channel or b.get('sourceChannel')!=channel or
        not isinstance(msg,int) or isinstance(msg,bool) or msg<1 or
        not isinstance(rid,int) or isinstance(rid,bool) or rid<1 or
        b.get('sourceIdentity')!=f'telegram:{channel}:{msg}' or
        not SHA.fullmatch(str(b.get('sourceDigest') or '')) or
        p.get('receiptKind')!='BINANCE_OFFICIAL_OBSERVATION' or
        p.get('sourceClass')!='APPROVED_BINANCE_OFFICIAL_RECEIPT' or
        b.get('relevant') is not True or b.get('relevanceReason')!='MATCH' or
        b.get('eventType') not in ('AIRDROP', 'ALPHA_EVENT')):
        hold('SOURCE_CUSTODY')
    action=s.get('confirmedAction')
    lifecycle=s.get('lifecycleAction')
    if (s.get('productFamily')!='ALPHA_EVENTS' or s.get('competitionExcluded') is not True or
        s.get('canonicalId')!=c.get('canonicalId') or s.get('symbol')!=c.get('symbol') or
        s.get('sourceIdentity')!=b['sourceIdentity'] or s.get('sourceDigest')!=b['sourceDigest'] or
        s.get('noticeStage')!='CONFIRMED' or s.get('preAnnouncement') is not False or
        action not in TERMINAL_LIFECYCLE):
        hold('OFFICIAL_ACTION_UNQUALIFIED')
    claim_open = lifecycle=='CLAIM_OPEN' and action=='AIRDROP' and b['eventType']=='AIRDROP'
    terminal = lifecycle==TERMINAL_LIFECYCLE[action] and (
        (action=='AIRDROP' and b['eventType']=='AIRDROP') or
        (action!='AIRDROP' and b['eventType']=='ALPHA_EVENT'))
    if terminal:
        if (s.get('originalTerminalVerified') is not True or
            s.get('terminalSourceIdentity') != b['sourceIdentity'] or
            s.get('terminalSourceDigest') != b['sourceDigest']):
            hold('TERMINAL_ORIGINAL_UNVERIFIED')
    if not (claim_open or terminal):
        hold('OFFICIAL_ACTION_UNQUALIFIED')
    contract=str(c.get('contractAddress') or '').lower()
    chain=str(s.get('verifiedChainId') or '')
    if (not EVM.fullmatch(contract) or s.get('verifiedTokenContract')!=contract or
        s.get('verifiedTokenSource')!='BINANCE_ALPHA_TOKEN_MASTER' or
        not re.fullmatch(r'[1-9][0-9]*',chain) or
        s.get('eventTime')!=c.get('eventTime') or not c.get('eventTime')):
        hold('EXACT_TOKEN_ROUND')
    _iso(c['eventTime'])
    if _iso(b.get('sourcePublishedAt'))>_iso(b.get('observedAt')):
        hold('PUBLISHED_AFTER_OBSERVED')
    subject_hash=hashlib.sha256(f'binance-official|{channel}|{msg}'.encode()).hexdigest()
    matches=[r for r in refs if isinstance(r,dict) and r.get('receiptId')==rid and
        r.get('sourceIdentity')==b['sourceIdentity'] and r.get('evidenceKind')==b['sourceKind'] and
        r.get('receiptKind')==p['receiptKind'] and r.get('subjectDigest')==subject_hash and
        r.get('immutableRecordId')==f'alpha-forecast-receipt:{rid}']
    evidence=[e for e in c['evidence'] if isinstance(e,dict) and
        e.get('sourceIdentity')==b['sourceIdentity'] and e.get('sourceDigest')==b['sourceDigest'] and
        e.get('rawRef')==f'alpha-forecast-receipt:{rid}' and e.get('kind')==b['sourceKind'] and
        e.get('eventTime')==c.get('eventTime') and
        str(e.get('chainId'))==chain and e.get('contractAddress')==contract]
    if len(matches)!=1 or len(evidence)!=1:
        hold('RECEIPT_REF_OR_EVIDENCE_MISMATCH')
    if any(r is not matches[0] and isinstance(r,dict) and (r.get('receiptId')==rid or r.get('sourceIdentity')==b['sourceIdentity']) for r in refs):
        hold('DUPLICATE_RECEIPT')
    return {'id':c['canonicalId'],'symbol':c['symbol'],'project_name':c.get('projectName'),
        'event_time':c['eventTime'],'event_type':c.get('eventType') or action,
        'contract_address':contract,'chain_id':chain,
        'officialConfirmation':p}

def parse_authenticated_sync_read(payload):
    if (not isinstance(payload,dict) or payload.get('schemaVersion')!=1 or
        payload.get('sourceClass')!='ALPHA_EVENTS_V2_SYNC' or
        not isinstance(payload.get('canonicalEvents'),list) or
        not isinstance(payload.get('canonicalCount'),int) or
        isinstance(payload.get('canonicalCount'),bool) or
        payload['canonicalCount']!=len(payload['canonicalEvents']) or
        len(payload['canonicalEvents'])>20000):
        hold('RPC_READ_SCHEMA')
    seen=set()
    out=[]
    for row in payload['canonicalEvents']:
        projected=_check_provenance(row)
        if projected is None:
            continue
        if projected['id'] in seen:
            hold('DUPLICATE_CANONICAL')
        seen.add(projected['id'])
        out.append(projected)
    return out

def read_authenticated_canonical_rows(*,publishable_key,machine_read_key,post=None):
    """Read existing authenticated RPC via stdlib HTTPS, never log credentials.

    Direct request only: environment proxy bypassed, TLS verified by OS CA,
    no redirects, bounded HTTP/JSON read. Tests may inject a fake `post`.
    """
    if (not isinstance(publishable_key,str) or len(publishable_key.strip())<32 or
        not isinstance(machine_read_key,str) or len(machine_read_key.strip())<32):
        hold('CREDENTIAL_UNAVAILABLE')
    url='https://'+HOST+PATH
    args=dict(json={'p_machine_key':machine_read_key,'p_after_receipt_id':0,'p_limit':1},
      headers={'apikey':publishable_key,'Accept':'application/json','Content-Type':'application/json'},
      timeout=8,allow_redirects=False,stream=True)
    try:
        if post is not None:
            return _consume_rpc_response(post(url,**args))
        payload=json.dumps(args['json'],separators=(',',':')).encode('utf-8')
        request=Request(url,data=payload,headers=args['headers'],method='POST')
        opener=build_opener(ProxyHandler({}),
                            HTTPSHandler(context=ssl.create_default_context()),
                            _NoRedirect())
        return _consume_rpc_response(opener.open(request,timeout=8))
    except (HTTPError,URLError,OSError,TimeoutError,ValueError):
        # No exception strings: exceptions can contain URLs, headers or env.
        hold('RPC_NETWORK')


def _consume_rpc_response(r):
    try:
        status=getattr(r,'status_code',None)
        if status is None:
            status=getattr(r,'status',None)
        if (status!=200 or
                not str(r.headers.get('Content-Type','')).lower().startswith('application/json')):
            hold('RPC_HTTP_OR_MIME')
        try:
            declared=int(r.headers.get('Content-Length','0') or 0)
        except (TypeError,ValueError):
            hold('RPC_INVALID_LENGTH')
        if declared<0 or declared>MAX_BYTES:
            hold('RPC_BYTES_EXCEEDED')
        chunks=[];size=0
        if callable(getattr(r,'iter_content',None)):
            itr=r.iter_content(chunk_size=16384)
        elif callable(getattr(r,'read',None)):
            itr=iter(lambda:r.read(16384),b'')
        elif isinstance(getattr(r,'content',None),bytes):
            itr=(r.content,)
        else:
            hold('RPC_RESPONSE_STREAM')
        for chunk in itr:
            if not isinstance(chunk,bytes):hold('RPC_RESPONSE_STREAM')
            size+=len(chunk)
            if size>MAX_BYTES:hold('RPC_BYTES_EXCEEDED')
            chunks.append(chunk)
        try: payload=json.loads(b''.join(chunks).decode('utf-8'))
        except (ValueError,UnicodeDecodeError):hold('RPC_JSON_INVALID')
        return parse_authenticated_sync_read(payload)
    finally:
        close=getattr(r,'close',None)
        if callable(close):close()
