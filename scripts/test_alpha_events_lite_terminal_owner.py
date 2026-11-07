"""No R2 or network; generated test-only keys never leave the test process."""
import copy
import hashlib
import json
import unittest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
from alpha_events_lite_terminal_owner import (
    SOURCE_FIELDS,SOURCE_DOMAIN,LiteTerminalHold,digest,jbytes,
    plan_verified_terminal_pair,verify_source_completion,
)

ID='ae:'+'a'*24
AT='2026-11-06T10:00:00.000Z'
PUB='2026-11-06T10:10:00.000Z'
OBS='2026-11-06T10:11:00.000Z'
KEY=Ed25519PrivateKey.generate()
PUBKEY=KEY.public_key().public_bytes(Encoding.PEM,PublicFormat.SubjectPublicKeyInfo).decode()
RAW=f'EVENT:AIRDROP;ACTION:AIRDROP_SETTLED;AT:{AT};CID:{ID}'.encode()
IDENTITY='verified-source-1'
DIGEST=digest(jbytes({'sourceIdentity':IDENTITY,'publishedAt':PUB,'text':RAW.decode()}))

def make():
    fields={'schema':1,'signerKeyId':'collector-v1','canonicalId':ID,'eventType':'AIRDROP',
            'action':'COMPLETED','actionCode':'AIRDROP_SETTLED','actualAt':AT,
            'sourcePublishedAt':PUB,'sourceDigest':DIGEST,
            'receiptRef':'alpha-forecast-receipt:148','receiptSubjectDigest':'b'*64,
            'sourceParserVersion':'r18-pinned-v1','observedAt':OBS,'noticeDigest':digest(RAW)}
    signature=KEY.sign(SOURCE_DOMAIN.encode()+jbytes([fields[k] for k in SOURCE_FIELDS]))
    import base64
    fields['signature']=base64.urlsafe_b64encode(signature).decode().rstrip('=')
    receipt={'receiptId':148,'receiptKind':'BINANCE_OFFICIAL_OBSERVATION',
             'sourceClass':'APPROVED_BINANCE_OFFICIAL_RECEIPT',
             'immutableRecordId':'alpha-forecast-receipt:148','subjectDigest':'b'*64,
             'serverRecordedAt':OBS,'binding':{'acquisitionMode':'PROSPECTIVE_POLL',
             'relevant':True,'sourceIdentity':IDENTITY,'sourceDigest':DIGEST,
             'sourcePublishedAt':PUB,'normalizedLifecycle':{'eventType':'AIRDROP'}}}
    catalog={'id':ID,'symbol':'TEST','event_time':'2026-11-06T09:00:00.000Z','event_type':'AIRDROP'}
    active={'id':ID,'symbol':'TEST','event_time':catalog['event_time'],
            'event_type':'AIRDROP','status':'active','firstActiveAt':'2026-11-06T09:30:00.000Z'}
    old=[{'id':None if i%3==0 else i,'symbol':'L'+str(i),
          'status':'ended','listing_price':{'vwap':i+1}} for i in range(450)]
    def parser(raw,version):
        if version!='r18-pinned-v1': return None
        parts=dict(t.split(':',1) for t in raw.decode().split(';'))
        return {'status':'EXACT_COMPLETION_ACTION','eventType':parts['EVENT'],
                'actionCode':parts['ACTION'],'actualAt':parts['AT'],
                'canonicalId':parts['CID'],'sourceDigest':digest(raw)}
    return {'attestation':fields,'original_notice':RAW,'receipt':receipt,
            'trusted_keys':{'collector-v1':PUBKEY},
            'approved_actions':{'AIRDROP':['AIRDROP_SETTLED']},
            'approved_versions':{'AIRDROP':'r18-pinned-v1'},'parse_original':parser,
            'catalog_event':catalog,
            'active_buckets':[{'key':'alpha-events/pending.json','rows':[active]},
                              {'key':'alpha-events/upcoming.json','rows':[]},
                              {'key':'alpha-events/live.json','rows':[]}],
            'all_before':copy.deepcopy(old),'history_before':copy.deepcopy(old)}

class TerminalTests(unittest.TestCase):
    def test_signed_completion_exact_event_adds_history_without_removing_active(self):
        f=make();p=plan_verified_terminal_pair(**f)
        self.assertEqual(len(p['all']),451)
        self.assertEqual(len(p['history']),451)
        self.assertEqual(p['history'][:450],f['history_before'])
        self.assertTrue(p['source_proof_verified']);self.assertFalse(p['retire_live'])
        self.assertEqual(p['all'][450],p['history'][450])
        self.assertEqual(p['history'][450]['actual_at'],AT)
    def test_partial_all_only_recovery_is_idempotent_and_price_safe(self):
        f=make();p=plan_verified_terminal_pair(**f)
        f['all_before']=copy.deepcopy(p['all'])
        f['all_before'][450]['listing_price']={'vwap':7}
        r=plan_verified_terminal_pair(**f)
        self.assertEqual(len(r['all']),451);self.assertEqual(len(r['history']),451)
        self.assertEqual(r['history'][-1]['listing_price'],{'vwap':7})
        f['all_before']=r['all'];f['history_before']=r['history']
        x=plan_verified_terminal_pair(**f)
        self.assertEqual((len(x['all']),len(x['history'])),(451,451))
    def test_unsigned_source_hold(self):
        f=make();f['attestation']['signature']='A'*86
        with self.assertRaisesRegex(LiteTerminalHold,'SOURCE_SIGNATURE'):
            plan_verified_terminal_pair(**f)
    def test_source_raw_substitution_hold(self):
        f=make();f['original_notice']=b'FAKE'
        with self.assertRaisesRegex(LiteTerminalHold,'SOURCE_BYTES_DIGEST'):
            plan_verified_terminal_pair(**f)
    def test_status_started_never_completes(self):
        f=make();f['parse_original']=lambda b,v:{'status':'STARTED'}
        with self.assertRaisesRegex(LiteTerminalHold,'ORIGINAL_ACTION_NOT_COMPLETED'):
            plan_verified_terminal_pair(**f)
    def test_wrong_catalog_identity_hold(self):
        f=make();f['catalog_event']['id']='ae:'+'f'*24
        with self.assertRaisesRegex(LiteTerminalHold,'CATALOG_IDENTITY'):
            plan_verified_terminal_pair(**f)
    def test_duplicate_active_membership_hold(self):
        f=make();f['active_buckets'][2]['rows'].append(dict(f['active_buckets'][0]['rows'][0]))
        with self.assertRaisesRegex(LiteTerminalHold,'EXACT_ACTIVE_MEMBERSHIP'):
            plan_verified_terminal_pair(**f)
    def test_legacy_history_row_loss_hold(self):
        f=make();f['all_before']=f['all_before'][:-1]
        with self.assertRaisesRegex(Exception,'HISTORY_NOT_REPRESENTED_IN_ALL'):
            plan_verified_terminal_pair(**f)
    def test_prior_unverified_all_row_holds(self):
        f=make();f['all_before'].append({'id':ID,'symbol':'FAKE'})
        with self.assertRaisesRegex(LiteTerminalHold,'PARTIAL_TERMINAL_CONFLICT'):
            plan_verified_terminal_pair(**f)
    def test_unapproved_action_hold(self):
        f=make();f['approved_actions']={'AIRDROP':['CLAIM_OPEN']}
        with self.assertRaisesRegex(LiteTerminalHold,'ACTION_OR_PARSER_NOT_APPROVED'):
            plan_verified_terminal_pair(**f)

    def test_catalog_receipt_link_allows_notice_without_internal_id(self):
        f=make()
        original_parser=f['parse_original']
        def parser(raw,version):
            d=original_parser(raw,version);d.pop('canonicalId',None);return d
        f['parse_original']=parser
        f['catalog_event']['evidence_refs']=[{'immutableRecordId':'alpha-forecast-receipt:148',
          'subjectDigest':'b'*64}]
        self.assertEqual(len(plan_verified_terminal_pair(**f)['history']),451)
        f['catalog_event']['evidence_refs'][0]['subjectDigest']='f'*64
        with self.assertRaisesRegex(LiteTerminalHold,'CATALOG_RECEIPT_NOT_LINKED'):
            plan_verified_terminal_pair(**f)
    def test_partial_terminal_cannot_erase_price(self):
        f=make()
        f['active_buckets'][0]['rows'][0]['listing_price']=8
        p=plan_verified_terminal_pair(**f)
        f['all_before']=p['all']
        f['all_before'][-1]['listing_price']=0
        with self.assertRaisesRegex(LiteTerminalHold,'PARTIAL_PRICE_ERASURE'):
            plan_verified_terminal_pair(**f)

    def test_real_private_db_snake_case_receipt_maps_without_forging_evidence(self):
        f=make();r=f['receipt']
        f['receipt']={'receipt_id':r['receiptId'],'receipt_kind':r['receiptKind'],
          'source_class':r['sourceClass'],'subject_digest':r['subjectDigest'],
          'binding':r['binding'],'server_recorded_at':'2026-11-06T10:11:00.000+00:00'}
        p=plan_verified_terminal_pair(**f)
        self.assertEqual(len(p['history']),451)
        self.assertTrue(p['source_proof_verified'])
    def test_actual_legacy_receipt_without_normalized_completion_holds(self):
        f=make();del f['receipt']['binding']['normalizedLifecycle']
        with self.assertRaisesRegex(LiteTerminalHold,'SOURCE_RECEIPT_BINDING'):
            plan_verified_terminal_pair(**f)
    def test_external_observation_is_not_forged_as_official_completion(self):
        f=make();f['receipt']['receiptKind']='EXTERNAL_ADVISORY_OBSERVATION'
        f['receipt']['sourceClass']='APPROVED_EXTERNAL_ADVISORY_RECEIPT'
        with self.assertRaisesRegex(LiteTerminalHold,'SOURCE_RECEIPT_BINDING'):
            plan_verified_terminal_pair(**f)
    def test_private_db_receipt_ambiguous_fields_holds(self):
        f=make();f['receipt']['receipt_id']=148
        with self.assertRaisesRegex(LiteTerminalHold,'RECEIPT_MIXED_FORMAT'):
            plan_verified_terminal_pair(**f)
    def test_private_db_receipt_submillisecond_timestamp_holds(self):
        f=make();r=f['receipt']
        f['receipt']={'receipt_id':148,'receipt_kind':r['receiptKind'],
          'source_class':r['sourceClass'],'subject_digest':r['subjectDigest'],
          'binding':r['binding'],'server_recorded_at':'2026-11-06T10:11:00.123456+00:00'}
        with self.assertRaisesRegex(LiteTerminalHold,'RECEIPT_TIMESTAMP_PRECISION'):
            plan_verified_terminal_pair(**f)

if __name__=='__main__':unittest.main()
