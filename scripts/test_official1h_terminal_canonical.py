"""Synthetic proof-chain regression only. No Binance, RPC, R2 or Production I/O."""
import copy
import hashlib
import unittest
from datetime import datetime, timezone
from alpha_events_official_onehour import qualify_confirmation
from canonical_rpc_readonly import parse_authenticated_sync_read, CanonicalReadHold
from official_history_policy import reconcile_authenticated, OfficialHistoryHold

NOW=datetime(2026,11,14,11,0,tzinfo=timezone.utc)
CID='ae:0123456789abcdef01234567'
SOURCE='telegram:binance_wallet_announcements:1866'
CONTRACT='0x'+'a'*40
ROUND='2026-11-14T10:00:00.000Z'

def mk(life='CLAIM_OPEN'):
    b={'sourceKind':'BINANCE_WALLET','sourceChannel':'binance_wallet_announcements',
       'sourceMessageId':1866,'sourceIdentity':SOURCE,'sourceDigest':'a'*64,
       'sourcePublishedAt':'2026-11-14T10:30:00.000Z',
       'observedAt':'2026-11-14T10:30:03.000Z',
       'relevant':True,'relevanceReason':'MATCH','eventType':'AIRDROP'}
    sub={'productFamily':'ALPHA_EVENTS','competitionExcluded':True,
       'canonicalId':CID,'symbol':'ABC','sourceIdentity':SOURCE,
       'sourceDigest':'a'*64,'noticeStage':'CONFIRMED','preAnnouncement':False,
       'confirmedAction':'AIRDROP','lifecycleAction':life,
       'originalTerminalVerified':True,'terminalSourceIdentity':SOURCE,
       'terminalSourceDigest':'a'*64,'verifiedTokenContract':CONTRACT,
       'verifiedChainId':'56','verifiedTokenSource':'BINANCE_ALPHA_TOKEN_MASTER',
       'eventTime':ROUND,
       'officialTerms':{'amount_per_user':270,'points_threshold':256,'points_cost':15}}
    return {'id':CID,'symbol':'ABC','event_time':ROUND,'chain_id':'56',
       'contract_address':CONTRACT,'event_type':'AIRDROP',
       'status':'expired_unverified','listing_price':{'vwap':1.2},
       'officialConfirmation':{'receiptId':698,'receiptKind':'BINANCE_OFFICIAL_OBSERVATION',
       'sourceClass':'APPROVED_BINANCE_OFFICIAL_RECEIPT','binding':b,'verifiedSubject':sub}}

def plan():
    r=mk();r.pop('officialConfirmation')
    return {'all':[r],'history':[copy.deepcopy(r)],'independent_writer':False,'retire_live':False}

class OfficialCanonicalOnly(unittest.TestCase):
    def canonical(self,terminal):
        r=mk(life='AIRDROP_DISTRIBUTION_COMPLETED' if terminal else 'CLAIM_OPEN')
        proof=copy.deepcopy(r['officialConfirmation']);b=proof['binding']
        rid=proof['receiptId'];channel=b['sourceChannel'];msg=b['sourceMessageId']
        ev={'kind':'BINANCE_WALLET','sourceIdentity':SOURCE,'sourceDigest':b['sourceDigest'],
            'rawRef':f'alpha-forecast-receipt:{rid}','contractAddress':CONTRACT,
            'chainId':'56','eventTime':ROUND}
        c={'canonicalId':CID,'symbol':'ABC','projectName':'TestOnly','eventType':'AIRDROP',
           'eventTime':ROUND,'contractAddress':CONTRACT,'evidence':[ev],
           'officialConfirmation':proof}
        ref={'receiptId':rid,'sourceIdentity':SOURCE,'receiptKind':proof['receiptKind'],
             'evidenceKind':b['sourceKind'],
             'subjectDigest':hashlib.sha256(f'binance-official|{channel}|{msg}'.encode()).hexdigest(),
             'immutableRecordId':f'alpha-forecast-receipt:{rid}'}
        return {'schemaVersion':1,'sourceClass':'ALPHA_EVENTS_V2_SYNC',
                'canonicalCount':1,'canonicalEvents':[{'canonicalId':CID,
                'revision':1,'stateDigest':'a'*64,'canonicalEvent':c,'evidenceRefs':[ref]}]}
    def test_claim_open_never_admits_history(self):
        rows=parse_authenticated_sync_read(self.canonical(False))
        self.assertFalse(qualify_confirmation(rows[0],NOW)['terminal'])
        p=plan()
        out=reconcile_authenticated(p,[],NOW,qualify_confirmation,durable_canonical_rows=rows)
        self.assertEqual(out['official_promoted'],[])
        self.assertEqual(out['history'],p['history'])
    def test_terminal_updates_history_even_with_empty_active(self):
        rows=parse_authenticated_sync_read(self.canonical(True))
        self.assertTrue(qualify_confirmation(rows[0],NOW)['terminal'])
        out=reconcile_authenticated(plan(),[],NOW,qualify_confirmation,durable_canonical_rows=rows)
        self.assertEqual(out['official_promoted'],[CID])
        self.assertEqual(out['history'][0]['listing_price'],{'vwap':1.2})
        self.assertEqual(out['history'][0]['status'],'ended')
        self.assertFalse(out['retire_live'])
    def test_source_digest_and_contract_mismatch_holds(self):
        for field,value in [('terminalSourceDigest','wrong'),('verifiedTokenContract','0x'+'f'*40)]:
            p=self.canonical(True)
            p['canonicalEvents'][0]['canonicalEvent']['officialConfirmation']['verifiedSubject'][field]=value
            with self.assertRaises(CanonicalReadHold,msg=field):
                parse_authenticated_sync_read(p)
    def test_r2_active_proof_cannot_replace_missing_canonical(self):
        with self.assertRaisesRegex(OfficialHistoryHold,'DURABLE_CANONICAL_REQUIRED'):
            reconcile_authenticated(plan(),[mk('AIRDROP_DISTRIBUTION_COMPLETED')],
                                    NOW,qualify_confirmation,durable_canonical_rows=[])

if __name__=='__main__':unittest.main()
