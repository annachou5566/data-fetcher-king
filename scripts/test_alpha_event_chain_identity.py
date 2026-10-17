"""Network-free Alpha Events chain identity and DRV price retry tests."""
import unittest
from unittest.mock import patch
import sync_listing_prices as s

DRV = "0x9d0e8f5b25384c7310cb8c6ae32c8fbeb645d083"
OTHER = "0x1111111111111111111111111111111111111111"


class ChainRestoration(unittest.TestCase):
    def official(self):
        return [
            {"symbol":"DRV","contractAddress":DRV.upper().replace("0X","0x"),"chainId":"8453"},
            {"symbol":"DRV","contractAddress":OTHER,"chainId":"56"},
            {"symbol":"DNA","contractAddress":"0x2222222222222222222222222222222222222222","chainId":"56"},
        ]

    def test_drv_base_restored_once_with_real_contract(self):
        idx=s._verified_alpha_chain_index(self.official())
        row={"symbol":"DRV","contract_address":DRV,"chain_id":None,
             "event_time":"2026-10-14T10:00:00.000Z"}
        self.assertEqual(s.restore_missing_event_chains([row],idx),1)
        self.assertEqual(row["chain_id"],"8453")
        self.assertEqual(s.restore_missing_event_chains([row],idx),0)

    def test_shared_ticker_different_contract_and_old_chain(self):
        idx=s._verified_alpha_chain_index(self.official())
        rows=[
            {"symbol":"DRV","contract_address":OTHER,"chain_id":None},
            {"symbol":"DRV","contract_address":"0x3333333333333333333333333333333333333333","chain_id":None},
            {"symbol":"DRV","contract_address":DRV,"chain_id":"56"},
        ]
        self.assertEqual(s.restore_missing_event_chains(rows,idx),1)
        self.assertEqual(rows[0]["chain_id"],"56")
        self.assertIsNone(rows[1]["chain_id"])
        self.assertEqual(rows[2]["chain_id"],"56")

    def test_conflicting_chain_for_identical_contract_must_hold(self):
        idx=s._verified_alpha_chain_index(self.official()+
            [{"symbol":"DRV","contractAddress":DRV,"chainId":"1"}])
        row={"symbol":"DRV","contract_address":DRV,"chain_id":None}
        self.assertEqual(s.restore_missing_event_chains([row],idx),0)
        self.assertIsNone(row["chain_id"])

    def test_unrecognized_chain_and_conflicting_camelcase_hold(self):
        idx=s._verified_alpha_chain_index(self.official()+
            [{"symbol":"BAD","contractAddress":OTHER,"chainId":"Base"}])
        row={"symbol":"DRV","contract_address":DRV,"chainId":"56","chain_id":None}
        self.assertEqual(s.restore_missing_event_chains([row],idx),0)
        self.assertIsNone(row["chain_id"])
        self.assertNotIn(("BAD",s._alpha_contract_key(OTHER)),idx)

    def test_drv_price_retry_gets_verified_base_and_exact_event_anchor(self):
        idx=s._verified_alpha_chain_index(self.official())
        row={"symbol":"DRV","contract_address":DRV,"chain_id":None,
             "event_time":"2026-10-14T10:00:00.000Z"}
        s.restore_missing_event_chains([row],idx)
        captured=[]
        def fake_price(chain, contract, date, **kw):
            captured.append((chain,contract,date,kw))
            return None
        with patch.object(s,"_get_alpha_info",return_value=("ALPHA_1246",None)), \
             patch.object(s,"fetch_listing_price",side_effect=fake_price):
            _,out=s._process_one(row,1,1)
        self.assertIsNone(out)
        self.assertEqual(captured[0][0],"8453")
        self.assertEqual(captured[0][1],DRV)
        self.assertEqual(captured[0][3]["alpha_id"],"ALPHA_1246")
        self.assertEqual(captured[0][3]["event_start_ms"],s._event_start_ms(row))

    def test_dna_bsc_identity_unchanged(self):
        idx=s._verified_alpha_chain_index(self.official())
        row={"symbol":"DNA","contract_address":"0x2222222222222222222222222222222222222222","chain_id":"56"}
        self.assertEqual(s.restore_missing_event_chains([row],idx),0)
        self.assertEqual(row["chain_id"],"56")


if __name__=="__main__":
    unittest.main()
