"""Bounded, network-free regression tests for per-event Alpha Claim/Peak pricing."""
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import sync_listing_prices as s


def ms(iso):
    return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000)


def candle(t, price, volume=5):
    return [str(t), str(price), str(price), str(price), str(price),
            str(volume), str(t + 59999), "0", 1, "0", "0", "0"]


class EventPriceAnchorTests(unittest.TestCase):
    EVENT_TIME = "2026-10-09T09:00:00.000Z"
    EVENT_MS = ms(EVENT_TIME)
    OLD_LISTING_MS = ms("2025-08-27T00:00:00Z")

    def fake_klines(self, alpha_id, interval, start_ms=None, end_ms=None, limit=1500):
        self.assertEqual(alpha_id, "ALPHA_777")
        if interval == "1m":
            self.assertEqual(start_ms, self.EVENT_MS)
            return [
                candle(self.OLD_LISTING_MS, 8.0),  # Must not be used
                candle(self.EVENT_MS - 60000, 7.0),  # Must not be used
                candle(self.EVENT_MS + 60000, .10),
                candle(self.EVENT_MS + 120000, .12),
                candle(self.EVENT_MS + 360000, .2),
            ]
        self.assertEqual(interval, "1d")
        self.assertEqual(start_ms, ms("2026-10-09T00:00:00Z"))
        return [
            candle(self.OLD_LISTING_MS, 8.0),
            candle(ms("2026-10-09T00:00:00Z"), .18),
            candle(ms("2026-10-10T00:00:00Z"), .30),
        ]

    def test_utc_time_with_and_without_fraction(self):
        self.assertEqual(s._event_start_ms({"event_time": self.EVENT_TIME}), self.EVENT_MS)
        self.assertEqual(s._event_start_ms({"event_time": "2026-10-09T09:00:00Z"}), self.EVENT_MS)

    def test_later_airdrop_ignores_old_listing_and_old_trades(self):
        with patch.object(s, "fetch_alpha_trade_klines_official", side_effect=self.fake_klines):
            p = s.fetch_listing_price("56", "0xabc", "2026-10-09", alpha_id="ALPHA_777",
                                      alpha_listing_time_ms=self.OLD_LISTING_MS,
                                      event_start_ms=self.EVENT_MS)
        self.assertEqual(p["date"], "2026-10-09")
        self.assertEqual(p["event_start_ms"], self.EVENT_MS)
        self.assertAlmostEqual(p["vwap"], .11, places=8)
        self.assertEqual(p["max_since"], {"price": .30, "date": "2026-10-10"})

    def test_future_listing_can_start_after_announcement(self):
        listing = ms("2026-10-11T12:00:00Z")
        calls = []
        def klines(alpha_id, interval, start_ms=None, end_ms=None, limit=1500):
            calls.append((interval, start_ms))
            if interval == "1m":
                return [candle(listing, .5), candle(listing + 60000, .6)]
            return [candle(ms("2026-10-11T00:00:00Z"), .55)]
        with patch.object(s, "fetch_alpha_trade_klines_official", side_effect=klines):
            p = s.fetch_listing_price("56", "0xabc", "2026-10-09", alpha_id="ALPHA_777",
                                      alpha_listing_time_ms=listing,
                                      event_start_ms=self.EVENT_MS)
        self.assertEqual(calls[0], ("1m", listing))
        self.assertEqual(p["date"], "2026-10-11")
        self.assertEqual(p["event_start_ms"], self.EVENT_MS)

    def test_no_postevent_volume_never_uses_pre_event_trade(self):
        with patch.object(s, "fetch_alpha_trade_klines_official",
                          return_value=[candle(self.EVENT_MS - 60000, 7.0)]), \
             patch.object(s, "API_AGG_KLINES", None):
            p = s.fetch_listing_price(None, "0xabc", "2026-10-09", alpha_id="ALPHA_777",
                                      alpha_listing_time_ms=self.OLD_LISTING_MS,
                                      event_start_ms=self.EVENT_MS)
        self.assertIsNone(p)

    def test_exact_campaign_ids_recompute_once_after_success(self):
        old = {"date": "2025-08-27", "vwap": 0.14}
        btr = {"id": "ae:38e6ce86b887e7b2002ba255", "symbol": "BTR",
               "event_time": self.EVENT_TIME, "listing_price": dict(old)}
        clo = {"id": "ae:7a19c82531ad7937fc8b9d6e", "symbol": "CLO",
               "event_time": self.EVENT_TIME, "listing_price": dict(old)}
        unrelated = {"id": "other", "symbol": "BTR",
                     "event_time": self.EVENT_TIME, "listing_price": dict(old)}
        events = [btr, clo, unrelated]
        self.assertEqual(s.mark_event_anchor_reprices(events), 2)
        self.assertTrue(btr["_vwap_daybound_recompute"])
        self.assertTrue(clo["_vwap_daybound_recompute"])
        self.assertNotIn("_vwap_daybound_recompute", unrelated)
        btr["listing_price"]["event_start_ms"] = self.EVENT_MS
        clo["listing_price"]["event_start_ms"] = self.EVENT_MS
        for row in events:
            row.pop("_vwap_daybound_recompute", None)
        self.assertEqual(s.mark_event_anchor_reprices(events), 0)


    def test_same_token_two_events_keep_independent_price_anchors(self):
        first = {"id": "a", "symbol": "BTR", "event_time": "2025-08-27T12:00:00Z",
                 "contract_address": "0xabc"}
        again = {"id": "b", "symbol": "BTR", "event_time": self.EVENT_TIME,
                 "contract_address": "0xabc"}
        price = {"vwap": .2, "open": .2, "close": .2, "date": "2026-10-09",
                 "max_since": {"price": .3, "date": "2026-10-10"}}
        with patch.object(s, "_get_alpha_info", return_value=("ALPHA_777", self.OLD_LISTING_MS)), \
             patch.object(s, "fetch_listing_price", return_value=price) as get:
            s._process_one(first, 1, 2)
            s._process_one(again, 2, 2)
        self.assertEqual(get.call_count, 2)
        self.assertEqual(get.call_args_list[0].kwargs["event_start_ms"],
                         ms("2025-08-27T12:00:00Z"))
        self.assertEqual(get.call_args_list[1].kwargs["event_start_ms"], self.EVENT_MS)

    def test_failed_reprice_preserves_prior_record(self):
        old = {"date": "2025-08-27", "vwap": .14,
               "max_since": {"price": .3, "date": "2025-09-01"}}
        event = {"id": "ae:38e6ce86b887e7b2002ba255", "symbol": "BTR",
                 "event_time": self.EVENT_TIME, "contract_address": "0xabc",
                 "listing_price": dict(old), "_vwap_daybound_recompute": True}
        with patch.object(s, "_process_one", return_value=(event, None)):
            self.assertEqual(s.enrich_events([event]), 0)
        self.assertEqual(event["listing_price"], old)
        self.assertNotIn("_vwap_daybound_recompute", event)


    def test_graduated_token_spot_fallback_uses_event_minutes_not_first_listing(self):
        old = self.OLD_LISTING_MS
        event = self.EVENT_MS
        def spot(symbol, interval, start_ms=None, limit=1000, retries=2):
            self.assertEqual(symbol, "BTR")
            if interval == "1m":
                self.assertEqual(start_ms, event)
                return [
                    candle(old, 8),
                    candle(event - 60000, 5),
                    candle(event + 60000, .10),
                    candle(event + 120000, .12),
                ]
            self.assertEqual(interval, "1d")
            self.assertEqual(start_ms, ms("2026-10-09T00:00:00Z"))
            return [
                candle(old, 8),
                candle(ms("2026-10-09T00:00:00Z"), .18),
                candle(ms("2026-10-10T00:00:00Z"), .25),
            ]
        with patch.object(s, "_binance_public_klines", side_effect=spot):
            p = s.fetch_event_price_public_spot("BTR", event)
        self.assertEqual(p["event_start_ms"], event)
        self.assertEqual(p["date"], "2026-10-09")
        self.assertAlmostEqual(p["vwap"], .11)
        self.assertEqual(p["max_since"], {"price": .25, "date": "2026-10-10"})
        self.assertEqual(p["source"], "public_spot_event")

    def test_spot_fallback_cannot_use_only_pre_event_trades(self):
        with patch.object(s, "_binance_public_klines",
                          return_value=[candle(self.EVENT_MS - 60000, 9)]):
            self.assertIsNone(s.fetch_event_price_public_spot("BTR", self.EVENT_MS))


if __name__ == "__main__":
    unittest.main()
