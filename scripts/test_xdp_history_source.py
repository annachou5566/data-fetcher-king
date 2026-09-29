import json
import pathlib
import unittest
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "airdrops.json"

EXPECTED_CONTRACT = "0x07b3D902783c3C12b077508c3B5c00113d1291D0"


class XdpHistorySourceTests(unittest.TestCase):
    def setUp(self):
        payload = json.loads(SOURCE.read_text(encoding="utf-8"))
        self.rows = payload["airdrops"]
        self.xdp = [row for row in self.rows if row.get("token") == "XDP"]

    def test_exactly_one_xdp_source_row(self):
        self.assertEqual(len(self.xdp), 1)

    def test_xdp_official_event_fields_are_frozen(self):
        row = self.xdp[0]
        self.assertEqual(row.get("name"), "Doppler Finance")
        self.assertEqual(row.get("date"), "2026-09-28")
        self.assertEqual(row.get("time"), "22:30")
        self.assertEqual(row.get("points"), "230")
        self.assertEqual(row.get("amount"), "1666")
        self.assertEqual(row.get("type"), "grab")
        self.assertEqual(row.get("phase"), 1)
        self.assertIs(row.get("completed"), True)
        self.assertEqual(row.get("contract_address"), EXPECTED_CONTRACT)
        self.assertEqual(row.get("chain_id"), "8453")
        self.assertIs(row.get("spot_listed"), False)
        self.assertIs(row.get("futures_listed"), False)

    def test_xdp_source_clock_normalizes_to_1430_utc(self):
        row = self.xdp[0]
        local = datetime.strptime(
            f"{row['date']}T{row['time']}:00", "%Y-%m-%dT%H:%M:%S"
        ).replace(tzinfo=timezone(timedelta(hours=8)))
        self.assertEqual(
            local.astimezone(timezone.utc).isoformat(),
            "2026-09-28T14:30:00+00:00",
        )

    def test_unproven_fields_are_not_fabricated(self):
        row = self.xdp[0]
        for field in ("system_timestamp", "total_amount", "market_cap", "fdv"):
            self.assertNotIn(field, row)


if __name__ == "__main__":
    unittest.main()
