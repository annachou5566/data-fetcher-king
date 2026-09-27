import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
P2P = (ROOT / "scripts/fetch_p2p.py").read_text(encoding="utf-8")
VCB = (ROOT / "scripts/fetch_vcb.py").read_text(encoding="utf-8")
SBV = (ROOT / "scripts/fetch_sbv.py").read_text(encoding="utf-8")


class P2PStorageIntegrityContractTest(unittest.TestCase):
    def test_python_sources_parse(self):
        for source in (P2P, VCB, SBV):
            ast.parse(source)

    def test_p2p_legacy_read_errors_fail_closed(self):
        self.assertIn("từ chối ghi đè archive", P2P)
        self.assertNotIn("Load R2 (legacy): {e} → tạo mới", P2P)

    def test_p2p_daily_read_errors_fail_closed(self):
        self.assertIn("từ chối ghi đè partition", P2P)
        self.assertNotIn("→ tạo mới cho ngày này", P2P)

    def test_p2p_manifest_read_errors_fail_closed(self):
        self.assertIn("từ chối ghi manifest mới", P2P)
        self.assertNotIn("→ bỏ qua cập nhật manifest lần này", P2P)

    def test_vcb_only_no_such_key_initializes_empty_archive(self):
        self.assertIn("except r2.exceptions.NoSuchKey:", VCB)
        self.assertIn("từ chối ghi đè archive", VCB)
        self.assertNotIn("Load R2 lỗi: {e} → tạo mới", VCB)

    def test_sbv_only_no_such_key_initializes_empty_archive(self):
        self.assertIn("except r2.exceptions.NoSuchKey:", SBV)
        self.assertIn("từ chối ghi đè archive", SBV)
        self.assertNotIn("except Exception:\n        return []", SBV)


if __name__ == "__main__":
    unittest.main()
