import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
P2P = (ROOT / "scripts/fetch_p2p.py").read_text(encoding="utf-8")
MIGRATE = (ROOT / "scripts/migrate_p2p_history.py").read_text(encoding="utf-8")
PARITY = (ROOT / "scripts/audit_p2p_parity.py").read_text(encoding="utf-8")
VCB = (ROOT / "scripts/fetch_vcb.py").read_text(encoding="utf-8")
SBV = (ROOT / "scripts/fetch_sbv.py").read_text(encoding="utf-8")
WORKFLOW = (ROOT / ".github/workflows/fetch_p2p.yml").read_text(encoding="utf-8")


class P2PStorageIntegrityContractTest(unittest.TestCase):
    def test_python_sources_parse(self):
        for source in (P2P, MIGRATE, PARITY, VCB, SBV):
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

    def test_legacy_writer_retirement_switch_defaults_on(self):
        self.assertIn('P2P_WRITE_LEGACY", "1"', P2P)
        self.assertIn("if WRITE_LEGACY:", P2P)
        self.assertIn("P2P_WRITE_LEGACY:     '1'", WORKFLOW)

    def test_parity_workflow_is_manual_only_and_bounded(self):
        self.assertIn("run_parity_audit:", WORKFLOW)
        self.assertIn("github.event_name == 'workflow_dispatch'", WORKFLOW)
        self.assertIn("python scripts/audit_p2p_parity.py --days", WORKFLOW)
        self.assertIn("options: ['30', '60', '90']", WORKFLOW)

    def test_migration_is_bounded_and_skips_unchanged_partitions(self):
        self.assertIn("--from-date", MIGRATE)
        self.assertIn("--to-date", MIGRATE)
        self.assertIn("if dry_run or added == 0:", MIGRATE)
        self.assertIn("Migration write không được bao gồm ngày UTC hiện tại", MIGRATE)
        self.assertIn("manifest_change=no", MIGRATE)

    def test_migration_dedupe_key_includes_record_type(self):
        self.assertIn("_record_type(r)", MIGRATE)
        self.assertRegex(
            MIGRATE,
            r"return \(\s*_record_type\(r\),\s*r\[\"ts\"\],\s*r\[\"exchange\"\],\s*r\[\"asset\"\],\s*r\[\"side\"\],\s*\)",
        )

    def test_migration_preserves_legacy_v1_and_v2(self):
        self.assertIn("def legacy_snapshot_to_records", MIGRATE)
        self.assertIn("if len(snap) >= 9:", MIGRATE)
        self.assertIn('("binance", "USDT", "BUY",  snap[1])', MIGRATE)
        self.assertNotIn("if not isinstance(snap, list) or len(snap) < 9:", MIGRATE)

    def test_parity_supports_bounded_historical_windows(self):
        self.assertIn("--end-date", PARITY)
        self.assertIn("--end-date phải dạng YYYY-MM-DD", PARITY)
        self.assertIn("legacy_range=", PARITY)
        self.assertIn("if end > manifest_end:", PARITY)

    def test_parity_report_only_is_read_only_and_compact(self):
        self.assertIn("--report-only", PARITY)
        self.assertIn("--details", PARITY)
        self.assertIn("legacy_only_dates=", PARITY)
        self.assertIn("legacy_only_shapes=", PARITY)
        self.assertNotIn("put_object(", PARITY)

    def test_parity_audit_is_read_only_and_bounded(self):
        self.assertIn("--days", PARITY)
        self.assertIn("args.days > 90", PARITY)
        self.assertIn("P2P_PARITY_BEGIN", PARITY)
        self.assertNotIn("put_object(", PARITY)

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
