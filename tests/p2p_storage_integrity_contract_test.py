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
QUALIFY = (ROOT / "scripts/qualify_p2p_market.py").read_text(encoding="utf-8")


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

    def test_market_snapshot_reuses_existing_binance_ads_fetch(self):
        self.assertIn("def fetch_binance_side", P2P)
        self.assertIn("stats, ads = fetch_binance_side(session, asset, side)", P2P)
        self.assertIn("build_liquidity_and_market", P2P)
        self.assertNotIn("fetch_binance_market_extra", P2P)

    def test_market_snapshot_uses_existing_r2_owner(self):
        self.assertIn('R2_MARKET_KEY   = "p2p-snapshots/_market-latest.json"', P2P)
        self.assertNotIn("LIQUIDATION_HISTORY_SERVICE", P2P)

    def test_market_snapshot_is_last_good_fail_closed(self):
        self.assertIn("if stats[\"is_partial\"] or not ads:", P2P)
        self.assertIn("market_complete = False", P2P)
        self.assertIn("Canonical market snapshot SKIPPED", P2P)
        self.assertIn("refusing to publish incomplete market snapshot", P2P)

    def test_market_snapshot_keeps_all_sanitized_ads_from_complete_fetch(self):
        self.assertIn("market_ads.append(normalized)", P2P)
        self.assertIn('"reported_ad_count": total_reported', P2P)
        self.assertIn('"ads": ads', P2P)

    def test_market_ads_store_only_bounded_public_fields(self):
        for token in (
            '"price": price',
            '"minFiat": min_fiat',
            '"maxFiat": max_fiat',
            '"availableCrypto": available',
            '"payTypes": _normalize_pay_methods(item)',
            '"merchant": merchant',
            '"monthOrders": month_orders',
            '"monthRate": round(finish_rate, 6)',
        ):
            self.assertIn(token, P2P)

    def test_market_snapshot_does_not_change_scheduler_or_legacy_switch(self):
        self.assertIn("github.event.schedule == '*/10 * * * *'", WORKFLOW)
        self.assertIn("P2P_WRITE_LEGACY:     '1'", WORKFLOW)

    def test_market_qualifier_is_read_only(self):
        self.assertIn("P2P_MARKET_QUAL_BEGIN", QUALIFY)
        self.assertIn("P2P_MARKET_QUAL_END", QUALIFY)
        self.assertNotIn("put_object", QUALIFY)
        self.assertNotIn("get_r2", QUALIFY)
        self.assertNotIn("R2_ACCESS_KEY_ID", QUALIFY)

    def test_market_qualifier_covers_presets_and_both_assets(self):
        self.assertIn("1_000_000", QUALIFY)
        self.assertIn("5_000_000", QUALIFY)
        self.assertIn("10_000_000", QUALIFY)
        self.assertIn("50_000_000", QUALIFY)
        self.assertIn("for asset in BNC_ASSETS", QUALIFY)
        self.assertIn('for side in ("BUY", "SELL")', QUALIFY)

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

    def test_migration_dedupes_only_against_existing_prices(self):
        self.assertIn('if _record_type(r) == "price"', MIGRATE)
        self.assertIn('r.get("side")', MIGRATE)
        self.assertNotIn('seen = {_record_key(r) for r in existing}', MIGRATE)

    def test_migration_dedupe_key_includes_record_type(self):
        self.assertIn("_record_type(r)", MIGRATE)
        self.assertIn("r.get(\"ts\")", MIGRATE)
        self.assertIn("r.get(\"exchange\")", MIGRATE)
        self.assertIn("r.get(\"asset\")", MIGRATE)
        self.assertIn("r.get(\"side\")", MIGRATE)

    def test_migration_preserves_legacy_v1_and_v2(self):
        self.assertIn("def legacy_snapshot_to_records", MIGRATE)
        self.assertIn("if len(snap) >= 9:", MIGRATE)
        self.assertIn('("binance", "USDT", "BUY",  snap[1])', MIGRATE)
        self.assertNotIn("if not isinstance(snap, list) or len(snap) < 9:", MIGRATE)

    def test_parity_recognizes_legacy_canonical_price_schema(self):
        self.assertIn("def is_price_record(r):", PARITY)
        self.assertIn('canonical_legacy_schema_price_rows=', PARITY)
        self.assertIn('r.get("record_type") in (None, "")', PARITY)
        self.assertIn('"price" in r', PARITY)

    def test_parity_distinguishes_missing_keys_from_duplicates(self):
        self.assertIn("missing_canonical_keys=", PARITY)
        self.assertIn("value_mismatch_keys=", PARITY)
        self.assertIn("legacy_duplicate_excess=", PARITY)
        self.assertIn("missing_legacy_keys=", PARITY)
        self.assertIn("blocking = bool(missing_objects or missing_canonical or value_mismatch_ids)", PARITY)

    def test_parity_supports_bounded_historical_windows(self):
        self.assertIn("--end-date", PARITY)
        self.assertIn("--end-date phải dạng YYYY-MM-DD", PARITY)
        self.assertIn("legacy_range=", PARITY)
        self.assertIn("if end > manifest_end:", PARITY)

    def test_parity_report_only_is_read_only_and_compact(self):
        self.assertIn("--report-only", PARITY)
        self.assertIn("--details", PARITY)
        self.assertIn("missing_canonical_dates=", PARITY)
        self.assertIn("missing_canonical_shapes=", PARITY)
        self.assertIn("missing_legacy_dates=", PARITY)
        self.assertIn("missing_legacy_shapes=", PARITY)
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
