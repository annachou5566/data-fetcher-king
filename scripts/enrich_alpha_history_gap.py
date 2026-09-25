"""
Targeted listing-price enrichment for the 12 Alpha History rows appended on
2026-09-25. Dry-run by default. This intentionally avoids the broad maintenance
behaviour of sync_listing_prices.py (status reconciliation, invalidations,
manual resets, Futures/spot updates, etc.).

Allowed mutation, only when --apply is explicitly passed:
  - listing_price
  - ath_since_listing_price
  - ath_since_listing_date
for the exact 12 target identities below, in alpha-events/all.json and
alpha-events/history.json.

All non-target rows must remain logically byte-equivalent under canonical JSON
serialization.
"""

import argparse
import copy
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import sync_listing_prices as lp

ALL_KEY = "alpha-events/all.json"
HISTORY_KEY = "alpha-events/history.json"

TARGETS = {
    ("KII", "2026-08-14T13:00:00+00:00", "0xeec6574eabba52bac3f0277f2cd5ac7e67197886"),
    ("STABLE", "2026-08-19T11:00:00+00:00", "0x011ebe7d75e2c9d1e0bd0be0bef5c36f0a90075f"),
    ("TMX", "2026-08-25T10:00:00+00:00", "0x3c2f61f2e27c865981d2e7aaf6b2cdf823030039"),
    ("DEBIT", "2026-08-26T10:00:00+00:00", "0x66661c7229901f568f16bd1551b3ba826f83ce49"),
    ("TMX", "2026-09-01T11:00:00+00:00", "0x3c2f61f2e27c865981d2e7aaf6b2cdf823030039"),
    ("SOON", "2026-09-01T11:00:00+00:00", "0xb9e1fd5a02d3a33b25a14d661414e6ed6954a721"),
    ("CP", "2026-09-04T07:00:00+00:00", "0x001aad84c21a5cd4d696c56d44866e9703c43f77"),
    ("CNPY", "2026-09-07T12:00:00+00:00", "0xc69b16cf18cea1e5d0bb6a1a9db802097790ddd2"),
    ("BSB", "2026-09-10T11:00:00+00:00", "0x595deaad1eb5476ff1e649fdb7efc36f1e4679cc"),
    ("DGAI", "2026-09-16T08:00:00+00:00", "0x10d4183389e99233db3cc981c43443ebd28ebd5e"),
    ("APM", "2026-09-17T13:00:00+00:00", "0x72a22faa6a522c81a8f5d508381e18af3da0921e"),
    ("CYS", "2026-09-23T10:00:00+00:00", "0x0c69199c1562233640e0db5ce2c399a88eb507c7"),
}

CP_KEY = ("CP", "2026-09-04T07:00:00+00:00", "0x001aad84c21a5cd4d696c56d44866e9703c43f77")
APPLY_TARGETS = TARGETS - {CP_KEY}

ALLOWED_TARGET_FIELDS = (
    "listing_price",
    "ath_since_listing_price",
    "ath_since_listing_date",
)


def target_key(row):
    return (
        str(row.get("symbol") or row.get("token") or "").upper(),
        str(row.get("event_time") or ""),
        str(row.get("contract_address") or "").lower(),
    )


def canonical_digest(rows):
    raw = json.dumps(
        rows,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def split_targets(rows):
    target_rows = []
    other_rows = []
    seen = set()
    for row in rows:
        key = target_key(row)
        if key in TARGETS:
            if key in seen:
                raise RuntimeError(f"duplicate target identity: {key}")
            seen.add(key)
            target_rows.append(row)
        else:
            other_rows.append(row)
    missing = TARGETS - seen
    if missing:
        raise RuntimeError(f"missing target identities: {sorted(missing)}")
    return target_rows, other_rows


def apply_price_result(row, result):
    if not result:
        return False
    row["listing_price"] = result
    ms = result.get("max_since") if isinstance(result, dict) else None
    row["ath_since_listing_price"] = ms.get("price") if isinstance(ms, dict) else None
    row["ath_since_listing_date"] = ms.get("date") if isinstance(ms, dict) else None
    return True


def verify_non_target_unchanged(before_rows, after_rows):
    _, before_other = split_targets(before_rows)
    _, after_other = split_targets(after_rows)
    return canonical_digest(before_other) == canonical_digest(after_other)


def verify_target_field_scope(before_rows, after_rows):
    before_targets, _ = split_targets(before_rows)
    after_targets, _ = split_targets(after_rows)
    before_map = {target_key(r): r for r in before_targets}
    after_map = {target_key(r): r for r in after_targets}

    for key in TARGETS:
        before = before_map[key]
        after = after_map[key]
        fields = set(before) | set(after)
        changed = {
            field for field in fields
            if before.get(field) != after.get(field)
        }
        if not changed.issubset(ALLOWED_TARGET_FIELDS):
            raise RuntimeError(
                f"target field-scope violation {key}: {sorted(changed)}"
            )
    return True


def load_required(r2, key):
    rows = lp.load_json(r2, key)
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"cannot read required canonical list {key}")
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--cp-only",
        action="store_true",
        help="Probe/apply only the exact CP target; all other rows must stay unchanged.",
    )
    parser.add_argument(
        "--require-data",
        action="store_true",
        help="Fail closed when the selected target has no qualified price result.",
    )
    args = parser.parse_args()

    r2 = lp.get_r2()
    all_rows = load_required(r2, ALL_KEY)
    history_rows = load_required(r2, HISTORY_KEY)

    if len(all_rows) != 443 or len(history_rows) != 443:
        raise RuntimeError(
            f"count guard failed all={len(all_rows)} history={len(history_rows)}"
        )

    all_before = copy.deepcopy(all_rows)
    history_before = copy.deepcopy(history_rows)

    all_targets, all_other = split_targets(all_rows)
    history_targets, history_other = split_targets(history_rows)

    if len(all_other) != 431 or len(history_other) != 431:
        raise RuntimeError(
            f"old-row guard failed all_other={len(all_other)} "
            f"history_other={len(history_other)}"
        )

    old_all_digest = canonical_digest(all_other)
    old_history_digest = canonical_digest(history_other)
    print(f"[guard] targets_all={len(all_targets)} targets_history={len(history_targets)}")
    print(f"[guard] old_all_rows={len(all_other)} old_history_rows={len(history_other)}")
    print(f"[guard] old_all_digest={old_all_digest}")
    print(f"[guard] old_history_digest={old_history_digest}")

    status_map = lp.fetch_alpha_token_status_map()
    lp._ALPHA_STATUS_MAP = status_map
    print(f"[market] alpha_status_tokens={len(status_map)}")

    first_ids = lp._first_occurrence_ids(all_rows)
    history_by_key = {target_key(row): row for row in history_targets}

    cp_all = next(row for row in all_targets if target_key(row) == CP_KEY)
    cp_history = history_by_key[CP_KEY]

    if args.cp_only:
        # CP-specific repair lane. The previous 11 qualified rows and all
        # 431 legacy rows are immutable in this mode.
        other_target_before = [
            copy.deepcopy(row) for row in all_targets
            if target_key(row) != CP_KEY
        ]
        if cp_all.get("listing_price") or cp_history.get("listing_price"):
            print("[result] CP already_enriched")
            print("[mode] DRY_RUN" if not args.apply else "[mode] APPLY_NOOP")
            print("[mutation] NONE")
            return

        probe = copy.deepcopy(cp_all)
        _event, cp_result = lp._process_one(
            probe, 1, 1, is_first_occurrence=id(cp_all) in first_ids
        )
        if not cp_result:
            print(f"[result] {CP_KEY[0]} {CP_KEY[1]} NO_DATA")
            print("[guard] non_target_rows_unchanged=PASS")
            print("[guard] other_11_targets_unchanged=PASS")
            print("[guard] target_field_scope=PASS")
            print("[mutation] NONE")
            if args.require_data or args.apply:
                raise RuntimeError("CP qualified price data unavailable")
            return

        apply_price_result(cp_all, cp_result)
        apply_price_result(cp_history, cp_result)

        if not verify_non_target_unchanged(all_before, all_rows):
            raise RuntimeError("CP dry-run changed non-target all.json rows")
        if not verify_non_target_unchanged(history_before, history_rows):
            raise RuntimeError("CP dry-run changed non-target history.json rows")
        verify_target_field_scope(all_before, all_rows)
        verify_target_field_scope(history_before, history_rows)

        other_target_after = [
            row for row in all_targets
            if target_key(row) != CP_KEY
        ]
        if canonical_digest(other_target_before) != canonical_digest(other_target_after):
            raise RuntimeError("CP repair changed one of the other 11 target rows")

        vwap = cp_result.get("vwap") if isinstance(cp_result, dict) else None
        peak = (cp_result.get("max_since") or {}).get("price") if isinstance(cp_result, dict) else None
        print(f"[result] CP {CP_KEY[1]} PASS vwap={vwap} peak={peak}")
        print("[guard] non_target_rows_unchanged=PASS")
        print("[guard] other_11_targets_unchanged=PASS")
        print("[guard] target_field_scope=PASS")

        if not args.apply:
            print("[mode] DRY_RUN")
            print("[mutation] NONE")
            return

        lp.upload_json(r2, ALL_KEY, all_rows)
        lp.upload_json(r2, HISTORY_KEY, history_rows)

        verify_all = load_required(r2, ALL_KEY)
        verify_history = load_required(r2, HISTORY_KEY)
        if not verify_non_target_unchanged(all_before, verify_all):
            raise RuntimeError("postcheck: CP apply changed non-target all.json rows")
        if not verify_non_target_unchanged(history_before, verify_history):
            raise RuntimeError("postcheck: CP apply changed non-target history.json rows")
        verify_target_field_scope(all_before, verify_all)
        verify_target_field_scope(history_before, verify_history)

        verify_targets, _ = split_targets(verify_history)
        verify_map = {target_key(row): row for row in verify_targets}
        if not verify_map[CP_KEY].get("listing_price"):
            raise RuntimeError("postcheck: CP listing_price missing after apply")
        verify_other_targets = [
            row for row in verify_targets
            if target_key(row) != CP_KEY
        ]
        if canonical_digest(other_target_before) != canonical_digest(verify_other_targets):
            raise RuntimeError("postcheck: one of the other 11 target rows changed")

        print(f"[postcheck] all={len(verify_all)} history={len(verify_history)}")
        print("[postcheck] cp_listing_price=1/1")
        print("[postcheck] other_11_targets_unchanged=PASS")
        print("[postcheck] non_target_rows_unchanged=PASS")
        print("[mutation] TARGETED_CP_PRICE_FIELDS")
        print("[done] targeted CP Alpha History price enrichment PASS")
        return

    if cp_all.get("listing_price") or cp_history.get("listing_price"):
        raise RuntimeError("CP guard failed: CP must remain NO_DATA for the legacy 11-row apply lane")

    work = []
    for row in all_targets:
        key = target_key(row)
        if key == CP_KEY:
            print("[skip] CP excluded_by_owner_approval")
            continue
        if row.get("listing_price"):
            print(f"[skip] {key[0]} {key[1]} already_enriched")
            continue
        work.append((row, key, id(row) in first_ids))

    results = {}
    total = len(work)

    def run_one(item, index):
        row, key, is_first = item
        probe = copy.deepcopy(row)
        _event, result = lp._process_one(
            probe, index, total, is_first_occurrence=is_first
        )
        return key, result

    with ThreadPoolExecutor(max_workers=max(1, min(args.workers, 4))) as pool:
        futures = {
            pool.submit(run_one, item, index): item
            for index, item in enumerate(work, start=1)
        }
        for future in as_completed(futures):
            key, result = future.result()
            results[key] = result

    filled = 0
    failed = 0
    for row in all_targets:
        key = target_key(row)
        if key == CP_KEY:
            print(f"[result] {key[0]} {key[1]} EXCLUDED_NO_DATA")
            continue
        if row.get("listing_price"):
            result = row.get("listing_price")
        else:
            result = results.get(key)

        if result:
            if not row.get("listing_price"):
                apply_price_result(row, result)
            history_row = history_by_key[key]
            if not history_row.get("listing_price"):
                apply_price_result(history_row, result)
            filled += 1
            vwap = result.get("vwap") if isinstance(result, dict) else None
            peak = (result.get("max_since") or {}).get("price") if isinstance(result, dict) else None
            print(f"[result] {key[0]} {key[1]} PASS vwap={vwap} peak={peak}")
        else:
            failed += 1
            print(f"[result] {key[0]} {key[1]} NO_DATA")

    if not verify_non_target_unchanged(all_before, all_rows):
        raise RuntimeError("non-target all.json rows changed")
    if not verify_non_target_unchanged(history_before, history_rows):
        raise RuntimeError("non-target history.json rows changed")
    verify_target_field_scope(all_before, all_rows)
    verify_target_field_scope(history_before, history_rows)

    print(f"[plan] target_total=12 apply_targets=11 enriched_or_existing={filled} no_data={failed}")
    print("[guard] non_target_rows_unchanged=PASS")
    print("[guard] target_field_scope=PASS")
    print("[guard] cp_untouched=PASS")

    if not args.apply:
        print("[mode] DRY_RUN")
        print("[mutation] NONE")
        return

    if filled != len(APPLY_TARGETS):
        raise RuntimeError(
            f"refusing apply: qualified target count changed "
            f"filled={filled} expected={len(APPLY_TARGETS)}"
        )

    lp.upload_json(r2, ALL_KEY, all_rows)
    lp.upload_json(r2, HISTORY_KEY, history_rows)

    verify_all = load_required(r2, ALL_KEY)
    verify_history = load_required(r2, HISTORY_KEY)
    if not verify_non_target_unchanged(all_before, verify_all):
        raise RuntimeError("postcheck: non-target all.json changed")
    if not verify_non_target_unchanged(history_before, verify_history):
        raise RuntimeError("postcheck: non-target history.json changed")
    verify_target_field_scope(all_before, verify_all)
    verify_target_field_scope(history_before, verify_history)

    verify_targets, _ = split_targets(verify_history)
    verify_map = {target_key(row): row for row in verify_targets}
    verify_filled = sum(
        1 for key, row in verify_map.items()
        if key in APPLY_TARGETS and row.get("listing_price")
    )
    if verify_map[CP_KEY].get("listing_price"):
        raise RuntimeError("postcheck: CP was modified despite owner exclusion")
    print(f"[postcheck] all={len(verify_all)} history={len(verify_history)}")
    print(f"[postcheck] approved_target_listing_price={verify_filled}/11")
    print("[postcheck] cp_untouched=PASS")
    print("[postcheck] non_target_rows_unchanged=PASS")
    print("[mutation] TARGETED_11_PRICE_FIELDS")
    print("[done] targeted Alpha History price enrichment PASS")


if __name__ == "__main__":
    main()
