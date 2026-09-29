"""
Targeted XDP Alpha History price enrichment.

Purpose:
- enrich exactly one canonical XDP History identity already appended to
  alpha-events/all.json and alpha-events/history.json;
- calculate claim/listing price and ATH using the current canonical
  sync_listing_prices.py logic;
- mutate ONLY:
    listing_price
    ath_since_listing_price
    ath_since_listing_date
- dry-run by default; --apply required for R2 writes.

This intentionally does NOT call broad maintenance paths in
sync_listing_prices.py.
"""

import argparse
import copy
import hashlib
import json

import sync_listing_prices as lp

ALL_KEY = "alpha-events/all.json"
HISTORY_KEY = "alpha-events/history.json"

TARGET = (
    "XDP",
    "2026-09-28T14:30:00+00:00",
    "0x07b3d902783c3c12b077508c3b5c00113d1291d0",
    "8453",
)

PRE_DIGEST = "bb7d79e92f4915abf44ba525d60fadb045cd7c19e3d00489a683709ebbebb9a4"

ALLOWED_FIELDS = {
    "listing_price",
    "ath_since_listing_price",
    "ath_since_listing_date",
}


def canonical_digest(rows):
    raw = json.dumps(
        rows,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def target_key(row):
    return (
        str(row.get("symbol") or row.get("token") or "").upper(),
        str(row.get("event_time") or ""),
        str(row.get("contract_address") or "").lower(),
        str(row.get("chain_id") or ""),
    )


def load_required(r2, key):
    rows = lp.load_json(r2, key)
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"cannot read required canonical list {key}")
    return rows


def split_target(rows):
    target_rows = [row for row in rows if target_key(row) == TARGET]
    if len(target_rows) != 1:
        raise RuntimeError(f"target uniqueness failed: {len(target_rows)}")
    other_rows = [row for row in rows if target_key(row) != TARGET]
    return target_rows[0], other_rows


def verify_field_scope(before, after):
    fields = set(before) | set(after)
    changed = {f for f in fields if before.get(f) != after.get(f)}
    if not changed.issubset(ALLOWED_FIELDS):
        raise RuntimeError(f"target field-scope violation: {sorted(changed)}")
    return changed


def apply_result(row, result):
    row["listing_price"] = result
    ms = result.get("max_since") if isinstance(result, dict) else None
    row["ath_since_listing_price"] = ms.get("price") if isinstance(ms, dict) else None
    row["ath_since_listing_date"] = ms.get("date") if isinstance(ms, dict) else None


def validate_result(result):
    if not isinstance(result, dict):
        raise RuntimeError("XDP price result missing")
    try:
        vwap = float(result.get("vwap"))
        peak = float((result.get("max_since") or {}).get("price"))
    except Exception as exc:
        raise RuntimeError("XDP price result incomplete") from exc
    peak_date = str((result.get("max_since") or {}).get("date") or "")
    if vwap <= 0 or peak <= 0 or not peak_date:
        raise RuntimeError("XDP price result failed positivity/date guard")
    if str(result.get("date") or "")[:10] != "2026-09-28":
        raise RuntimeError(f"XDP listing date unexpected: {result.get('date')}")
    return vwap, peak, peak_date


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    r2 = lp.get_r2()
    all_rows = load_required(r2, ALL_KEY)
    history_rows = load_required(r2, HISTORY_KEY)

    if len(all_rows) != 444 or len(history_rows) != 444:
        raise RuntimeError(
            f"count guard failed all={len(all_rows)} history={len(history_rows)}"
        )

    all_digest = canonical_digest(all_rows)
    history_digest = canonical_digest(history_rows)
    print(f"[guard] all_count={len(all_rows)} history_count={len(history_rows)}")
    print(f"[guard] all_digest={all_digest}")
    print(f"[guard] history_digest={history_digest}")
    if all_digest != PRE_DIGEST or history_digest != PRE_DIGEST:
        raise RuntimeError("canonical pre-digest guard failed")

    all_before = copy.deepcopy(all_rows)
    history_before = copy.deepcopy(history_rows)

    xdp_all, all_other = split_target(all_rows)
    xdp_history, history_other = split_target(history_rows)

    if canonical_digest(all_other) != canonical_digest(history_other):
        raise RuntimeError("non-target all/history baseline mismatch")

    non_target_digest = canonical_digest(all_other)
    print(f"[guard] non_target_rows={len(all_other)}")
    print(f"[guard] non_target_digest={non_target_digest}")

    existing_all = xdp_all.get("listing_price")
    existing_history = xdp_history.get("listing_price")
    if existing_all or existing_history:
        if existing_all != existing_history:
            raise RuntimeError("XDP all/history listing_price mismatch")
        print("[result] XDP already_enriched")
        print(json.dumps({
            "listing_price": existing_all,
            "ath_since_listing_price": xdp_all.get("ath_since_listing_price"),
            "ath_since_listing_date": xdp_all.get("ath_since_listing_date"),
        }, ensure_ascii=False, sort_keys=True))
        print("[mutation] NONE")
        return

    status_map = lp.fetch_alpha_token_status_map()
    lp._ALPHA_STATUS_MAP = status_map

    st = status_map.get("XDP")
    if st:
        contract = str(st.get("contractAddress") or "").lower()
        chain = str(st.get("chainId") or "")
        if contract and contract != TARGET[2]:
            raise RuntimeError(f"XDP Binance contract mismatch: {contract}")
        if chain and chain != TARGET[3]:
            raise RuntimeError(f"XDP Binance chain mismatch: {chain}")
        print(json.dumps({
            "binance_symbol": "XDP",
            "alphaId": st.get("alphaId"),
            "listingTime": st.get("listingTime"),
            "contractAddress": contract or None,
            "chainId": chain or None,
        }, sort_keys=True))

    first_ids = lp._first_occurrence_ids(all_rows)
    is_first = id(xdp_all) in first_ids
    probe = copy.deepcopy(xdp_all)
    _event, result = lp._process_one(
        probe, 1, 1, is_first_occurrence=is_first
    )

    vwap, peak, peak_date = validate_result(result)
    claim_value = vwap * 1666.0

    print(f"[result] XDP_CLAIM_VWAP={vwap:.12g}")
    print(f"[result] XDP_ATH_CLOSE={peak:.12g}")
    print(f"[result] XDP_ATH_DATE={peak_date}")
    print(f"[result] XDP_1666_VALUE_AT_CLAIM={claim_value:.12g}")

    apply_result(xdp_all, result)
    apply_result(xdp_history, result)

    if canonical_digest(all_other) != non_target_digest:
        raise RuntimeError("non-target all rows changed")
    if canonical_digest(history_other) != non_target_digest:
        raise RuntimeError("non-target history rows changed")

    changed_all = verify_field_scope(
        split_target(all_before)[0], xdp_all
    )
    changed_history = verify_field_scope(
        split_target(history_before)[0], xdp_history
    )
    if changed_all != changed_history:
        raise RuntimeError("all/history target field changes differ")

    planned_all_digest = canonical_digest(all_rows)
    planned_history_digest = canonical_digest(history_rows)
    if planned_all_digest != planned_history_digest:
        raise RuntimeError("planned all/history digest mismatch")

    print(f"[plan] changed_fields={','.join(sorted(changed_all))}")
    print(f"[plan] post_digest={planned_all_digest}")
    print("[guard] non_target_rows_unchanged=PASS")
    print("[guard] target_field_scope=PASS")

    if not args.apply:
        print("[mode] DRY_RUN")
        print("[mutation] NONE")
        return

    lp.upload_json(r2, ALL_KEY, all_rows)
    lp.upload_json(r2, HISTORY_KEY, history_rows)

    verify_all = load_required(r2, ALL_KEY)
    verify_history = load_required(r2, HISTORY_KEY)

    if len(verify_all) != 444 or len(verify_history) != 444:
        raise RuntimeError("postcheck count failed")

    verify_xdp_all, verify_all_other = split_target(verify_all)
    verify_xdp_history, verify_history_other = split_target(verify_history)

    if canonical_digest(verify_all_other) != non_target_digest:
        raise RuntimeError("postcheck non-target all rows changed")
    if canonical_digest(verify_history_other) != non_target_digest:
        raise RuntimeError("postcheck non-target history rows changed")

    if canonical_digest(verify_all) != planned_all_digest:
        raise RuntimeError("postcheck all digest mismatch")
    if canonical_digest(verify_history) != planned_history_digest:
        raise RuntimeError("postcheck history digest mismatch")

    for row in (verify_xdp_all, verify_xdp_history):
        if row.get("listing_price") != result:
            raise RuntimeError("postcheck XDP listing_price mismatch")
        if float(row.get("ath_since_listing_price") or 0) != peak:
            raise RuntimeError("postcheck XDP ATH mismatch")
        if str(row.get("ath_since_listing_date") or "") != peak_date:
            raise RuntimeError("postcheck XDP ATH date mismatch")

    print(f"[postcheck] all=444 history=444")
    print(f"[postcheck] digest={planned_all_digest}")
    print("[postcheck] XDP_unique=1/1")
    print("[postcheck] non_target_rows_unchanged=PASS")
    print("[mutation] TARGETED_XDP_PRICE_FIELDS")
    print("[done] targeted XDP Alpha History price enrichment PASS")


if __name__ == "__main__":
    main()
