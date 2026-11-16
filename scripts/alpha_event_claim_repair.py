"""Bounded, evidence-based migration of legacy Alpha Event Claim/Peak prices.

No R2 writes here: updates existing in-memory All/History event rows, which
are published only through the existing CAS-verified sync_listing_prices writer.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import math

EXACT_CLAIM_METHOD = "event_5m_quote_vwap"
RETRY_FIELD = "_claim_5m_repair_retry_after_utc"


def positive(value):
    try:
        n = float(value)
        return math.isfinite(n) and n > 0
    except (ValueError, TypeError, OverflowError):
        return False


def instant(value):
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def requires_repair(event):
    lp = event.get("listing_price")
    return (isinstance(lp, dict)
            and positive(lp.get("vwap"))
            and lp.get("claim_method") != EXACT_CLAIM_METHOD
            and bool(event.get("contract_address"))
            and bool(event.get("event_time"))
            and instant(event.get("event_time")) is not None)


def candidates(events, *, now=None, limit=16):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if not isinstance(limit, int) or not 1 <= limit <= 32:
        raise ValueError("bounded repair batch required")

    items = []
    for row in events:
        if not requires_repair(row):
            continue
        lp = row["listing_price"]
        retry = instant(lp.get(RETRY_FIELD))
        if retry and retry > now:
            continue
        claim = float(lp["vwap"])
        peak = lp.get("max_since")
        peak_value = peak.get("price") if isinstance(peak, dict) else None
        broken = not positive(peak_value) or float(peak_value) < claim
        # Inconsistent rows first; within them prioritize existing Spot rows
        # where verified one-minute history can correct historical 24h VWAP.
        spot = bool(row.get("spot_listed") or row.get("is_spot_listed"))
        items.append((0 if broken else 1, 0 if spot else 1,
                      -instant(row.get("event_time") or row.get("date")).timestamp(),
                      row))
    items.sort(key=lambda t: (t[0], t[1], t[2]), reverse=False)
    return [t[-1] for t in items[:limit]]


def verified_replacement(event, replacement):
    if not isinstance(replacement, dict):
        return False
    if replacement.get("claim_method") != EXACT_CLAIM_METHOD:
        return False
    if not positive(replacement.get("vwap")) or not positive(replacement.get("claim_quote_usd")):
        return False
    anchor = instant(event.get("event_time") or event.get("date"))
    start = replacement.get("event_start_ms")
    try:
        if anchor is None or int(start) != int(anchor.timestamp() * 1000):
            return False
    except (ValueError, TypeError, OverflowError):
        return False
    first_day = instant(str(replacement.get("date") or "")[:10])
    return bool(first_day and first_day.date() >= anchor.date())


def apply_replacement(event, replacement):
    """Replace only when Claim source/time and exact volume are verified.

    A genuine traded 5m Claim VWAP is an achievable benchmark and thus
    a valid lower bound for Peak. Never floor Peak with old 24h VWAP.
    """
    if not verified_replacement(event, replacement):
        return False
    old = event["listing_price"]
    claim = float(replacement["vwap"])
    old_peak = old.get("max_since")
    new_peak = claim
    peak_date = replacement["date"]
    if (isinstance(old_peak, dict)
            and positive(old_peak.get("price"))
            and float(old_peak["price"]) >= claim
            and instant(old_peak.get("date"))
            and instant(old_peak["date"]).date() >= instant(event.get("event_time") or event.get("date")).date()):
        # Last-good legacy high remains *provisional* until full 5m scan.
        new_peak, peak_date = float(old_peak["price"]), old_peak["date"]

    updated = dict(old)
    updated.update(replacement)
    updated["max_since"] = {"price": new_peak, "date": peak_date}
    updated["peak_quality"] = "pending_liquidity_scan"
    # Archive guard forbids removing any non-null legacy subfield.
    # Retain old retry/peak receipts; new claim_method already means the
    # row will not be selected for this legacy migration again.
    if isinstance(old.get("_peak_liquidity_v1"), dict):
        # Preserve prior verified scanning work for the same event anchor.
        updated["_peak_liquidity_v1"] = old["_peak_liquidity_v1"]

    event["listing_price"] = updated
    event["ath_since_listing_price"] = new_peak
    event["ath_since_listing_date"] = peak_date
    return True


def repair_legacy_claims(events, *, fetcher, now=None, limit=16, max_workers=4):
    now = now or datetime.now(timezone.utc)
    selected = candidates(events, now=now, limit=limit)
    total_pending = sum(int(requires_repair(e)) for e in events)
    counts = {"pending": total_pending, "selected": len(selected), "repaired": 0,
              "held": 0, "legacy_below_claim_remaining": 0}
    if selected:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            tasks = {pool.submit(fetcher, row): row for row in selected}
            for future in as_completed(tasks):
                row = tasks[future]
                try:
                    fetched = future.result()
                except Exception:
                    fetched = None
                old_claim = float(row["listing_price"]["vwap"])
                old_peak = (row["listing_price"].get("max_since") or {}).get("price")
                if apply_replacement(row, fetched):
                    counts["repaired"] += 1
                    lp = row["listing_price"]
                    print(f"  [claim-5m-repaired] {row.get('symbol') or row.get('token')} "
                          f"Claim={old_claim:.9g}->{lp['vwap']:.9g} "
                          f"Peak={old_peak}->{lp['max_since']['price']:.9g} "
                          f"source={lp['claim_source']}", flush=True)
                else:
                    row["listing_price"][RETRY_FIELD] = (now + timedelta(hours=24)).isoformat()
                    counts["held"] += 1
                    print(f"  [claim-5m-held] {row.get('symbol') or row.get('token')}: "
                          "quote-volume/contract/anchor not fully verified; keeping old prices", flush=True)

    for row in events:
        lp = row.get("listing_price")
        if not isinstance(lp, dict) or not positive(lp.get("vwap")):
            continue
        peak = lp.get("max_since")
        if isinstance(peak, dict) and positive(peak.get("price")) and float(peak["price"]) < float(lp["vwap"]):
            counts["legacy_below_claim_remaining"] += 1
    return counts