"""Historical Binance Spot recovery for migrated, identity-fenced Alpha events.

Writes *only* in-memory event fields. The existing sync_listing_prices.py
CAS-paired writer persists them to the same All / History R2 keys.

Daily HIGH is used exclusively as a mathematically valid upper bound:
a day's highest traded five-minute VWAP cannot exceed that day's highest
trade. Every published Peak is derived from actual executed quote/base
volume and is gated by a minimum traded notional and trade count.
"""
from datetime import datetime, timezone
import math
import time

from alpha_event_peak_liquidity import (
    METHOD, WINDOW_MS, bar_vwap, qualify_bar, quote_floor, utc_day, parse_day_ms
)
from alpha_event_spot_allowlist import SPOT_MISSING_PEAK_ALLOWLIST

DAY_MS = 86_400_000
REPAIR_KEY = "_spot_peak_recovery_v1"
RECOVERY_VERSION = 1
CLAIM_METHOD = "event_5m_quote_vwap"


def _positive(value):
    try:
        n = float(value)
        return math.isfinite(n) and n > 0
    except (ValueError, TypeError, OverflowError):
        return False


def _event_ms(event):
    try:
        raw = event.get("event_time")
        stamp = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return int((stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _identity(event):
    """Contract identity is pinned; first-day Spot trading is checked separately."""
    sym = str(event.get("symbol") or event.get("token") or "").upper().strip()
    ref = SPOT_MISSING_PEAK_ALLOWLIST.get(sym)
    if not ref or str(event.get("chain_id")) != ref["chain"]:
        return None
    addr = str(event.get("contract_address") or "")
    if ref["chain"] == "501":  # Solana addresses are case-sensitive
        same = addr == ref["contract"]
    else:
        same = addr.lower() == ref["contract"].lower()
    if not same:
        return None
    if not (event.get("spot_listed") or event.get("is_spot_listed")):
        return None
    day = str(event.get("spot_listed_at") or "")[:10]
    quoted = event.get("spot_listing_price")
    if (day != ref["first_spot_day"] or not isinstance(quoted, dict)
            or str(quoted.get("date") or "")[:10] != day
            or not _positive(quoted.get("vwap"))):
        return None
    if not isinstance(event.get("listing_price"), dict):
        return None
    return sym, parse_day_ms(day)


def _klines(fetch, symbol, interval, start_ms, end_ms=None, limit=1000):
    try:
        data = fetch(symbol, interval, start_ms, end_ms, limit)
        return data if isinstance(data, list) else None
    except Exception:
        return None


def _exact_claim(event, symbol, start_ms, fetch):
    """VWAP for first five clock minutes that actually trade after event/Spot start."""
    rows = _klines(fetch, symbol, "1m", start_ms, None, 12)
    if not rows:
        return None
    try:
        candidates = sorted(
            [k for k in rows if int(float(k[0])) >= start_ms and
             bar_vwap(k) is not None],
            key=lambda k: int(float(k[0])),
        )
    except (IndexError, TypeError, ValueError, OverflowError):
        return None
    if not candidates:
        return None
    first_ms = int(float(candidates[0][0]))
    eligible = [k for k in candidates if first_ms <= int(float(k[0])) < first_ms + WINDOW_MS]
    base = sum(float(k[5]) for k in eligible)
    quote = sum(float(k[7]) for k in eligible)
    trades = sum(int(float(k[8])) for k in eligible)
    if base <= 0 or quote <= 0 or trades < 3:
        return None
    claim = quote / base
    if quote < quote_floor(event, claim):
        return None
    return claim, quote, first_ms, trades


def _daily_envelope(symbol, start_ms, now_ms, fetch):
    """Require contiguous daily witness data from the event's Spot entry day."""
    today = (now_ms // DAY_MS) * DAY_MS
    rows = _klines(fetch, symbol, "1d", start_ms, now_ms, 1000)
    if not rows:
        return None
    parsed = {}
    for bar in rows:
        try:
            ts = int(float(bar[0]))
            high = float(bar[2])
            quote = float(bar[7])
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
        if ts % DAY_MS or ts < start_ms or ts > today or not _positive(high):
            continue
        parsed[ts] = {"day_ms": ts, "high": high, "quote": quote}
    expected = (today - start_ms) // DAY_MS + 1
    if expected > 1000 or len(parsed) != expected:
        return None
    return list(parsed.values())


def _verified_day_bars(symbol, day, first_spot_day_ms, now_ms, fetch):
    day_ms = day["day_ms"]
    end_ms = min(day_ms + DAY_MS - 1, now_ms - WINDOW_MS)
    if end_ms < day_ms:
        return None
    rows = _klines(fetch, symbol, "5m", day_ms, end_ms, 288)
    if not rows:
        return None
    seen = set()
    total_quote = 0.0
    for bar in rows:
        try:
            ts = int(float(bar[0]))
            quote = float(bar[7])
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if ts < day_ms or ts > end_ms or ts % WINDOW_MS or ts in seen or quote < 0:
            return None
        seen.add(ts)
        total_quote += quote
    ordered = sorted(rows, key=lambda b: int(float(b[0])))
    last_closed = (end_ms // WINDOW_MS) * WINDOW_MS
    if int(float(ordered[-1][0])) != last_closed:
        return None
    if day_ms != first_spot_day_ms and int(float(ordered[0][0])) != day_ms:
        return None
    first = int(float(ordered[0][0]))
    if len(seen) != ((last_closed - first) // WINDOW_MS + 1):
        return None
    # For a closed historical day the 5m volumes must agree with the 1d
    # total. This catches missing/corrupted pagination and false completeness.
    if day_ms < (now_ms // DAY_MS) * DAY_MS and day["quote"] > 0:
        if abs(total_quote - day["quote"]) > max(1.0, day["quote"] * .0001):
            return None
    return ordered


def _recover_one(event, *, fetch, now_ms, max_candidate_days):
    spot = _identity(event)
    if not spot:
        return False, "identity_unverified", False
    symbol, day_ms = spot
    if day_ms is None:
        return False, "spot_day_invalid", False

    start = _event_ms(event)
    if start is None or start > now_ms or day_ms > now_ms:
        return False, "event_time_invalid", False
    # Exchange listing and reward announcement can be on different days.
    # Price the event only after BOTH the reward event and real Spot trading.
    anchor = max(start, day_ms)
    first_day = _klines(fetch, symbol, "1d", 0, None, 1)
    if not first_day:
        return False, "first_trade_day_unavailable", False
    try:
        listed_day = int(float(first_day[0][0]))
    except (TypeError, ValueError, IndexError, OverflowError):
        return False, "first_trade_day_invalid", False
    if listed_day != day_ms:
        return False, "ticker_first_day_mismatch", False

    lp = event["listing_price"]
    state = lp.get(REPAIR_KEY) if isinstance(lp.get(REPAIR_KEY), dict) else {}
    if (state.get("version") != RECOVERY_VERSION or
            state.get("anchor_ms") != anchor or
            state.get("contract") != str(event.get("contract_address"))):
        state = {"version": RECOVERY_VERSION, "anchor_ms": anchor,
                 "contract": event["contract_address"], "checked_days": [],
                 "best_price": None, "best_date": None}

    # Claim can only be seeded by real volume, not the old (possibly
    # impossible) 24-hour HLC3 price in R2.
    claim_data = _exact_claim(event, symbol, anchor, fetch)
    if not claim_data:
        return False, "spot_claim_1m_unavailable", False
    claim, claim_quote, first_minute, trades = claim_data
    best = claim
    best_date = utc_day(first_minute)
    previous = float(state.get("best_price")) if _positive(state.get("best_price")) else 0
    if previous > best and state.get("best_date"):
        best, best_date = previous, state["best_date"]
    # Partial scans preserve all fully checked historical candidate dates.
    already = set(str(x) for x in state.get("checked_days", []) if isinstance(x, str))
    scan_start = max(day_ms, (anchor // DAY_MS) * DAY_MS)
    days = _daily_envelope(symbol, scan_start, now_ms, fetch)
    if not days:
        return False, "spot_daily_coverage_unavailable", False

    queried = 0
    held = False
    for day in sorted(days, key=lambda v: (-v["high"], v["day_ms"])):
        dstr = utc_day(day["day_ms"])
        current_day = day["day_ms"] == (now_ms // DAY_MS) * DAY_MS
        if day["high"] <= best:
            continue  # daily HIGH is an upper bound, not a reported Peak
        if not current_day and dstr in already:
            continue
        if queried >= max_candidate_days:
            held = True
            break
        bars = _verified_day_bars(symbol, day, day_ms, now_ms, fetch)
        if bars is None:
            held = True
            break
        queried += 1
        floor = quote_floor(event, claim)
        for bar in bars:
            ts = int(float(bar[0]))
            if ts < anchor:
                continue
            v = qualify_bar(bar, floor)
            if v is not None and v > best:
                best = v
                best_date = utc_day(ts)
        if not current_day:
            already.add(dstr)

    pending = sum(
        1 for d in days if d["high"] > best and (
            d["day_ms"] == (now_ms // DAY_MS) * DAY_MS or
            utc_day(d["day_ms"]) not in already)
    )
    complete = not held and not pending

    old_claim = lp.get("vwap")
    old_peak = lp.get("max_since")
    old_p = float(old_peak["price"]) if isinstance(old_peak, dict) and _positive(old_peak.get("price")) else None
    # Same in-memory event; existing CAS writer publishes both fields as one
    # tuple, so website never observes an invalid half-updated record.
    lp.update({
        "vwap": claim, "date": utc_day(first_minute),
        "event_start_ms": start, "claim_method": CLAIM_METHOD,
        "claim_quote_usd": claim_quote, "claim_source": "binance_spot_1m_historical",
        "max_since": {"price": best, "date": best_date},
        "peak_method": METHOD,
        "peak_quality": "spot_only_liquidity_verified_asof" if complete else "partial_spot_history",
        "peak_scope": "spot_since_verified_listing",  # no claim of unobserved Alpha history
    })
    # Preserve legacy retry receipts even after successful recovery.
    # The existing archived-price guard forbids erasing non-null fields
    # inside listing_price. The new claim_method / recovery checkpoint
    # already prevent these stale retry timestamps from re-triggering work.
    lp[REPAIR_KEY] = {
        **state, "version": RECOVERY_VERSION, "anchor_ms": anchor,
        "contract": event["contract_address"],
        "first_spot_day_ms": day_ms,
        "checked_days": sorted(already),
        "best_price": best, "best_date": best_date,
        "checked_at": datetime.fromtimestamp(now_ms / 1000,timezone.utc).isoformat(),
        "status": "spot_complete_asof" if complete else "spot_partial",
        "pending_days": pending,
        "claim_start_ms": first_minute,
        "claim_trades": trades,
    }
    event["ath_since_listing_price"] = best
    event["ath_since_listing_date"] = best_date
    print(f"  [spot-peak-recovered] {symbol}: "
          f"claim={old_claim}->{claim:.9g} peak={old_p}->{best:.9g} "
          f"trades={trades} scanned_days={queried} pending={pending} "
          f"scope=spot-only", flush=True)
    return True, "spot_complete_asof" if complete else "spot_partial", old_p is None


def repair_missing_spot_peaks(events, *, fetch, now_ms=None,
                              max_events=25, max_candidate_days=10,
                              max_seconds=240):
    """Bounded recovery; no additional service or external write.

    Missing price rows first, then rotating previously recovered Spot rows.
    Unknown/reused ticker+contract combinations are not admitted.
    """
    now_ms = int(now_ms or time.time() * 1000)
    deadline = time.monotonic() + max_seconds
    selected = []
    for e in events:
        info = _identity(e)
        if info is None:
            continue
        lp = e["listing_price"]
        previous = lp.get(REPAIR_KEY)
        peak = lp.get("max_since")
        missing = not (isinstance(peak, dict) and _positive(peak.get("price")))
        # Explicit ROBO repair only: Binance's 2026-03-04 Spot listing
        # announcement independently confirms this contract/chain and pair.
        # Do not apply a ticker-based contradiction fix to other assets.
        verified_robo_contradiction = (
            str(e.get("symbol") or "").upper() == "ROBO"
            and _positive(lp.get("vwap"))
            and isinstance(peak, dict)
            and _positive(peak.get("price"))
            and float(peak["price"]) < float(lp["vwap"])
            and lp.get("claim_method") != CLAIM_METHOD
        )
        if (not missing and not verified_robo_contradiction
                and not (isinstance(previous, dict)
                         and previous.get("version") == RECOVERY_VERSION)):
            continue
        checked_at = (previous or {}).get("checked_at") or ""
        selected.append((0 if missing else 1, str(checked_at), str(e.get("event_time") or ""), e))
    selected.sort(key=lambda item: (item[0], item[1], item[2]))
    chosen = selected[:max_events]
    counts = {"eligible": len(selected), "selected": len(chosen),
              "recovered": 0, "updated": 0, "held": 0,
              "complete_asof": 0, "budget_exhausted": 0}
    for _,_,_,e in chosen:
        if time.monotonic() >= deadline:
            counts["budget_exhausted"] += 1
            break
        success, status, recovered = _recover_one(
            e, fetch=fetch, now_ms=now_ms, max_candidate_days=max_candidate_days)
        counts["updated"] += int(success)
        counts["recovered"] += int(recovered)
        counts["complete_asof"] += int(status == "spot_complete_asof")
        counts["held"] += int(not success)
        if not success:
            print(f"  [spot-peak-held] {e.get('symbol')}: {status}", flush=True)
    return counts