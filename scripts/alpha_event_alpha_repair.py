"""Exact-contract Binance Alpha Claim and volume-qualified Peak repair.

For legacy 24h/HLC3 rows and missing Claim, use the original Binance Alpha
symbol+chain+contract and trade-open clock. No ticker-only fallback.

Executed-price Claim: first five minutes of Binance 1m quote/base volume.
Peak: highest volume-qualified 5m VWAP. Daily HIGH is ONLY an upper-bound
filter. Missing/gapped data is marked partial and cannot certify a global ATH.

Updates existing in-memory event price objects; the original All/History
CAS writer is the only production writer. No frontend or Cloudflare changes.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
import math
import re
import time

from alpha_event_peak_liquidity import (
    METHOD, WINDOW_MS, bar_vwap, qualify_bar, quote_floor, utc_day
)
from alpha_event_spot_backfill import DAY_MS, _verified_day_bars

CLAIM_METHOD = "event_5m_quote_vwap"
RECOVERY_KEY = "_alpha_legacy_repair_v1"
MAX_ANCHOR_LAG_MS = 24 * 60 * 60 * 1000


def positive(value):
    try:
        n = float(value)
        return math.isfinite(n) and n > 0
    except (ValueError, TypeError, OverflowError):
        return False


def event_ms(event):
    """Accept the exact +00 shorthand from old events, never invent a clock."""
    raw = str(event.get("event_time") or "").strip()
    if not raw:
        return None
    raw = re.sub(r"([+-]\d\d)$", r"\1:00", raw.replace("Z", "+00:00"))
    try:
        value = datetime.fromisoformat(raw)
        if not value.tzinfo or value.utcoffset() is None:
            return None  # no interpreted local or fabricated timezone
        return int(value.timestamp() * 1000)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def is_candidate(event):
    lp = event.get("listing_price")
    if not lp or not positive(lp.get("vwap")):
        return True
    peak = lp.get("max_since")
    price = peak.get("price") if isinstance(peak, dict) else None
    if not positive(price) or float(price) < float(lp["vwap"]):
        return True
    # An event with only partially scanned 5m Peak history is not finished.
    # Revisit it on a bounded cadence using its existing checked-day state.
    return (lp.get("peak_quality") == "partial_alpha_history"
            and isinstance(lp.get(RECOVERY_KEY), dict)
            and lp[RECOVERY_KEY].get("version") == 1)


def fetch_rows(fetch, alpha_id, interval, start_ms, end_ms=None, limit=1500):
    try:
        data = fetch(alpha_id, interval, start_ms, end_ms, limit)
        return data if isinstance(data, list) else None
    except Exception:
        return None


def verified_claim(event, alpha_id, listing_ms, fetch):
    """Direct official Alpha 1m read; not dependent on an aggregate proxy.

    startTime is the later of the event instant and contract-matched
    Binance listingTime. No attempt to value before trading opens.
    """
    event_start = event_ms(event)
    if event_start is None:
        return None, "event_time_unverified"
    try:
        listing = int(listing_ms or 0)
    except (TypeError, ValueError, OverflowError):
        return None, "listing_time_invalid"
    anchor = max(event_start, listing)
    rows = fetch_rows(fetch, alpha_id, "1m", anchor, anchor + 15 * 60000, 15)
    if not rows:
        return None, "alpha_1m_unavailable"

    valid = []
    for row in rows:
        try:
            stamp = int(float(row[0]))
            v = bar_vwap(row)
            if v and anchor <= stamp <= anchor + 15 * 60000:
                valid.append((stamp, row))
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
    if not valid:
        # Some Alpha listings have zero executed volume for the initial
        # 15 minutes. Find the FIRST later traded 5m bar (within 24h)
        # and verify its price using fresh official 1m executions.
        # Do not shift the window when any early trade occurred: that
        # would cherry-pick a better Claim instead of a sell-now estimate.
        try:
            empty_only = all(float(row[5]) <= 0 and float(row[7]) <= 0
                             for row in rows)
        except (ValueError, TypeError, IndexError, OverflowError):
            empty_only = False
        if not empty_only:
            return None, "alpha_1m_unverifiable_volume"
        later = fetch_rows(fetch, alpha_id, "5m",
                           anchor + 15 * 60000, anchor + MAX_ANCHOR_LAG_MS, 288)
        if not later:
            return None, "alpha_1m_no_trade"
        possible = []
        for row in later:
            try:
                stamp = int(float(row[0]))
                if (anchor + 15 * 60000 <= stamp <= anchor + MAX_ANCHOR_LAG_MS
                        and bar_vwap(row) is not None):
                    possible.append(stamp)
            except (TypeError, ValueError, IndexError, OverflowError):
                continue
        if not possible:
            return None, "alpha_1m_no_trade"
        first_five = min(possible)
        fresh = fetch_rows(fetch, alpha_id, "1m",
                           first_five, first_five + WINDOW_MS - 1, 6)
        if not fresh:
            return None, "alpha_1m_delayed_trade_unavailable"
        valid = []
        for row in fresh:
            try:
                stamp = int(float(row[0]))
                if first_five <= stamp < first_five + WINDOW_MS and bar_vwap(row):
                    valid.append((stamp, row))
            except (TypeError, ValueError, IndexError, OverflowError):
                continue
        if not valid:
            return None, "alpha_1m_delayed_trade_unavailable"
    valid.sort(key=lambda z: z[0])
    first = valid[0][0]
    if first - anchor > MAX_ANCHOR_LAG_MS:
        return None, "trade_time_too_late"

    window = [(ts, row) for ts, row in valid if first <= ts < first + WINDOW_MS]
    if not window:
        return None, "no_claim_window"
    base = sum(float(row[5]) for _, row in window)
    quote = sum(float(row[7]) for _, row in window)
    trades = sum(int(float(row[8])) for _, row in window)
    if base <= 0 or quote <= 0 or trades < 3:
        return None, "insufficient_executed_trades"
    price = quote / base
    if quote < quote_floor(event, price):
        return None, "insufficient_claim_liquidity"
    return {
        "vwap": price, "quote": quote, "trades": trades,
        "first_ms": first, "anchor_ms": anchor,
        "event_start_ms": event_start,
        "open": float(window[0][1][1]),
        "close": float(window[-1][1][4]),
    }, "ok"


def _daily_covers(data, start_ms, now_ms):
    """Return daily candles, plus a separate coverage-completeness flag.

    Empty/invalid daily rows are never proof that the market was inactive.
    """
    if not data:
        return [], False
    start_day = start_ms // DAY_MS * DAY_MS
    today = now_ms // DAY_MS * DAY_MS
    parsed = {}
    for row in data:
        try:
            stamp = int(float(row[0]))
            high = float(row[2])
            quote = float(row[7])
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
        if (stamp % DAY_MS or stamp < start_day or stamp > today or
                not positive(high) or not math.isfinite(quote) or quote < 0):
            continue
        parsed[stamp] = {"day_ms": stamp, "high": high, "quote": quote}
    days = [parsed[k] for k in sorted(parsed)]
    expected = (today - start_day) // DAY_MS + 1
    complete = len(days) == expected and (not days or days[0]["day_ms"] == start_day)
    return days, complete


def qualified_peak(event, claim, alpha_id, fetch, now_ms, max_days=5):
    """Highest verified 5m VWAP, not the daily wick or daily close."""
    best = claim["vwap"]
    best_date = utc_day(claim["first_ms"])
    start_ms = claim["first_ms"] // DAY_MS * DAY_MS
    existing = (event.get("listing_price") or {}).get(RECOVERY_KEY)
    valid_state = (isinstance(existing, dict) and existing.get("version") == 1
                   and existing.get("anchor_ms") == claim["anchor_ms"]
                   and existing.get("alpha_id") == alpha_id)
    state = existing if valid_state else {}
    if positive(state.get("best_price")) and state.get("best_date"):
        if float(state["best_price"]) > best:
            best, best_date = float(state["best_price"]), state["best_date"]
    checked = set(str(s) for s in state.get("checked_days", []) if isinstance(s, str))
    data = fetch_rows(fetch, alpha_id, "1d", start_ms, now_ms, 1500)
    days, full_daily = _daily_covers(data, start_ms, now_ms)
    if not days:
        # A retriable partial record must still have a versioned checkpoint.
        # Without this, is_candidate() would silently abandon the Peak
        # after repairing only its first-five-minute Claim floor.
        pending = dict(state)
        pending.update({
            "version": 1, "alpha_id": alpha_id,
            "anchor_ms": claim["anchor_ms"],
            "best_price": best, "best_date": best_date,
            "checked_days": sorted(checked),
            "status": "alpha_daily_unavailable",
            "checked_at": datetime.fromtimestamp(
                now_ms / 1000, timezone.utc).isoformat()
        })
        return best, best_date, "daily_unavailable", pending, 0

    visited = 0
    partial = False
    for day in sorted(days, key=lambda d: (-d["high"], d["day_ms"])):
        date = utc_day(day["day_ms"])
        current = day["day_ms"] == now_ms // DAY_MS * DAY_MS
        if day["high"] <= best:
            continue
        if not current and date in checked:
            continue
        if visited >= max_days:
            partial = True
            break
        # _verified_day_bars checks that historical 5m volume sums to 1d
        # quote volume, the last complete bar exists, and no 5m gaps exist.
        bars = _verified_day_bars(alpha_id, day, start_ms, now_ms,
                                  lambda key,interval,start,end,lim:
                                  fetch_rows(fetch,key,interval,start,end,lim))
        if bars is None:
            partial = True
            continue
        visited += 1
        floor = quote_floor(event, claim["vwap"])
        for row in bars:
            stamp = int(float(row[0]))
            if stamp < claim["first_ms"]:
                continue
            vwap = qualify_bar(row, floor)
            if vwap is not None and vwap > best:
                best, best_date = vwap, utc_day(stamp)
        if not current:
            checked.add(date)

    pending = sum(1 for day in days if day["high"] > best and
                  (day["day_ms"] == (now_ms // DAY_MS) * DAY_MS or
                   utc_day(day["day_ms"]) not in checked))
    complete = full_daily and not partial and not pending
    new_state = {
        "version": 1, "alpha_id": alpha_id,
        "anchor_ms": claim["anchor_ms"], "checked_days": sorted(checked),
        "best_price": best, "best_date": best_date,
        "status": "alpha_complete_asof" if complete else "alpha_partial",
        "pending_days": pending, "days_checked_this_run": visited,
        "checked_at": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat()
    }
    return best, best_date, new_state["status"], new_state, visited


def recover_one(event, alpha_id, listing_ms, *, fetch, now_ms=None, max_days=5):
    """Atomic in-memory update, preserving all old non-null archive metadata."""
    now_ms = int(now_ms or time.time() * 1000)
    if not alpha_id or not is_candidate(event):
        return False, "not_qualified"
    claim, reason = verified_claim(event, alpha_id, listing_ms, fetch)
    if claim is None:
        return False, reason
    best, date, peak_status, state, visited = qualified_peak(
        event, claim, alpha_id, fetch, now_ms, max_days=max_days)

    lp = event.get("listing_price")
    if not isinstance(lp, dict):
        lp = {}
        event["listing_price"] = lp
    old_claim = lp.get("vwap")
    old_peak = lp.get("max_since")
    old_value = float(old_peak["price"]) if isinstance(old_peak, dict) and positive(old_peak.get("price")) else None
    # A partial history cannot disprove an old larger peak. It remains
    # visibly provisional until 5m history is fully verified.
    if peak_status != "alpha_complete_asof" and old_value and old_value > best:
        best = old_value
        date = old_peak.get("date") or date
        status = "alpha_partial_old_peak_preserved"
    else:
        status = peak_status
    lp.update({
        "vwap": claim["vwap"], "open": claim["open"], "close": claim["close"],
        "date": utc_day(claim["first_ms"]),
        "event_start_ms": claim["event_start_ms"],
        "claim_method": CLAIM_METHOD,
        "claim_quote_usd": claim["quote"],
        "claim_source": "binance_alpha_1m_direct",
        "max_since": {"price": best, "date": date},
        "peak_quality": ("alpha_liquidity_verified_asof" if status == "alpha_complete_asof"
                         else "partial_alpha_history"),
        "peak_scope": "alpha_since_event_or_verified_listing",
    })
    # Set peak_method only when the output is proven from qualified 5m
    # data; otherwise avoid falsely certifying a legacy daily close.
    if status == "alpha_complete_asof" or best == claim["vwap"] or (
            status == "alpha_partial" and positive(state.get("best_price"))
            and best == state["best_price"]):
        lp["peak_method"] = METHOD
    if status != "alpha_complete_asof":
        # Persist a retry hint inside the existing listing_price subobject.
        # The archived-price guard prohibits deleting previous non-null fields.
        state["retry_after"] = (datetime.fromtimestamp(now_ms / 1000,timezone.utc)
                                + timedelta(hours=6)).isoformat()
    lp[RECOVERY_KEY] = state
    event["ath_since_listing_price"] = best
    event["ath_since_listing_date"] = date
    print(f"  [alpha-legacy-repaired] {event.get('symbol')}: "
          f"Claim={old_claim}->{claim['vwap']:.9g} "
          f"Peak={old_value}->{best:.9g} "
          f"5m_days={visited} status={status}", flush=True)
    return True, status


def repair_exact_alpha_legacy(events, *, exact_info, fetch, now_ms=None,
                              max_events=128, max_days=5, max_workers=5,
                              max_seconds=210):
    """Bounded fair retry. Only exact Binance Alpha contract/chain qualifies."""
    now_ms = int(now_ms or time.time() * 1000)
    now_dt = datetime.fromtimestamp(now_ms / 1000, timezone.utc)
    eligible = []
    for event in events:
        if not is_candidate(event):
            continue
        aid, listed_ms = exact_info(event)
        if not aid:
            continue
        lp = event.get("listing_price") or {}
        last = lp.get(RECOVERY_KEY) or {}
        if not isinstance(last, dict):
            last = {}
        try:
            retry = datetime.fromisoformat(str(last.get("retry_after") or ""))
        except (ValueError, TypeError):
            retry = None
        if retry is not None and retry.tzinfo is not None and retry > now_dt:
            continue
        missing = not positive(lp.get("vwap"))
        eligible.append((0 if missing else 1,
                         str(last.get("checked_at") or ""),
                         str(event.get("event_time") or ""), event, aid, listed_ms))
    eligible.sort(key=lambda r:(r[0],r[1],r[2]))
    chosen = eligible[:max_events]
    deadline = time.monotonic() + max_seconds
    result = {"eligible": len(eligible), "selected": len(chosen),
              "repaired": 0, "held": 0, "history_complete": 0,
              "budget_exhausted": 0}

    def one(rec):
        _,_,_,event,aid,listed = rec
        return recover_one(event,aid,listed,fetch=fetch,now_ms=now_ms,max_days=max_days)

    # Do not submit every REST-heavy job before consulting max_seconds:
    # process bounded batches and let unfinished items wait for the next
    # existing cron run instead of risking a GitHub Actions timeout.
    submitted = 0
    batch_size = min(12, max_workers * 3)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for start in range(0,len(chosen),batch_size):
            if time.monotonic() >= deadline:
                result["budget_exhausted"] += len(chosen)-start
                break
            batch = chosen[start:start+batch_size]
            futures = [(rec,pool.submit(one,rec)) for rec in batch]
            submitted += len(futures)
            for rec,future in futures:
                event=rec[3]
                try:
                    did,status=future.result()
                except Exception:
                    did,status=False,"unexpected_provider_failure"
                result["repaired"] += int(did)
                result["history_complete"] += int(did and status == "alpha_complete_asof")
                result["held"] += int(not did)
                if not did:
                    lp=event.get("listing_price")
                    if isinstance(lp,dict):
                        current=lp.get(RECOVERY_KEY)
                        if not isinstance(current,dict):
                            current={}
                        current.update({
                            "checked_at":now_dt.isoformat(), "last_status":status,
                            "retry_after":(now_dt + timedelta(hours=6)).isoformat()
                        })
                        lp[RECOVERY_KEY]=current
                    print(f"  [alpha-legacy-held] {event.get('symbol')}: {status}",flush=True)
    result["selected"] = submitted
    return result