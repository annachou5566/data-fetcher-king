"""Incremental, event-anchored, liquidity-qualified Alpha/Spot Peak scan.

Uses *existing* alpha-events/all.json listing_price object as its checkpoint.
No additional cron, R2 object, service, or frontend contract is required.
"""
from datetime import datetime, timezone
import math
import time

WINDOW_MS = 5 * 60_000
STATE_KEY = "_peak_liquidity_v1"
METHOD = "highest_qualified_5m_quote_vwap"
MIN_QUOTE_USD = 100.0
MIN_TRADES = 3


def number(v):
    try:
        x = float(v)
        return x if math.isfinite(x) and x >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def bar_vwap(bar):
    """Actual quote traded / base traded, never a candle wick or daily close."""
    if not isinstance(bar, (list, tuple)) or len(bar) < 9:
        return None
    base, quote = number(bar[5]), number(bar[7])
    if not base or not quote:
        return None
    price = quote / base
    low, high = number(bar[3]), number(bar[2])
    if not low or not high or low > high or not (low * 0.999 <= price <= high * 1.001):
        return None
    trades = number(bar[8])
    if trades is None:
        return None
    return price, quote, int(trades)


def qualify_bar(bar, minimum_quote):
    v = bar_vwap(bar)
    if not v:
        return None
    price, quote, trades = v
    return price if quote >= minimum_quote and trades >= MIN_TRADES else None


def claim_sale_notional(event, claim):
    amount = number(event.get("amount_per_user") or event.get("amount"))
    # Some events use free-form award strings. With no verified amount use the
    # absolute 100 USD floor, not a guessed notional.
    return (amount * claim) if amount and claim else 0.0


def quote_floor(event, claim):
    # At least 5x the theoretical reward sale notional must trade in 5m.
    # This is only a liquidity proxy: it does not guarantee execution.
    return max(MIN_QUOTE_USD, 5.0 * claim_sale_notional(event, claim))


def utc_day(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")


def parse_day_ms(day):
    if not isinstance(day, str) or len(day) < 10:
        return None
    try:
        return int(datetime.strptime(day[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    except (TypeError, ValueError, OverflowError):
        return None


def _process(event, *, anchor, alpha_id, alpha_listing_ms, spot_ms, fetch_alpha,
             fetch_spot, now_ms, pages):
    lp = event["listing_price"]
    anchor = max(anchor, int(alpha_listing_ms or 0)) if not spot_ms or anchor < spot_ms else anchor
    if anchor > now_ms:
        return False, "not_listed_yet", False
    state = lp.get(STATE_KEY)
    if not isinstance(state, dict) or state.get("version") != 1 or state.get("anchor_ms") != anchor:
        state = {
            "version": 1, "anchor_ms": anchor, "cursor_ms": anchor,
            "best_price": None, "best_date": None, "method": METHOD,
        }
    try:
        cursor = max(anchor, int(state.get("cursor_ms") or anchor))
    except (ValueError, TypeError, OverflowError):
        cursor = anchor
    best = number(state.get("best_price")) or 0.0
    best_date = state.get("best_date")
    if lp.get("claim_method") == "event_5m_quote_vwap" and number(lp.get("claim_quote_usd")):
        # A verified executed 5m Claim VWAP is an actual price benchmark,
        # so a genuine hold Peak cannot fall below it.
        claim = number(lp.get("vwap")) or 0.0
        if claim > best:
            best, best_date = claim, lp.get("date")
    floor = quote_floor(event, number(lp.get("vwap")))
    scanned = 0
    holds = 0
    status = "partial"

    for _ in range(pages):
        if cursor >= now_ms - WINDOW_MS:
            status = "caught_up"
            break
        spot_phase = bool(spot_ms is not None and cursor >= spot_ms)
        if spot_phase:
            page_bars = 1000
            source = "spot"
        else:
            page_bars = 1500
            source = "alpha"
        page_end = min(cursor + page_bars * WINDOW_MS - 1,
                       now_ms - WINDOW_MS)
        if spot_ms is not None and not spot_phase:
            page_end = min(page_end, spot_ms - 1)
        if page_end < cursor:
            cursor = max(cursor + 1, spot_ms or cursor + 1)
            continue
        try:
            if spot_phase:
                rows = fetch_spot(str(event.get("symbol") or event.get("token") or "").upper(),
                                  cursor, page_end, page_bars)
            else:
                rows = fetch_alpha(alpha_id, cursor, page_end, page_bars)
        except Exception:
            rows = None
        # Provider failure is not proof of an empty price history.
        if rows is None:
            status = "provider_unavailable_" + source
            holds += 1
            break
        for bar in rows:
            try:
                timestamp = int(float(bar[0]))
            except (ValueError, TypeError, IndexError, OverflowError):
                continue
            if not (cursor <= timestamp <= page_end):
                continue
            vwap = qualify_bar(bar, floor)
            if vwap is not None and vwap > best:
                best = vwap
                best_date = utc_day(timestamp)
        cursor = page_end + 1
        scanned += 1

    if cursor >= now_ms - WINDOW_MS:
        status = "caught_up"

    # A 1-day recent window is fetched separately from the historical
    # checkpoint. This makes recently liquid Spot peaks visible quickly
    # without pretending the entire old history was already scanned.
    # Only completed five-minute bars with real quote volume can qualify.
    recent_peak = False
    recent_start = max(anchor, now_ms - 288 * WINDOW_MS)
    if spot_ms is not None and now_ms >= spot_ms + WINDOW_MS:
        recent_start = max(recent_start, spot_ms)
    if recent_start < now_ms - WINDOW_MS:
        spot_recent = bool(spot_ms is not None and recent_start >= spot_ms)
        try:
            if spot_recent:
                recent_rows = fetch_spot(
                    str(event.get("symbol") or event.get("token") or "").upper(),
                    recent_start, now_ms - WINDOW_MS, 288)
            else:
                recent_rows = fetch_alpha(alpha_id, recent_start, now_ms - WINDOW_MS, 288)
        except Exception:
            recent_rows = None
        if isinstance(recent_rows, list):
            for bar in recent_rows:
                try:
                    stamp = int(float(bar[0]))
                except (TypeError, ValueError, IndexError, OverflowError):
                    continue
                if not recent_start <= stamp <= now_ms - WINDOW_MS:
                    continue
                price = qualify_bar(bar, floor)
                if price is not None and price > best:
                    best, best_date, recent_peak = price, utc_day(stamp), True

    state.update({
        "cursor_ms": cursor, "best_price": best or None,
        "best_date": best_date, "quote_floor_usd": floor,
        "status": status, "checked_at": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(),
        "pages_advanced": int(state.get("pages_advanced") or 0) + scanned,
    })
    # Mark only state, no false peak publication if source is unavailable.
    lp[STATE_KEY] = state
    if not best or not best_date:
        return bool(scanned), status, False

    old_peak = lp.get("max_since")
    old_value = number(old_peak.get("price")) if isinstance(old_peak, dict) else None
    already_liquid = lp.get("peak_method") == METHOD

    # Never discard a last-good value on a partial history scan. When the
    # historical scan catches up, the verified definition supersedes old
    # daily-close-based values, even if that corrects them down.
    if status == "caught_up" or old_value is None or best > old_value:
        if already_liquid and old_value is not None and old_value > best:
            # Preserve the true previously seen qualified peak across
            # provider revisions or temporary missing data.
            best, best_date = old_value, old_peak.get("date")
        lp["max_since"] = {"price": best, "date": best_date}
        lp["peak_method"] = METHOD
        lp["peak_quality"] = ("history_verified_to_latest_complete_5m"
                              if status == "caught_up" else "partial_history")
        if old_value is None or best > old_value:
            print(f"  [peak-5m-updated] {event.get('symbol') or event.get('token')} "
                  f"Peak={old_value}->{best:.9g} status={status}", flush=True)
        event["ath_since_listing_price"] = best
        event["ath_since_listing_date"] = best_date
        return True, status, bool(recent_peak)
    return bool(scanned), status, False


def refresh_liquidity_peaks(events, *, event_ms, alpha_info, fetch_alpha,
                            fetch_spot, spot_date_resolver=None,
                            now_ms=None, max_events=12, pages_per_event=3):
    """Bounded fair round-robin across old event-anchored Claim records.

    Never guesses a spot identity: an Alpha id validated against the exact
    contract is required before using a ticker-based Spot price series.
    """
    now_ms = now_ms or int(time.time() * 1000)
    eligible = []
    for e in events:
        lp = e.get("listing_price")
        if not isinstance(lp, dict) or not number(lp.get("vwap")):
            continue
        anchor = event_ms(e)
        if not anchor or anchor > now_ms or not e.get("contract_address"):
            continue
        sym = str(e.get("symbol") or e.get("token") or "").upper().strip()
        if not sym:
            continue
        alpha_id, listing_ms = alpha_info(sym, e["contract_address"])
        if not alpha_id:
            continue
        # The upstream Binance token-list identity was checked against this
        # exact event contract by alpha_info(); ticker alone is never enough.
        state = lp.get(STATE_KEY) or {}
        priority = 0 if lp.get("peak_quality") == "pending_liquidity_scan" else 1
        eligible.append((priority, str(state.get("checked_at") or ""), e, anchor,
                         sym, alpha_id, listing_ms))
    eligible.sort(key=lambda x: (x[0], x[1], x[3], str(x[2].get("id") or "")))
    selected = eligible[:max_events]
    cache = {}

    def one(item):
        _, _, e, anchor, sym, alpha_id, listing_ms = item
        is_spot = bool(e.get("spot_listed") or e.get("is_spot_listed"))
        spot_ms = None
        if is_spot:
            spot_ms = parse_day_ms(e.get("spot_listed_at"))
            if spot_ms is None:
                spot_price = e.get("spot_listing_price")
                if isinstance(spot_price, dict):
                    spot_ms = parse_day_ms(spot_price.get("date"))
            if spot_ms is None and spot_date_resolver:
                # Recover a verified listing day once, without inventing it.
                # Cache per symbol within this cron run for repeat campaigns.
                if sym not in cache:
                    cache[sym] = spot_date_resolver(sym, utc_day(anchor))
                spot_ms = parse_day_ms(cache[sym])
                if spot_ms is not None:
                    e["spot_listed_at"] = utc_day(spot_ms)
            if spot_ms is None:
                # Record this HOLD in the same existing per-event checkpoint.
                # Otherwise unresolvable Spot events would occupy the first
                # batch forever and starve all later events.
                lp = e["listing_price"]
                state = lp.get(STATE_KEY)
                if not isinstance(state, dict):
                    state = {"version": 1, "anchor_ms": anchor,
                             "cursor_ms": anchor, "best_price": None,
                             "best_date": None, "method": METHOD}
                state["checked_at"] = datetime.fromtimestamp(
                    now_ms / 1000, timezone.utc).isoformat()
                state["status"] = "spot_listing_day_unavailable"
                lp[STATE_KEY] = state
                return False, "spot_listing_day_unavailable", False
        return _process(e, anchor=anchor, alpha_id=alpha_id,
                        alpha_listing_ms=listing_ms, spot_ms=spot_ms,
                        fetch_alpha=fetch_alpha, fetch_spot=fetch_spot,
                        now_ms=now_ms, pages=pages_per_event)

    result = {"eligible": len(eligible), "selected": len(selected),
              "advanced": 0, "caught_up": 0, "held": 0, "recent_updated": 0}
    # Serial processing avoids racing the per-run spot date resolution cache;
    # each HTTP request is already bounded by the upstream 15s timeouts.
    for item in selected:
        advanced, status, recent_updated = one(item)
        result["advanced"] += int(advanced)
        result["recent_updated"] += int(recent_updated)
        result["caught_up"] += int(status == "caught_up")
        result["held"] += int(status.startswith("provider_unavailable") or status.endswith("_unavailable"))
    return result