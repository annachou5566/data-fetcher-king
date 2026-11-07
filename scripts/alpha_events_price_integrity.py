"""Bounded, event-anchored price integrity for the EXISTING Alpha Events writer.

Only the verified exact row may be retried. Keep the last-good Claim value,
never use symbol-only history, and never manufacture missing OHLCV or Peak.
"""
from datetime import datetime, timedelta, timezone
import math

PRICE_RETRY_HOURS = 24
RETRY_FIELD = "_price_retry_after_utc"


def _instant(value):
    try:
        d = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def _day(value):
    if not value:
        return None
    d = _instant(str(value)[:10] + "T00:00:00+00:00")
    return d.date() if d else None


def _positive(value):
    try:
        f = float(value)
        return math.isfinite(f) and f > 0
    except (TypeError, ValueError, OverflowError):
        return False


def _event_instant(row):
    return _instant(row.get("event_time") or row.get("date"))


def anchor_is_before_event(row):
    """A claim price dated before its own event cannot be used as this event's Claim."""
    lp = row.get("listing_price")
    event = _event_instant(row)
    return bool(isinstance(lp, dict) and event and _positive(lp.get("vwap"))
                and _day(lp.get("date")) and _day(lp["date"]) < event.date())


def has_qualified_peak(row):
    lp = row.get("listing_price")
    if not isinstance(lp, dict) or not _positive(lp.get("vwap")):
        return False
    peak = lp.get("max_since")
    event = _event_instant(row)
    return bool(isinstance(peak, dict) and _positive(peak.get("price"))
                and _day(peak.get("date")) and event
                and _day(peak["date"]) >= event.date())


def candidate_is_event_anchored(row, candidate):
    """Fail closed on stale listing-day or pre-event Peak, keeping the prior price."""
    event = _event_instant(row)
    if not event or not isinstance(candidate, dict) or not _positive(candidate.get("vwap")):
        return False
    date = _day(candidate.get("date"))
    if not date or date < event.date():
        return False
    event_ms = int(event.timestamp() * 1000)
    try:
        if int(candidate.get("event_start_ms")) != event_ms:
            return False
    except (ValueError, TypeError, OverflowError):
        return False
    peak = candidate.get("max_since")
    if peak is not None:
        if not isinstance(peak, dict) or not _positive(peak.get("price")):
            return False
        peak_date = _day(peak.get("date"))
        if not peak_date or peak_date < event.date():
            return False
    return True


def select_price_integrity_retries(rows, now=None):
    """Mark only already-priced rows; existing missing-Claim path is unchanged.

    Missing Peak and bad Claim anchor receive one attempt at most per 24h
    when a provider stays unavailable. Temporary mode is removed by the writer.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    counts = {"anchor": 0, "peak": 0}
    for row in rows:
        if not _event_instant(row) or not row.get("contract_address"):
            continue
        lp = row.get("listing_price")
        if not isinstance(lp, dict) or not _positive(lp.get("vwap")):
            continue
        retry_after = _instant(lp.get(RETRY_FIELD))
        if retry_after and retry_after > now:
            continue
        if anchor_is_before_event(row):
            row["_price_integrity_mode"] = "anchor"
            row["_vwap_daybound_recompute"] = True
            counts["anchor"] += 1
        elif not has_qualified_peak(row) and not row.get("_vwap_daybound_recompute"):
            row["_price_integrity_mode"] = "peak"
            counts["peak"] += 1
    return counts


def snooze_unavailable_price(row, now=None):
    lp = row.get("listing_price")
    if not isinstance(lp, dict) or not _positive(lp.get("vwap")):
        return
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    lp = dict(lp)
    lp[RETRY_FIELD] = (now + timedelta(hours=PRICE_RETRY_HOURS)).isoformat()
    row["listing_price"] = lp


def apply_peak_only(row, candidate):
    """Overlay qualified Peak without ever overwriting the existing Claim."""
    if not candidate_is_event_anchored(row, candidate):
        return False
    peak = candidate.get("max_since")
    if not isinstance(peak, dict) or not _positive(peak.get("price")):
        return False
    lp = row.get("listing_price")
    if not isinstance(lp, dict) or not _positive(lp.get("vwap")):
        return False
    lp = dict(lp)
    lp["max_since"] = {"price": peak["price"], "date": peak["date"]}
    lp.pop(RETRY_FIELD, None)
    row["listing_price"] = lp
    row["ath_since_listing_price"] = peak["price"]
    row["ath_since_listing_date"] = peak["date"]
    return True
