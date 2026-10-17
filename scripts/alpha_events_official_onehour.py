"""Fail-closed Binance official confirmation policy for the EXISTING History writer.

SOURCE CANDIDATE: no extra writer, job, R2 object or network operation.
Only a signed Oracle-to-Pages Active row whose authenticated receipt binding and
unique canonical event subject were qualified may carry officialConfirmation.
Call-site and Pages retirement remain independently gated.
"""
import copy
import re
from datetime import datetime, timedelta, timezone

from history24h_cutover import _compatible, _indexes, _overlay_without_erasing, _rows, _stamp

CID = re.compile(r"^ae:[0-9a-f]{24}$")
DIGEST = re.compile(r"^[0-9a-f]{64}$")
QUALIFIED_ACTIONS = {"LISTING", "TGE", "AIRDROP"}
SOURCE_CHANNEL = {
    "BINANCE_WALLET": "binance_wallet_announcements",
    "BINANCE_ANNOUNCEMENTS": "binance_announcements",
}
HOUR = timedelta(hours=1)


class OfficialConfirmationHold(RuntimeError):
    pass


def hold(reason):
    raise OfficialConfirmationHold("WA_AE_OFFICIAL1H_HOLD_" + reason)


def qualify_confirmation(row, now):
    """Return trusted publication and deadline or None when no proof exists.

    The upstream Oracle signer MUST have matched the raw Binance source text to
    the exact token/event and classified confirmed vs teaser. This function
    validates the immutable receipt-to-subject handoff; it cannot inspect raw
    Telegram text or independently authenticate a DB receipt over public R2.
    """
    proof = row.get("officialConfirmation")
    if proof is None:
        return None
    if not isinstance(proof, dict):
        hold("PROOF_OBJECT")
    b = proof.get("binding")
    sub = proof.get("verifiedSubject")
    if not isinstance(b, dict) or not isinstance(sub, dict):
        hold("PROOF_BINDING")
    cid = row.get("id")
    symbol = row.get("symbol")
    if not isinstance(cid, str) or not CID.fullmatch(cid) or not isinstance(symbol, str) or not symbol.strip():
        hold("IDENTITY")
    if (proof.get("receiptKind") != "BINANCE_OFFICIAL_OBSERVATION"
            or proof.get("sourceClass") != "APPROVED_BINANCE_OFFICIAL_RECEIPT"
            or not isinstance(proof.get("receiptId"), int)
            or isinstance(proof.get("receiptId"), bool)
            or proof["receiptId"] < 1):
        hold("OFFICIAL_RECEIPT")
    channel = SOURCE_CHANNEL.get(b.get("sourceKind"))
    number = b.get("sourceMessageId")
    if not (channel is not None and b.get("sourceChannel") == channel
            and isinstance(number, int) and not isinstance(number, bool) and number > 0
            and b.get("sourceIdentity") == f"telegram:{channel}:{number}"
            and isinstance(b.get("sourceDigest"), str)
            and DIGEST.fullmatch(b["sourceDigest"])
            and b.get("relevant") is True and b.get("relevanceReason") == "MATCH"):
        hold("OFFICIAL_SOURCE")
    action = sub.get("confirmedAction")
    # Alpha Competition notifications are a separate product and NEVER
    # evidence for the Alpha Events History lifecycle. Only the authenticated
    # Oracle content classifier may assert these explicit family proofs.
    if (sub.get("productFamily") != "ALPHA_EVENTS"
            or sub.get("competitionExcluded") is not True):
        hold("COMPETITION_OR_UNQUALIFIED_PRODUCT_FAMILY")
    if (sub.get("canonicalId") != cid or sub.get("symbol") != symbol
            or sub.get("sourceIdentity") != b.get("sourceIdentity")
            or sub.get("sourceDigest") != b.get("sourceDigest")
            or sub.get("noticeStage") != "CONFIRMED"
            or sub.get("preAnnouncement") is not False
            or action not in QUALIFIED_ACTIONS):
        hold("CONFIRMED_SUBJECT")
    if (b.get("eventType") not in ("AIRDROP", "ALPHA_EVENT")
            or (b.get("eventType") == "AIRDROP" and action != "AIRDROP")):
        hold("ACTION_CONFLICT")
    published = _stamp(b.get("sourcePublishedAt"))
    observed = _stamp(b.get("observedAt"))
    if published > observed or observed > now:
        hold("OFFICIAL_CLOCK")
    return {"published": published, "deadline": published + HOUR,
            "sourceIdentity": b["sourceIdentity"], "receiptId": proof["receiptId"],
            "action": action}


def plan_qualified_official_onehour(plan_value, active_rows, now):
    """Immediately archive only matching confirmed official events, not old forecasts.

    The 60-minute threshold is an SLA deadline FROM source publication, not a
    mandatory waiting period. Do not infer confirmation from event_time or
    firstActiveAt. Never authorize deleting physical Active without History CAS.
    """
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() != timedelta(0):
        hold("UTC_NOW")
    if (not isinstance(plan_value, dict) or plan_value.get("retire_live") is not False
            or plan_value.get("independent_writer") is not False):
        hold("UNQUALIFIED_PLAN")
    active = _rows(active_rows, "OFFICIAL_ACTIVE")
    all_rows = copy.deepcopy(_rows(plan_value["all"], "OFFICIAL_ALL"))
    history_rows = copy.deepcopy(_rows(plan_value["history"], "OFFICIAL_HISTORY"))
    all_ids = _indexes(all_rows, "OFFICIAL_ALL")
    history_ids = _indexes(history_rows, "OFFICIAL_HISTORY")
    seen = set()
    promoted = []
    overdue = []
    for row in active:
        cid = row.get("id")
        if not isinstance(cid, str) or not CID.fullmatch(cid) or cid in seen:
            hold("ACTIVE_DUPLICATE_OR_ID")
        seen.add(cid)
        confirmation = qualify_confirmation(row, now)
        if confirmation is None:
            continue  # Unconfirmed forecast remains Active; never archive.
        old_all = all_rows[all_ids[cid]] if cid in all_ids else None
        old_history = history_rows[history_ids[cid]] if cid in history_ids else None
        for prev in (old_all, old_history):
            if prev is not None:
                _compatible(prev, row)
                for key in ("event_time", "chain_id", "project_name", "firstActiveAt", "officialConfirmation"):
                    if prev.get(key) is not None and prev.get(key) != row.get(key):
                        hold("IMMUTABLE_PROOF_OR_IDENTITY")
        combined = copy.deepcopy(row)
        if old_all is not None:
            combined = _overlay_without_erasing(combined, old_all)
        if old_history is not None:
            combined = _overlay_without_erasing(combined, old_history)
        combined["status"] = "ended"
        combined["lifecycle_state"] = "history"
        # Exact proof carried into both versions for independent Pages check.
        for result, positions in ((all_rows, all_ids), (history_rows, history_ids)):
            if cid in positions:
                result[positions[cid]] = copy.deepcopy(combined)
            else:
                positions[cid] = len(result)
                result.append(copy.deepcopy(combined))
        promoted.append(cid)
        if now > confirmation["deadline"]:
            overdue.append(cid)
    return {**plan_value, "all": all_rows, "history": history_rows,
            "official_promoted": sorted(promoted), "official_overdue": sorted(overdue),
            "retire_live": False, "retire_pending": False,
            "retire_public_active": False}
