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
# A confirmed action can be nonterminal (e.g. verified claim opening).
# These terminal states may ONLY originate from the authenticated original
# Binance content classifier with the exact durable Canonical receipt.
TERMINAL_LIFECYCLE = {
    "AIRDROP": "AIRDROP_DISTRIBUTION_COMPLETED",
    "LISTING": "LISTING_LIVE",
    "TGE": "TGE_COMPLETED",
}


def _terminal_verified(row, binding, subject, action):
    lifecycle = subject.get("lifecycleAction")
    if lifecycle in (None, "CLAIM_OPEN"):
        # The currently deployed R26R Oracle CAS only emits CLAIM_OPEN.
        # Never certify event completion from noticeStage=CONFIRMED alone.
        return False
    if lifecycle != TERMINAL_LIFECYCLE[action]:
        hold("UNQUALIFIED_TERMINAL_ACTION")
    if (subject.get("originalTerminalVerified") is not True
            or subject.get("terminalSourceIdentity") != binding.get("sourceIdentity")
            or subject.get("terminalSourceDigest") != binding.get("sourceDigest")):
        hold("TERMINAL_ORIGINAL_CUSTODY")
    if (not isinstance(row.get("chain_id"), str)
            or not re.fullmatch(r"[1-9][0-9]*", row["chain_id"])
            or subject.get("verifiedChainId") != row["chain_id"]
            or not isinstance(row.get("contract_address"), str)
            or not re.fullmatch(r"0x[0-9a-fA-F]{40}", row["contract_address"])
            or not isinstance(subject.get("verifiedTokenContract"), str)
            or subject["verifiedTokenContract"].lower() != row["contract_address"].lower()
            or subject.get("verifiedTokenSource") != "BINANCE_ALPHA_TOKEN_MASTER"):
        hold("TERMINAL_TOKEN_CONTRACT")
    try:
        if (not isinstance(row.get("event_time"), str)
                or not isinstance(subject.get("eventTime"), str)
                or _stamp(row["event_time"]) != _stamp(subject["eventTime"])):
            hold("TERMINAL_ROUND_MISMATCH")
    except (ValueError, TypeError, OverflowError):
        hold("TERMINAL_ROUND_MISMATCH")
    return True


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
    terminal = _terminal_verified(row, b, sub, action)
    return {"published": published, "deadline": published + HOUR,
            "sourceIdentity": b["sourceIdentity"], "receiptId": proof["receiptId"],
            "action": action, "terminal": terminal}


def plan_qualified_official_onehour(*args, **kwargs):
    """Retired Active/R2-only entrypoint: never authorize History from public R2.

    Use official_history_policy.reconcile_authenticated with a genuine
    post-CAS Canonical V2 readback in the EXISTING writer instead.
    """
    hold("ACTIVE_ONLY_HISTORY_AUTHORITY_RETIRED")
