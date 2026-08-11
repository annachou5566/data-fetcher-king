#!/usr/bin/env python3
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

ENDPOINTS = [
    ("summary", "https://api-hyperliquid.asxn.xyz/api/node/liquidations/summary"),
    ("recent", "https://api-hyperliquid.asxn.xyz/api/node/liquidations?limit=5"),
]

TIMEOUT_SECONDS = 20
MAX_BODY_BYTES = 2 * 1024 * 1024
SAFE_KEY_TOKENS = (
    "liquid", "long", "short", "total", "notional", "volume", "usd",
    "count", "time", "timestamp", "updated", "created", "date", "window",
)
SENSITIVE_KEY_TOKENS = (
    "wallet", "address", "user", "account", "hash", "tx", "tid",
    "liquidator", "trader", "owner", "maker", "taker",
)


def safe_key(name):
    lower = str(name).lower()
    return any(token in lower for token in SAFE_KEY_TOKENS) and not any(
        token in lower for token in SENSITIVE_KEY_TOKENS
    )


def safe_scalar(value):
    if value is None or isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        text = value.strip()
        if not text or len(text) > 96:
            return False
        try:
            float(text.replace(",", ""))
            return True
        except ValueError:
            pass
        return False
    return False


def collect_safe_signals(value, prefix="", depth=0, out=None):
    if out is None:
        out = []
    if depth > 4 or len(out) >= 80:
        return out
    if isinstance(value, dict):
        for key, child in value.items():
            if len(out) >= 80:
                break
            path = f"{prefix}.{key}" if prefix else str(key)
            if safe_key(key) and safe_scalar(child):
                out.append({"path": path, "value": child})
            elif isinstance(child, (dict, list)):
                collect_safe_signals(child, path, depth + 1, out)
    elif isinstance(value, list):
        for index, child in enumerate(value[:5]):
            collect_safe_signals(child, f"{prefix}[{index}]", depth + 1, out)
    return out


def shape(payload):
    if isinstance(payload, dict):
        return {
            "topLevelType": "object",
            "topLevelKeys": sorted(str(key) for key in payload.keys())[:80],
            "safeSignals": collect_safe_signals(payload),
        }
    if isinstance(payload, list):
        item_keys = []
        for item in payload[:5]:
            if isinstance(item, dict):
                item_keys.append(sorted(str(key) for key in item.keys())[:80])
        return {
            "topLevelType": "array",
            "listLength": len(payload),
            "sampleItemKeys": item_keys,
            "safeSignals": collect_safe_signals(payload),
        }
    return {
        "topLevelType": type(payload).__name__,
        "safeSignals": [],
    }


def probe(label, url):
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
        ),
        "Origin": "https://hyperscreener.asxn.xyz",
        "Referer": "https://hyperscreener.asxn.xyz/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    request = urllib.request.Request(url, headers=headers, method="GET")
    started = time.monotonic()
    status = None
    content_type = None
    body = b""
    error_class = None
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            status = int(response.status)
            content_type = response.headers.get("Content-Type")
            body = response.read(MAX_BODY_BYTES + 1)
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        content_type = exc.headers.get("Content-Type") if exc.headers else None
        body = exc.read(MAX_BODY_BYTES + 1)
        error_class = "HTTPError"
    except Exception as exc:
        error_class = type(exc).__name__

    elapsed_ms = round((time.monotonic() - started) * 1000)
    result = {
        "label": label,
        "host": urllib.parse.urlparse(url).hostname,
        "httpStatus": status,
        "ok": status is not None and 200 <= status < 300,
        "contentType": content_type,
        "elapsedMs": elapsed_ms,
        "errorClass": error_class,
        "rawPayloadPrinted": False,
        "walletValuesPrinted": False,
    }

    if len(body) > MAX_BODY_BYTES:
        result["bodyTooLarge"] = True
        return result

    if body:
        try:
            payload = json.loads(body.decode("utf-8"))
            result["json"] = True
            result.update(shape(payload))
        except Exception:
            result["json"] = False
            result["bodyBytes"] = len(body)
    else:
        result["json"] = False
        result["bodyBytes"] = 0

    if status == 403:
        result["vantageInterpretation"] = (
            "Oracle egress received 403; this is stronger evidence against unattended Oracle access, "
            "but still not proof that ASXN UI/manual access or other supported access models are unavailable."
        )
    elif status is not None and 200 <= status < 300:
        result["vantageInterpretation"] = "Oracle egress can reach this ASXN endpoint."
    return result


def main():
    results = [probe(label, url) for label, url in ENDPOINTS]
    statuses = [row.get("httpStatus") for row in results]
    any_ok = any(row.get("ok") for row in results)
    print(json.dumps({
        "checkedAtEpochMs": int(time.time() * 1000),
        "provider": "ASXN HyperScreener",
        "vantage": "oracle-vm-egress",
        "readOnly": True,
        "credentialsUsed": False,
        "runtimeMutation": False,
        "rawPayloadPrinted": False,
        "walletValuesPrinted": False,
        "results": results,
        "anyEndpointReachable": any_ok,
        "statuses": statuses,
        "productionEligible": False,
        "aggregateEligible": False,
        "nextGate": (
            "inspect schema and benchmark only; no production activation"
            if any_ok
            else "keep ASXN research/manual benchmark only unless a supported access contract is found"
        ),
    }, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
