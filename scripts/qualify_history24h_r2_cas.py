"""One-shot isolated R2 CAS+recovery proof before Alpha Events legacy cutover.

Canonical History is read-only. The unique ephemeral qualification object is
always deletion-attempted and must be absent before this step can succeed.
"""
import hashlib
import os
import sys
from urllib.parse import urlsplit
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError

BUCKET = "wave-alpha-data"
ACCOUNT = "0f534c1b6f9bc097235b37c07d1dc32e"
HISTORY_KEY = "alpha-events/history.json"
HISTORY_SHA = "3a1bc754664a115d49ca244a761b26452a8f17212793704c802546338d30526f"


def get(client, key):
    obj = client.get_object(Bucket=BUCKET, Key=key)
    body = obj["Body"].read(2_000_001)
    if len(body) > 2_000_000 or not obj.get("ETag"):
        raise RuntimeError("R2_READ_BOUNDS_OR_ETAG")
    return body, obj["ETag"]


def run():
    endpoint = os.getenv("R2_ENDPOINT_URL", "")
    if os.getenv("R2_BUCKET_NAME") != BUCKET or (
        urlsplit(endpoint).hostname != ACCOUNT + ".r2.cloudflarestorage.com"
    ):
        raise RuntimeError("WRONG_R2_OWNER")
    s3 = boto3.client(
        "s3", endpoint_url=endpoint, region_name="auto",
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )
    history, _ = get(s3, HISTORY_KEY)
    if hashlib.sha256(history).hexdigest() != HISTORY_SHA:
        raise RuntimeError("HISTORY_BASELINE_MOVED")

    key = "alpha-events/.qualification/history24h-cas-" + uuid4().hex + ".json"
    original = b'{"check":"original"}'
    changed = b'{"check":"changed"}'
    qualified = False
    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=original,
                      IfNoneMatch="*", ContentType="application/json")
        actual, old_etag = get(s3, key)
        if actual != original:
            raise RuntimeError("INITIAL_READBACK")
        s3.put_object(Bucket=BUCKET, Key=key, Body=changed,
                      IfMatch=old_etag, ContentType="application/json")
        actual, changed_etag = get(s3, key)
        if actual != changed or changed_etag == old_etag:
            raise RuntimeError("CAS_READBACK")
        try:
            s3.put_object(Bucket=BUCKET, Key=key, Body=b'{"bad":true}',
                          IfMatch=old_etag)
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code")) not in (
                "412", "PreconditionFailed"
            ):
                raise
        else:
            raise RuntimeError("STALE_CAS_ACCEPTED")
        actual, changed_etag = get(s3, key)
        if actual != changed:
            raise RuntimeError("REJECTED_CAS_CHANGED_BODY")
        s3.put_object(Bucket=BUCKET, Key=key, Body=original,
                      IfMatch=changed_etag, ContentType="application/json")
        actual, _ = get(s3, key)
        if actual != original:
            raise RuntimeError("RESTORE_MISMATCH")
        qualified = True
    finally:
        # Unique random object, never a canonical object.
        s3.delete_object(Bucket=BUCKET, Key=key)
        try:
            s3.head_object(Bucket=BUCKET, Key=key)
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code")) not in (
                "404", "NoSuchKey", "NotFound"
            ):
                raise
        else:
            raise RuntimeError("SCRATCH_NOT_CLEANED")

    if not qualified:
        raise RuntimeError("INCOMPLETE_QUALIFICATION")
    output = os.environ.get("GITHUB_OUTPUT")
    if not output:
        raise RuntimeError("GITHUB_OUTPUT_MISSING")
    with open(output, "a", encoding="utf-8") as f:
        f.write("rollback_verified=1\n")
    print("R2_ISOLATED_CAS_RESTORE=PASS")
    print("R2_STALE_ETAG_REJECTION=PASS")
    print("R2_SCRATCH_CLEANUP=PASS")
    print("R2_CANONICAL_WRITES=0")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print("R2_CAS_QUALIFICATION=HOLD_" + type(exc).__name__)
        sys.exit(1)
