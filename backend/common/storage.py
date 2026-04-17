"""
S3-compatible object storage helper (MinIO / AWS S3).

All pipeline payloads live under one bucket with the prefix scheme:
    {dataset_id}/raw.json
    {dataset_id}/preprocessed.json
    {dataset_id}/computed.json
    {dataset_id}/result.json

Celery messages carry only the dataset_id; tasks pull/push payloads here.
"""

from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import settings


# Stage key names (source of truth)
KEY_RAW = "raw.json"
KEY_PREPROCESSED = "preprocessed.json"
KEY_COMPUTED = "computed.json"
KEY_RESULT = "result.json"


_client: BaseClient | None = None


def get_client() -> BaseClient:
    """Return a cached boto3 S3 client configured for MinIO / S3."""
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            endpoint_url=settings.S3_ENDPOINT,
            aws_access_key_id=settings.S3_ACCESS_KEY,
            aws_secret_access_key=settings.S3_SECRET_KEY,
            region_name=settings.S3_REGION,
            config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
        )
    return _client


def ensure_bucket() -> None:
    """Create the dataset bucket if it doesn't already exist."""
    client = get_client()
    try:
        client.head_bucket(Bucket=settings.S3_BUCKET)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket", "NotFound"):
            client.create_bucket(Bucket=settings.S3_BUCKET)
        else:
            raise


def _key(dataset_id: str, name: str) -> str:
    return f"{dataset_id}/{name}"


def put_json(dataset_id: str, name: str, payload: Any) -> str:
    """Serialize payload as JSON and upload. Returns the object key."""
    key = _key(dataset_id, name)
    body = json.dumps(payload).encode("utf-8")
    get_client().put_object(
        Bucket=settings.S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return key


def get_json(dataset_id: str, name: str) -> Any:
    """Download and JSON-decode the stage payload."""
    key = _key(dataset_id, name)
    obj = get_client().get_object(Bucket=settings.S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def delete_prefix(dataset_id: str) -> int:
    """
    Delete every object under the dataset's prefix. Returns number deleted.
    Safe to call even when no objects exist.
    """
    client = get_client()
    prefix = f"{dataset_id}/"
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=settings.S3_BUCKET, Prefix=prefix):
        contents = page.get("Contents") or []
        if not contents:
            continue
        client.delete_objects(
            Bucket=settings.S3_BUCKET,
            Delete={"Objects": [{"Key": c["Key"]} for c in contents]},
        )
        deleted += len(contents)
    return deleted
