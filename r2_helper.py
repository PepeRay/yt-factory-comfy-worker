"""
Cloudflare R2 helper — S3-compatible object storage.
Shared module across Audio, Images, and Video handlers.

R2 key convention mirrors NV path structure:
  {channel}/{content_id}/source/{type}/filename
  {channel}/{content_id}/output/{platform}/filename

Env vars required:
  R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET
"""

import os
import boto3
from botocore.config import Config

_client = None


def get_r2_client():
    """Get or create a cached boto3 S3 client for Cloudflare R2."""
    global _client
    if _client is None:
        endpoint = os.environ.get("R2_ENDPOINT")
        access_key = os.environ.get("R2_ACCESS_KEY_ID")
        secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")

        if not all([endpoint, access_key, secret_key]):
            raise RuntimeError(
                "R2 credentials not configured. "
                "Set R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY env vars."
            )

        _client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "adaptive"},
            ),
        )
    return _client


def get_bucket():
    """Get the R2 bucket name from environment."""
    return os.environ.get("R2_BUCKET", "yt-factory")


def upload_file(local_path, r2_key):
    """Upload a local file to R2. Returns the R2 key."""
    client = get_r2_client()
    client.upload_file(local_path, get_bucket(), r2_key)
    return r2_key


def download_file(r2_key, local_path):
    """Download a file from R2 to local path. Creates directories as needed."""
    client = get_r2_client()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    client.download_file(get_bucket(), r2_key, local_path)
    return local_path


def list_files(prefix):
    """List all files under a given R2 prefix. Returns list of keys."""
    client = get_r2_client()
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=get_bucket(), Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def file_exists(r2_key):
    """Check if a file exists in R2."""
    client = get_r2_client()
    try:
        client.head_object(Bucket=get_bucket(), Key=r2_key)
        return True
    except client.exceptions.ClientError:
        return False


def presigned_url(r2_key, expires_in=604800):
    """Generate a presigned URL for downloading a file from R2.
    Default expiry: 7 days (604800 seconds)."""
    client = get_r2_client()
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": get_bucket(), "Key": r2_key},
        ExpiresIn=expires_in,
    )
