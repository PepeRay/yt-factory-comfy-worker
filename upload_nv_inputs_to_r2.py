"""
Upload NV input files to Cloudflare R2 — one-time migration script.

Run this on any machine with the Network Volume mounted (dev pod or serverless worker).
It scans two locations:
  1. /runpod-volume/ComfyUI/input/  → R2: inputs/audio/{filename}
  2. /runpod-volume/projects/*/music/ → R2: {channel}/music/{filename}

After uploading, the start scripts can use download_r2_inputs.py instead of NV symlinks.

Usage:
  python upload_nv_inputs_to_r2.py                    # upload everything
  python upload_nv_inputs_to_r2.py --dry-run           # list files without uploading
  python upload_nv_inputs_to_r2.py --skip-existing      # skip files already in R2
"""

import argparse
import os
import sys
import glob as glob_module

import boto3
from botocore.config import Config


# ── R2 connection ─────────────────────────────────────────────

R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "https://f5f00fc23d9cca2a37108b826160bf59.r2.cloudflarestorage.com")
R2_ACCESS_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID", "e625c788f5ed41b7b8b36ef6800affca")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "cbade7d513cd53348efb9bbdcf57f185e8de649b042ecd4770964c4bc496d55b")
R2_BUCKET = os.environ.get("R2_BUCKET", "yt-factory")

NV_PATH = os.environ.get("RUNPOD_NETWORK_VOLUME_PATH", "/runpod-volume")
NV_INPUT_DIR = os.path.join(NV_PATH, "ComfyUI", "input")
NV_PROJECTS_DIR = os.path.join(NV_PATH, "projects")


def get_r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "adaptive"},
        ),
    )


def r2_file_exists(client, key):
    try:
        client.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except client.exceptions.ClientError:
        return False


def size_mb(path):
    return round(os.path.getsize(path) / 1024 / 1024, 2)


# ── Scan functions ────────────────────────────────────────────

def scan_comfyui_inputs():
    """Scan /runpod-volume/ComfyUI/input/ for all files."""
    files = []
    if not os.path.isdir(NV_INPUT_DIR):
        print(f"[SKIP] ComfyUI input directory not found: {NV_INPUT_DIR}")
        return files

    for entry in sorted(os.listdir(NV_INPUT_DIR)):
        full_path = os.path.join(NV_INPUT_DIR, entry)
        if os.path.isfile(full_path):
            r2_key = f"inputs/audio/{entry}"
            files.append({"local_path": full_path, "r2_key": r2_key, "category": "input"})

    return files


def scan_music_dirs():
    """Scan /runpod-volume/projects/*/music/ for all music files."""
    files = []
    if not os.path.isdir(NV_PROJECTS_DIR):
        print(f"[SKIP] Projects directory not found: {NV_PROJECTS_DIR}")
        return files

    for channel in sorted(os.listdir(NV_PROJECTS_DIR)):
        music_dir = os.path.join(NV_PROJECTS_DIR, channel, "music")
        if not os.path.isdir(music_dir):
            continue
        for entry in sorted(os.listdir(music_dir)):
            full_path = os.path.join(music_dir, entry)
            if os.path.isfile(full_path):
                r2_key = f"{channel}/music/{entry}"
                files.append({
                    "local_path": full_path,
                    "r2_key": r2_key,
                    "category": f"music/{channel}",
                })

    return files


# ── Main ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Upload NV input files to R2")
    parser.add_argument("--dry-run", action="store_true", help="List files without uploading")
    parser.add_argument("--skip-existing", action="store_true", help="Skip files already in R2")
    args = parser.parse_args()

    print("=" * 60)
    print("NV → R2 Input File Migration")
    print("=" * 60)
    print(f"NV root:    {NV_PATH}")
    print(f"R2 bucket:  {R2_BUCKET}")
    print(f"R2 endpoint: {R2_ENDPOINT}")
    print(f"Dry run:    {args.dry_run}")
    print(f"Skip exist: {args.skip_existing}")
    print()

    # Scan
    all_files = []

    print("── Scanning ComfyUI inputs ──")
    inputs = scan_comfyui_inputs()
    all_files.extend(inputs)
    print(f"   Found {len(inputs)} files")

    print("── Scanning music directories ──")
    music = scan_music_dirs()
    all_files.extend(music)
    print(f"   Found {len(music)} files")
    print()

    if not all_files:
        print("No files found to upload. Is the Network Volume mounted?")
        sys.exit(0)

    # Print manifest
    print(f"{'Category':<20} {'Size (MB)':>10}  R2 Key")
    print("-" * 70)
    total_size = 0
    for f in all_files:
        s = size_mb(f["local_path"])
        total_size += s
        print(f"{f['category']:<20} {s:>10.2f}  {f['r2_key']}")
    print("-" * 70)
    print(f"{'TOTAL':<20} {total_size:>10.2f}  ({len(all_files)} files)")
    print()

    if args.dry_run:
        print("[DRY RUN] No files uploaded.")
        return

    # Upload
    client = get_r2_client()
    uploaded = 0
    skipped = 0
    errors = []

    for f in all_files:
        if args.skip_existing and r2_file_exists(client, f["r2_key"]):
            print(f"  [EXISTS] {f['r2_key']}")
            skipped += 1
            continue

        try:
            client.upload_file(f["local_path"], R2_BUCKET, f["r2_key"])
            print(f"  [OK]     {f['r2_key']} ({size_mb(f['local_path'])} MB)")
            uploaded += 1
        except Exception as e:
            print(f"  [ERROR]  {f['r2_key']}: {e}")
            errors.append(f["r2_key"])

    print()
    print("=" * 60)
    print(f"Uploaded: {uploaded}  |  Skipped: {skipped}  |  Errors: {len(errors)}")
    if errors:
        print("Failed keys:")
        for k in errors:
            print(f"  - {k}")
    print("=" * 60)


if __name__ == "__main__":
    main()
