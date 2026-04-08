"""
Download input files from R2 to local disk — replaces NV symlinks in start scripts.

Called at container startup (before handler.py) to ensure ComfyUI inputs and
channel music tracks are available locally. Uses r2_helper.py for consistency.

What it downloads:
  1. inputs/audio/*  → /comfyui/input/    (voice references for TTS)
  2. {channel}/music/* → /tmp/music/{channel}/  (background music for compose)

The music download is lazy — only fetched for a specific channel. Call
download_music(channel) from the handler when a compose job arrives,
or pass --channel at startup to pre-fetch.

Usage in start scripts:
  python download_r2_inputs.py                        # download ComfyUI inputs only
  python download_r2_inputs.py --channel dominion      # also pre-fetch music for channel
  python download_r2_inputs.py --all-music             # download music for ALL channels

Usage from handler (runtime):
  from download_r2_inputs import download_music, ensure_comfyui_inputs
  ensure_comfyui_inputs()
  music_dir = download_music("dominion")  # returns local path to music dir
"""

import argparse
import os
import sys

# Import r2_helper — same module used by handlers
try:
    import r2_helper
except ImportError:
    # If running standalone, add current directory to path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import r2_helper


COMFYUI_INPUT_DIR = os.environ.get("COMFYUI_INPUT_DIR", "/comfyui/input")
MUSIC_CACHE_DIR = os.environ.get("MUSIC_CACHE_DIR", "/tmp/music")

# R2 prefixes
R2_INPUTS_PREFIX = "inputs/audio/"


def ensure_comfyui_inputs():
    """Download all ComfyUI input files (voice references etc.) from R2.
    Skips files that already exist locally (idempotent)."""
    os.makedirs(COMFYUI_INPUT_DIR, exist_ok=True)

    try:
        keys = r2_helper.list_files(R2_INPUTS_PREFIX)
    except Exception as e:
        print(f"[WARN] Failed to list R2 inputs: {e}")
        return []

    if not keys:
        print("[INFO] No input files found in R2 under inputs/audio/")
        return []

    downloaded = []
    for key in keys:
        filename = os.path.basename(key)
        if not filename:
            continue
        local_path = os.path.join(COMFYUI_INPUT_DIR, filename)

        if os.path.exists(local_path):
            print(f"  [EXISTS] {filename}")
            continue

        try:
            r2_helper.download_file(key, local_path)
            size_mb = round(os.path.getsize(local_path) / 1024 / 1024, 2)
            print(f"  [OK]     {filename} ({size_mb} MB)")
            downloaded.append(local_path)
        except Exception as e:
            print(f"  [ERROR]  {filename}: {e}")

    print(f"[INFO] ComfyUI inputs: {len(downloaded)} downloaded, {len(keys) - len(downloaded)} already present")
    return downloaded


def download_music(channel):
    """Download music tracks for a channel from R2. Returns local music directory path.
    Idempotent — skips files that already exist.

    R2 layout: {channel}/music/{filename}
    Local: /tmp/music/{channel}/{filename}
    """
    r2_prefix = f"{channel}/music/"
    local_dir = os.path.join(MUSIC_CACHE_DIR, channel)
    os.makedirs(local_dir, exist_ok=True)

    try:
        keys = r2_helper.list_files(r2_prefix)
    except Exception as e:
        print(f"[WARN] Failed to list R2 music for {channel}: {e}")
        return local_dir

    if not keys:
        print(f"[INFO] No music files found in R2 for channel '{channel}'")
        return local_dir

    downloaded = 0
    for key in keys:
        filename = os.path.basename(key)
        if not filename:
            continue
        local_path = os.path.join(local_dir, filename)

        if os.path.exists(local_path):
            continue

        try:
            r2_helper.download_file(key, local_path)
            downloaded += 1
        except Exception as e:
            print(f"  [ERROR]  music/{channel}/{filename}: {e}")

    if downloaded:
        print(f"[INFO] Music for '{channel}': {downloaded} new files downloaded to {local_dir}")

    return local_dir


def download_all_music():
    """Download music for all channels found in R2.
    Discovers channels by listing top-level prefixes that have a music/ subfolder."""
    # List everything, find unique channel prefixes with /music/
    try:
        # List all objects, filter for music paths
        keys = r2_helper.list_files("")
    except Exception as e:
        print(f"[ERROR] Failed to list R2 bucket: {e}")
        return

    channels = set()
    for key in keys:
        # Pattern: {channel}/music/{filename}
        parts = key.split("/")
        if len(parts) >= 3 and parts[1] == "music":
            channels.add(parts[0])

    if not channels:
        print("[INFO] No music directories found in R2")
        return

    print(f"[INFO] Found music for channels: {', '.join(sorted(channels))}")
    for channel in sorted(channels):
        download_music(channel)


# ── CLI ───────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Download input files from R2 (replaces NV symlinks)"
    )
    parser.add_argument(
        "--channel",
        help="Pre-fetch music for this channel (e.g., dominion)",
    )
    parser.add_argument(
        "--all-music",
        action="store_true",
        help="Download music for ALL channels found in R2",
    )
    parser.add_argument(
        "--skip-inputs",
        action="store_true",
        help="Skip ComfyUI input files (only download music)",
    )
    args = parser.parse_args()

    print("=" * 50)
    print("R2 Input File Download")
    print("=" * 50)

    if not args.skip_inputs:
        print("\n── ComfyUI Input Files ──")
        ensure_comfyui_inputs()

    if args.all_music:
        print("\n── Music (all channels) ──")
        download_all_music()
    elif args.channel:
        print(f"\n── Music ({args.channel}) ──")
        download_music(args.channel)

    print("\nDone.")


if __name__ == "__main__":
    main()
