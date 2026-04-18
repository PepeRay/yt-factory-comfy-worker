"""
YT Factory — Puppeteer endpoint handler

Renders an HTML infographic (CSS @keyframes animations) to MP4 via headless
Chromium controlled from Node.js (render.js), then encodes the captured PNG
frames to MP4 with ffmpeg and uploads the result to R2.

Input (event['input']):
  html_url:     string — presigned R2 URL or public URL of the HTML to render.
                Alternatively, an R2 key (no http prefix) — handler downloads
                via boto3.
  duration_sec: float  — total recording duration in seconds (e.g. 8.5).
  scene_id:     int|str — scene identifier (used in MP4 filename).
  video_id:     string — content_id (e.g. 0001_Who_Owns_39_Trillion).
  channel:      string — brand (e.g. dominion). Default: dominion.

Output:
  mp4_url:              string — public R2 URL (or key if R2_PUBLIC_URL unset).
  r2_key:               string — R2 key of the uploaded MP4.
  duration_actual_sec:  float  — actual rendered duration (frames / fps).
  frames_captured:      int    — number of PNG frames captured.
  file_size_bytes:      int    — final MP4 file size.
  elapsed_sec:          float  — total job time.

R2 paths:
  HTML in:  {channel}/{content_id}/source/infographics/scene_{scene_id}.html
  MP4 out:  {channel}/{content_id}/source/video/scene_{scene_id}_infographic.mp4

Notes:
  - PROJECTS_ROOT = /tmp/projects. Lección #46: never use /runpod-volume/ on
    workers without NV attached — that path exists in the rootfs but is not
    auto-cleaned between jobs, causing ENOSPC on long-running workers.
  - Cleanup wrapper try/finally (lección #47) — work_dir always removed even
    on exceptions, so a failed render does not accumulate disk on the worker.
  - One job per worker (RunPod manages concurrency via workersMax).
"""

import os
import shutil
import subprocess
import time
import requests
import runpod

import r2_helper

PROJECTS_ROOT = "/tmp/projects"
DEFAULT_FPS = 30
RENDER_TIMEOUT_SEC = 240   # 4 min hard cap for Puppeteer render
ENCODE_TIMEOUT_SEC = 120   # 2 min hard cap for ffmpeg encode


def handler(event):
    job_input = event.get("input", {}) or {}

    html_url = job_input.get("html_url")
    duration_sec = job_input.get("duration_sec")
    scene_id = job_input.get("scene_id")
    video_id = job_input.get("video_id")
    channel = job_input.get("channel", "dominion")

    # Validate input
    missing = [
        k for k, v in [
            ("html_url", html_url),
            ("duration_sec", duration_sec),
            ("scene_id", scene_id),
            ("video_id", video_id),
        ] if not v
    ]
    if missing:
        return {"error": f"Missing required input: {', '.join(missing)}"}

    try:
        duration_sec = float(duration_sec)
    except (TypeError, ValueError):
        return {"error": f"Invalid duration_sec: {duration_sec!r}"}

    if duration_sec <= 0 or duration_sec > 60:
        return {"error": f"duration_sec out of range (0, 60]: {duration_sec}"}

    scene_id = str(scene_id)
    work_dir = os.path.join(
        PROJECTS_ROOT, channel, video_id, f"infographic_{scene_id}"
    )
    os.makedirs(work_dir, exist_ok=True)

    try:
        return _handler_impl(
            work_dir, html_url, duration_sec, scene_id, video_id, channel
        )
    finally:
        # Lección #47 — cleanup wrapper try/finally guarantees no leftover
        # disk usage even if Puppeteer or ffmpeg throws mid-job
        shutil.rmtree(work_dir, ignore_errors=True)


def _handler_impl(work_dir, html_url, duration_sec, scene_id, video_id, channel):
    t0 = time.time()

    # ── 1. Download HTML ──
    html_path = os.path.join(work_dir, "infographic.html")
    print(f"[1/4] Downloading HTML from {html_url[:100]}...")

    if html_url.startswith(("http://", "https://")):
        r = requests.get(html_url, timeout=60)
        r.raise_for_status()
        with open(html_path, "wb") as f:
            f.write(r.content)
    else:
        # Treat as R2 key
        r2_helper.download_file(html_url, html_path)

    html_size = os.path.getsize(html_path)
    print(f"    HTML downloaded: {html_size} bytes")

    if html_size < 500:
        raise RuntimeError(
            f"HTML file suspiciously small ({html_size} bytes) — likely "
            f"a wrong URL or empty file"
        )

    # ── 2. Render HTML → PNG frames via Node.js Puppeteer ──
    frames_dir = os.path.join(work_dir, "frames")
    os.makedirs(frames_dir, exist_ok=True)

    fps = DEFAULT_FPS
    target_frames = int(round(duration_sec * fps))
    print(
        f"[2/4] Rendering ~{target_frames} frames @ {fps}fps "
        f"({duration_sec}s) via Puppeteer"
    )

    render_cmd = [
        "node", "/app/render.js",
        html_path,
        frames_dir,
        str(duration_sec),
        str(fps),
    ]

    render_result = subprocess.run(
        render_cmd,
        capture_output=True,
        text=True,
        timeout=RENDER_TIMEOUT_SEC,
    )

    if render_result.returncode != 0:
        raise RuntimeError(
            f"Puppeteer render failed (exit {render_result.returncode}):\n"
            f"STDOUT: {render_result.stdout[-2000:]}\n"
            f"STDERR: {render_result.stderr[-2000:]}"
        )

    captured = sorted(
        f for f in os.listdir(frames_dir)
        if f.startswith("frame_") and f.endswith(".png")
    )
    actual_frame_count = len(captured)
    print(f"    Captured {actual_frame_count} frames")

    if actual_frame_count == 0:
        raise RuntimeError(
            "Puppeteer captured 0 frames — render failed silently. "
            f"Renderer stdout:\n{render_result.stdout[-1000:]}"
        )

    if actual_frame_count < target_frames * 0.5:
        print(
            f"    WARN: captured {actual_frame_count} < 50% of target "
            f"{target_frames} — output will be shorter than requested"
        )

    # ── 3. Encode frames → MP4 with ffmpeg ──
    mp4_path = os.path.join(work_dir, f"scene_{scene_id}_infographic.mp4")
    print(f"[3/4] Encoding {actual_frame_count} frames -> MP4 (libx264 CRF 18)")

    encode_cmd = [
        "ffmpeg", "-y",
        "-loglevel", "warning",
        "-framerate", str(fps),
        "-i", os.path.join(frames_dir, "frame_%05d.png"),
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        mp4_path,
    ]

    encode_result = subprocess.run(
        encode_cmd,
        capture_output=True,
        text=True,
        timeout=ENCODE_TIMEOUT_SEC,
    )

    if encode_result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg encode failed (exit {encode_result.returncode}):\n"
            f"STDERR: {encode_result.stderr[-2000:]}"
        )

    file_size = os.path.getsize(mp4_path)
    print(f"    MP4 encoded: {file_size} bytes")

    # ── 4. Upload MP4 to R2 ──
    r2_key = (
        f"{channel}/{video_id}/source/video/scene_{scene_id}_infographic.mp4"
    )
    print(f"[4/4] Uploading to R2: {r2_key}")
    r2_helper.upload_file(mp4_path, r2_key)

    r2_public_url = (os.environ.get("R2_PUBLIC_URL") or "").rstrip("/")
    mp4_url = f"{r2_public_url}/{r2_key}" if r2_public_url else r2_key

    elapsed = time.time() - t0
    print(f"=== DONE in {elapsed:.1f}s ===")

    return {
        "mp4_url": mp4_url,
        "r2_key": r2_key,
        "duration_actual_sec": round(actual_frame_count / fps, 3),
        "frames_captured": actual_frame_count,
        "file_size_bytes": file_size,
        "elapsed_sec": round(elapsed, 2),
    }


runpod.serverless.start({"handler": handler})
