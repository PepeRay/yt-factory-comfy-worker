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
DEFAULT_OUTPUT_FPS = 30
CAPTURE_FPS = 15            # Chromium screenshot loop rate; encoding upsamples to OUTPUT_FPS
# CSS animation playback rate via CDP. 1.0 = real-time. <1.0 = slower.
# Lesson #84 (2026-04-25): templates visual-infographics ahora son multi-stage
# diseñadas en timeline NATIVO de 11s (Stage 1 entry 0-3s, Stage 2 reveal 3-7s,
# Stage 3 emphasis 7-11s). NO hace falta slow down — playbackRate 1.0 ahora.
# Anti-patrón anterior: 0.6 stretching anims que de todos modos eran 1.5s — daba
# 2.5s de motion en una capture window de 5s con 65-75% de tiempo estático.
ANIMATION_PLAYBACK_RATE = 1.0
# Active capture window — debe cubrir el full timeline 0-11s del nuevo stage system.
# Dynamic: cap a 11s (todo el ciclo Stage 1+2+3) o duration_sec - 2 si es menor
# para garantizar al menos 2s de hold final para lectura.
# Función `get_animation_capture_sec(duration_sec)` debajo.
ANIMATION_CAPTURE_SEC_MAX = 11.0
ANIMATION_CAPTURE_SEC_MIN = 5.0
ANIMATION_HOLD_MIN_SEC = 2.0
RENDER_TIMEOUT_SEC = 360    # 6 min hard cap for Puppeteer render (was 4 min)
ENCODE_TIMEOUT_SEC = 180    # 3 min hard cap for ffmpeg encode (was 2 min)


def get_animation_capture_sec(duration_sec: float) -> float:
    """
    Compute capture window per Lesson #84 multi-stage system.
    Templates visual-infographics span 0-11s natively (Stage 1+2+3).
    For scenes shorter than 11s+min-hold, cap to leave 2s static hold for reading.
    For scenes longer than 13s, capture the full 11s timeline + tpad clone the rest.
    """
    if duration_sec <= ANIMATION_CAPTURE_SEC_MIN + ANIMATION_HOLD_MIN_SEC:
        return max(3.0, duration_sec - ANIMATION_HOLD_MIN_SEC)
    return min(ANIMATION_CAPTURE_SEC_MAX, duration_sec - ANIMATION_HOLD_MIN_SEC)


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

    output_fps = DEFAULT_OUTPUT_FPS
    # Active capture window — animation phase. Lesson #84 multi-stage system:
    # cap a 11s (Stage 1+2+3) o duration-2 si es menor para 2s hold mínimo.
    animation_sec = get_animation_capture_sec(duration_sec)
    hold_final_sec = max(0.0, duration_sec - animation_sec)
    target_capture_frames = int(round(animation_sec * CAPTURE_FPS))

    print(
        f"[2/4] Rendering {target_capture_frames} frames @ {CAPTURE_FPS}fps "
        f"capture rate over {animation_sec}s active animation "
        f"(playbackRate={ANIMATION_PLAYBACK_RATE}x, +{hold_final_sec:.1f}s static hold "
        f"= {duration_sec}s total MP4)"
    )

    render_cmd = [
        "node", "/app/render.js",
        html_path,
        frames_dir,
        str(animation_sec),
        str(CAPTURE_FPS),
        str(ANIMATION_PLAYBACK_RATE),
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

    # Print renderer stdout for telemetry (real fps achieved, etc.)
    if render_result.stdout:
        for line in render_result.stdout.strip().split('\n'):
            print(f"    [renderer] {line}")

    captured = sorted(
        f for f in os.listdir(frames_dir)
        if f.startswith("frame_") and f.endswith(".png")
    )
    actual_frame_count = len(captured)

    if actual_frame_count == 0:
        raise RuntimeError(
            "Puppeteer captured 0 frames — render failed silently. "
            f"Renderer stdout:\n{render_result.stdout[-1000:]}"
        )

    if actual_frame_count < 10:
        print(
            f"    WARN: captured only {actual_frame_count} frames "
            f"— animation may look choppy"
        )

    # ── 3. Encode frames → MP4 with ffmpeg (animation + static hold) ──
    # Strategy:
    #   1. ffmpeg input: PNG sequence captured at uniform CAPTURE_FPS rate.
    #      Real capture rate may be lower if screenshots are slow on CPU,
    #      so we compute actual_capture_fps = captured / animation_sec and
    #      use that as input rate (ensures animations play at real time).
    #   2. tpad filter: clones the LAST frame for hold_final_sec seconds,
    #      giving the viewer time to read the final composition.
    #   3. Output rate forced to OUTPUT_FPS (ffmpeg duplicates as needed).
    # Result: smooth uniform playback during animation + N seconds static
    # hold of the completed infographic + total MP4 duration == duration_sec.
    mp4_path = os.path.join(work_dir, f"scene_{scene_id}_infographic.mp4")
    actual_capture_fps = actual_frame_count / animation_sec
    print(
        f"[3/4] Encoding {actual_frame_count} frames @ {actual_capture_fps:.2f} real fps "
        f"+ {hold_final_sec:.2f}s static hold "
        f"-> MP4 ({duration_sec}s total, {output_fps}fps output, libx264 CRF 18)"
    )

    # Conditional filter: tpad only if hold_final_sec > 0
    # Lesson #88 (2026-04-26): render.js now uses deviceScaleFactor=2,
    # so PNG inputs are 3840x2160. We downscale to 1920x1080 with lanczos
    # for crisp typography (Playfair italic + 1px borders) without doubling
    # final MP4 size. tpad must run BEFORE scale to clone full-res frames.
    vf_chain = []
    if hold_final_sec > 0.05:
        vf_chain.append(f"tpad=stop_mode=clone:stop_duration={hold_final_sec:.3f}")
    vf_chain.append("scale=1920:1080:flags=lanczos")
    encode_cmd = [
        "ffmpeg", "-y",
        "-loglevel", "warning",
        "-framerate", f"{actual_capture_fps:.3f}",
        "-i", os.path.join(frames_dir, "frame_%05d.png"),
        "-vf", ",".join(vf_chain),
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-r", str(output_fps),
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
        "duration_actual_sec": round(duration_sec, 3),  # MP4 duration matches request
        "animation_sec": round(animation_sec, 3),
        "hold_final_sec": round(hold_final_sec, 3),
        "playback_rate": ANIMATION_PLAYBACK_RATE,
        "frames_captured": actual_frame_count,
        "real_capture_fps": round(actual_capture_fps, 2),
        "output_fps": output_fps,
        "file_size_bytes": file_size,
        "elapsed_sec": round(elapsed, 2),
    }


runpod.serverless.start({"handler": handler})
