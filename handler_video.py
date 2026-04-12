"""
YouTube Factory — Video Endpoint Handler
Job types: img-vid, compose, download
Models: Wan 2.2 I2V, FFmpeg
"""

import json
import base64
import os
import time
import uuid
import subprocess
import shutil
import urllib.request
import urllib.error
import glob as glob_module

import runpod
import websocket

# R2-based input downloader (music, ComfyUI inputs)
try:
    from download_r2_inputs import download_music as _download_music_r2
except ImportError:
    _download_music_r2 = None

# R2 storage (Cloudflare) — enabled when env vars are set
try:
    import r2_helper
    R2_ENABLED = bool(
        os.environ.get("R2_ENDPOINT")
        and os.environ.get("R2_ACCESS_KEY_ID")
        and os.environ.get("R2_SECRET_ACCESS_KEY")
    )
    if R2_ENABLED:
        print("[INFO] R2 storage enabled — dual-write mode (NV + R2)")
except ImportError:
    R2_ENABLED = False

COMFY_HOST = "127.0.0.1:8188"
COMFY_API_AVAILABLE_INTERVAL_MS = int(os.environ.get("COMFY_API_AVAILABLE_INTERVAL_MS", 500))
COMFY_API_AVAILABLE_MAX_RETRIES = int(os.environ.get("COMFY_API_AVAILABLE_MAX_RETRIES", 480))
COMFY_EXECUTION_TIMEOUT = int(os.environ.get("COMFY_EXECUTION_TIMEOUT", 600))
NETWORK_VOLUME_PATH = os.environ.get("RUNPOD_NETWORK_VOLUME_PATH", "/runpod-volume")
if os.path.isdir(NETWORK_VOLUME_PATH):
    PROJECTS_ROOT = os.path.join(NETWORK_VOLUME_PATH, "projects")
else:
    PROJECTS_ROOT = "/tmp/projects"
os.makedirs(PROJECTS_ROOT, exist_ok=True)

_OUTPUT_DIR_CANDIDATES = [
    "/comfyui/output",
    f"{NETWORK_VOLUME_PATH}/ComfyUI/output",
    "/comfyui/temp",
]

ACCEPTED_JOB_TYPES = {
    "img-vid": "video",
}

# ── NVENC encoder args (GPU hardware encoding) ────────────────
# Migrated from libx264 software encoding to h264_nvenc to leverage the
# RTX 4090 GPU that sits idle during compose. Expected cost reduction:
# ~$0.40 → ~$0.05-0.10 per compose.
#
# NVENC preset mapping (p1=fastest → p7=slowest/best quality).
# p5 + -tune hq ≈ libx264 fast/medium perceptual quality.
# -cq is constant-quality (equivalent to libx264 -crf). cq 19 ≈ crf 18.
NVENC_HQ_ARGS = [
    "-c:v", "h264_nvenc",
    "-preset", "p5",
    "-tune", "hq",
    "-rc", "vbr",
    "-cq", "19",
    "-b:v", "0",
    "-pix_fmt", "yuv420p",
    "-profile:v", "high",
    "-spatial-aq", "1",
    "-temporal-aq", "1",
]

# Lower-quality preset for video clip normalization (replaces libx264 crf 26).
# cq 23 ≈ crf 26 for the Wan 2.2 clip re-encode path.
NVENC_NORM_ARGS = [
    "-c:v", "h264_nvenc",
    "-preset", "p5",
    "-tune", "hq",
    "-rc", "vbr",
    "-cq", "23",
    "-b:v", "0",
    "-pix_fmt", "yuv420p",
]

# libx264 fallback equivalents (used only when NVENC is unavailable).
_X264_HQ_FALLBACK = [
    "-c:v", "libx264", "-preset", "fast", "-crf", "18", "-pix_fmt", "yuv420p",
]
_X264_NORM_FALLBACK = [
    "-c:v", "libx264", "-preset", "fast", "-crf", "26", "-pix_fmt", "yuv420p",
]

# Stderr markers that indicate NVENC is not usable on this host (cold start,
# driver mismatch, etc.). On match, we retry the command with libx264.
_NVENC_FAILURE_MARKERS = (
    "nvenc",
    "No NVENC capable devices",
    "Cannot load nvcuda",
    "CUDA_ERROR",
    "encoder not found",
    "Unknown encoder 'h264_nvenc'",
)


def _swap_nvenc_for_x264(cmd):
    """Return a new cmd list with h264_nvenc args replaced by libx264 equivalents.
    Handles both HQ and NORM presets by detecting the -cq value."""
    new_cmd = []
    i = 0
    # Quick pass: find the NVENC block bounds (from "-c:v" "h264_nvenc" to
    # the last NVENC-specific flag). We do a token-by-token rewrite: any
    # occurrence of "h264_nvenc" triggers replacement of the surrounding
    # encoder flags with the libx264 equivalent.
    #
    # Strategy: copy tokens through, but when we see -c:v h264_nvenc, skip
    # all encoder-related flags that follow and splice in the x264 fallback.
    nvenc_encoder_flags = {
        "-c:v", "-preset", "-tune", "-rc", "-cq", "-b:v",
        "-pix_fmt", "-profile:v", "-spatial-aq", "-temporal-aq",
        "-rc-lookahead", "-bf",
    }
    # Detect whether this command uses HQ or NORM by scanning for cq value
    fallback = _X264_HQ_FALLBACK
    try:
        cq_idx = cmd.index("-cq")
        if cmd[cq_idx + 1] == "23":
            fallback = _X264_NORM_FALLBACK
    except (ValueError, IndexError):
        pass

    swapped = False
    while i < len(cmd):
        tok = cmd[i]
        if not swapped and tok == "-c:v" and i + 1 < len(cmd) and cmd[i + 1] == "h264_nvenc":
            new_cmd.extend(fallback)
            i += 2
            # Skip any subsequent NVENC encoder flags (they come in -flag value pairs)
            while i < len(cmd) and cmd[i] in nvenc_encoder_flags:
                # Skip flag + value
                i += 2
            swapped = True
            continue
        new_cmd.append(tok)
        i += 1
    return new_cmd


def _run_ffmpeg_with_nvenc_fallback(cmd, *, timeout=180, label="ffmpeg"):
    """Run an ffmpeg command that uses NVENC; on NVENC-specific failure, retry
    once with libx264. Returns the CompletedProcess of the (possibly retried)
    invocation. Caller still inspects returncode for other errors."""
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode == 0:
        return result
    stderr_lc = (result.stderr or "").lower()
    if any(marker.lower() in stderr_lc for marker in _NVENC_FAILURE_MARKERS):
        fallback_cmd = _swap_nvenc_for_x264(cmd)
        print(f"[{label}] NVENC unavailable, falling back to libx264")
        result = subprocess.run(fallback_cmd, capture_output=True, text=True, timeout=timeout)
    return result

def free_vram():
    """Call ComfyUI /free endpoint to unload models and release VRAM/RAM between jobs."""
    try:
        data = json.dumps({"unload_models": True, "free_memory": True}).encode("utf-8")
        req = urllib.request.Request(
            f"http://{COMFY_HOST}/free",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=30)
    except Exception as e:
        print(f"[WARN] free_vram failed: {e}")

# Platform format specs: (gen_width, gen_height, target_width, target_height, fps)
# gen_ dimensions = multiple of 16 for VAE, target_ = final crop
PLATFORM_FORMATS = {
    "youtube":   (1920, 1088, 1920, 1080, 30),
    "shorts":    (1088, 1920, 1080, 1920, 30),
    "tiktok":    (1088, 1920, 1080, 1920, 30),
    "instagram": (1088, 1088, 1080, 1080, 30),
    "facebook":  (1088, 1088, 1080, 1080, 30),
    "linkedin":  (1920, 1088, 1920, 1080, 30),
}
DEFAULT_FORMAT = (1920, 1088, 1920, 1080, 30)



def project_dir(channel, content_id):
    return os.path.join(PROJECTS_ROOT, channel, content_id)


def source_dir(channel, content_id, media_type):
    return os.path.join(project_dir(channel, content_id), "source", media_type)


def output_dir_for(channel, content_id, platform):
    return os.path.join(project_dir(channel, content_id), "outputs", platform)


# ── ComfyUI Communication ──────────────────────────────────

def wait_for_comfyui():
    retries = 0
    while True:
        try:
            req = urllib.request.Request(f"http://{COMFY_HOST}/system_stats")
            urllib.request.urlopen(req, timeout=5)
            return True
        except Exception:
            retries += 1
            if COMFY_API_AVAILABLE_MAX_RETRIES > 0 and retries >= COMFY_API_AVAILABLE_MAX_RETRIES:
                raise RuntimeError(f"ComfyUI not available after {retries} retries")
            time.sleep(COMFY_API_AVAILABLE_INTERVAL_MS / 1000)


def queue_prompt(workflow_json, client_id):
    data = json.dumps({"prompt": workflow_json, "client_id": client_id}).encode("utf-8")
    req = urllib.request.Request(
        f"http://{COMFY_HOST}/prompt",
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ComfyUI rejected workflow (HTTP {e.code}): {error_body[:500]}")

    result = json.loads(resp.read())
    if "error" in result:
        raise RuntimeError(f"ComfyUI prompt error: {result['error']}")
    if "node_errors" in result and result["node_errors"]:
        raise RuntimeError(f"ComfyUI node errors: {json.dumps(result['node_errors'])[:500]}")

    prompt_id = result.get("prompt_id")
    if not prompt_id:
        raise RuntimeError(f"No prompt_id returned: {json.dumps(result)[:300]}")
    return prompt_id


def wait_for_completion(ws, prompt_id):
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > COMFY_EXECUTION_TIMEOUT:
            raise RuntimeError(f"Workflow execution timed out after {COMFY_EXECUTION_TIMEOUT}s.")

        try:
            msg = ws.recv()
        except websocket.WebSocketTimeoutException:
            try:
                history = get_history(prompt_id)
                if prompt_id in history:
                    return True
            except Exception:
                pass
            continue
        except websocket.WebSocketConnectionClosedException:
            try:
                history = get_history(prompt_id)
                if prompt_id in history:
                    return True
            except Exception:
                pass
            raise RuntimeError("WebSocket connection closed unexpectedly")

        if isinstance(msg, str):
            data = json.loads(msg)
            msg_type = data.get("type")
            if msg_type == "executing":
                exec_data = data.get("data", {})
                if exec_data.get("node") is None and exec_data.get("prompt_id") == prompt_id:
                    return True
            elif msg_type == "execution_error":
                error_data = data.get("data", {})
                if error_data.get("prompt_id") == prompt_id:
                    raise RuntimeError(f"ComfyUI execution error: {error_data}")


def get_history(prompt_id):
    req = urllib.request.Request(f"http://{COMFY_HOST}/history/{prompt_id}")
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


def find_output_file(filename, subfolder=""):
    if not filename:
        return None
    output_dirs = [d for d in _OUTPUT_DIR_CANDIDATES if os.path.isdir(d)]
    for out_dir in output_dirs:
        candidate = os.path.join(out_dir, subfolder, filename)
        if os.path.exists(candidate):
            return candidate
    for out_dir in output_dirs:
        pattern = os.path.join(out_dir, "**", filename)
        matches = glob_module.glob(pattern, recursive=True)
        if matches:
            return matches[0]
    return None


def collect_and_move(prompt_id, dest_dir, prefix, index=None):
    history = get_history(prompt_id)
    prompt_history = history.get(prompt_id, {})
    outputs = prompt_history.get("outputs", {})

    os.makedirs(dest_dir, exist_ok=True)

    all_files = []
    for node_id, node_output in outputs.items():
        # Video outputs (primary — gifs key is how ComfyUI returns video)
        if "gifs" in node_output:
            for vid in node_output["gifs"]:
                src = find_output_file(vid.get("filename"), vid.get("subfolder", ""))
                if src:
                    ext = os.path.splitext(vid["filename"])[1] or ".mp4"
                    all_files.append({"src": src, "ext": ext, "node_id": node_id, "type": "video"})

        # Images (possible intermediate frames)
        if "images" in node_output:
            for img in node_output["images"]:
                if img.get("type") != "temp":
                    src = find_output_file(img.get("filename"), img.get("subfolder", ""))
                    if src:
                        ext = os.path.splitext(img["filename"])[1] or ".png"
                        all_files.append({"src": src, "ext": ext, "node_id": node_id, "type": "image"})

    results = []
    for i, item in enumerate(all_files):
        if index is not None and len(all_files) == 1:
            dest_name = f"{prefix}_{index:03d}{item['ext']}"
        elif index is not None:
            dest_name = f"{prefix}_{index:03d}_{i + 1:03d}{item['ext']}"
        else:
            dest_name = f"{prefix}_{i + 1:03d}{item['ext']}"

        dest_path = os.path.join(dest_dir, dest_name)
        shutil.copy2(item["src"], dest_path)

        entry = {
            "filename": dest_name,
            "path": dest_path,
            "node_id": item["node_id"],
            "size_mb": round(os.path.getsize(dest_path) / 1024 / 1024, 2),
        }
        results.append(entry)

    return results


# ── Compose (FFmpeg) ────────────────────────────────────────

def run_compose(channel, content_id, platform, compose_config):
    src = os.path.join(project_dir(channel, content_id), "source")
    dest = output_dir_for(channel, content_id, platform)
    os.makedirs(dest, exist_ok=True)

    compose_type = compose_config.get("type", "concat_audio")

    if compose_type == "concat_audio":
        return _compose_concat_audio(src, dest, content_id)
    elif compose_type == "concat_video":
        return _compose_concat_video(src, dest, content_id)
    elif compose_type == "full":
        return _compose_full(src, dest, content_id, compose_config)
    elif compose_type == "scene_manifest":
        return _compose_scene_manifest(src, dest, content_id, compose_config, channel, platform)
    else:
        raise RuntimeError(f"Unknown compose type: {compose_type}")


def _get_video_duration(path):
    """Get video duration in seconds using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe failed for {path}: {result.stderr[-200:]}")
    return float(result.stdout.strip())


def _parse_time_to_seconds(value):
    """Parse a time value to seconds.
    Accepts float/int, or strings in 'HH:MM:SS,mmm' / 'HH:MM:SS.mmm' / 'MM:SS' / 'SS' format.
    Returns None if value is None or unparseable.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip().replace(",", ".")
    if not s:
        return None
    try:
        if ":" in s:
            parts = s.split(":")
            parts = [float(p) for p in parts]
            if len(parts) == 3:
                h, m, sec = parts
                return h * 3600 + m * 60 + sec
            if len(parts) == 2:
                m, sec = parts
                return m * 60 + sec
        return float(s)
    except (ValueError, TypeError):
        return None


def _pad_segment_to_duration(seg_path, out_path, pad_duration, width, height, fps):
    """Create a new segment = seg_path + freeze-frame tail of pad_duration seconds."""
    vf = (
        f"scale={width}:{height}:force_original_aspect_ratio=increase,"
        f"crop={width}:{height},setsar=1,"
        f"tpad=stop_mode=clone:stop_duration={pad_duration:.4f}"
    )
    cmd = [
        "ffmpeg", "-y", "-i", seg_path,
        "-vf", vf,
        "-r", str(fps),
        *NVENC_NORM_ARGS, "-an", out_path,
    ]
    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=180, label="pad_segment")
    if result.returncode != 0:
        raise RuntimeError(f"pad segment failed: {result.stderr[-200:]}")
    return out_path


def _ensure_compose_inputs(channel, content_id, src, dest):
    """Always download ALL compose inputs from R2 (NV-free: workers don't share filesystem).
    Without NV, each worker only has files it generated locally. The compose worker
    must download all files from R2 to get a complete picture."""
    if not R2_ENABLED:
        return
    for subdir in ("images", "video", "audio", "srt"):
        local_dir = os.path.join(src, subdir)
        r2_prefix = f"{channel}/{content_id}/source/{subdir}/"
        try:
            keys = r2_helper.list_files(r2_prefix)
            if keys:
                os.makedirs(local_dir, exist_ok=True)
                downloaded = 0
                for key in keys:
                    local_path = os.path.join(local_dir, os.path.basename(key))
                    if not os.path.exists(local_path):
                        r2_helper.download_file(key, local_path)
                        downloaded += 1
                print(f"[INFO] {subdir}/: {len(keys)} files in R2, {downloaded} downloaded")
        except Exception as e:
            print(f"[WARN] R2 download failed for {subdir}: {e}")

    # Also check for concat audio in outputs dir
    audio_in_dest = os.path.join(dest, f"{content_id}_audio.flac")
    audio_in_src = os.path.join(src, "audio", f"{content_id}_audio.flac")
    if not os.path.exists(audio_in_dest) and not os.path.exists(audio_in_src):
        # Try R2 output path (from Audio endpoint compose)
        for r2_key in [
            f"{channel}/{content_id}/output/youtube/{content_id}_audio.flac",
            f"{channel}/{content_id}/output/{content_id}_audio.flac",
        ]:
            try:
                if r2_helper.file_exists(r2_key):
                    os.makedirs(os.path.dirname(audio_in_src), exist_ok=True)
                    r2_helper.download_file(r2_key, audio_in_src)
                    print(f"[INFO] Downloaded concat audio from R2: {r2_key}")
                    break
            except Exception as e:
                print(f"[WARN] R2 concat audio download failed: {e}")


# ── Scene Director visual effects ──────────────────────────
# The Scene Director (LLM) emits one of 16 effect strings per image-based
# scene. Everything below maps an effect name to a concrete ffmpeg filter
# chain. All effects output WxH @ fps, h264 yuv420p, exact duration_sec.
# Output is an MP4 with no audio (ambient silence is generated separately).

PARTICLES_ASSET_CANDIDATES = [
    "/workspace/assets/particles.mov",
    "/comfyui/input/particles.mov",
]

# Zoompan is notoriously finicky: it needs a large pre-scaled input to avoid
# jitter (the classic "wobble" artifact on integer pixel snaps).
# scale=8000:-1 mirrors what the existing ken_burns branch was already doing.
_ZOOMPAN_PRESCALE = "scale=8000:-1"


def _zoompan_chain(z_expr, x_expr, y_expr, frames, w, h, fps):
    """Build a standard zoompan filter string for an effect."""
    return (
        f"{_ZOOMPAN_PRESCALE},"
        f"zoompan=z='{z_expr}':x='{x_expr}':y='{y_expr}'"
        f":d={frames}:s={w}x{h}:fps={fps},"
        f"format=yuv420p"
    )


def _run_single_image_effect(img_path, filter_complex, duration, seg_path):
    """Run ffmpeg for a single-image effect. Centralized so every effect
    uses the same encoder flags (h264_nvenc HQ preset, yuv420p)."""
    cmd = [
        "ffmpeg", "-y", "-loop", "1", "-i", img_path,
        "-filter_complex", filter_complex,
        "-t", f"{duration:.4f}",
        *NVENC_HQ_ARGS, "-an", seg_path,
    ]
    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=180, label="effect")
    if result.returncode != 0:
        raise RuntimeError(f"effect render failed: {result.stderr[-300:]}")
    return seg_path


def _find_particles_asset():
    for p in PARTICLES_ASSET_CANDIDATES:
        if os.path.isfile(p):
            return p
    return None


def _render_image_effect(effect, img_a, img_b, duration, seg_path, w, h, fps):
    """Route a scene effect to the correct ffmpeg filter chain.

    effect:   One of the 16 Scene Director effect strings. Unknown → static.
    img_a:    Primary image path (required).
    img_b:    Secondary image path (only for parallax / match_cut). May be None.
    duration: Exact seconds for the segment.
    seg_path: Output mp4 path.
    w,h,fps:  Platform target dims / framerate.
    Returns seg_path. Raises on ffmpeg failure.
    """
    frames = max(1, int(round(duration * fps)))
    # Centered x/y expressions used by most zoompan variants
    cx = "iw/2-(iw/zoom/2)"
    cy = "ih/2-(ih/zoom/2)"

    # Normalize unknown / missing effect → fallback chain.
    if not effect:
        effect = "ken_burns_slow"
    original_effect = effect

    # Parallax / match_cut need two images. Fallback silently if only one.
    if effect in ("parallax", "match_cut") and not img_b:
        print(f"[effect] {effect} requested but only 1 image available, fallback → ken_burns_slow")
        effect = "ken_burns_slow"

    # ---- Ken Burns family (zoom in/out, centered) ------------------------
    # Fix D: duration-aware z_rate so zoom reaches cap/floor exactly on the
    # last frame — no static tail on long scenes.
    if effect == "ken_burns_in":
        # Slow zoom from 1.0 → 1.15 over the whole duration
        z_rate = 0.15 / max(frames - 1, 1)
        print(f"[effect] {effect} z_rate={z_rate:.8f} frames={frames}")
        z = f"min(zoom+{z_rate:.8f},1.15)"
        fc = _zoompan_chain(z, cx, cy, frames, w, h, fps)
    elif effect == "ken_burns_out":
        # Start at 1.15x and zoom out toward 1.0x. zoompan's z is a running
        # accumulator; we initialize it at 1.15 and decrement each frame.
        z_rate = 0.15 / max(frames - 1, 1)
        print(f"[effect] {effect} z_rate={z_rate:.8f} frames={frames}")
        z = f"if(eq(on,0),1.15,max(zoom-{z_rate:.8f},1.0))"
        fc = _zoompan_chain(z, cx, cy, frames, w, h, fps)
    elif effect == "ken_burns_slow":
        z_rate = 0.08 / max(frames - 1, 1)
        print(f"[effect] {effect} z_rate={z_rate:.8f} frames={frames}")
        z = f"min(zoom+{z_rate:.8f},1.08)"
        fc = _zoompan_chain(z, cx, cy, frames, w, h, fps)
    elif effect == "ken_burns_fast":
        z_rate = 0.35 / max(frames - 1, 1)
        print(f"[effect] {effect} z_rate={z_rate:.8f} frames={frames}")
        z = f"min(zoom+{z_rate:.8f},1.35)"
        fc = _zoompan_chain(z, cx, cy, frames, w, h, fps)

    # ---- Pan L/R: horizontal camera slide, slight zoom so crop is valid --
    elif effect == "pan_left":
        # Camera moves left→right across the frame (image appears to slide L)
        z = "1.20"
        x = f"(iw-iw/zoom)*on/{frames}"
        y = cy
        fc = _zoompan_chain(z, x, y, frames, w, h, fps)
    elif effect == "pan_right":
        z = "1.20"
        x = f"(iw-iw/zoom)*(1-on/{frames})"
        y = cy
        fc = _zoompan_chain(z, x, y, frames, w, h, fps)

    # ---- Tilt up/down: vertical camera slide ----------------------------
    elif effect == "tilt_up":
        # Camera rises: y goes from bottom→top
        z = "1.20"
        x = cx
        y = f"(ih-ih/zoom)*(1-on/{frames})"
        fc = _zoompan_chain(z, x, y, frames, w, h, fps)
    elif effect == "tilt_down":
        z = "1.20"
        x = cx
        y = f"(ih-ih/zoom)*on/{frames}"
        fc = _zoompan_chain(z, x, y, frames, w, h, fps)

    # ---- Rapid zoom in/out: more aggressive than Ken Burns fast ---------
    # Fix D: duration-aware z_rate (same rationale as ken_burns family).
    elif effect == "zoom_in":
        z_rate = 0.50 / max(frames - 1, 1)
        print(f"[effect] {effect} z_rate={z_rate:.8f} frames={frames}")
        z = f"min(zoom+{z_rate:.8f},1.50)"
        fc = _zoompan_chain(z, cx, cy, frames, w, h, fps)
    elif effect == "zoom_out":
        z_rate = 0.50 / max(frames - 1, 1)
        print(f"[effect] {effect} z_rate={z_rate:.8f} frames={frames}")
        z = f"if(eq(on,0),1.50,max(zoom-{z_rate:.8f},1.0))"
        fc = _zoompan_chain(z, cx, cy, frames, w, h, fps)

    # ---- Whip pan: intra-scene fast horizontal pan with motion blur -----
    # Strategy: the first ~0.35s is a whip (zoompan with huge x delta + tmix
    # motion blur) and the remainder is stable ken_burns_slow. Chosen as
    # entrance because the Director emits whip_pan to *introduce* a scene.
    elif effect == "whip_pan":
        whip_dur = min(0.35, duration * 0.25)
        whip_frames = max(1, int(round(whip_dur * fps)))
        stable_frames = max(1, frames - whip_frames)
        fc = (
            f"{_ZOOMPAN_PRESCALE},"
            f"zoompan=z='1.25':x='(iw-iw/zoom)*on/{whip_frames}':y='{cy}'"
            f":d={whip_frames}:s={w}x{h}:fps={fps},"
            f"tmix=frames=5:weights='1 1 1 1 1',"
            f"format=yuv420p"
        )
        # Two-pass: render whip segment then append ken_burns_slow segment
        # via concat. Keeping it single-pass is simpler but visually worse;
        # for v1 we fake motion blur with tmix and let the whole duration
        # share the same filter chain to avoid a second concat step.
        # Override: single zoompan over full duration with tmix blur.
        fc = (
            f"{_ZOOMPAN_PRESCALE},"
            f"zoompan=z='1.25':x='if(lt(on,{whip_frames}),(iw-iw/zoom)*on/{whip_frames},(iw-iw/zoom))':y='{cy}'"
            f":d={frames}:s={w}x{h}:fps={fps},"
            f"tmix=frames=5:weights='1 1 1 1 1',"
            f"format=yuv420p"
        )

    # ---- Static: single image, no movement ------------------------------
    elif effect == "static":
        fc = (
            f"scale={w}:{h}:force_original_aspect_ratio=increase,"
            f"crop={w}:{h},setsar=1,format=yuv420p"
        )

    # ---- Particles: overlay particles.mov on ken_burns_slow -------------
    elif effect == "particles":
        particles = _find_particles_asset()
        if particles:
            print(f"[effect] particles using asset {particles}")
            # Two-input overlay — handled below with a dedicated ffmpeg call
            return _render_particles(img_a, particles, duration, seg_path, w, h, fps)
        print(f"[effect] particles fallback → ken_burns_slow + noise (no asset found)")
        z = f"min(zoom+0.0006,1.08)"
        fc = (
            f"{_ZOOMPAN_PRESCALE},"
            f"zoompan=z='{z}':x='{cx}':y='{cy}'"
            f":d={frames}:s={w}x{h}:fps={fps},"
            f"noise=alls=8:allf=t+u,format=yuv420p"
        )

    # ---- Parallax: fg/bg layers at different speeds --------------------
    elif effect == "parallax":
        # img_a = foreground (sharper), img_b = background (blurred, slower).
        # We overlay a scaled fg over a slowly panning, blurred bg.
        return _render_parallax(img_a, img_b, duration, seg_path, w, h, fps)

    # ---- Match cut: two images back-to-back, no transition -------------
    elif effect == "match_cut":
        return _render_match_cut(img_a, img_b, duration, seg_path, w, h, fps)

    # ---- Unknown effect: fallback to static -----------------------------
    else:
        print(f"[effect] unknown '{original_effect}', fallback → static")
        fc = (
            f"scale={w}:{h}:force_original_aspect_ratio=increase,"
            f"crop={w}:{h},setsar=1,format=yuv420p"
        )

    print(f"[effect] {original_effect} d={duration:.2f}s")
    return _run_single_image_effect(img_a, fc, duration, seg_path)


def _render_parallax(img_fg, img_bg, duration, seg_path, w, h, fps):
    """Parallax: blurred bg pans slow, fg pans faster (2 images required)."""
    frames = max(1, int(round(duration * fps)))
    # bg: heavy blur, subtle pan; fg: full res, twice the pan speed.
    # Both scaled to w*1.25 so panning has travel room.
    fc = (
        f"[0:v]scale={int(w*1.25)}:-1,crop={w}:{h}:x='(iw-{w})*(on/{frames})*0.5':y='(ih-{h})/2',setsar=1[fg];"
        f"[1:v]scale={int(w*1.4)}:-1,boxblur=20:2,crop={w}:{h}:x='(iw-{w})*(on/{frames})*0.2':y='(ih-{h})/2',setsar=1[bg];"
        f"[bg][fg]overlay=0:0:format=auto,format=yuv420p"
    )
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", img_fg,
        "-loop", "1", "-i", img_bg,
        "-filter_complex", fc,
        "-t", f"{duration:.4f}", "-r", str(fps),
        *NVENC_HQ_ARGS, "-an", seg_path,
    ]
    print(f"[effect] parallax d={duration:.2f}s (fg={os.path.basename(img_fg)} bg={os.path.basename(img_bg)})")
    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=180, label="parallax")
    if result.returncode != 0:
        raise RuntimeError(f"parallax render failed: {result.stderr[-300:]}")
    return seg_path


def _render_match_cut(img_a, img_b, duration, seg_path, w, h, fps):
    """Match cut: img_a for first half, img_b for second half, no transition.
    Hard cut at exactly duration/2. Director guarantees composition alignment."""
    half = duration / 2.0
    fc = (
        f"[0:v]scale={w}:{h}:force_original_aspect_ratio=increase,"
        f"crop={w}:{h},setsar=1,format=yuv420p,trim=duration={half:.4f}[a];"
        f"[1:v]scale={w}:{h}:force_original_aspect_ratio=increase,"
        f"crop={w}:{h},setsar=1,format=yuv420p,trim=duration={half:.4f}[b];"
        f"[a][b]concat=n=2:v=1:a=0[v]"
    )
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", img_a,
        "-loop", "1", "-i", img_b,
        "-filter_complex", fc, "-map", "[v]",
        "-t", f"{duration:.4f}", "-r", str(fps),
        *NVENC_HQ_ARGS, "-an", seg_path,
    ]
    print(f"[effect] match_cut d={duration:.2f}s (a={os.path.basename(img_a)} b={os.path.basename(img_b)})")
    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=180, label="match_cut")
    if result.returncode != 0:
        raise RuntimeError(f"match_cut render failed: {result.stderr[-300:]}")
    return seg_path


def _render_particles(img_path, particles_path, duration, seg_path, w, h, fps):
    """Blend looping particles.mov on top of a ken_burns_slow base using screen
    mode. Source is Mixkit MP4 on pure black background (no alpha) — screen
    blend treats black as transparent and keeps bright particles visible."""
    frames = max(1, int(round(duration * fps)))
    cx = "iw/2-(iw/zoom/2)"
    cy = "ih/2-(ih/zoom/2)"
    z = f"min(zoom+0.0006,1.08)"
    fc = (
        f"[0:v]{_ZOOMPAN_PRESCALE},"
        f"zoompan=z='{z}':x='{cx}':y='{cy}'"
        f":d={frames}:s={w}x{h}:fps={fps},format=yuv420p[base];"
        f"[1:v]scale={w}:{h},fps={fps},eq=brightness=-0.05,format=yuv420p[parts];"
        f"[base][parts]blend=all_mode=screen:shortest=1,format=yuv420p"
    )
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1", "-i", img_path,
        "-stream_loop", "-1", "-i", particles_path,
        "-filter_complex", fc,
        "-t", f"{duration:.4f}", "-r", str(fps),
        *NVENC_HQ_ARGS, "-an", seg_path,
    ]
    print(f"[effect] particles d={duration:.2f}s")
    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=180, label="particles")
    if result.returncode != 0:
        raise RuntimeError(f"particles render failed: {result.stderr[-300:]}")
    return seg_path


def _apply_particles_overlay(video_path, seg_path, duration, w, h, fps):
    """Overlay particles.mov on top of an already-rendered base effect video.
    Uses screen blend mode (black=transparent). If particles asset is missing
    or ffmpeg fails, silently returns the base video unchanged."""
    particles = _find_particles_asset()
    if not particles:
        print(f"[particles_overlay] no asset found, skipping overlay")
        return video_path
    fc = (
        f"[0:v]format=yuv420p[base];"
        f"[1:v]scale={w}:{h},fps={fps},eq=brightness=-0.05,format=yuv420p[parts];"
        f"[base][parts]blend=all_mode=screen:shortest=1,format=yuv420p"
    )
    cmd = [
        "ffmpeg", "-y",
        "-i", video_path,
        "-stream_loop", "-1", "-i", particles,
        "-filter_complex", fc,
        "-t", f"{duration:.4f}", "-r", str(fps),
        *NVENC_HQ_ARGS, "-an", seg_path,
    ]
    print(f"[effect] particles_overlay on base d={duration:.2f}s")
    try:
        result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=180, label="particles_overlay")
        if result.returncode != 0:
            print(f"WARN: particles_overlay failed, using base: {result.stderr[-200:]}")
            if os.path.exists(video_path) and video_path != seg_path:
                os.rename(video_path, seg_path)
            return seg_path
        # Clean up temp base file
        if os.path.exists(video_path) and video_path != seg_path:
            os.remove(video_path)
        return seg_path
    except Exception as e:
        print(f"WARN: particles_overlay exception, using base: {e}")
        if os.path.exists(video_path) and video_path != seg_path:
            os.rename(video_path, seg_path)
        return seg_path


def _compose_scene_manifest(src, dest, content_id, config, channel, platform="youtube"):
    """
    Compose full video from scenes.json v2 manifest.
    Phase 1: Render each scene to a normalized segment (1920x1080, 30fps, h264)
    Phase 2: Apply transitions between segments (cut, dissolve, fade_black, fade_white)
    Phase 3: Mux with audio, background music, and optional subtitles
    """
    # Ensure all inputs are available locally (downloads from R2 if NV missing)
    _ensure_compose_inputs(channel, content_id, src, dest)

    # Load scenes - prefer inline from payload, fallback to NV file
    if config.get("scenes"):
        scenes = config["scenes"]
        scenes_data = {"scenes": scenes, "music_acts": config.get("music_acts", [])}
    else:
        scenes_path = config.get("scenes_path", os.path.join(src, "..", "config", "scenes.json"))
        if not os.path.exists(scenes_path):
            scenes_path = os.path.join(os.path.dirname(src), "config", "scenes.json")
        if not os.path.exists(scenes_path):
            raise RuntimeError(f"scenes.json not found at {scenes_path}")
        with open(scenes_path, "r") as f:
            scenes_data = json.load(f)
        scenes = scenes_data.get("scenes", [])
    if not scenes:
        raise RuntimeError("No scenes provided (inline or file)")

    img_dir = os.path.join(src, "images")
    vid_dir = os.path.join(src, "video")
    segments_dir = os.path.join(dest, "_segments")
    os.makedirs(segments_dir, exist_ok=True)

    _gen_w, _gen_h, WIDTH, HEIGHT, FPS = PLATFORM_FORMATS.get(platform, DEFAULT_FORMAT)
    # NOTE: NORM string is unused (kept for backwards reference). Actual
    # normalization uses the NVENC_NORM_ARGS list built inline below.
    NORM = f"-vf scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=increase,crop={WIDTH}:{HEIGHT},setsar=1 -r {FPS} -c:v h264_nvenc -preset p5 -tune hq -rc vbr -cq 23 -b:v 0 -pix_fmt yuv420p -an"

    # Directory for extracted ambient audio from video clips
    ambient_dir = os.path.join(segments_dir, "_ambient")
    os.makedirs(ambient_dir, exist_ok=True)

    def _find_image(img_dir, sid, frame_type):
        """Find best image for a scene. Prefers refined (_002) over base (_001).
        Pattern: scene_{sid}_{frame_type}_{index}_{file}.png
        Example: scene_000_initial_000_002.png (refined)
        """
        # Try exact match first (legacy naming)
        exact = os.path.join(img_dir, f"scene_{sid:03d}_{frame_type}.png")
        if os.path.exists(exact):
            return exact
        # Glob for actual naming pattern, sorted → last = refined
        candidates = sorted(glob_module.glob(
            os.path.join(img_dir, f"scene_{sid:03d}_{frame_type}_*.png")))
        if candidates:
            return candidates[-1]  # Last file = most refined version
        return None


    # ── Phase 1: Render each scene to a normalized segment ──
    segment_paths = []
    skipped = 0

    for scene in scenes:
        sid = scene["scene_id"]
        # New Director schema uses `effect` for image-based scenes and
        # `video_clip` as a special render_type. If render_type is absent
        # but `effect` is present, treat it as an image-based scene.
        render_type = scene.get("render_type")
        if not render_type:
            render_type = "video_clip" if scene.get("effect") == "video_clip" else "ken_burns"
        duration = scene.get("duration_sec", 5)
        seg_path = os.path.join(segments_dir, f"seg_{sid:04d}.mp4")
        amb_path = os.path.join(ambient_dir, f"amb_{sid:04d}.aac")

        try:
            if render_type == "video_clip":
                # Use video-generated clip, normalize it
                clip_path = os.path.join(vid_dir, f"scene_{sid:03d}.mp4")
                if not os.path.exists(clip_path):
                    # Fallback: try with different naming
                    candidates = glob_module.glob(os.path.join(vid_dir, f"*{sid:03d}*"))
                    clip_path = candidates[0] if candidates else clip_path
                if not os.path.exists(clip_path):
                    print(f"WARN: scene {sid} video_clip missing {clip_path}, falling back to ken_burns")
                    render_type = "ken_burns"
                else:
                    # Probe real clip duration. Wan 2.2 I2V emits exactly 5.0625s
                    # (81 frames @ 16fps) but Scene Director assigns target
                    # duration_sec from Whisper SRT timestamps (e.g. 5.26s, 6.7s).
                    # Mismatch causes cumulative audio/video drift — pad with
                    # freeze frame (or trim if longer) to match target exactly.
                    target_dur = float(duration)
                    try:
                        clip_real_dur = _get_video_duration(clip_path)
                    except Exception as e:
                        print(f"WARN: scene {sid} ffprobe failed ({e}), assuming clip matches target")
                        clip_real_dur = target_dur
                    delta = target_dur - clip_real_dur

                    # Base normalization args (as list to allow injecting filters).
                    # Uses NVENC_NORM_ARGS (cq 23 ≈ libx264 crf 26).
                    norm_args = [
                        "-vf",
                        f"scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=increase,"
                        f"crop={WIDTH}:{HEIGHT},setsar=1",
                        "-r", str(FPS),
                        *NVENC_NORM_ARGS, "-an",
                    ]

                    if delta > 0.05:
                        # Fix B: Instead of freezing the last frame (which looks
                        # dead), keep the camera "alive" by applying a very subtle
                        # ken_burns zoompan (1.0 → 1.06x) over the last frame for
                        # `delta` seconds, then concat with the normalized clip.
                        # Two-pass approach — more robust than a conditional
                        # zoompan filter, and guarantees the first tail frame
                        # matches the last clip frame (same source pixel).
                        clip_seg = os.path.join(segments_dir, f"seg_{sid:04d}_clip.mp4")
                        tail_seg = os.path.join(segments_dir, f"seg_{sid:04d}_tail.mp4")
                        tail_png = os.path.join(segments_dir, f"seg_{sid:04d}_last.png")
                        concat_list = os.path.join(segments_dir, f"seg_{sid:04d}_concat.txt")

                        # Pass 1: normalize the Wan 2.2 clip as-is (no tpad).
                        cmd_clip = ["ffmpeg", "-y", "-i", clip_path] + norm_args + [clip_seg]
                        result = _run_ffmpeg_with_nvenc_fallback(cmd_clip, timeout=180, label=f"video_clip_norm sid={sid} (clip)")
                        if result.returncode != 0:
                            raise RuntimeError(f"segment {sid} video_clip (clip pass): {result.stderr[-200:]}")

                        # Pass 2a: extract last frame of the normalized clip
                        # (already at WIDTH×HEIGHT, so no re-scale needed).
                        extract_cmd = [
                            "ffmpeg", "-y", "-sseof", "-0.1", "-i", clip_seg,
                            "-vsync", "0", "-frames:v", "1", "-q:v", "2", tail_png
                        ]
                        ext_res = subprocess.run(extract_cmd, capture_output=True, text=True, timeout=30)
                        if ext_res.returncode != 0 or not os.path.exists(tail_png):
                            raise RuntimeError(f"segment {sid} video_clip: failed to extract last frame: {ext_res.stderr[-200:]}")

                        # Pass 2b: render ken_burns zoompan from the PNG for
                        # `delta` seconds. Zoom 1.0 → 1.06 centered, linear.
                        tail_frames = max(1, int(round(delta * FPS)))
                        # zoompan expression: z starts at 1.0, linearly reaches
                        # 1.06 at the last frame. on=frame index, d=tail_frames.
                        # Use `on/(tail_frames-1)` ratio clamped to [0,1].
                        zoom_end = 1.06
                        if tail_frames <= 1:
                            zoom_expr = "1.0"
                        else:
                            zoom_expr = f"1.0+({zoom_end-1.0:.4f})*on/{tail_frames-1}"
                        # Render at higher internal resolution to avoid zoompan
                        # jitter, then downscale to WIDTH×HEIGHT.
                        zp_filter = (
                            f"scale={WIDTH*4}:{HEIGHT*4}:flags=lanczos,"
                            f"zoompan=z='{zoom_expr}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
                            f"d={tail_frames}:s={WIDTH}x{HEIGHT}:fps={FPS},"
                            f"setsar=1"
                        )
                        cmd_tail = [
                            "ffmpeg", "-y",
                            "-loop", "1", "-i", tail_png,
                            "-vf", zp_filter,
                            "-t", f"{delta:.4f}",
                            "-r", str(FPS),
                            *NVENC_NORM_ARGS, "-an",
                            tail_seg,
                        ]
                        result = _run_ffmpeg_with_nvenc_fallback(cmd_tail, timeout=120, label=f"video_clip_norm sid={sid} (tail)")
                        if result.returncode != 0:
                            raise RuntimeError(f"segment {sid} video_clip (tail pass): {result.stderr[-200:]}")

                        # Pass 3: concat demuxer — lossless stitch.
                        with open(concat_list, "w") as f:
                            f.write(f"file '{os.path.basename(clip_seg)}'\n")
                            f.write(f"file '{os.path.basename(tail_seg)}'\n")
                        cmd_concat = [
                            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                            "-i", concat_list, "-c", "copy", seg_path,
                        ]
                        cc_res = subprocess.run(cmd_concat, capture_output=True, text=True, timeout=60)
                        if cc_res.returncode != 0:
                            # Fallback: re-encode the concat if copy fails
                            # (e.g. slight timebase mismatch).
                            cmd_concat_re = [
                                "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                                "-i", concat_list, "-r", str(FPS),
                                *NVENC_NORM_ARGS, "-an", seg_path,
                            ]
                            cc_res = _run_ffmpeg_with_nvenc_fallback(cmd_concat_re, timeout=120, label=f"video_clip_norm sid={sid} (concat reencode)")
                            if cc_res.returncode != 0:
                                raise RuntimeError(f"segment {sid} video_clip (concat): {cc_res.stderr[-200:]}")

                        # Cleanup intermediates (best effort).
                        for _tmp in (clip_seg, tail_seg, tail_png, concat_list):
                            try:
                                os.remove(_tmp)
                            except Exception:
                                pass

                        print(f"[video_clip sid={sid}] freeze-tail ken_burns zoompan +{delta:.4f}s (real={clip_real_dur:.4f} target={target_dur:.4f})")
                        # Extract ambient audio from the original clip.
                        ext_cmd = ["ffmpeg", "-y", "-i", clip_path, "-vn", "-c:a", "aac", "-b:a", "128k", amb_path]
                        ext_result = subprocess.run(ext_cmd, capture_output=True, text=True, timeout=30)
                        if ext_result.returncode != 0:
                            sil_cmd = ["ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo", "-t", str(duration), "-c:a", "aac", amb_path]
                            subprocess.run(sil_cmd, capture_output=True, text=True, timeout=30)
                        segment_paths.append({"path": seg_path, "scene": scene})
                        continue
                    elif delta < -0.05:
                        # Clip longer than target — trim to target_dur.
                        cmd = ["ffmpeg", "-y", "-i", clip_path] + norm_args + ["-t", f"{target_dur:.4f}", seg_path]
                        print(f"[video_clip sid={sid}] trim to {target_dur:.4f}s (real={clip_real_dur:.4f})")
                    else:
                        # Within 50ms tolerance — normalize as-is.
                        cmd = ["ffmpeg", "-y", "-i", clip_path] + norm_args + [seg_path]

                    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=180, label=f"video_clip_norm sid={sid}")
                    if result.returncode != 0:
                        raise RuntimeError(f"segment {sid} video_clip: {result.stderr[-200:]}")
                    # Extract ambient audio from video clip (best effort)
                    ext_cmd = ["ffmpeg", "-y", "-i", clip_path, "-vn", "-c:a", "aac", "-b:a", "128k", amb_path]
                    ext_result = subprocess.run(ext_cmd, capture_output=True, text=True, timeout=30)
                    if ext_result.returncode != 0:
                        # No audio in clip — generate silence
                        sil_cmd = ["ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo", "-t", str(duration), "-c:a", "aac", amb_path]
                        subprocess.run(sil_cmd, capture_output=True, text=True, timeout=30)
                    segment_paths.append({"path": seg_path, "scene": scene})
                    continue

            if render_type == "crossfade":
                # Crossfade between two images
                img_a = _find_image(img_dir, sid, "initial")
                img_b = _find_image(img_dir, sid, "final")
                if not img_a:
                    print(f"WARN: scene {sid} crossfade missing initial image, skipping")
                    skipped += 1
                    continue
                if not img_b:
                    # Fallback to ken_burns if no final image
                    render_type = "ken_burns"
                else:
                    frames = int(duration * FPS)
                    cmd = [
                        "ffmpeg", "-y",
                        "-loop", "1", "-i", img_a,
                        "-loop", "1", "-i", img_b,
                        "-filter_complex",
                        f"[0:v]scale={WIDTH}:{HEIGHT},setsar=1,format=yuv420p[a];"
                        f"[1:v]scale={WIDTH}:{HEIGHT},setsar=1,format=yuv420p[b];"
                        f"[a][b]blend=all_expr='A*(1-T/{duration})+B*(T/{duration})':shortest=1",
                        "-t", str(duration), "-r", str(FPS),
                        *NVENC_HQ_ARGS, seg_path
                    ]
                    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=120, label=f"crossfade sid={sid}")
                    if result.returncode != 0:
                        raise RuntimeError(f"segment {sid} crossfade: {result.stderr[-200:]}")
                    # Generate silence for non-video scenes
                    sil_cmd = ["ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo", "-t", str(duration), "-c:a", "aac", amb_path]
                    subprocess.run(sil_cmd, capture_output=True, text=True, timeout=30)
                    segment_paths.append({"path": seg_path, "scene": scene})
                    continue

            if render_type == "ken_burns":
                # Image-based scene. New Scene Director emits a `effect` field
                # with one of 16 possible values. Legacy scenes without it
                # default to ken_burns_slow for backward compatibility.
                effect = scene.get("effect") or "ken_burns_slow"
                img_a = _find_image(img_dir, sid, "initial")
                if not img_a:
                    print(f"WARN: scene {sid} missing image, skipping")
                    skipped += 1
                    continue
                # parallax / match_cut may request a second image. Director
                # sets images_needed=2 for those. We look for the "final"
                # frame slot (already used by legacy crossfade).
                img_b = None
                if effect in ("parallax", "match_cut") or scene.get("images_needed") == 2:
                    img_b = _find_image(img_dir, sid, "final")

                try:
                    # If particles overlay requested, render base to temp first
                    if scene.get("_particles_overlay"):
                        base_path = seg_path.replace(".mp4", "_base.mp4")
                        _render_image_effect(effect, img_a, img_b, float(duration),
                                             base_path, WIDTH, HEIGHT, FPS)
                        _apply_particles_overlay(base_path, seg_path, float(duration),
                                                 WIDTH, HEIGHT, FPS)
                    else:
                        _render_image_effect(effect, img_a, img_b, float(duration),
                                             seg_path, WIDTH, HEIGHT, FPS)
                except Exception as e:
                    print(f"WARN: scene {sid} effect '{effect}' failed ({e}), falling back to static")
                    # Last-resort fallback: static render. Never let a bad
                    # effect string kill the whole compose run.
                    _render_image_effect("static", img_a, None, float(duration),
                                         seg_path, WIDTH, HEIGHT, FPS)

                # Generate silence for non-video scenes
                sil_cmd = ["ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo", "-t", str(duration), "-c:a", "aac", amb_path]
                subprocess.run(sil_cmd, capture_output=True, text=True, timeout=30)
                segment_paths.append({"path": seg_path, "scene": scene})

        except Exception as e:
            print(f"ERROR scene {sid}: {e}")
            skipped += 1

    if not segment_paths:
        raise RuntimeError("No segments rendered successfully")

    print(f"Phase 1 done: {len(segment_paths)} segments, {skipped} skipped")

    # ── Phase 1.5: Insert freeze-frame fillers for gaps between scenes ──
    # Scene Director may emit gaps between consecutive scenes (scene[i+1].start_time
    # != scene[i].start_time + scene[i].duration_sec). Narration audio respects
    # those gaps but video would compress them, causing cumulative desync.
    total_duration_sec = _parse_time_to_seconds(scenes_data.get("total_duration_sec"))
    padded_segments = []
    gap_fill_count = 0
    for idx, entry in enumerate(segment_paths):
        padded_segments.append(entry)
        this_scene = entry["scene"]
        this_start = _parse_time_to_seconds(this_scene.get("start_time"))
        this_dur = float(this_scene.get("duration_sec", 0) or 0)

        # Determine next boundary: next scene's start_time OR total_duration_sec for last
        next_start = None
        if idx + 1 < len(segment_paths):
            next_start = _parse_time_to_seconds(segment_paths[idx + 1]["scene"].get("start_time"))
        else:
            next_start = total_duration_sec

        if this_start is None or next_start is None:
            continue

        gap = next_start - (this_start + this_dur)
        if gap > 0.05:
            sid = this_scene.get("scene_id", idx)
            filler_src = entry["path"]
            filler_out = os.path.join(segments_dir, f"seg_{sid:04d}_gap.mp4")
            try:
                _pad_segment_to_duration(filler_src, filler_out, gap, WIDTH, HEIGHT, FPS)
                padded_segments.append({"path": filler_out, "scene": {
                    "scene_id": f"{sid}_gap",
                    "duration_sec": gap,
                    "transition_in": "cut",
                    "transition_duration_sec": 0,
                }})
                # Generate matching silence for ambient track continuity
                amb_gap = os.path.join(ambient_dir, f"amb_{sid:04d}_gap.aac")
                sil_cmd = ["ffmpeg", "-y", "-f", "lavfi", "-i",
                           "anullsrc=r=44100:cl=stereo", "-t", f"{gap:.4f}",
                           "-c:a", "aac", amb_gap]
                subprocess.run(sil_cmd, capture_output=True, text=True, timeout=30)
                gap_fill_count += 1
                print(f"[gap-fill sid={sid}] +{gap:.4f}s freeze-frame (scene_end={this_start + this_dur:.4f} next_start={next_start:.4f})")
            except Exception as e:
                print(f"WARN: gap-fill sid={sid} failed: {e}")

    if gap_fill_count:
        segment_paths = padded_segments
        print(f"Phase 1.5 done: {gap_fill_count} gap fillers inserted ({len(segment_paths)} total segments)")

    # Concatenate ambient audio track from all segments
    ambient_files = sorted(glob_module.glob(os.path.join(ambient_dir, "amb_*.aac")))
    ambient_concat_path = os.path.join(segments_dir, "ambient_full.aac")
    if ambient_files:
        amb_list = os.path.join(segments_dir, "ambient_list.txt")
        with open(amb_list, "w") as f:
            for af in ambient_files:
                f.write(f"file '{af}'\n")
        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", amb_list, "-c:a", "aac", ambient_concat_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            print(f"WARN: ambient concat failed, continuing without ambient audio")
            ambient_concat_path = None
        else:
            print(f"Ambient audio: {len(ambient_files)} segments concatenated")
        os.remove(amb_list)
    else:
        ambient_concat_path = None
    # ── Phase 2: Apply transitions ──
    merged_path = segment_paths[0]["path"]

    for i in range(1, len(segment_paths)):
        seg = segment_paths[i]
        scene = seg["scene"]
        transition = scene.get("transition_in", "cut")
        t_dur = scene.get("transition_duration_sec", 0)

        if transition == "cut" or t_dur <= 0:
            # Simple concat (no re-encode)
            concat_out = os.path.join(segments_dir, f"merged_{i:04d}.mp4")
            concat_list = os.path.join(segments_dir, f"concat_{i}.txt")
            with open(concat_list, "w") as f:
                f.write(f"file '{merged_path}'\nfile '{seg['path']}'\n")
            cmd = [
                "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                "-i", concat_list, "-c", "copy", concat_out
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            os.remove(concat_list)
            if result.returncode != 0:
                print(f"WARN: concat {i} failed, trying re-encode")
                cmd = [
                    "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                    "-i", concat_list, *NVENC_HQ_ARGS, concat_out
                ]
                # Recreate concat list for retry
                with open(concat_list, "w") as f:
                    f.write(f"file '{merged_path}'\nfile '{seg['path']}'\n")
                result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=300, label=f"concat_reencode {i}")
                os.remove(concat_list)
            merged_path = concat_out
        else:
            # xfade transition (dissolve, fadeblack, fadewhite)
            xfade_name = {
                "dissolve": "dissolve",
                "fade_black": "fadeblack",
                "fade_white": "fadewhite",
            }.get(transition, "dissolve")

            merged_dur = _get_video_duration(merged_path)
            offset = max(0, merged_dur - t_dur)
            xfade_out = os.path.join(segments_dir, f"merged_{i:04d}.mp4")

            cmd = [
                "ffmpeg", "-y", "-i", merged_path, "-i", seg["path"],
                "-filter_complex",
                f"[0:v][1:v]xfade=transition={xfade_name}:duration={t_dur}:offset={offset}",
                *NVENC_HQ_ARGS, xfade_out
            ]
            result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=300, label=f"xfade {xfade_name}")
            if result.returncode != 0:
                print(f"WARN: xfade {transition} failed at scene {scene['scene_id']}, falling back to cut")
                # Fallback to concat
                concat_list = os.path.join(segments_dir, f"concat_{i}.txt")
                with open(concat_list, "w") as f:
                    f.write(f"file '{merged_path}'\nfile '{seg['path']}'\n")
                cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", concat_list, "-c", "copy", xfade_out]
                subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                os.remove(concat_list)
            merged_path = xfade_out

    print(f"Phase 2 done: transitions applied")

    # ── Phase 2.5: Safety net — pad merged video to match total_duration_sec ──
    # Guarantees video length >= narration target regardless of what Director emits.
    target_total = _parse_time_to_seconds(scenes_data.get("total_duration_sec"))
    if target_total and target_total > 0:
        try:
            merged_real = _get_video_duration(merged_path)
            delta_total = target_total - merged_real
            if delta_total > 0.1:
                safety_out = os.path.join(segments_dir, "merged_safety.mp4")
                cmd = [
                    "ffmpeg", "-y", "-i", merged_path,
                    "-vf", f"tpad=stop_mode=clone:stop_duration={delta_total:.4f}",
                    "-r", str(FPS),
                    *NVENC_HQ_ARGS, "-an", safety_out,
                ]
                result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=600, label="phase2.5_safety")
                if result.returncode == 0:
                    merged_path = safety_out
                    print(f"Phase 2.5: safety pad +{delta_total:.4f}s (merged={merged_real:.4f} target={target_total:.4f})")
                else:
                    print(f"WARN: safety pad failed: {result.stderr[-200:]}")
            else:
                print(f"Phase 2.5: no pad needed (merged={merged_real:.4f} target={target_total:.4f} delta={delta_total:.4f})")
        except Exception as e:
            print(f"WARN: Phase 2.5 safety check failed: {e}")

    # ── Phase 3: Mux with narration + music + ambient + subtitles ──
    audio_path = os.path.join(dest, f"{content_id}_audio.flac")
    if not os.path.exists(audio_path):
        audio_path = os.path.join(src, "audio", f"{content_id}_audio.flac")
    srt_path = os.path.join(src, "srt", "subtitles.srt")


    # ── Music assembly: multi-act tracks with crossfades + fade in/out ──
    music_path = None
    music_dir = os.path.join(PROJECTS_ROOT, channel, "music")
    # Fallback: download music from R2 if NV music dir is missing/empty
    if not os.path.isdir(music_dir) or not os.listdir(music_dir):
        if R2_ENABLED and _download_music_r2:
            try:
                music_dir = _download_music_r2(channel)
                print(f"[INFO] Music downloaded from R2 → {music_dir}")
            except Exception as e:
                print(f"[WARN] R2 music download failed: {e}")
    if os.path.isdir(music_dir):
        # Read per-act moods or fallback to single music_mood
        music_acts = scenes_data.get("music_acts", None)
        if music_acts is None:
            mood = scenes_data.get("music_mood", "default")
            music_acts = [{"from_scene": scenes[0]["scene_id"],
                           "to_scene": scenes[-1]["scene_id"], "mood": mood}]

        # Calculate cumulative scene durations
        scene_dur_map = {}
        cumulative = 0.0
        for scene in scenes:
            sid = scene["scene_id"]
            dur = scene.get("duration_sec", 5)
            scene_dur_map[sid] = {"start": cumulative, "dur": dur}
            cumulative += dur
        total_video_dur = cumulative

        # Build per-act music segments
        music_build_dir = os.path.join(segments_dir, "_music")
        os.makedirs(music_build_dir, exist_ok=True)
        act_segments = []

        for i, act in enumerate(music_acts):
            mood = act["mood"]
            from_s = act["from_scene"]
            to_s = act["to_scene"]

            track = os.path.join(music_dir, f"{mood}.mp3")
            if not os.path.isfile(track):
                track = os.path.join(music_dir, "default.mp3")
            if not os.path.isfile(track):
                print(f"Music: no track for mood '{mood}', skipping act {i}")
                continue

            # Calculate act duration from scene range
            if from_s in scene_dur_map and to_s in scene_dur_map:
                act_start = scene_dur_map[from_s]["start"]
                act_end = scene_dur_map[to_s]["start"] + scene_dur_map[to_s]["dur"]
                act_dur = act_end - act_start
            else:
                print(f"Music: scene range {from_s}-{to_s} not found, skipping act {i}")
                continue

            # Loop track + trim to act duration
            seg_path = os.path.join(music_build_dir, f"act_{i:02d}.aac")
            cmd = ["ffmpeg", "-y", "-stream_loop", "-1", "-i", track,
                   "-t", str(act_dur), "-c:a", "aac", "-b:a", "128k", seg_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                print(f"Music: failed to build act {i}: {result.stderr[-200:]}")
                continue
            act_segments.append({"path": seg_path, "dur": act_dur, "mood": mood})

        if act_segments:
            # Crossfade between act segments (2s transition)
            if len(act_segments) == 1:
                assembled = act_segments[0]["path"]
            else:
                current = act_segments[0]["path"]
                for j in range(1, len(act_segments)):
                    out = os.path.join(music_build_dir, f"xfade_{j:02d}.aac")
                    cmd = ["ffmpeg", "-y", "-i", current, "-i", act_segments[j]["path"],
                           "-filter_complex", "acrossfade=d=2:c1=tri:c2=tri",
                           "-c:a", "aac", "-b:a", "128k", out]
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                    if result.returncode != 0:
                        # Fallback: simple concat without crossfade
                        clist = os.path.join(music_build_dir, f"concat_{j}.txt")
                        with open(clist, "w") as fl:
                            fl.write(f"file '{current}'\nfile '{act_segments[j]['path']}'\n")
                        cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
                               "-i", clist, "-c:a", "aac", out]
                        subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                        os.remove(clist)
                    current = out
                assembled = current

            # Apply fade in (3s) and fade out (3s)
            music_path = os.path.join(music_build_dir, "music_final.aac")
            fade_out_start = max(0, total_video_dur - 3)
            cmd = ["ffmpeg", "-y", "-i", assembled,
                   "-af", f"afade=t=in:d=3,afade=t=out:st={fade_out_start}:d=3",
                   "-c:a", "aac", "-b:a", "128k", music_path]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if result.returncode != 0:
                music_path = assembled
                print(f"Music: fade failed, using without fades")

            moods_used = [s["mood"] for s in act_segments]
            print(f"Music: {len(act_segments)} acts ({' > '.join(moods_used)}), fade in/out 3s")
        else:
            print(f"Music: no valid acts, continuing without music")
    else:
        print(f"Music: directory {music_dir} not found, continuing without music")

    output_path = os.path.join(dest, f"{content_id}_final.mp4")
    has_narration = os.path.exists(audio_path)
    has_music = music_path is not None
    has_ambient = ambient_concat_path is not None and os.path.isfile(ambient_concat_path)
    has_srt = os.path.exists(srt_path)

    # Build FFmpeg command with dynamic audio mixing
    # Input indices: 0=video, then narration/music/ambient in order
    cmd = ["ffmpeg", "-y", "-i", merged_path]
    input_idx = 1
    narr_idx = music_idx = amb_idx = None

    if has_narration:
        cmd += ["-i", audio_path]
        narr_idx = input_idx
        input_idx += 1
    if has_music:
        cmd += ["-i", music_path]
        music_idx = input_idx
        input_idx += 1
    if has_ambient:
        cmd += ["-i", ambient_concat_path]
        amb_idx = input_idx
        input_idx += 1

    # Build audio filter chain
    audio_inputs = []
    filter_parts = []
    if narr_idx is not None:
        filter_parts.append(f"[{narr_idx}:a]volume=1.0[narr]")
        audio_inputs.append("[narr]")
    if music_idx is not None:
        filter_parts.append(f"[{music_idx}:a]volume=0.20[music]")
        audio_inputs.append("[music]")
    if amb_idx is not None:
        # Ambient disabled — video models generate full clips with audio handled separately
        # filter_parts.append(f"[{amb_idx}:a]volume=0.08[amb]")
        # audio_inputs.append("[amb]")  # ambient stream not defined, don't reference it
        has_ambient = False

    srt_filter = f"subtitles={srt_path}:force_style='FontSize=22,PrimaryColour=&H00FFFFFF'" if has_srt else None

    if audio_inputs:
        if len(audio_inputs) > 1:
            mix_filter = "".join(audio_inputs) + f"amix=inputs={len(audio_inputs)}:duration=longest[aout]"
            filter_parts.append(mix_filter)
            # When using filter_complex, subtitles must be inside it (can't mix -vf and -filter_complex)
            if srt_filter:
                filter_parts.insert(0, f"[0:v]{srt_filter}[vout]")
                cmd += ["-filter_complex", ";".join(filter_parts), "-map", "[vout]", "-map", "[aout]"]
            else:
                cmd += ["-filter_complex", ";".join(filter_parts), "-map", "0:v", "-map", "[aout]"]
        else:
            # Single audio source, no mixing needed
            src_idx = narr_idx or music_idx or amb_idx
            cmd += ["-map", "0:v", "-map", f"{src_idx}:a"]
            if srt_filter:
                cmd += ["-vf", srt_filter]
        cmd += [*NVENC_HQ_ARGS,
                "-c:a", "aac", "-b:a", "192k", output_path]
    elif has_srt:
        cmd += ["-vf", srt_filter,
                *NVENC_HQ_ARGS, output_path]
    else:
        shutil.copy2(merged_path, output_path)
        cmd = None

    if cmd:
        layers = []
        if has_narration: layers.append("narration")
        if has_music: layers.append("music")
        if has_ambient: layers.append("ambient")
        print(f"Phase 3: mixing {' + '.join(layers) if layers else 'video only'}")
        result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=900, label="phase3_mux")
        if result.returncode != 0:
            raise RuntimeError(f"Phase 3 mux failed: {result.stderr[-500:]}")

    # Cleanup segments
    shutil.rmtree(segments_dir, ignore_errors=True)

    final_size = round(os.path.getsize(output_path) / 1024 / 1024, 2)
    print(f"Phase 3 done: {output_path} ({final_size} MB)")

    result_entry = {
        "filename": f"{content_id}_final.mp4",
        "path": output_path,
        "size_mb": final_size,
        "segments_rendered": len(segment_paths),
        "segments_skipped": skipped,
    }

    # Upload final video to R2
    if R2_ENABLED:
        r2_key = f"{channel}/{content_id}/output/{platform}/{content_id}_final.mp4"
        try:
            r2_helper.upload_file(output_path, r2_key)
            result_entry["r2_key"] = r2_key
            result_entry["r2_url"] = r2_helper.presigned_url(r2_key)
            print(f"[INFO] Final video uploaded to R2: {r2_key}")
        except Exception as e:
            print(f"[WARN] R2 upload failed for final video: {e}")

    return [result_entry]


def _compose_concat_audio(src, dest, content_id):
    audio_dir = os.path.join(src, "audio")
    if not os.path.isdir(audio_dir):
        raise RuntimeError(f"No audio directory found: {audio_dir}")

    chunks = sorted(glob_module.glob(os.path.join(audio_dir, "chunk_*")))
    if not chunks:
        raise RuntimeError(f"No audio chunks found in {audio_dir}")

    concat_list = os.path.join(dest, "concat_list.txt")
    with open(concat_list, "w") as f:
        for chunk in chunks:
            f.write(f"file '{chunk}'\n")

    output_path = os.path.join(dest, f"{content_id}_audio.flac")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", concat_list, "-c:a", "flac", output_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg concat_audio failed: {result.stderr[-500:]}")

    os.remove(concat_list)
    return [{"filename": f"{content_id}_audio.flac", "path": output_path,
             "size_mb": round(os.path.getsize(output_path) / 1024 / 1024, 2)}]


def _compose_concat_video(src, dest, content_id):
    video_dir = os.path.join(src, "video")
    if not os.path.isdir(video_dir):
        raise RuntimeError(f"No video directory found: {video_dir}")

    clips = sorted(glob_module.glob(os.path.join(video_dir, "scene_*")))
    if not clips:
        raise RuntimeError(f"No video clips found in {video_dir}")

    concat_list = os.path.join(dest, "concat_list.txt")
    with open(concat_list, "w") as f:
        for clip in clips:
            f.write(f"file '{clip}'\n")

    output_path = os.path.join(dest, f"{content_id}_video.mp4")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", concat_list, "-c", "copy", output_path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg concat_video failed: {result.stderr[-500:]}")

    os.remove(concat_list)
    return [{"filename": f"{content_id}_video.mp4", "path": output_path,
             "size_mb": round(os.path.getsize(output_path) / 1024 / 1024, 2)}]


def _compose_full(src, dest, content_id, config):
    video_in = config.get("video_path", os.path.join(dest, f"{content_id}_video.mp4"))
    audio_in = config.get("audio_path", os.path.join(dest, f"{content_id}_audio.flac"))
    srt_in = config.get("srt_path")

    if not os.path.exists(video_in):
        raise RuntimeError(f"Video not found: {video_in}")
    if not os.path.exists(audio_in):
        raise RuntimeError(f"Audio not found: {audio_in}")

    output_path = os.path.join(dest, f"{content_id}_final.mp4")
    cmd = ["ffmpeg", "-y", "-i", video_in, "-i", audio_in]

    if srt_in and os.path.exists(srt_in):
        cmd += ["-vf", f"subtitles={srt_in}"]

    cmd += [*NVENC_HQ_ARGS, "-c:a", "aac", "-shortest", output_path]

    result = _run_ffmpeg_with_nvenc_fallback(cmd, timeout=900, label="compose_full")
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg full compose failed: {result.stderr[-500:]}")

    return [{"filename": f"{content_id}_final.mp4", "path": output_path,
             "size_mb": round(os.path.getsize(output_path) / 1024 / 1024, 2)}]


_INPUTS_DOWNLOADED = False


def _upload_to_comfyui(local_path, subfolder=""):
    """Upload a file to ComfyUI via HTTP API to register in its cache."""
    url = f"http://{COMFY_HOST}/upload/image"
    filename = os.path.basename(local_path)
    with open(local_path, "rb") as f:
        files = {"image": (filename, f, "application/octet-stream")}
        data = {"subfolder": subfolder, "type": "input", "overwrite": "true"}
        try:
            import requests
            resp = requests.post(url, files=files, data=data, timeout=30)
            return resp.status_code == 200
        except Exception:
            return False


def _ensure_r2_inputs():
    """Download all R2 input files and register with ComfyUI."""
    global _INPUTS_DOWNLOADED
    if _INPUTS_DOWNLOADED or not R2_ENABLED:
        return
    try:
        input_dir = "/comfyui/input"
        os.makedirs(input_dir, exist_ok=True)
        keys = r2_helper.list_files("inputs/audio/")
        downloaded = 0
        uploaded = 0
        for key in keys:
            fname = os.path.basename(key)
            if not fname:
                continue
            local_path = os.path.join(input_dir, fname)
            if not os.path.exists(local_path):
                r2_helper.download_file(key, local_path)
                downloaded += 1
            if _upload_to_comfyui(local_path):
                uploaded += 1
        print(f"[INFO] R2 inputs: {downloaded} downloaded, {uploaded} registered with ComfyUI")
        _INPUTS_DOWNLOADED = True
    except Exception as e:
        print(f"[WARN] R2 input sync failed: {e}")


def _ensure_img_vid_inputs(channel, content_id):
    """Download scene images from R2 to /comfyui/input/ for img-vid jobs.

    Wan 2.2 I2V workflow uses LoadImage nodes that expect files in /comfyui/input/.
    R2 stores them under {channel}/{content_id}/source/images/scene_XXX_{initial,final}.png.
    This function bridges that gap for img-vid job type.
    """
    if not R2_ENABLED:
        return
    input_dir = "/comfyui/input"
    os.makedirs(input_dir, exist_ok=True)
    r2_prefix = f"{channel}/{content_id}/source/images/"
    try:
        keys = r2_helper.list_files(r2_prefix)
        downloaded = 0
        uploaded = 0
        for key in keys:
            fname = os.path.basename(key)
            if not fname:
                continue
            local_path = os.path.join(input_dir, fname)
            if not os.path.exists(local_path):
                r2_helper.download_file(key, local_path)
                downloaded += 1
            if _upload_to_comfyui(local_path):
                uploaded += 1
        print(f"[INFO] img-vid inputs: {len(keys)} in R2, {downloaded} downloaded, {uploaded} registered with ComfyUI")
    except Exception as e:
        print(f"[WARN] Failed to download img-vid inputs: {e}")


def handler(job):
    """
    Video Endpoint Handler.
    Accepts: img-vid (Wan 2.2), compose (FFmpeg assembly), download (NV file retrieval)
    """
    job_input = job.get("input", {})

    job_type = job_input.get("job_type")
    channel = job_input.get("channel")
    content_id = job_input.get("content_id")

    if not job_type:
        return {"error": "Missing required field: job_type"}

    _ensure_r2_inputs()
    if not channel:
        return {"error": "Missing required field: channel"}
    if not content_id:
        return {"error": "Missing required field: content_id"}

    # Download jobs (retrieve file from NV or R2 as base64)
    if job_type == "download":
        platform = job_input.get("platform", "youtube")
        filename = job_input.get("filename")
        if not filename:
            return {"error": "Missing required field: filename"}
        file_path = os.path.join(
            output_dir_for(channel, content_id, platform), filename
        )
        # Fallback: download from R2 if not on local disk
        if not os.path.isfile(file_path) and R2_ENABLED:
            r2_key = f"{channel}/{content_id}/output/{platform}/{filename}"
            try:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                r2_helper.download_file(r2_key, file_path)
                print(f"[INFO] Downloaded {r2_key} from R2 for download job")
            except Exception as e:
                print(f"[WARN] R2 download fallback failed: {e}")
        if not os.path.isfile(file_path):
            return {"error": f"File not found: {file_path}"}
        size_mb = round(os.path.getsize(file_path) / 1024 / 1024, 2)
        with open(file_path, "rb") as f:
            data_b64 = base64.b64encode(f.read()).decode("ascii")
        return {
            "status": "success",
            "job_type": job_type,
            "channel": channel,
            "content_id": content_id,
            "filename": filename,
            "size_mb": size_mb,
            "data_b64": data_b64,
        }

    # Compose jobs (FFmpeg, no ComfyUI needed)
    if job_type == "compose":
        platform = job_input.get("platform", "youtube")
        compose_config = job_input.get("compose", {})
        try:
            results = run_compose(channel, content_id, platform, compose_config)
            return {
                "status": "success",
                "job_type": job_type,
                "channel": channel,
                "content_id": content_id,
                "platform": platform,
                "outputs": results,
            }
        except RuntimeError as e:
            return {"error": str(e), "job_type": job_type}

    # ComfyUI workflow jobs
    if job_type not in ACCEPTED_JOB_TYPES:
        return {"error": f"Video endpoint accepts: {list(ACCEPTED_JOB_TYPES.keys()) + ['compose']}. Got: {job_type}"}

    workflow = job_input.get("workflow")
    if not workflow:
        return {"error": "Missing required field: workflow"}

    prefix = job_input.get("prefix", "scene")
    index = job_input.get("index")
    media_type = ACCEPTED_JOB_TYPES[job_type]
    dest = source_dir(channel, content_id, media_type)

    # Pre-download scene images from R2 to /comfyui/input/
    # (Wan 2.2 I2V LoadImage nodes need files in ComfyUI's default input dir)
    if job_type == "img-vid":
        _ensure_img_vid_inputs(channel, content_id)

    try:
        wait_for_comfyui()
    except RuntimeError as e:
        return {"error": str(e)}

    client_id = str(uuid.uuid4())
    ws_url = f"ws://{COMFY_HOST}/ws?clientId={client_id}"
    try:
        ws = websocket.WebSocket()
        ws.settimeout(COMFY_EXECUTION_TIMEOUT)
        ws.connect(ws_url)
    except Exception as e:
        return {"error": f"Failed to connect websocket: {str(e)}"}

    # Resolve LoadImage paths (handle naming variations: scene_000_initial.png vs scene_000_initial_000.png)
    # ComfyUI LoadImage resolves `image` against /comfyui/input/, so we must check existence
    # and glob against that directory — not the worker's cwd.
    comfy_input_dir = "/comfyui/input"
    for node_id, node_data in workflow.items():
        if node_data.get("class_type") == "LoadImage":
            img_path = node_data.get("inputs", {}).get("image", "")
            if not img_path:
                continue
            full_path = os.path.join(comfy_input_dir, img_path)
            if not os.path.exists(full_path):
                # Try glob fallback for naming variations
                base, ext = os.path.splitext(img_path)
                candidates = sorted(glob_module.glob(
                    os.path.join(comfy_input_dir, f"{base}_*{ext}")
                ))
                if candidates:
                    # Pass basename back to ComfyUI (it resolves against input/)
                    node_data["inputs"]["image"] = os.path.basename(candidates[-1])

    try:
        prompt_id = queue_prompt(workflow, client_id)
    except Exception as e:
        ws.close()
        return {"error": f"Failed to queue prompt: {str(e)}"}

    try:
        wait_for_completion(ws, prompt_id)
    except RuntimeError as e:
        return {"error": f"Execution failed: {str(e)}"}
    finally:
        ws.close()

    results = collect_and_move(prompt_id, dest, prefix, index=index)

    # Upload to R2 (dual-write: NV + R2 during migration)
    if R2_ENABLED:
        r2_prefix = f"{channel}/{content_id}/source/{media_type}"
        for result in results:
            filepath = result.get("path")
            if filepath and os.path.exists(filepath):
                r2_key = f"{r2_prefix}/{result['filename']}"
                try:
                    r2_helper.upload_file(filepath, r2_key)
                    result["r2_key"] = r2_key
                    result["r2_url"] = r2_helper.presigned_url(r2_key)
                except Exception as e:
                    print(f"[WARN] R2 upload failed for {result['filename']}: {e}")

    free_vram()

    return {
        "status": "success",
        "job_type": job_type,
        "channel": channel,
        "content_id": content_id,
        "output_dir": dest,
        "outputs": results,
    }


runpod.serverless.start({"handler": handler})
