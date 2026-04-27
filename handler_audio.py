"""
YouTube Factory — Audio Endpoint Handler
Job types: txt-voice, voice-srt, compose (concat_audio only), upload-inputs (temp)
Models: Qwen3-TTS (EN), CosyVoice3 (ES), Whisper STT
"""

import json
import os
import subprocess
import time
import uuid
import shutil
import urllib.request
import urllib.error
import glob as glob_module

import runpod
import websocket

# Python Whisper direct (B' refactor 2026-04-21): voice-srt jobs bypass
# ComfyUI entirely and run Whisper in-process to emit word-level timestamps
# alongside segment-level (superset JSON). Enables perfect per-word caption
# sync in downstream short-form render (handler_video.py v5). Lazy-loaded
# to keep handler import time fast — model loads on first voice-srt job.
try:
    import whisper as _whisper_lib
    _WHISPER_AVAILABLE = True
except ImportError:
    _whisper_lib = None
    _WHISPER_AVAILABLE = False

_WHISPER_MODEL_CACHE = {}  # keyed by model name (e.g., "large-v3")

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
# NV→R2 migration completed 2026-04-15. Workers run without Network Volume
# attached; PROJECTS_ROOT is always ephemeral tmpfs. The previous conditional
# `if os.path.isdir('/runpod-volume')` fell through to a false-positive because
# the base image contains an empty `/runpod-volume/` dir, so PROJECTS_ROOT ended
# up on the container rootfs (same bug pattern as handler_video.py lesson #44).
PROJECTS_ROOT = "/tmp/projects"
os.makedirs(PROJECTS_ROOT, exist_ok=True)

_OUTPUT_DIR_CANDIDATES = [
    "/comfyui/output",
    "/comfyui/temp",
]

ACCEPTED_JOB_TYPES = {
    "txt-voice": "audio",
    "voice-srt": "srt",
}


def project_dir(channel, content_id):
    return os.path.join(PROJECTS_ROOT, channel, content_id)


def source_dir(channel, content_id, media_type):
    return os.path.join(project_dir(channel, content_id), "source", media_type)


def output_dir_for(channel, content_id, platform):
    """Return the outputs directory for a platform render."""
    return os.path.join(project_dir(channel, content_id), "outputs", platform)


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



def free_vram():
    """Call ComfyUI /free endpoint to unload models and release VRAM between jobs."""
    try:
        data = json.dumps({"unload_models": True, "free_memory": True}).encode("utf-8")
        req = urllib.request.Request(
            f"http://{COMFY_HOST}/free",
            data=data,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=30)
    except Exception as e:
        # Log but do not fail the job — VRAM cleanup is best-effort
        print(f"[WARN] free_vram failed: {e}")

def collect_and_move(prompt_id, dest_dir, prefix, index=None):
    history = get_history(prompt_id)
    prompt_history = history.get(prompt_id, {})
    outputs = prompt_history.get("outputs", {})

    os.makedirs(dest_dir, exist_ok=True)

    all_files = []
    for node_id, node_output in outputs.items():
        # Audio outputs (primary for this endpoint)
        if "audio" in node_output:
            for aud in node_output["audio"]:
                src = find_output_file(aud.get("filename"), aud.get("subfolder", ""))
                if src:
                    ext = os.path.splitext(aud["filename"])[1] or ".flac"
                    all_files.append({"src": src, "ext": ext, "node_id": node_id, "type": "audio"})

        # Text outputs (SRT files from Whisper)
        if "text" in node_output:
            for txt_item in node_output["text"]:
                all_files.append({"content": txt_item, "ext": ".txt", "node_id": node_id, "type": "text"})

        # Images (unlikely for audio endpoint but handle gracefully)
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

        if item["type"] == "text":
            with open(dest_path, "w", encoding="utf-8") as f:
                content = item["content"]
                f.write(content if isinstance(content, str) else json.dumps(content))
        else:
            shutil.copy2(item["src"], dest_path)

        entry = {"filename": dest_name, "path": dest_path, "node_id": item["node_id"]}
        if item.get("src"):
            entry["size_mb"] = round(os.path.getsize(dest_path) / 1024 / 1024, 2)
        if item["type"] == "text":
            entry["content"] = item["content"] if isinstance(item["content"], str) else json.dumps(item["content"])
        results.append(entry)

    return results


# ── Audio helpers (duration probe + loudness normalization) ──

def _probe_duration(filepath):
    """Return audio duration in seconds via ffprobe. Returns 0.0 on failure."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", filepath],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except (ValueError, subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return 0.0


def _apply_loudnorm(filepath, target_i=-16.0, target_tp=-1.5, target_lra=11.0):
    """
    Normalize audio loudness to EBU R128 target via ffmpeg loudnorm filter.
    Homologates inter-chunk volume variance so concat output sounds consistent.

    Single-pass linear mode (~0.3-0.5s per chunk overhead). Two-pass would be
    more accurate but not worth the latency for narration content where the
    source is already close to target (Qwen3/CosyVoice3 TTS output sits around
    -18 to -14 LUFS naturally; single-pass linear nudges it to -16 cleanly).

    YouTube target: -16 LUFS integrated, -1.5 dBTP true peak, LRA 11.
    Industry standard for podcasts + YouTube narration (ATSC A/85 loose).

    On failure, logs warning and leaves the original file untouched (fail-open).
    """
    tmp_path = filepath + ".norm.flac"
    loudnorm_filter = f"loudnorm=I={target_i}:TP={target_tp}:LRA={target_lra}:linear=true"
    cmd = [
        "ffmpeg", "-y", "-i", filepath,
        "-af", loudnorm_filter,
        "-c:a", "flac",
        tmp_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0 and os.path.exists(tmp_path) and os.path.getsize(tmp_path) > 0:
            os.replace(tmp_path, filepath)
            return True
        else:
            print(f"[WARN] loudnorm failed for {os.path.basename(filepath)}: rc={result.returncode} stderr={result.stderr[-200:]}")
    except subprocess.TimeoutExpired:
        print(f"[WARN] loudnorm timeout for {os.path.basename(filepath)}")
    except Exception as e:
        print(f"[WARN] loudnorm exception for {os.path.basename(filepath)}: {e}")
    # Cleanup temp on any failure
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except OSError:
        pass
    return False


# ── Compose (FFmpeg) — concat_audio only ────────────────────

def _compose_concat_audio(src, dest, content_id, channel=None, platform=None):
    """Concatenate all audio chunks into a single file."""
    audio_dir = os.path.join(src, "audio")
    chunks = []
    r2_temp_dir = None

    # Always download ALL chunks from R2 first (NV-free: workers don't share filesystem)
    if R2_ENABLED and channel:
        r2_prefix = f"{channel}/{content_id}/source/audio/"
        try:
            r2_keys = r2_helper.list_files(r2_prefix)
            chunk_keys = sorted(
                [k for k in r2_keys if os.path.basename(k).startswith("chunk_")]
            )
            if chunk_keys:
                os.makedirs(audio_dir, exist_ok=True)
                for key in chunk_keys:
                    local_path = os.path.join(audio_dir, os.path.basename(key))
                    r2_helper.download_file(key, local_path)
                chunks = sorted(glob_module.glob(os.path.join(audio_dir, "chunk_*")))
                print(f"[INFO] Downloaded {len(chunks)} audio chunks from R2")
        except Exception as e:
            print(f"[WARN] R2 chunk download failed: {e}")

    # Fallback: check local filesystem (NV or previously downloaded)
    if not chunks and os.path.isdir(audio_dir):
        chunks = sorted(glob_module.glob(os.path.join(audio_dir, "chunk_*")))

    if not chunks:
        raise RuntimeError(
            f"No audio chunks found in NV ({audio_dir}) or R2"
        )

    # Loudness homologation (2026-04-15): normalize each chunk to EBU R128 target
    # BEFORE concat so inter-chunk volume variance is flattened. Qwen3/CosyVoice3
    # TTS output can drift ±2-3 LUFS between chunks depending on text intensity;
    # human ear perceives this as "subtle volume jumps" in the final narration.
    # loudnorm per-chunk targeting I=-16:TP=-1.5:LRA=11 homologates them cleanly.
    # Cost: ~0.3-0.5s per chunk (~5-8s total for a 10-chunk video). Negligible.
    # Fail-open: if loudnorm fails on any chunk, concat proceeds with the
    # un-normalized original (better degraded audio than no audio).
    norm_ok = 0
    norm_fail = 0
    for chunk_path in chunks:
        if _apply_loudnorm(chunk_path):
            norm_ok += 1
        else:
            norm_fail += 1
    print(f"[INFO] Loudness homologation: {norm_ok}/{len(chunks)} chunks normalized ({norm_fail} failed/skipped)")

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
        raise RuntimeError(f"FFmpeg concat_audio failed: {result.stderr[:500]}")

    os.remove(concat_list)

    # Clean up R2 temp files
    if r2_temp_dir and os.path.isdir(r2_temp_dir):
        shutil.rmtree(r2_temp_dir, ignore_errors=True)

    results = [{"filename": f"{content_id}_audio.flac", "path": output_path,
                "size_mb": round(os.path.getsize(output_path) / 1024 / 1024, 2)}]

    # Upload compose output to R2
    if R2_ENABLED and channel and platform:
        r2_key = f"{channel}/{content_id}/output/{platform}/{content_id}_audio.flac"
        try:
            r2_helper.upload_file(output_path, r2_key)
            results[0]["r2_key"] = r2_key
            results[0]["r2_url"] = r2_helper.presigned_url(r2_key)
            print(f"[INFO] Compose output uploaded to R2: {r2_key}")
        except Exception as e:
            print(f"[WARN] R2 upload failed for compose output: {e}")

    return results


def run_compose(channel, content_id, platform, compose_config):
    """
    FFmpeg composition job — Audio endpoint only supports concat_audio.
    Reads chunks from source/audio/, writes to outputs/{platform}/.
    """
    src = os.path.join(project_dir(channel, content_id), "source")
    dest = output_dir_for(channel, content_id, platform)
    os.makedirs(dest, exist_ok=True)

    compose_type = compose_config.get("type", "concat_audio")

    if compose_type == "concat_audio":
        return _compose_concat_audio(src, dest, content_id, channel=channel, platform=platform)
    else:
        raise RuntimeError(
            f"Audio endpoint only supports compose type 'concat_audio'. Got: {compose_type}"
        )


# ── Python Whisper direct (B' refactor) ────────────────────

def _get_whisper_model(model_name="large-v3"):
    """Lazy-load and cache a Whisper model. Single load per worker lifetime."""
    if not _WHISPER_AVAILABLE:
        raise RuntimeError("openai-whisper library not available in this worker")
    if model_name not in _WHISPER_MODEL_CACHE:
        print(f"[INFO] Whisper: loading {model_name} (first use on this worker)")
        t0 = time.time()
        _WHISPER_MODEL_CACHE[model_name] = _whisper_lib.load_model(model_name)
        print(f"[INFO] Whisper: {model_name} loaded in {time.time()-t0:.1f}s")
    return _WHISPER_MODEL_CACHE[model_name]


_WHISPER_LANG_MAP = {
    "english": "en", "spanish": "es", "french": "fr", "german": "de",
    "portuguese": "pt", "italian": "it", "dutch": "nl", "polish": "pl",
}


def _run_python_whisper_srt(workflow, dest, prefix, index=None):
    """B' refactor: run Whisper directly (bypassing ComfyUI) for voice-srt.

    Reads cfg from the incoming workflow payload (ComfyUI-shaped for backward
    compat) and emits a superset JSON per segment:
        [{value, start, end, words: [{word, start, end}, ...]}, ...]

    Word-level data is what handler_video.py v5 uses for perfect per-word
    reveal sync in short-form captions. Segment-level fields are preserved
    so the legacy n8n Extract SRT Content node (converts JSON → standard SRT
    for Drive) keeps working without modification.

    Returns a results list in the same shape `collect_and_move` produces so
    the rest of the handler flow (R2 upload, cost tracking, response) reuses
    existing code paths without branching."""
    model_name = "large-v3"
    language = "en"
    initial_prompt = None
    audio_path = None
    for nid, node in workflow.items():
        if not isinstance(node, dict):
            continue
        cls = node.get("class_type")
        if cls == "Apply Whisper":
            inputs = node.get("inputs", {}) or {}
            model_name = inputs.get("model", model_name) or model_name
            language = inputs.get("language", language) or language
            initial_prompt = inputs.get("prompt") or None
        elif cls == "VHS_LoadAudio":
            audio_path = (node.get("inputs", {}) or {}).get("audio_file")

    if not audio_path:
        raise RuntimeError("voice-srt workflow has no VHS_LoadAudio.audio_file")
    if not os.path.exists(audio_path):
        raise RuntimeError(f"voice-srt audio not found locally: {audio_path}")

    # Normalize language name → ISO code (Whisper accepts both but prefers code).
    lang_code = _WHISPER_LANG_MAP.get(str(language).lower().strip(), language)

    model = _get_whisper_model(model_name)

    # Anti-hallucination params (lesson #18, already patched at library level
    # but passing explicitly here in case future Dockerfile rebuilds drop it).
    kwargs = {
        "word_timestamps": True,
        "condition_on_previous_text": False,
        "temperature": 0,
        "no_speech_threshold": 0.55,
        "hallucination_silence_threshold": 2.0,
        "language": lang_code,
    }
    if initial_prompt:
        kwargs["initial_prompt"] = initial_prompt

    print(f"[INFO] voice-srt (python whisper): transcribing {audio_path} "
          f"model={model_name} lang={lang_code} prompt={bool(initial_prompt)}")
    t0 = time.time()
    result = model.transcribe(audio_path, **kwargs)
    elapsed = time.time() - t0

    # Build superset JSON [{value, start, end, words}]
    out_segments = []
    total_words = 0
    for seg in result.get("segments", []) or []:
        entry = {
            "value": (seg.get("text") or "").strip(),
            "start": round(float(seg.get("start") or 0.0), 3),
            "end": round(float(seg.get("end") or 0.0), 3),
        }
        seg_words = seg.get("words") or []
        if seg_words:
            wl = []
            for w in seg_words:
                wtxt = (w.get("word") or "").strip()
                if not wtxt:
                    continue
                wl.append({
                    "word": wtxt,
                    "start": round(float(w.get("start") or 0.0), 3),
                    "end": round(float(w.get("end") or 0.0), 3),
                })
            if wl:
                entry["words"] = wl
                total_words += len(wl)
        if entry["value"]:
            out_segments.append(entry)

    print(f"[INFO] voice-srt: whisper done in {elapsed:.1f}s — "
          f"{len(out_segments)} segments, {total_words} word timings")

    # Write to dest/{prefix}_{index:03d}.txt matching ComfyUI naming so the
    # downstream n8n `Extract SRT Content` + R2 upload paths read the same file.
    os.makedirs(dest, exist_ok=True)
    if index is not None:
        filename = f"{prefix}_{int(index):03d}.txt"
    else:
        filename = f"{prefix}.txt"
    dest_path = os.path.join(dest, filename)
    content_str = json.dumps(out_segments, ensure_ascii=False)
    with open(dest_path, "w", encoding="utf-8") as f:
        f.write(content_str)

    return [{
        "filename": filename,
        "path": dest_path,
        "node_id": "python_whisper",
        "content": content_str,
        "size_mb": round(os.path.getsize(dest_path) / 1024 / 1024, 3),
    }]


# ── Main Handler ────────────────────────────────────────────

_INPUTS_DOWNLOADED = False


def _upload_to_comfyui(local_path, subfolder=""):
    """Upload a file to ComfyUI's input dir via its HTTP API.
    This bypasses ComfyUI's cached file list issue."""
    url = f"http://{COMFY_HOST}/upload/image"
    filename = os.path.basename(local_path)
    with open(local_path, "rb") as f:
        files = {"image": (filename, f, "application/octet-stream")}
        data = {"subfolder": subfolder, "type": "input", "overwrite": "true"}
        try:
            import requests
            resp = requests.post(url, files=files, data=data, timeout=30)
            if resp.status_code == 200:
                return True
            print(f"[WARN] ComfyUI upload failed for {filename}: {resp.status_code} {resp.text[:200]}")
        except Exception as e:
            print(f"[WARN] ComfyUI upload error for {filename}: {e}")
    return False


def _ensure_r2_inputs():
    """Download all R2 input files and upload to ComfyUI via API (first job only)."""
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
            # Always upload via ComfyUI API to register in its cache
            if _upload_to_comfyui(local_path):
                uploaded += 1
        print(f"[INFO] R2 inputs: {downloaded} downloaded, {uploaded} registered with ComfyUI")
        _INPUTS_DOWNLOADED = True
    except Exception as e:
        print(f"[WARN] R2 input sync failed: {e}")


def handler(job):
    """
    Audio Endpoint Handler.
    Accepts: txt-voice (TTS), voice-srt (Whisper STT), compose (concat_audio), upload-inputs (temp)
    """
    job_input = job.get("input", {})

    job_type = job_input.get("job_type")
    channel = job_input.get("channel")
    content_id = job_input.get("content_id")

    if not job_type:
        return {"error": "Missing required field: job_type"}

    # Ensure voice references are downloaded from R2 (first job only)
    _ensure_r2_inputs()

    # Note: the legacy `upload-inputs` job_type (NV→R2 one-shot migration)
    # was removed 2026-04-15 after the migration completed. If any scheduled
    # dispatch still references it, the router below will return a clean
    # "Unsupported job_type" error rather than silently reading stale NV paths.

    if not channel:
        return {"error": "Missing required field: channel"}
    if not content_id:
        return {"error": "Missing required field: content_id"}

    # ── Compose jobs don't need ComfyUI ──
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

    # ── ComfyUI workflow jobs ──
    if job_type not in ACCEPTED_JOB_TYPES:
        return {"error": f"Audio endpoint only accepts: {list(ACCEPTED_JOB_TYPES.keys()) + ['compose']}. Got: {job_type}"}

    workflow = job_input.get("workflow")
    if not workflow:
        return {"error": "Missing required field: workflow"}

    default_prefixes = {"txt-voice": "chunk", "voice-srt": "chunk"}
    prefix = job_input.get("prefix", default_prefixes.get(job_type, "output"))
    index = job_input.get("index")
    media_type = ACCEPTED_JOB_TYPES[job_type]
    dest = source_dir(channel, content_id, media_type)

    # Fix I4c (2026-04-14): short-circuit TTS regeneration if the flac chunk
    # already exists in R2.
    #
    # Problem: when the Audio Pipeline retries (e.g., after a max_tokens fix,
    # transient HTTP error on a specific chunk, or manual re-run), it re-submits
    # ALL chunk jobs to the endpoint — even though the previously-generated
    # chunks are sitting in R2 at the expected key. Full scripts split into
    # 10-20 chunks (~$0.04 + ~25 sec each). Every retry burned ~$0.50 and ~5 min
    # regenerating bit-identical TTS output.
    #
    # Fix: at the top of the txt-voice job path, compute the expected R2 key
    # and check r2_helper.file_exists(). If present, download to local dest
    # and return immediately with a success response matching the normal shape
    # (including r2_key, r2_url, size_mb, plus a `skipped: true` flag).
    #
    # Safety:
    # - Only runs for job_type == "txt-voice" with R2 enabled and `index`
    #   provided (the normal Audio Pipeline always provides both).
    # - voice-srt (Whisper STT) and compose (concat_audio) are NOT short-
    #   circuited — voice-srt regeneration is cheap (Whisper is fast) and
    #   the SRT is single-shot per video, not per chunk.
    # - Fail-open: any exception falls through to normal TTS (never blocks
    #   legitimate re-generation).
    # - Size sanity check (>= 100KB) rejects partial/corrupted R2 uploads;
    #   on mismatch, removes the local download and proceeds with TTS.
    # - Matches the exact pattern from handler_video.py Fix I4 / handler_images.py
    #   Fix I4b for consistency.
    if job_type == "txt-voice":
        if R2_ENABLED and index is not None:
            try:
                index_str = f"{int(index):03d}"
                existing_r2_key = f"{channel}/{content_id}/source/{media_type}/{prefix}_{index_str}.flac"
                if r2_helper.file_exists(existing_r2_key):
                    os.makedirs(dest, exist_ok=True)
                    local_path = os.path.join(dest, f"{prefix}_{index_str}.flac")
                    r2_helper.download_file(existing_r2_key, local_path)
                    local_size = os.path.getsize(local_path) if os.path.exists(local_path) else 0
                    # I4c size-proportional check (2026-04-15, lesson #37):
                    # If n8n sends `word_count`, compute expected size from
                    # empirical Qwen3 flac @ 24kHz mono ratio (~330 bytes/word)
                    # and require actual >= 85% of expected. Catches chunks
                    # truncated to ~30-70% by the Qwen3 sub-chunk seam bug
                    # (lesson #34). Falls back to the old 100KB floor when
                    # word_count is missing (backward compat with older pipelines).
                    word_count = job_input.get("word_count")
                    if word_count and word_count > 0:
                        expected_bytes = int(word_count) * 330
                        min_accept = int(expected_bytes * 0.85)
                    else:
                        min_accept = 100_000
                    if local_size >= min_accept:
                        print(f"[INFO] txt-voice chunk={index_str} REUSE: flac in R2 ({local_size/1024/1024:.2f} MB, min_accept={min_accept}, word_count={word_count}), skipping TTS")
                        return {
                            "status": "success",
                            "job_type": job_type,
                            "channel": channel,
                            "content_id": content_id,
                            "output_dir": dest,
                            "outputs": [{
                                "filename": f"{prefix}_{index_str}.flac",
                                "path": local_path,
                                "node_id": "reused_from_r2",
                                "size_mb": round(local_size / 1024 / 1024, 2),
                                "r2_key": existing_r2_key,
                                "r2_url": r2_helper.presigned_url(existing_r2_key),
                                "skipped": True,
                            }],
                        }
                    else:
                        ratio = local_size / max(min_accept, 1)
                        print(f"[WARN] txt-voice chunk={index_str} REJECT REUSE: size={local_size} < min_accept={min_accept} (ratio={ratio:.2f}, word_count={word_count}), re-generating")
                        try:
                            os.remove(local_path)
                        except OSError:
                            pass
            except Exception as e:
                print(f"[WARN] txt-voice R2 existence check failed ({e}), proceeding with TTS")

    # Cross-worker audio pre-fetch (2026-04-18): voice-srt jobs receive a
    # workflow with VHS_LoadAudio pointing to a /tmp/projects/... path that
    # was produced by the concat_audio job on a *different* worker. Since
    # /tmp is per-container tmpfs, this worker's filesystem doesn't have the
    # file, and ComfyUI rejects the prompt with custom_validation_failed
    # ("Invalid file path: /tmp/projects/..."). Fix: detect the missing local
    # file, resolve the R2 key (explicit from payload or derived from the
    # path pattern /tmp/projects/{ch}/{cid}/outputs/{plat}/{fname} →
    # {ch}/{cid}/output/{plat}/{fname}), download from R2, and let the
    # workflow proceed with the now-present local path. Matches the I4c
    # pattern: pre-fetch before queue_prompt, no changes to the workflow
    # shape downstream. Fail-loud if audio can't be resolved — SRT can't
    # run without its source audio.
    if job_type == "voice-srt" and R2_ENABLED:
        try:
            audio_node_id = None
            audio_file_path = None
            for nid, node in workflow.items():
                if isinstance(node, dict) and node.get("class_type") == "VHS_LoadAudio":
                    audio_node_id = nid
                    audio_file_path = node.get("inputs", {}).get("audio_file")
                    break
            if audio_file_path and not os.path.exists(audio_file_path):
                audio_r2_key = job_input.get("audio_r2_key")
                if not audio_r2_key and audio_file_path.startswith(PROJECTS_ROOT + "/") and "/outputs/" in audio_file_path:
                    rel = audio_file_path[len(PROJECTS_ROOT) + 1:]
                    left, sep, right = rel.partition("/outputs/")
                    if sep and left and right:
                        audio_r2_key = f"{left}/output/{right}"
                if not audio_r2_key:
                    return {"error": f"voice-srt: audio_file missing locally and no R2 key derivable: {audio_file_path}"}
                if not r2_helper.file_exists(audio_r2_key):
                    return {"error": f"voice-srt: audio_file not in R2 either (key={audio_r2_key}, local={audio_file_path})"}
                os.makedirs(os.path.dirname(audio_file_path), exist_ok=True)
                r2_helper.download_file(audio_r2_key, audio_file_path)
                size_mb = os.path.getsize(audio_file_path) / 1024 / 1024
                print(f"[INFO] voice-srt: pre-fetched audio from R2 ({audio_r2_key}, {size_mb:.2f} MB) to {audio_file_path}")
        except Exception as e:
            return {"error": f"voice-srt audio R2 pre-fetch failed: {e}"}

    # B' refactor (2026-04-21): voice-srt bypasses ComfyUI entirely and runs
    # Python Whisper direct to emit word-level timestamps. Output format is
    # a superset of the old segment-level JSON, so downstream n8n Extract SRT
    # Content + R2 upload + handler_video consumers keep working unchanged.
    # Benefits: single Whisper pass (vs ComfyUI + python merge), half the
    # VRAM, word-level sync for shorts captions in v5 render handler.
    if job_type == "voice-srt":
        try:
            results = _run_python_whisper_srt(workflow, dest, prefix, index=index)
        except Exception as e:
            return {"error": f"voice-srt (python whisper) failed: {e}"}
        # Skip ComfyUI path entirely — jump to R2 upload + response below.
        # Use a sentinel to signal the post-ComfyUI code path already has results.
        prompt_id = "python_whisper_no_comfyui"
    else:
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

    # Duration QA (2026-04-15, lesson #34 Nivel 3 safety net):
    # After TTS generation, verify each chunk's actual duration matches the
    # expected duration derived from word_count. The Qwen3 sub-chunk seam bug
    # (lesson #34) can produce chunks truncated to ~30-70% of expected length
    # without any visible error — Whisper's language model priors later fill
    # the gap with hallucinations, masking the bug until human ears catch it.
    # This check detects truncation at source time and flags the result so
    # downstream (n8n Validate SRT) can reject-and-retry instead of shipping
    # broken audio. Threshold: actual >= 0.85 * expected (empirical ~0.35
    # sec/word for Qwen3 at default speed — recalibrate if voice params change).
    # Fail-open: missing word_count → no check (backward compat).
    if job_type == "txt-voice" and results:
        word_count = job_input.get("word_count")
        if word_count and word_count > 0:
            expected_dur = float(word_count) * 0.35
            min_accept_dur = expected_dur * 0.85
            for result in results:
                filepath = result.get("path")
                if filepath and os.path.exists(filepath):
                    actual_dur = _probe_duration(filepath)
                    result["duration_sec"] = round(actual_dur, 3)
                    result["expected_duration_sec"] = round(expected_dur, 3)
                    if actual_dur > 0 and actual_dur < min_accept_dur:
                        ratio = actual_dur / expected_dur
                        warn_msg = f"TRUNCATED? actual={actual_dur:.2f}s < min_accept={min_accept_dur:.2f}s (expected {expected_dur:.2f}s, ratio={ratio:.2f}, word_count={word_count})"
                        print(f"[WARN] txt-voice chunk={result.get('filename','?')}: {warn_msg}")
                        result["duration_warning"] = warn_msg

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

    # Release VRAM so next job starts with clean GPU memory
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
