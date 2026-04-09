"""
YouTube Factory — Audio Endpoint Handler
Job types: txt-voice, voice-srt, compose (concat_audio only), upload-inputs (temp)
Models: VibeVoice TTS, Whisper STT
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


# ── Main Handler ────────────────────────────────────────────

_INPUTS_DOWNLOADED = False


def _ensure_r2_inputs():
    """Download all R2 input files to /comfyui/input/ on first job (NV-free mode)."""
    global _INPUTS_DOWNLOADED
    if _INPUTS_DOWNLOADED or not R2_ENABLED:
        return
    try:
        input_dir = "/comfyui/input"
        os.makedirs(input_dir, exist_ok=True)
        keys = r2_helper.list_files("inputs/audio/")
        downloaded = 0
        for key in keys:
            fname = os.path.basename(key)
            if not fname:
                continue
            local_path = os.path.join(input_dir, fname)
            if not os.path.exists(local_path):
                r2_helper.download_file(key, local_path)
                downloaded += 1
        print(f"[INFO] R2 inputs synced: {downloaded} new files in {input_dir}")
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

    # ── Upload-inputs: scan NV and upload input files to R2 ──
    if job_type == "upload-inputs":
        if not R2_ENABLED:
            return {"error": "R2 is not enabled — cannot upload inputs"}

        uploaded = []
        input_dir = os.path.join(NETWORK_VOLUME_PATH, "ComfyUI", "input")

        # 1) Upload all files from ComfyUI/input/
        if os.path.isdir(input_dir):
            for fname in os.listdir(input_dir):
                fpath = os.path.join(input_dir, fname)
                if os.path.isfile(fpath):
                    r2_key = f"inputs/audio/{fname}"
                    try:
                        r2_helper.upload_file(fpath, r2_key)
                        uploaded.append({
                            "local_path": fpath,
                            "r2_key": r2_key,
                            "r2_url": r2_helper.presigned_url(r2_key),
                        })
                    except Exception as e:
                        print(f"[WARN] upload-inputs: failed {fpath}: {e}")

        # 2) Upload music/ dirs from each channel under projects/
        if os.path.isdir(PROJECTS_ROOT):
            for channel_name in os.listdir(PROJECTS_ROOT):
                music_dir = os.path.join(PROJECTS_ROOT, channel_name, "music")
                if os.path.isdir(music_dir):
                    for fname in os.listdir(music_dir):
                        fpath = os.path.join(music_dir, fname)
                        if os.path.isfile(fpath):
                            r2_key = f"{channel_name}/music/{fname}"
                            try:
                                r2_helper.upload_file(fpath, r2_key)
                                uploaded.append({
                                    "local_path": fpath,
                                    "r2_key": r2_key,
                                    "r2_url": r2_helper.presigned_url(r2_key),
                                })
                            except Exception as e:
                                print(f"[WARN] upload-inputs: failed {fpath}: {e}")

        return {
            "status": "success",
            "job_type": "upload-inputs",
            "uploaded": uploaded,
        }

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
