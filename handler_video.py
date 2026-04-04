"""
YouTube Factory — Video Endpoint Handler
Job types: img-vid, compose
Models: LTX-Video 2.3, FFmpeg
"""

import json
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

COMFY_HOST = "127.0.0.1:8188"
COMFY_API_AVAILABLE_INTERVAL_MS = int(os.environ.get("COMFY_API_AVAILABLE_INTERVAL_MS", 500))
COMFY_API_AVAILABLE_MAX_RETRIES = int(os.environ.get("COMFY_API_AVAILABLE_MAX_RETRIES", 240))
COMFY_EXECUTION_TIMEOUT = int(os.environ.get("COMFY_EXECUTION_TIMEOUT", 600))
NETWORK_VOLUME_PATH = os.environ.get("RUNPOD_NETWORK_VOLUME_PATH", "/runpod-volume")
PROJECTS_ROOT = os.path.join(NETWORK_VOLUME_PATH, "projects")

OUTPUT_DIRS = [
    "/comfyui/output",
    f"{NETWORK_VOLUME_PATH}/ComfyUI/output",
    "/comfyui/temp",
]

ACCEPTED_JOB_TYPES = {
    "img-vid": "video",
}


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
    for out_dir in OUTPUT_DIRS:
        candidate = os.path.join(out_dir, subfolder, filename)
        if os.path.exists(candidate):
            return candidate
    for out_dir in OUTPUT_DIRS:
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
    else:
        raise RuntimeError(f"Unknown compose type: {compose_type}")


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
        raise RuntimeError(f"FFmpeg concat_audio failed: {result.stderr[:500]}")

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
        raise RuntimeError(f"FFmpeg concat_video failed: {result.stderr[:500]}")

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

    cmd += ["-c:v", "libx264", "-c:a", "aac", "-shortest", output_path]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg full compose failed: {result.stderr[:500]}")

    return [{"filename": f"{content_id}_final.mp4", "path": output_path,
             "size_mb": round(os.path.getsize(output_path) / 1024 / 1024, 2)}]


def handler(job):
    """
    Video Endpoint Handler.
    Accepts: img-vid (LTX-Video), compose (FFmpeg assembly)
    """
    job_input = job.get("input", {})

    job_type = job_input.get("job_type")
    channel = job_input.get("channel")
    content_id = job_input.get("content_id")

    if not job_type:
        return {"error": "Missing required field: job_type"}
    if not channel:
        return {"error": "Missing required field: channel"}
    if not content_id:
        return {"error": "Missing required field: content_id"}

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

    try:
        wait_for_comfyui()
    except RuntimeError as e:
        return {"error": str(e)}

    client_id = str(uuid.uuid4())
    ws_url = f"ws://{COMFY_HOST}/ws?clientId={client_id}"
    try:
        ws = websocket.WebSocket()
        ws.settimeout(30)
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

    return {
        "status": "success",
        "job_type": job_type,
        "channel": channel,
        "content_id": content_id,
        "output_dir": dest,
        "outputs": results,
    }


runpod.serverless.start({"handler": handler})
