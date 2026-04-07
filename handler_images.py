"""
YouTube Factory — Images Endpoint Handler
Job types: txt-img
Models: Flux 2 Klein + LoRAs
"""

import json
import os
import time
import uuid
import shutil
import urllib.request
import urllib.error
import glob as glob_module

import subprocess
import base64
import runpod
import websocket

COMFY_HOST = "127.0.0.1:8188"
COMFY_API_AVAILABLE_INTERVAL_MS = int(os.environ.get("COMFY_API_AVAILABLE_INTERVAL_MS", 500))
COMFY_API_AVAILABLE_MAX_RETRIES = int(os.environ.get("COMFY_API_AVAILABLE_MAX_RETRIES", 240))
COMFY_EXECUTION_TIMEOUT = int(os.environ.get("COMFY_EXECUTION_TIMEOUT", 300))
NETWORK_VOLUME_PATH = os.environ.get("RUNPOD_NETWORK_VOLUME_PATH", "/runpod-volume")
PROJECTS_ROOT = os.path.join(NETWORK_VOLUME_PATH, "projects")

OUTPUT_DIRS = [
    "/comfyui/output",
    f"{NETWORK_VOLUME_PATH}/ComfyUI/output",
    "/comfyui/temp",
]

ACCEPTED_JOB_TYPES = {
    "txt-img": "images",
}

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




def project_dir(channel, content_id):
    return os.path.join(PROJECTS_ROOT, channel, content_id)


def source_dir(channel, content_id, media_type):
    return os.path.join(project_dir(channel, content_id), "source", media_type)


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
        # Images (primary for this endpoint)
        if "images" in node_output:
            for img in node_output["images"]:
                if img.get("type") != "temp":
                    src = find_output_file(img.get("filename"), img.get("subfolder", ""))
                    if src:
                        ext = os.path.splitext(img["filename"])[1] or ".png"
                        all_files.append({"src": src, "ext": ext, "node_id": node_id, "type": "image"})

    results = []
    for i, item in enumerate(all_files):
        is_last = (i == len(all_files) - 1)
        if index is not None:
            if is_last:
                # Last output = refined → clean name (prefix already has scene_id)
                dest_name = f"{prefix}{item['ext']}"
            else:
                # Single file or draft → include counter
                dest_name = f"{prefix}_{i + 1:03d}{item['ext']}"
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


FONT_DIR = "/usr/local/share/fonts/bebas-neue"
DEFAULT_FONT = os.path.join(FONT_DIR, "BebasNeue-Regular.ttf")


def post_process_thumbnail(image_path, output_path):
    """
    Apply post-processing to AI-generated thumbnail before text overlay.
    Saturation +30%, contrast +12, sharpening, levels adjustment.
    Research: AI models generate flat colors — this step is non-negotiable.
    """
    cmd = [
        "convert", image_path,
        "-modulate", "105,130,100",       # brightness 105%, saturation +30%, hue unchanged
        "-brightness-contrast", "5x12",   # slight brightness boost + contrast +12
        "-unsharp", "0x1+0.8+0.05",       # sharpening (radius 0, sigma 1, amount 0.8, threshold 0.05)
        "-level", "5%,95%,1.05",          # crush blacks/whites slightly, mild gamma boost
        output_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"[WARN] Post-process failed: {result.stderr[:300]}")
            return None
        return output_path
    except Exception as e:
        print(f"[WARN] Post-process error: {e}")
        return None


def apply_text_overlay(image_path, overlay_text, output_path, config=None):
    """
    Apply text overlay using ImageMagick.
    Resizes to 1280x720 and adds text in upper-third with outline + drop shadow.
    Only runs if overlay_text is provided — otherwise returns None.
    """
    if not overlay_text or not overlay_text.strip():
        return None

    cfg = config or {}
    font = DEFAULT_FONT
    color = cfg.get("thumb_color", "#FFFFFF")
    outline_color = cfg.get("thumb_outline", "#000000")
    pointsize = cfg.get("thumb_pointsize", "96")
    stroke_width = cfg.get("thumb_stroke_width", "5")

    # ImageMagick: resize → shadow layer → outlined text → fill text
    # gravity North + offset +0+80 = upper ~25% of frame
    # Avoids YouTube timestamp (bottom-right) and progress bar (bottom)
    cmd = [
        "convert", image_path,
        "-resize", "1280x720!",
        # Semi-transparent dark gradient behind text for "oasis of contrast"
        "(", "-size", "1280x200", "gradient:rgba(0,0,0,0.7)-rgba(0,0,0,0)",
        ")",
        "-gravity", "North", "-composite",
        # Blurred shadow behind text for depth (80% opacity, 3px blur, +5+5 offset)
        "(", "-clone", "0",
        "-fill", "none",
        "-font", font,
        "-pointsize", str(pointsize),
        "-gravity", "North",
        "-stroke", "black",
        "-strokewidth", "8",
        "-annotate", "+5+85", overlay_text,
        "-blur", "0x3",
        ")",
        "-composite",
        # Crisp text with outline
        "-font", font,
        "-pointsize", str(pointsize),
        "-gravity", "North",
        "-stroke", outline_color,
        "-strokewidth", str(stroke_width),
        "-annotate", "+0+80", overlay_text,
        # Fill text on top of outline
        "-stroke", "none",
        "-fill", color,
        "-annotate", "+0+80", overlay_text,
        output_path,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            print(f"[WARN] ImageMagick failed: {result.stderr[:300]}")
            return None
        return output_path
    except Exception as e:
        print(f"[WARN] Text overlay error: {e}")
        return None


def image_to_base64(image_path):
    """Read image file and return base64 string."""
    with open(image_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def handler(job):
    """
    Images Endpoint Handler.
    Accepts: txt-img (Flux 2 Klein image generation)
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

    if job_type not in ACCEPTED_JOB_TYPES:
        return {"error": f"Images endpoint only accepts: {list(ACCEPTED_JOB_TYPES.keys())}. Got: {job_type}"}

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

    # ── Thumbnail post-processing + text overlay (only if overlay_text provided) ──
    overlay_text = job_input.get("overlay_text")
    thumbnail_b64 = None
    overlay_applied = False

    if overlay_text and results:
        # Use the last (refined/detailed) image as base
        base_image = results[-1]["path"]

        # Post-process: saturation +30%, contrast, sharpening, levels
        pp_output = os.path.join(dest, f"{prefix}_pp.png")
        pp_result = post_process_thumbnail(base_image, pp_output)
        if pp_result and os.path.exists(pp_result):
            base_image = pp_result  # Use post-processed image for text overlay
            print(f"[INFO] Post-processing applied: {pp_output}")

        thumb_output = os.path.join(dest, f"{prefix}_final.png")

        overlay_config = {
            "thumb_color": job_input.get("thumb_color", "#FFFFFF"),
            "thumb_outline": job_input.get("thumb_outline", "#000000"),
            "thumb_pointsize": job_input.get("thumb_pointsize", "80"),
            "thumb_stroke_width": job_input.get("thumb_stroke_width", "5"),
        }

        result_path = apply_text_overlay(base_image, overlay_text, thumb_output, overlay_config)
        if result_path and os.path.exists(result_path):
            overlay_applied = True
            thumbnail_b64 = image_to_base64(result_path)
            results.append({
                "filename": f"{prefix}_final.png",
                "path": result_path,
                "node_id": "text_overlay",
                "size_mb": round(os.path.getsize(result_path) / 1024 / 1024, 2),
            })

    free_vram()

    return {
        "status": "success",
        "job_type": job_type,
        "channel": channel,
        "content_id": content_id,
        "output_dir": dest,
        "outputs": results,
        "overlay_applied": overlay_applied,
        "thumbnail_b64": thumbnail_b64,
    }


runpod.serverless.start({"handler": handler})
