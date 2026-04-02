"""
YouTube Factory — ComfyUI Serverless Handler
Based on blib-la/runpod-worker-comfy, extended for audio + video output.

Camino A: Output files are saved to Network Volume, caller retrieves via S3 API.
Also supports base64 return for smaller files (SRT, images).
"""

import json
import os
import time
import base64
import urllib.request
import urllib.parse
import glob as glob_module

import runpod
import websocket

COMFY_HOST = "127.0.0.1:8188"
COMFY_API_AVAILABLE_INTERVAL_MS = int(os.environ.get("COMFY_API_AVAILABLE_INTERVAL_MS", 500))
COMFY_API_AVAILABLE_MAX_RETRIES = int(os.environ.get("COMFY_API_AVAILABLE_MAX_RETRIES", 0))
COMFY_POLLING_INTERVAL_MS = int(os.environ.get("COMFY_POLLING_INTERVAL_MS", 250))
COMFY_EXECUTION_TIMEOUT = int(os.environ.get("COMFY_EXECUTION_TIMEOUT", 300))  # 5 min default
NETWORK_VOLUME_PATH = os.environ.get("RUNPOD_NETWORK_VOLUME_PATH", "/runpod-volume")

# Map of ComfyUI output subfolders to check for results
OUTPUT_DIRS = [
    "/comfyui/output",
    f"{NETWORK_VOLUME_PATH}/ComfyUI/output",
    "/comfyui/temp",
]


def wait_for_comfyui():
    """Wait until ComfyUI API is responsive."""
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


def queue_prompt(workflow_json):
    """Submit a workflow to ComfyUI and return the prompt_id."""
    data = json.dumps({"prompt": workflow_json}).encode("utf-8")
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


def open_websocket():
    """Open a websocket connection to ComfyUI for monitoring."""
    ws_url = f"ws://{COMFY_HOST}/ws?clientId=runpod-worker"
    ws = websocket.WebSocket()
    ws.settimeout(30)
    ws.connect(ws_url)
    return ws


def wait_for_completion(ws, prompt_id):
    """Wait for the workflow to finish via websocket, with timeout."""
    start_time = time.time()

    try:
        while True:
            elapsed = time.time() - start_time
            if elapsed > COMFY_EXECUTION_TIMEOUT:
                raise RuntimeError(
                    f"Workflow execution timed out after {COMFY_EXECUTION_TIMEOUT}s. "
                    "Check that your workflow has valid output nodes."
                )

            try:
                msg = ws.recv()
            except websocket.WebSocketTimeoutException:
                # No message in 30s — check if prompt is in history (already done)
                try:
                    history = get_history(prompt_id)
                    if prompt_id in history:
                        return True  # Already completed
                except Exception:
                    pass
                continue

            if isinstance(msg, str):
                data = json.loads(msg)
                msg_type = data.get("type")

                if msg_type == "executing":
                    exec_data = data.get("data", {})
                    if exec_data.get("node") is None and exec_data.get("prompt_id") == prompt_id:
                        return True

                elif msg_type == "execution_error":
                    error_data = data.get("data", {})
                    raise RuntimeError(f"ComfyUI execution error: {error_data}")

                elif msg_type == "execution_cached":
                    pass  # Normal, nodes cached
    finally:
        ws.close()


def get_history(prompt_id):
    """Get execution history for a prompt."""
    req = urllib.request.Request(f"http://{COMFY_HOST}/history/{prompt_id}")
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


def collect_outputs(prompt_id):
    """
    Collect ALL outputs from ComfyUI execution — images, audio, video, text.
    Returns a dict with categorized results.
    """
    history = get_history(prompt_id)
    prompt_history = history.get(prompt_id, {})
    outputs = prompt_history.get("outputs", {})

    results = {
        "images": [],
        "audio": [],
        "video": [],
        "text": [],
        "files": [],
    }

    for node_id, node_output in outputs.items():
        # Handle images
        if "images" in node_output:
            for img in node_output["images"]:
                if img.get("type") == "temp":
                    continue
                file_path = find_output_file(img.get("filename"), img.get("subfolder", ""))
                if file_path:
                    results["images"].append({
                        "filename": img["filename"],
                        "node_id": node_id,
                        "path": file_path,
                        "base64": encode_file_base64(file_path),
                    })

        # Handle audio (VHS, VibeVoice, etc.)
        if "audio" in node_output:
            for aud in node_output["audio"]:
                file_path = find_output_file(aud.get("filename"), aud.get("subfolder", ""))
                if file_path:
                    ext = os.path.splitext(aud["filename"])[1].lower()
                    results["audio"].append({
                        "filename": aud["filename"],
                        "node_id": node_id,
                        "path": file_path,
                        "size_mb": round(os.path.getsize(file_path) / 1024 / 1024, 2),
                        # Only base64 for small audio (<10MB)
                        "base64": encode_file_base64(file_path) if os.path.getsize(file_path) < 10 * 1024 * 1024 else None,
                    })

        # Handle video/gifs (VHS_VideoCombine output)
        if "gifs" in node_output:
            for vid in node_output["gifs"]:
                file_path = find_output_file(vid.get("filename"), vid.get("subfolder", ""))
                if file_path:
                    results["video"].append({
                        "filename": vid["filename"],
                        "node_id": node_id,
                        "path": file_path,
                        "size_mb": round(os.path.getsize(file_path) / 1024 / 1024, 2),
                        # Video files are too large for base64, use S3 path
                        "network_volume_path": file_path if file_path.startswith(NETWORK_VOLUME_PATH) else None,
                    })

        # Handle text output (SRT from Whisper, etc.)
        if "text" in node_output:
            for txt_item in node_output["text"]:
                if isinstance(txt_item, str):
                    results["text"].append({
                        "content": txt_item,
                        "node_id": node_id,
                    })
                elif isinstance(txt_item, dict):
                    results["text"].append({
                        "content": txt_item,
                        "node_id": node_id,
                    })

        # Handle any other output types
        known_keys = {"images", "audio", "gifs", "text"}
        for key in node_output:
            if key not in known_keys:
                results["files"].append({
                    "type": key,
                    "node_id": node_id,
                    "data": str(node_output[key])[:500],
                })

    return results


def find_output_file(filename, subfolder=""):
    """Find an output file in the known output directories."""
    if not filename:
        return None
    for output_dir in OUTPUT_DIRS:
        candidate = os.path.join(output_dir, subfolder, filename)
        if os.path.exists(candidate):
            return candidate
    # Fallback: glob search
    for output_dir in OUTPUT_DIRS:
        pattern = os.path.join(output_dir, "**", filename)
        matches = glob_module.glob(pattern, recursive=True)
        if matches:
            return matches[0]
    return None


def encode_file_base64(file_path):
    """Encode a file as base64 string."""
    try:
        with open(file_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")
    except Exception:
        return None


def copy_to_network_volume(file_path, job_id, category="output"):
    """
    Copy output file to a structured location on the Network Volume.
    This makes it retrievable via S3 API.
    Returns the path on the network volume.
    """
    if not os.path.exists(NETWORK_VOLUME_PATH):
        return None

    dest_dir = os.path.join(NETWORK_VOLUME_PATH, "jobs", job_id, category)
    os.makedirs(dest_dir, exist_ok=True)

    filename = os.path.basename(file_path)
    dest_path = os.path.join(dest_dir, filename)

    # Only copy if not already on the network volume
    if not file_path.startswith(NETWORK_VOLUME_PATH):
        import shutil
        shutil.copy2(file_path, dest_path)

    return dest_path


def inject_inputs(workflow, input_images=None, input_audio=None):
    """
    Inject base64-encoded input files into ComfyUI's input folder.
    Used when n8n sends images/audio as part of the request.
    """
    if input_images:
        for img_name, img_b64 in input_images.items():
            save_path = os.path.join("/comfyui/input", img_name)
            with open(save_path, "wb") as f:
                f.write(base64.b64decode(img_b64))

    if input_audio:
        for aud_name, aud_b64 in input_audio.items():
            save_path = os.path.join("/comfyui/input", aud_name)
            with open(save_path, "wb") as f:
                f.write(base64.b64decode(aud_b64))


def handler(job):
    """
    RunPod Serverless handler.

    Expected input format:
    {
        "workflow": { ... ComfyUI workflow JSON ... },
        "input_images": { "name.png": "<base64>" },  // optional
        "input_audio": { "voice.wav": "<base64>" },   // optional
    }
    """
    job_input = job.get("input", {})
    job_id = job.get("id", "unknown")

    workflow = job_input.get("workflow")
    if not workflow:
        return {"error": "No workflow provided"}

    # Wait for ComfyUI to be ready
    try:
        wait_for_comfyui()
    except RuntimeError as e:
        return {"error": str(e)}

    # Inject input files if provided
    inject_inputs(
        workflow,
        input_images=job_input.get("input_images"),
        input_audio=job_input.get("input_audio"),
    )

    # Open websocket BEFORE queuing to avoid missing completion messages
    try:
        ws = open_websocket()
    except Exception as e:
        return {"error": f"Failed to connect websocket: {str(e)}"}

    # Submit workflow
    try:
        prompt_id = queue_prompt(workflow)
    except Exception as e:
        ws.close()
        return {"error": f"Failed to queue prompt: {str(e)}"}

    # Wait for execution
    try:
        wait_for_completion(ws, prompt_id)
    except RuntimeError as e:
        return {"error": f"Execution failed: {str(e)}"}

    # Collect all outputs
    results = collect_outputs(prompt_id)

    # Copy outputs to Network Volume for S3 retrieval
    for category in ["images", "audio", "video"]:
        for item in results[category]:
            if item.get("path"):
                nv_path = copy_to_network_volume(item["path"], job_id, category)
                if nv_path:
                    # Store the S3-accessible path (relative to volume root)
                    item["s3_key"] = nv_path.replace(NETWORK_VOLUME_PATH + "/", "")

    # Summary for logging
    summary = {
        "images": len(results["images"]),
        "audio": len(results["audio"]),
        "video": len(results["video"]),
        "text": len(results["text"]),
    }

    return {
        "output": results,
        "summary": summary,
        "job_id": job_id,
    }


# Start the serverless worker
runpod.serverless.start({"handler": handler})
