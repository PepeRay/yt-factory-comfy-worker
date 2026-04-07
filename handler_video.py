"""
YouTube Factory — Video Endpoint Handler
Job types: img-vid, compose, download
Models: LTX-Video 2.3, FFmpeg
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
        raise RuntimeError(f"ffprobe failed for {path}: {result.stderr[:200]}")
    return float(result.stdout.strip())


def _compose_scene_manifest(src, dest, content_id, config, channel, platform="youtube"):
    """
    Compose full video from scenes.json v2 manifest.
    Phase 1: Render each scene to a normalized segment (1920x1080, 30fps, h264)
    Phase 2: Apply transitions between segments (cut, dissolve, fade_black, fade_white)
    Phase 3: Mux with audio, background music, and optional subtitles
    """
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
    NORM = f"-vf scale={WIDTH}:{HEIGHT}:force_original_aspect_ratio=increase,crop={WIDTH}:{HEIGHT},setsar=1 -r {FPS} -c:v libx264 -preset fast -crf 18 -pix_fmt yuv420p -an"

    # Directory for extracted ambient audio from LTX clips
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
        render_type = scene.get("render_type", "ken_burns")
        duration = scene.get("duration_sec", 5)
        seg_path = os.path.join(segments_dir, f"seg_{sid:04d}.mp4")
        amb_path = os.path.join(ambient_dir, f"amb_{sid:04d}.aac")

        try:
            if render_type == "video_clip":
                # Use LTX-generated clip, normalize it
                clip_path = os.path.join(vid_dir, f"scene_{sid:03d}.mp4")
                if not os.path.exists(clip_path):
                    # Fallback: try with different naming
                    candidates = glob_module.glob(os.path.join(vid_dir, f"*{sid:03d}*"))
                    clip_path = candidates[0] if candidates else clip_path
                if not os.path.exists(clip_path):
                    print(f"WARN: scene {sid} video_clip missing {clip_path}, falling back to ken_burns")
                    render_type = "ken_burns"
                else:
                    cmd = f"ffmpeg -y -i {clip_path} {NORM} {seg_path}"
                    result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=120)
                    if result.returncode != 0:
                        raise RuntimeError(f"segment {sid} video_clip: {result.stderr[:200]}")
                    # Extract ambient audio from LTX clip (best effort)
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
                        "-c:v", "libx264", "-preset", "fast", "-crf", "18",
                        "-pix_fmt", "yuv420p", seg_path
                    ]
                    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                    if result.returncode != 0:
                        raise RuntimeError(f"segment {sid} crossfade: {result.stderr[:200]}")
                    # Generate silence for non-video scenes
                    sil_cmd = ["ffmpeg", "-y", "-f", "lavfi", "-i", f"anullsrc=r=44100:cl=stereo", "-t", str(duration), "-c:a", "aac", amb_path]
                    subprocess.run(sil_cmd, capture_output=True, text=True, timeout=30)
                    segment_paths.append({"path": seg_path, "scene": scene})
                    continue

            if render_type == "ken_burns":
                # Slow zoom on single image
                img_path = _find_image(img_dir, sid, "initial")
                if not img_path:
                    print(f"WARN: scene {sid} ken_burns missing image, skipping")
                    skipped += 1
                    continue
                frames = int(duration * FPS)
                # Zoom from 1.0x to 1.15x centered
                cmd = [
                    "ffmpeg", "-y", "-i", img_path,
                    "-filter_complex",
                    f"scale=8000:-1,"
                    f"zoompan=z='1+0.15*on/{frames}':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"
                    f":d={frames}:s={WIDTH}x{HEIGHT}:fps={FPS},"
                    f"format=yuv420p",
                    "-t", str(duration),
                    "-c:v", "libx264", "-preset", "fast", "-crf", "18",
                    seg_path
                ]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if result.returncode != 0:
                    raise RuntimeError(f"segment {sid} ken_burns: {result.stderr[:200]}")
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
                    "-i", concat_list, "-c:v", "libx264", "-preset", "fast",
                    "-crf", "18", concat_out
                ]
                # Recreate concat list for retry
                with open(concat_list, "w") as f:
                    f.write(f"file '{merged_path}'\nfile '{seg['path']}'\n")
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
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
                "-c:v", "libx264", "-preset", "fast", "-crf", "18",
                "-pix_fmt", "yuv420p", xfade_out
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
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

    # ── Phase 3: Mux with narration + music + ambient + subtitles ──
    audio_path = os.path.join(dest, f"{content_id}_audio.flac")
    if not os.path.exists(audio_path):
        audio_path = os.path.join(src, "audio", f"{content_id}_audio.flac")
    srt_path = os.path.join(src, "srt", "subtitles.srt")


    # ── Music assembly: multi-act tracks with crossfades + fade in/out ──
    music_path = None
    music_dir = os.path.join(PROJECTS_ROOT, channel, "music")
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
                print(f"Music: failed to build act {i}: {result.stderr[:200]}")
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
        filter_parts.append(f"[{music_idx}:a]volume=0.12[music]")
        audio_inputs.append("[music]")
    if amb_idx is not None:
        filter_parts.append(f"[{amb_idx}:a]volume=0.08[amb]")
        audio_inputs.append("[amb]")

    srt_filter = f"subtitles={srt_path}:force_style='FontSize=22,PrimaryColour=&H00FFFFFF'" if has_srt else None

    if audio_inputs:
        if len(audio_inputs) > 1:
            mix_filter = "".join(audio_inputs) + f"amix=inputs={len(audio_inputs)}:duration=first[aout]"
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
        cmd += ["-c:v", "libx264", "-preset", "fast", "-crf", "18",
                "-c:a", "aac", "-b:a", "192k", "-shortest", output_path]
    elif has_srt:
        cmd += ["-vf", srt_filter,
                "-c:v", "libx264", "-preset", "fast", "-crf", "18", output_path]
    else:
        shutil.copy2(merged_path, output_path)
        cmd = None

    if cmd:
        layers = []
        if has_narration: layers.append("narration")
        if has_music: layers.append("music")
        if has_ambient: layers.append("ambient")
        print(f"Phase 3: mixing {' + '.join(layers) if layers else 'video only'}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        if result.returncode != 0:
            raise RuntimeError(f"Phase 3 mux failed: {result.stderr[:500]}")

    # Cleanup segments
    shutil.rmtree(segments_dir, ignore_errors=True)

    final_size = round(os.path.getsize(output_path) / 1024 / 1024, 2)
    print(f"Phase 3 done: {output_path} ({final_size} MB)")

    return [{
        "filename": f"{content_id}_final.mp4",
        "path": output_path,
        "size_mb": final_size,
        "segments_rendered": len(segment_paths),
        "segments_skipped": skipped,
    }]


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
    Accepts: img-vid (LTX-Video), compose (FFmpeg assembly), download (NV file retrieval)
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

    # Download jobs (retrieve file from NV as base64)
    if job_type == "download":
        platform = job_input.get("platform", "youtube")
        filename = job_input.get("filename")
        if not filename:
            return {"error": "Missing required field: filename"}
        file_path = os.path.join(
            output_dir_for(channel, content_id, platform), filename
        )
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
    for node_id, node_data in workflow.items():
        if node_data.get("class_type") == "LoadImage":
            img_path = node_data.get("inputs", {}).get("image", "")
            if img_path and not os.path.exists(img_path):
                # Try glob fallback for naming variations
                base, ext = os.path.splitext(img_path)
                candidates = sorted(glob_module.glob(f"{base}_*{ext}"))
                if candidates:
                    resolved = candidates[-1]  # Last = most refined
                    node_data["inputs"]["image"] = resolved

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
