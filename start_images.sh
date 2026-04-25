#!/bin/bash
set -e

echo "=== YouTube Factory — Images Endpoint ==="

# Images endpoint: NV-free (2026-04-15). All models + custom nodes baked in image.
echo "INFO: NV-free mode — using baked models"
echo "Downloading R2 inputs to /comfyui/input/..."
mkdir -p /comfyui/input
/opt/venv/bin/python <<'PYEOF'
import os, sys
sys.path.insert(0, '/')
try:
    import r2_helper
    keys = r2_helper.list_files("inputs/audio/")
    print(f"Found {len(keys)} files in R2 under inputs/audio/")
    for key in keys:
        fname = os.path.basename(key)
        if not fname:
            continue
        local_path = os.path.join("/comfyui/input", fname)
        if not os.path.exists(local_path):
            r2_helper.download_file(key, local_path)
            print(f"  [OK] {fname}")
        else:
            print(f"  [SKIP] {fname} exists")
    print("R2 input download complete")
except Exception as e:
    import traceback
    print(f"ERROR: {e}")
    traceback.print_exc()
PYEOF

# Download LoRAs from R2 (added 2026-04-25 for Path 0 LoRA validation per session)
# LoRAs live in R2 under dominion/loras/ to enable iteration without rebuilding
# the Docker image. ComfyUI scans /comfyui/models/loras/ for available LoRAs.
echo "Downloading LoRAs from R2 to /comfyui/models/loras/..."
mkdir -p /comfyui/models/loras
/opt/venv/bin/python <<'PYEOF'
import os, sys
sys.path.insert(0, '/')
try:
    import r2_helper
    keys = r2_helper.list_files("dominion/loras/")
    print(f"Found {len(keys)} LoRA files in R2 under dominion/loras/")
    for key in keys:
        fname = os.path.basename(key)
        if not fname or not fname.endswith('.safetensors'):
            continue
        local_path = os.path.join("/comfyui/models/loras", fname)
        if not os.path.exists(local_path):
            r2_helper.download_file(key, local_path)
            size_mb = os.path.getsize(local_path) / 1024 / 1024
            print(f"  [OK] {fname} ({size_mb:.1f} MB)")
        else:
            print(f"  [SKIP] {fname} exists")
    print("LoRA download complete")
except Exception as e:
    import traceback
    print(f"WARN: LoRA download failed (non-fatal, ComfyUI will run without): {e}")
    traceback.print_exc()
PYEOF

echo "Launching ComfyUI on port 8188..."
cd /comfyui
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &

echo "Starting Images handler..."
cd /
python handler.py
