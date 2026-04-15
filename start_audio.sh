#!/bin/bash
set -e

echo "=== YouTube Factory — Audio Endpoint ==="

# Custom nodes + models are baked in Docker image — NV-free mode (2026-04-15)
# Previously had a dual-path "if NV exists then symlink else R2 download" block;
# endpoints no longer run with Network Volume attached (networkVolumeId="").
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

echo "Launching ComfyUI on port 8188..."
cd /comfyui
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &

echo "Starting Audio handler..."
cd /
python handler.py
