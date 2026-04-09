#!/bin/bash
set -e

echo "=== YouTube Factory — Video Endpoint ==="

# Custom nodes are baked in Docker image — no NV symlinks needed

if [ -d "/runpod-volume/ComfyUI" ]; then
    echo "Network Volume detected"

    # Link model directories (LTX-Video, VAE)
    if [ -d "/runpod-volume/ComfyUI/models" ]; then
        echo "Linking model directories..."
        for src in /runpod-volume/ComfyUI/models/*/; do
            [ -d "$src" ] || continue
            dir_name=$(basename "$src")
            dst="/comfyui/models/$dir_name"
            if [ ! -e "$dst" ]; then
                ln -sf "$src" "$dst"
                echo "  ✓ Linked: $dir_name"
            fi
        done
    fi

    # Link input files
    if [ -d "/runpod-volume/ComfyUI/input" ]; then
        echo "Linking input files..."
        for f in /runpod-volume/ComfyUI/input/*; do
            [ -e "$f" ] || continue
            fname=$(basename "$f")
            if [ ! -e "/comfyui/input/$fname" ]; then
                ln -sf "$f" "/comfyui/input/$fname"
            fi
        done
    fi

    # Link output directory (video may need to read previous outputs)
    if [ -d "/runpod-volume/ComfyUI/output" ]; then
        echo "Linking output files..."
        for f in /runpod-volume/ComfyUI/output/*; do
            [ -e "$f" ] || continue
            fname=$(basename "$f")
            if [ ! -e "/comfyui/output/$fname" ]; then
                ln -sf "$f" "/comfyui/output/$fname"
            fi
        done
    fi
else
    echo "INFO: No Network Volume — using baked models"
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
fi

mkdir -p /runpod-volume/jobs 2>/dev/null || true

echo "Launching ComfyUI on port 8188..."
cd /comfyui
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &

echo "Starting Video handler..."
cd /
python handler.py
