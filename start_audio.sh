#!/bin/bash
set -e

echo "=== YouTube Factory — Audio Endpoint ==="

# Custom nodes are baked in Docker image — no NV symlinks needed

if [ -d "/runpod-volume/ComfyUI" ]; then
    echo "Network Volume detected"

    # Link model directories
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

    # Link input files (reference voices)
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
else
    echo "INFO: No Network Volume — using baked models"
    echo "No Network Volume — downloading inputs from R2..."
    python /download_r2_inputs.py 2>/dev/null || echo "R2 input download skipped"
fi

mkdir -p /runpod-volume/jobs 2>/dev/null || true

echo "Launching ComfyUI on port 8188..."
cd /comfyui
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &

echo "Starting Audio handler..."
cd /
python handler.py
