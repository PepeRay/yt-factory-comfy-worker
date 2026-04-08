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
    echo "WARNING: Network Volume not found"
fi

mkdir -p /runpod-volume/jobs 2>/dev/null || true

echo "Launching ComfyUI on port 8188..."
cd /comfyui
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &

echo "Starting Video handler..."
cd /
python handler.py
