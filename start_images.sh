#!/bin/bash
set -e

echo "=== YouTube Factory — Images Endpoint ==="

# Images endpoint: NO nodes from Network Volume needed
# All image nodes are installed at build time in the Docker image

if [ -d "/runpod-volume/ComfyUI" ]; then
    echo "Network Volume detected"

    # Link model directories (Flux, LoRAs, CLIP, VAE)
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
else
    echo "WARNING: Network Volume not found"
fi

mkdir -p /runpod-volume/jobs 2>/dev/null || true

echo "Launching ComfyUI on port 8188..."
cd /comfyui
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &

echo "Starting Images handler..."
cd /
python handler.py
