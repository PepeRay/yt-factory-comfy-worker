#!/bin/bash
set -e

echo "=== YouTube Factory — Audio Endpoint ==="

# Only audio-related nodes from Network Volume
WHITELIST_NODES=(
    "VibeVoice-ComfyUI"
    "TTS-Audio-Suite"
)

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

    # Whitelist audio nodes only
    if [ -d "/runpod-volume/ComfyUI/custom_nodes" ]; then
        echo "Linking whitelisted audio nodes..."
        for node_name in "${WHITELIST_NODES[@]}"; do
            node_dir="/runpod-volume/ComfyUI/custom_nodes/$node_name"
            if [ -d "$node_dir" ]; then
                if [ -d "/comfyui/custom_nodes/$node_name" ] && [ ! -L "/comfyui/custom_nodes/$node_name" ]; then
                    rm -rf "/comfyui/custom_nodes/$node_name"
                fi
                if [ ! -e "/comfyui/custom_nodes/$node_name" ]; then
                    ln -sf "$node_dir" "/comfyui/custom_nodes/$node_name"
                    echo "  ✓ Linked: $node_name"
                fi
            else
                echo "  ✗ Not found: $node_name"
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

echo "Starting Audio handler..."
cd /
python handler.py
