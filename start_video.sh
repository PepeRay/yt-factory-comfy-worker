#!/bin/bash
set -e

echo "=== YouTube Factory — Video Endpoint ==="

# Video nodes from Network Volume
WHITELIST_NODES=(
    "RES4LYF"
)

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

    # Whitelist video nodes only
    if [ -d "/runpod-volume/ComfyUI/custom_nodes" ]; then
        echo "Linking whitelisted video nodes..."
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

echo "Starting Video handler..."
cd /
python handler.py
