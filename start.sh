#!/bin/bash
set -e

echo "=== YouTube Factory Serverless Worker ==="

# ── WHITELIST: Only these nodes get linked from Network Volume ──
# Private repos that can't clone at build time, or repos too large to clone.
# NEVER link everything — random nodes destroy the container's Python env.
WHITELIST_NODES=(
    "VibeVoice-ComfyUI"
    "TTS-Audio-Suite"
    "RES4LYF"
)

# ── Network Volume: MODELS + INPUTS only ────────────────────
if [ -d "/runpod-volume/ComfyUI" ]; then
    echo "Network Volume detected at /runpod-volume/ComfyUI"

    # Link input files (reference voices, images, etc.)
    if [ -d "/runpod-volume/ComfyUI/input" ]; then
        echo "Linking input files from Network Volume..."
        for f in /runpod-volume/ComfyUI/input/*; do
            [ -e "$f" ] || continue
            fname=$(basename "$f")
            if [ ! -e "/comfyui/input/$fname" ]; then
                ln -sf "$f" "/comfyui/input/$fname"
            fi
        done
    fi

    # Link output directory so ComfyUI can read previous outputs
    if [ -d "/runpod-volume/ComfyUI/output" ]; then
        echo "Linking output files from Network Volume..."
        for f in /runpod-volume/ComfyUI/output/*; do
            [ -e "$f" ] || continue
            fname=$(basename "$f")
            if [ ! -e "/comfyui/output/$fname" ]; then
                ln -sf "$f" "/comfyui/output/$fname"
            fi
        done
    fi

    # ── Whitelist-only: link SPECIFIC custom nodes from Network Volume ──
    if [ -d "/runpod-volume/ComfyUI/custom_nodes" ]; then
        echo "Linking whitelisted custom nodes from Network Volume..."
        for node_name in "${WHITELIST_NODES[@]}"; do
            node_dir="/runpod-volume/ComfyUI/custom_nodes/$node_name"
            if [ -d "$node_dir" ] && [ ! -d "/comfyui/custom_nodes/$node_name" ]; then
                echo "  ✓ Linking: $node_name"
                ln -sf "$node_dir" "/comfyui/custom_nodes/$node_name"
                # DO NOT run pip install here — deps are pre-installed in the
                # Docker image. Runtime pip installs destroy the Python env
                # by downgrading numpy, transformers, protobuf, etc.
            elif [ -d "/comfyui/custom_nodes/$node_name" ]; then
                echo "  ● Already in image: $node_name"
            else
                echo "  ✗ Not found on Network Volume: $node_name"
            fi
        done
    fi
else
    echo "WARNING: Network Volume not found at /runpod-volume"
fi

# Create jobs output directory
mkdir -p /runpod-volume/jobs 2>/dev/null || true

# ── Start ComfyUI ───────────────────────────────────────────
echo "Launching ComfyUI on port 8188..."
cd /comfyui
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &
COMFY_PID=$!
echo "ComfyUI PID: $COMFY_PID"

# ── Start RunPod Handler ────────────────────────────────────
echo "Starting RunPod handler..."
cd /
python handler.py
