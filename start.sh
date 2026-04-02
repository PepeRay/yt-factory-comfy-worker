#!/bin/bash
set -e

echo "=== YouTube Factory Serverless Worker ==="

# ── Network Volume: MODELS + INPUTS only ────────────────────
if [ -d "/runpod-volume/ComfyUI" ]; then
    echo "Network Volume detected at /runpod-volume/ComfyUI"

    # Link input files (reference voices, images, etc.)
    if [ -d "/runpod-volume/ComfyUI/input" ]; then
        echo "Linking input files from Network Volume..."
        for f in /runpod-volume/ComfyUI/input/*; do
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
            fname=$(basename "$f")
            if [ ! -e "/comfyui/output/$fname" ]; then
                ln -sf "$f" "/comfyui/output/$fname"
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
