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

    # Link ALL model subdirectories from Network Volume into /comfyui/models/
    # Some nodes (e.g., VibeVoice) discover models relative to checkpoints dir,
    # bypassing extra_model_paths.yaml. Symlinks ensure all models are found.
    if [ -d "/runpod-volume/ComfyUI/models" ]; then
        echo "Linking model directories from Network Volume..."
        for src in /runpod-volume/ComfyUI/models/*/; do
            [ -d "$src" ] || continue
            dir_name=$(basename "$src")
            dst="/comfyui/models/$dir_name"
            if [ ! -e "$dst" ]; then
                ln -sf "$src" "$dst"
                echo "  ✓ Linked: $dir_name"
            else
                echo "  ● Exists: $dir_name"
            fi
        done
    fi

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
    # REPLACE build-time clones with Network Volume versions — the NV version
    # is the one tested on the dev pod and known to work with its models.
    # DO NOT run pip install here — deps are pre-installed in the Docker image.
    if [ -d "/runpod-volume/ComfyUI/custom_nodes" ]; then
        echo "Linking whitelisted custom nodes from Network Volume..."
        for node_name in "${WHITELIST_NODES[@]}"; do
            node_dir="/runpod-volume/ComfyUI/custom_nodes/$node_name"
            if [ -d "$node_dir" ]; then
                # Remove build-time clone if it exists (may be outdated)
                if [ -d "/comfyui/custom_nodes/$node_name" ] && [ ! -L "/comfyui/custom_nodes/$node_name" ]; then
                    echo "  ↻ Replacing build-time clone: $node_name"
                    rm -rf "/comfyui/custom_nodes/$node_name"
                fi
                if [ ! -e "/comfyui/custom_nodes/$node_name" ]; then
                    ln -sf "$node_dir" "/comfyui/custom_nodes/$node_name"
                    echo "  ✓ Linked: $node_name"
                fi
            else
                echo "  ✗ Not found on Network Volume: $node_name"
            fi
        done
    fi
else
    echo "INFO: No Network Volume — using baked models"
    echo "No Network Volume — downloading inputs from R2..."
    python /download_r2_inputs.py 2>/dev/null || echo "R2 input download skipped"
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
