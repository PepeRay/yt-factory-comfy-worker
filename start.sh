#!/bin/bash
set -e

echo "=== YouTube Factory Serverless Worker ==="
echo "Starting ComfyUI server..."

# Check if network volume is mounted
if [ -d "/runpod-volume/ComfyUI" ]; then
    echo "Network Volume detected at /runpod-volume/ComfyUI"

    # Sync custom_nodes from network volume if they exist there
    if [ -d "/runpod-volume/ComfyUI/custom_nodes" ]; then
        echo "Syncing custom nodes from Network Volume..."
        # Copy nodes that exist on volume but not in container
        for node_dir in /runpod-volume/ComfyUI/custom_nodes/*/; do
            node_name=$(basename "$node_dir")
            if [ ! -d "/comfyui/custom_nodes/$node_name" ]; then
                echo "  Linking: $node_name"
                ln -sf "$node_dir" "/comfyui/custom_nodes/$node_name"
            fi
        done
    fi

    # Link input files from network volume
    if [ -d "/runpod-volume/ComfyUI/input" ]; then
        echo "Linking input files from Network Volume..."
        for f in /runpod-volume/ComfyUI/input/*; do
            fname=$(basename "$f")
            if [ ! -e "/comfyui/input/$fname" ]; then
                ln -sf "$f" "/comfyui/input/$fname"
            fi
        done
    fi
else
    echo "WARNING: Network Volume not found at /runpod-volume"
fi

# Create jobs output directory on network volume
mkdir -p /runpod-volume/jobs 2>/dev/null || true

# Ensure ComfyUI dependencies are installed (WITHOUT upgrading PyTorch)
echo "Checking ComfyUI dependencies..."
cd /comfyui
grep -v -i -E '^(torch|torchvision|torchaudio|nvidia)' requirements.txt > /tmp/reqs_no_torch.txt
pip install -r /tmp/reqs_no_torch.txt 2>&1 | tail -5
rm /tmp/reqs_no_torch.txt
echo "Dependencies check complete."

# Start ComfyUI in background
echo "Launching ComfyUI on port 8188..."
python main.py --listen 0.0.0.0 --port 8188 --disable-auto-launch &

# Start the RunPod handler
echo "Starting RunPod handler..."
cd /
python handler.py
