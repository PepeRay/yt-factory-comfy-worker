# ============================================================
# YouTube Factory — ComfyUI Serverless Worker
# Based on: runpod-workers/worker-comfyui patterns
# Custom: Audio (VibeVoice, Whisper) + Video (LTX-Video)
# ============================================================
# IMPORTANT: Network Volume = MODELS ONLY. All custom nodes
# are installed here at build time with their Python deps.
# ============================================================

ARG BASE_IMAGE=nvidia/cuda:12.6.3-cudnn-runtime-ubuntu24.04

FROM ${BASE_IMAGE} AS base

ARG COMFYUI_VERSION=latest

ENV DEBIAN_FRONTEND=noninteractive
ENV PIP_PREFER_BINARY=1
ENV PYTHONUNBUFFERED=1
ENV CMAKE_BUILD_PARALLEL_LEVEL=8

# System dependencies
RUN apt-get update && apt-get install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    git \
    wget \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    ffmpeg \
    libsndfile1 \
    build-essential \
    && ln -sf /usr/bin/python3.12 /usr/bin/python \
    && ln -sf /usr/bin/pip3 /usr/bin/pip \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Install uv + venv
RUN wget -qO- https://astral.sh/uv/install.sh | sh \
    && ln -s /root/.local/bin/uv /usr/local/bin/uv \
    && ln -s /root/.local/bin/uvx /usr/local/bin/uvx \
    && uv venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

# ── ComfyUI Core ────────────────────────────────────────────
RUN uv pip install comfy-cli pip setuptools wheel

RUN /usr/bin/yes | comfy --workspace /comfyui install --version "${COMFYUI_VERSION}" --nvidia

# Force PyTorch cu126 (comfy-cli may install version requiring newer CUDA)
RUN uv pip install --force-reinstall torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu126

WORKDIR /comfyui

# Install ComfyUI requirements WITHOUT overwriting our PyTorch
# The filter preserves torchsde (needed by SDE samplers)
RUN grep -v -i -E '^(torch==|torch>=|torch<=|torchvision==|torchvision>=|torchaudio==|torchaudio>=|nvidia)' requirements.txt > /tmp/reqs_no_torch.txt && \
    uv pip install -r /tmp/reqs_no_torch.txt && \
    rm /tmp/reqs_no_torch.txt

# ── Model paths: Network Volume ─────────────────────────────
# Network Volume is for MODELS ONLY (not custom nodes)
COPY extra_model_paths.yaml ./

# ── Custom Nodes (ALL installed at build time) ──────────────
# Every node the workflows need must be here with its deps.
# DO NOT rely on Network Volume for custom nodes.

# --- From Comfy Registry (single layer) ---
RUN comfy node install comfyui-gguf || true && \
    comfy node install comfyui-ltxvideo || true && \
    comfy node install comfyui-videohelpersuite || true && \
    comfy node install comfyui-detail-daemon || true && \
    comfy node install comfyui-kjnodes || true && \
    comfy node install comfyui-whisper || true && \
    comfy node install comfyui-ttp-toolset || true && \
    comfy node install rgthree-comfy || true && \
    comfy node install comfymath || true && \
    comfy node install comfyui-impact-pack || true && \
    comfy node install comfyui-custom-scripts || true

# --- From Git (single layer, each clone is independent) ---
RUN cd custom_nodes \
    && git clone https://github.com/cubiq/ComfyUI_essentials.git \
    && (cd ComfyUI_essentials && pip install -r requirements.txt 2>/dev/null || true) \
    && git clone https://github.com/yolain/ComfyUI-Easy-Use.git \
    && (cd ComfyUI-Easy-Use && pip install -r requirements.txt 2>/dev/null || true) \
    ; cd /comfyui/custom_nodes \
    && (git clone https://github.com/ClownsharkBatwing/RES4LYF.git \
        && cd RES4LYF && pip install -r requirements.txt 2>/dev/null || true) \
    ; cd /comfyui/custom_nodes \
    && (git clone https://github.com/DualOrion/VibeVoice-ComfyUI.git \
        && cd VibeVoice-ComfyUI && pip install -r requirements.txt 2>/dev/null \
        || echo "WARN: VibeVoice clone failed — will link from Network Volume") \
    ; cd /comfyui/custom_nodes \
    && (git clone https://github.com/DualOrion/TTS-Audio-Suite.git \
        && cd TTS-Audio-Suite && pip install -r requirements.txt 2>/dev/null \
        || echo "WARN: TTS-Audio-Suite clone failed — will link from Network Volume") \
    ; cd /comfyui/custom_nodes \
    && (git clone https://github.com/vrgamedevgirl/comfyui-vrgamedevgirl.git 2>/dev/null || true) \
    && (git clone https://github.com/Fannovel16/comfyui_controlnet_aux.git \
        && cd comfyui_controlnet_aux && pip install -r requirements.txt 2>/dev/null || true) \
    ; cd /comfyui/custom_nodes \
    && (git clone https://github.com/wallish77/wlsh_nodes.git 2>/dev/null || true)

WORKDIR /

# ── Handler ──────────────────────────────────────────────────
RUN uv pip install runpod requests websocket-client

COPY handler.py start.sh ./
RUN chmod +x /start.sh

ENV RUNPOD_NETWORK_VOLUME_PATH="/runpod-volume"

CMD ["/start.sh"]
