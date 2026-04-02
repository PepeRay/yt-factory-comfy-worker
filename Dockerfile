# ============================================================
# YouTube Factory — ComfyUI Serverless Worker
# Base: blib-la/runpod-worker-comfy (v5.8.5)
# Custom: Audio (VibeVoice, Whisper, RVC) + Video (LTX-Video)
# ============================================================

ARG BASE_IMAGE=nvidia/cuda:12.6.3-cudnn-runtime-ubuntu24.04

# ── Stage 1: Base ────────────────────────────────────────────
FROM ${BASE_IMAGE} AS base

ARG COMFYUI_VERSION=latest

ENV DEBIAN_FRONTEND=noninteractive
ENV PIP_PREFER_BINARY=1
ENV PYTHONUNBUFFERED=1
ENV CMAKE_BUILD_PARALLEL_LEVEL=8

# System dependencies (includes ffmpeg for video assembly)
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
    openssh-server \
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

# Install comfy-cli
RUN uv pip install comfy-cli pip setuptools wheel

# Install ComfyUI
RUN /usr/bin/yes | comfy --workspace /comfyui install --version "${COMFYUI_VERSION}" --nvidia

WORKDIR /comfyui

# Install ComfyUI's own requirements (alembic, comfy_aimdo, etc.)
RUN pip install -r requirements.txt

# ── Model paths: Network Volume ─────────────────────────────
# This tells ComfyUI to also look for models in /runpod-volume
COPY extra_model_paths.yaml ./

# ── Custom Nodes ─────────────────────────────────────────────
# Install custom nodes using comfy-cli from the Comfy Registry
# These match the nodes on your Network Volume

# Core pipeline nodes
RUN comfy node install comfyui-gguf || pip install -e custom_nodes/ComfyUI-GGUF 2>/dev/null || true
RUN comfy node install comfyui-ltxvideo || pip install -e custom_nodes/ComfyUI-LTXVideo 2>/dev/null || true
RUN comfy node install comfyui-videohelpersuite || true
RUN comfy node install comfyui-detail-daemon || true
RUN comfy node install comfyui-kjnodes || true

# Audio nodes
RUN comfy node install comfyui-whisper || true

# TTP Toolset (first/middle/last frame control for LTX loops)
RUN comfy node install comfyui-ttp-toolset || true

# Utility nodes
RUN comfy node install comfyui-essentials || true
RUN comfy node install rgthree-comfy || true
RUN comfy node install comfymath || true
RUN comfy node install comfyui-impact-pack || true
RUN comfy node install comfyui-custom-scripts || true

# VibeVoice + TTS-Audio-Suite — not in registry, install from git
RUN cd custom_nodes && \
    git clone https://github.com/DualOrion/VibeVoice-ComfyUI.git && \
    cd VibeVoice-ComfyUI && pip install -r requirements.txt 2>/dev/null || true

RUN cd custom_nodes && \
    git clone https://github.com/DualOrion/TTS-Audio-Suite.git && \
    cd TTS-Audio-Suite && pip install -r requirements.txt 2>/dev/null || true

# RES4LYF (advanced samplers for LTX)
RUN cd custom_nodes && \
    git clone https://github.com/ClownsharkBatwing/RES4LYF.git && \
    cd RES4LYF && pip install -r requirements.txt 2>/dev/null || true

# VRGDG nodes (video assembly)
RUN cd custom_nodes && \
    git clone https://github.com/vrgamedevgirl/comfyui-vrgamedevgirl.git 2>/dev/null || true

# Layer Style
RUN cd custom_nodes && \
    git clone https://github.com/chflame163/ComfyUI_LayerStyle.git && \
    cd ComfyUI_LayerStyle && pip install -r requirements.txt 2>/dev/null || true

# ControlNet aux
RUN cd custom_nodes && \
    git clone https://github.com/Fannovel16/comfyui_controlnet_aux.git && \
    cd comfyui_controlnet_aux && pip install -r requirements.txt 2>/dev/null || true

# WLSH nodes
RUN cd custom_nodes && \
    git clone https://github.com/wallish77/wlsh_nodes.git 2>/dev/null || true

WORKDIR /

# ── Handler ──────────────────────────────────────────────────
# RunPod serverless handler + dependencies
RUN uv pip install runpod requests websocket-client boto3

# Our custom handler that supports images, audio, AND video
COPY handler.py ./
COPY start.sh ./
RUN chmod +x /start.sh

# ── Environment defaults ─────────────────────────────────────
# S3 config for reading/writing to Network Volume via S3 API
ENV RUNPOD_NETWORK_VOLUME_PATH="/runpod-volume"

CMD ["/start.sh"]
