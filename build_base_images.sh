#!/bin/bash
# === Build Base Images for YouTube Factory ===
# Run this on a RunPod GPU pod (or any machine with Docker + fast internet)
#
# Prerequisites:
#   1. Docker installed and running
#   2. Logged into ghcr.io: echo $GITHUB_TOKEN | docker login ghcr.io -u PepeRay --password-stdin
#   3. Enough disk space (~120GB free for all 3 images)
#
# Usage:
#   ./build_base_images.sh [audio|images|video|all]
#
# Each image is built and pushed independently.
# If a build fails, fix the issue and re-run just that one.

set -e

REGISTRY="ghcr.io/peperay"
VERSION="v1"

build_audio() {
    echo "=========================================="
    echo "Building Audio base image (~36GB, ~20 min)"
    echo "=========================================="
    docker build \
        -f Dockerfile.base.audio \
        -t "${REGISTRY}/yt-factory-audio-base:${VERSION}" \
        -t "${REGISTRY}/yt-factory-audio-base:latest" \
        --progress=plain \
        .
    echo "Pushing Audio base image..."
    docker push "${REGISTRY}/yt-factory-audio-base:${VERSION}"
    docker push "${REGISTRY}/yt-factory-audio-base:latest"
    echo "=== Audio base DONE ==="
}

build_images() {
    echo "============================================"
    echo "Building Images base image (~44GB, ~25 min)"
    echo "============================================"
    docker build \
        -f Dockerfile.base.images \
        -t "${REGISTRY}/yt-factory-images-base:${VERSION}" \
        -t "${REGISTRY}/yt-factory-images-base:latest" \
        --progress=plain \
        .
    echo "Pushing Images base image..."
    docker push "${REGISTRY}/yt-factory-images-base:${VERSION}"
    docker push "${REGISTRY}/yt-factory-images-base:latest"
    echo "=== Images base DONE ==="
}

build_video() {
    echo "============================================"
    echo "Building Video base image (~60GB, ~35 min)"
    echo "============================================"
    docker build \
        -f Dockerfile.base.video \
        -t "${REGISTRY}/yt-factory-video-base:${VERSION}" \
        -t "${REGISTRY}/yt-factory-video-base:latest" \
        --progress=plain \
        .
    echo "Pushing Video base image..."
    docker push "${REGISTRY}/yt-factory-video-base:${VERSION}"
    docker push "${REGISTRY}/yt-factory-video-base:latest"
    echo "=== Video base DONE ==="
}

TARGET="${1:-all}"

case "$TARGET" in
    audio)  build_audio ;;
    images) build_images ;;
    video)  build_video ;;
    all)
        build_audio
        build_images
        build_video
        echo ""
        echo "==============================="
        echo "ALL 3 BASE IMAGES BUILT & PUSHED"
        echo "==============================="
        echo "Audio:  ${REGISTRY}/yt-factory-audio-base:${VERSION}"
        echo "Images: ${REGISTRY}/yt-factory-images-base:${VERSION}"
        echo "Video:  ${REGISTRY}/yt-factory-video-base:${VERSION}"
        ;;
    *)
        echo "Usage: $0 [audio|images|video|all]"
        exit 1
        ;;
esac
