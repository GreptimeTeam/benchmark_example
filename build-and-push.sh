#!/usr/bin/env bash

set -x

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if podman or docker is installed
if command_exists podman; then
    echo "Using podman for container operations"
    CONTAINER_CMD="podman"
elif command_exists docker; then
    echo "Using docker for container operations"
    CONTAINER_CMD="docker"
else
    echo "Error: Neither podman nor docker is installed"
    exit 1
fi

# Use environment variable IMAGE_TAG if set, otherwise use default value
IMAGE_TAG=${IMAGE_TAG:-"v0.1.4"}

# Check if -y flag is provided
if [[ "$1" != "-y" ]]; then
    # Ask for user confirmation before proceeding
    read -p "Do you want to continue with building and pushing the image with tag: ${IMAGE_TAG}? (y/n): " confirm
    if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
        echo "Operation cancelled by user."
        exit 1
    fi
fi

$CONTAINER_CMD build -f ./Dockerfile -t greptime-registry.cn-hangzhou.cr.aliyuncs.com/tools/benchmark_example:${IMAGE_TAG} .

$CONTAINER_CMD push greptime-registry.cn-hangzhou.cr.aliyuncs.com/tools/benchmark_example:${IMAGE_TAG}
