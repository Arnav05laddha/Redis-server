#!/bin/bash
set -e

echo "=== Checking current redis-benchmark ==="
redis-benchmark --help 2>&1 | head -1 || true
redis-cli --version

echo ""
echo "=== Adding official Redis 7.x repository ==="
DISTRO=$(lsb_release -cs 2>/dev/null || echo "focal")
echo "Detected distro: $DISTRO"

curl -fsSL https://packages.redis.io/gpg \
    | sudo gpg --batch --yes --dearmor \
    -o /usr/share/keyrings/redis-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] \
https://packages.redis.io/deb ${DISTRO} main" \
    | sudo tee /etc/apt/sources.list.d/redis.list > /dev/null

echo ""
echo "=== Updating package list ==="
sudo apt-get update -qq

echo ""
echo "=== Installing redis-tools (includes redis-cli + redis-benchmark) ==="
sudo apt-get install -y redis-tools

echo ""
echo "=== Installed versions ==="
redis-cli --version
redis-benchmark --help 2>&1 | head -3
echo "Done!"
