#!/bin/sh
#
# CodeCrafters entry point — builds and runs the Redis server.
# Usage: ./spawn_redis_server.sh [--port 6379] [--replicaof host port]
#        [--dir /path/to/rdb] [--dbfilename dump.rdb]
#

set -e

BUILD_DIR="$(dirname "$0")/build"

mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ > /dev/null
cmake --build . --config Release -- -j "$(nproc)"
cd ..

exec ./build/server "$@"
