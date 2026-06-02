#!/bin/bash
# ============================================================
#  Redis Benchmark Suite (using official redis-benchmark)
#  Runs inside WSL natively for maximum I/O performance
# ============================================================

PORT=6399
HOST=127.0.0.1
CLIENTS=100
REQUESTS=100000
PIPELINE=10

echo "============================================================"
echo "  CUSTOM REDIS SERVER BENCHMARK (WSL NATIVE)"
echo "  Port: $PORT | Clients: $CLIENTS | Requests: $REQUESTS | Pipeline: $PIPELINE"
echo "============================================================"
echo ""

# Start server fresh
pkill -f "build/server --port $PORT" 2>/dev/null
sleep 0.3
cd /mnt/c/Users/laddh/Downloads/files
./build/server --port $PORT > /tmp/bench_server.log 2>&1 &
SERVER_PID=$!
sleep 1

if ! redis-cli -p $PORT PING > /dev/null 2>&1; then
    echo "ERROR: Server failed to start"
    cat /tmp/bench_server.log
    exit 1
fi

echo "✅ Server started (PID=$SERVER_PID)"
echo ""

# ── Run benchmark with specific tests ────────────────────────────────────────
run_bench() {
    local label="$1"
    local tests="$2"
    local extra_args="${3:-}"
    echo "──────────────────────────────────────────────────────────"
    echo "  $label"
    echo "──────────────────────────────────────────────────────────"
    redis-benchmark -p $PORT -q \
        -c $CLIENTS \
        -n $REQUESTS \
        -P $PIPELINE \
        -t "$tests" \
        $extra_args
    echo ""
}

run_bench "1. PING, SET, GET" "ping,set,get"
run_bench "2. Atomic Counters (INCR)" "incr"
run_bench "3. Lists (LPUSH, LPOP, RPUSH, RPOP)" "lpush,lpop,rpush,rpop"
run_bench "4. Sets (SADD, SPOP)" "sadd,spop"
run_bench "5. Sorted Sets (ZADD)" "zadd"
run_bench "6. Hashes (HSET)" "hset"

echo "──────────────────────────────────────────────────────────"
echo "  7. High Pipelining (P=50) for SET/GET"
echo "──────────────────────────────────────────────────────────"
redis-benchmark -p $PORT -q -c 50 -n $REQUESTS -P 50 -t set,get
echo ""

# Kill server
kill $SERVER_PID 2>/dev/null
echo "Server stopped."
echo "Benchmark finished."
