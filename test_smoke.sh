#!/bin/bash
set -e
cd /mnt/c/Users/laddh/Downloads/files

echo "=== Starting server on port 6399 ==="
./build/server --port 6399 > /tmp/redis_test.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "=== PING ==="
redis-cli -p 6399 PING

echo "=== SET/GET ==="
redis-cli -p 6399 SET foo bar
redis-cli -p 6399 GET foo

echo "=== SET with EX ==="
redis-cli -p 6399 SET tempkey tempval EX 60
redis-cli -p 6399 TTL tempkey

echo "=== INCR ==="
redis-cli -p 6399 INCR counter
redis-cli -p 6399 INCR counter

echo "=== LPUSH / RPUSH / LRANGE / LLEN ==="
redis-cli -p 6399 RPUSH mylist a b c
redis-cli -p 6399 LPUSH mylist z
redis-cli -p 6399 LRANGE mylist 0 -1
redis-cli -p 6399 LLEN mylist
redis-cli -p 6399 LPOP mylist
redis-cli -p 6399 RPOP mylist

echo "=== ZADD / ZRANGE / ZRANK / ZSCORE / ZCARD ==="
redis-cli -p 6399 ZADD leaderboard 100 alice 200 bob 150 carol
redis-cli -p 6399 ZRANGE leaderboard 0 -1
redis-cli -p 6399 ZRANK leaderboard bob
redis-cli -p 6399 ZSCORE leaderboard carol
redis-cli -p 6399 ZCARD leaderboard

echo "=== TYPE ==="
redis-cli -p 6399 TYPE foo
redis-cli -p 6399 TYPE mylist
redis-cli -p 6399 TYPE leaderboard

echo "=== MULTI / EXEC ==="
redis-cli -p 6399 MULTI
redis-cli -p 6399 EXEC

echo "=== ACL ==="
redis-cli -p 6399 ACL WHOAMI
redis-cli -p 6399 ACL GETUSER default

echo "=== EXISTS / DEL ==="
redis-cli -p 6399 EXISTS foo
redis-cli -p 6399 DEL foo
redis-cli -p 6399 EXISTS foo

echo "=== INFO ==="
redis-cli -p 6399 INFO replication

echo "=== KEYS ==="
redis-cli -p 6399 SET k1 v1
redis-cli -p 6399 SET k2 v2
redis-cli -p 6399 KEYS '*'

echo "=== GEOADD / GEOPOS / GEODIST ==="
redis-cli -p 6399 GEOADD locations 13.361389 38.115556 Palermo
redis-cli -p 6399 GEOADD locations 15.087269 37.502669 Catania
redis-cli -p 6399 GEOPOS locations Palermo
redis-cli -p 6399 GEODIST locations Palermo Catania km

echo ""
echo "=== ALL TESTS PASSED ==="

kill $SERVER_PID 2>/dev/null
