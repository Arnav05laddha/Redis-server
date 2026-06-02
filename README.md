# Build Your Own Redis — C++ Solution (All 115 Stages)

A complete C++ implementation of the [CodeCrafters "Build Your Own Redis"](https://app.codecrafters.io/courses/redis/overview)
challenge, covering all 115 stages.

## Stages Covered

### Basics (7 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 1 | Bind to a port, accept connections | ✅ |
| 2 | `PING` | ✅ |
| 3 | Respond to multiple PINGs | ✅ |
| 4 | Handle concurrent clients | ✅ |
| 5 | `ECHO` | ✅ |
| 6 | `SET` / `GET` | ✅ |
| 7 | Key expiry (`EX`, `PX`, `EXAT`, `PXAT`) | ✅ |

### Lists (11 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 8-9 | `RPUSH` (one & multiple elements) | ✅ |
| 10 | Append multiple elements | ✅ |
| 11-12 | `LRANGE` (positive & negative indexes) | ✅ |
| 13 | `LPUSH` | ✅ |
| 14 | `LLEN` | ✅ |
| 15-16 | `LPOP` / `RPOP` (single & multiple) | ✅ |
| 17-18 | `BLPOP` (blocking + timeout) | ✅ |

### Streams (10 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 19 | `TYPE` | ✅ |
| 20-23 | `XADD` (full, partial, auto IDs) | ✅ |
| 24-26 | `XRANGE` (with `-`/`+`) | ✅ |
| 27-28 | `XREAD` (single & multi stream) | ✅ |
| 29-31 | `XREAD BLOCK` (with timeout, forever, `$`) | ✅ |

### INCR (3 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 32-34 | `INCR` (new key, existing, non-integer) | ✅ |

### Transactions (8 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 35 | `MULTI` | ✅ |
| 36 | `EXEC` | ✅ |
| 37 | Empty transaction | ✅ |
| 38 | Queueing commands | ✅ |
| 39 | Executing a transaction | ✅ |
| 40 | `DISCARD` | ✅ |
| 41 | Failures within transactions | ✅ |
| 42 | Multiple transactions (per-connection) | ✅ |

### WATCH (8 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 43-44 | `WATCH` (one key, inside transaction) | ✅ |
| 45-46 | Tracking modifications, multiple keys | ✅ |
| 47 | Watching missing keys | ✅ |
| 48 | `UNWATCH` | ✅ |
| 49-50 | Unwatch on EXEC / DISCARD | ✅ |

### Replication (11 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 51 | Configure listening port | ✅ |
| 52-53 | `INFO replication` (master & replica) | ✅ |
| 54 | Initial replication ID and offset | ✅ |
| 55-57 | Send handshake (3 stages) | ✅ |
| 58-59 | Receive handshake (2 stages) | ✅ |
| 60 | Empty RDB transfer | ✅ |
| 61-62 | Single/multi-replica propagation | ✅ |
| 63-66 | `WAIT` (ACKs, no replicas, no cmds, with cmds) | ✅ |

### RDB Persistence (5 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 67 | RDB file config (`CONFIG GET dir/dbfilename`) | ✅ |
| 68-71 | Read key, string, multiple keys/values, expiry | ✅ |

### AOF Persistence (10 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 72-73 | Default AOF options, options from flags | ✅ |
| 74-76 | Create AOF dir, file, manifest | ✅ |
| 77-78 | Write single/multiple commands | ✅ |
| 79 | Filter write commands | ✅ |
| 80-81 | Replay single/multiple commands | ✅ |

### Pub/Sub (7 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 82-83 | `SUBSCRIBE` (one & multiple channels) | ✅ |
| 84 | Enter subscribed mode | ✅ |
| 85 | `PING` in subscribed mode | ✅ |
| 86 | `PUBLISH` | ✅ |
| 87 | Deliver messages | ✅ |
| 88 | `UNSUBSCRIBE` | ✅ |

### Sorted Sets (7 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 89-90 | `ZADD` (create set, add members) | ✅ |
| 91 | `ZRANK` | ✅ |
| 92-93 | `ZRANGE` (positive & negative indexes) | ✅ |
| 94 | `ZCARD` | ✅ |
| 95 | `ZSCORE` | ✅ |
| 96 | `ZREM` | ✅ |

### Geospatial (8 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 97-98 | `GEOADD` (accept & validate coordinates) | ✅ |
| 99-100 | Store location (Geohash52 encoding) | ✅ |
| 101-102 | `GEOPOS` (decode coordinates) | ✅ |
| 103 | `GEODIST` (Haversine formula) | ✅ |
| 104 | `GEOSEARCH` (within radius) | ✅ |

### Authentication (8 stages)
| Stage | Feature | Status |
|-------|---------|--------|
| 105-106 | `ACL WHOAMI`, `ACL GETUSER` | ✅ |
| 107-108 | `nopass` flag, `passwords` property | ✅ |
| 109 | Setting default user password (`--requirepass`) | ✅ |
| 110 | `AUTH` command | ✅ |
| 111-112 | Enforce authentication, authenticate via AUTH | ✅ |

## Project Structure

```
.
├── CMakeLists.txt
├── spawn_redis_server.sh   ← CodeCrafters entry point
├── resp.hpp                ← RESP protocol parser & serializer
├── store.hpp               ← In-memory KV store (strings, lists, streams, zsets, geo)
├── rdb.hpp                 ← RDB v9 file loader
├── replication.hpp         ← Master/replica state management
├── pubsub.hpp              ← Pub/Sub channel registry
├── aof.hpp                 ← AOF persistence (write + replay)
├── commands.hpp            ← All Redis command handlers + per-connection state
└── server.cpp              ← TCP server + main entry point
```

## Key Design Decisions

### Thread Safety
- **`std::mutex`** protects all Store operations (get/set/list/zset/stream/geo)
- **`std::condition_variable`** enables blocking BLPOP and XREAD BLOCK without busy-waiting
- **`WatchRegistry`** uses its own mutex for concurrent WATCH/write tracking
- **`PubSubRegistry`** is fully mutex-protected for concurrent subscribe/publish/unsubscribe
- **`AOFWriter`** serializes disk writes with a mutex

### OOP Architecture
- `Store` — pure data layer with all data types
- `CommandHandler` — stateless command dispatcher (takes `ClientState&` per call)
- `ClientState` — per-connection state: `TxState` (MULTI queue), watched keys, subscribed mode, auth
- `PubSubRegistry` — global pub/sub hub
- `AOFWriter` / `AOFReplayer` — persistence layer

### Per-Connection State
Each client thread gets a `ClientState` struct on the stack, avoiding any global or heap-allocated per-client state. This keeps cleanup trivial.

## Building

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -- -j$(nproc)
```

## Running

```bash
# Master (default port 6379)
./build/server

# With RDB persistence
./build/server --dir /path/to/rdb --dbfilename dump.rdb

# With AOF persistence
./build/server --appendonly yes --appenddirname /var/lib/redis --appendfilename appendonly.aof

# With authentication
./build/server --requirepass mypassword

# As replica
./build/server --port 6380 --replicaof localhost 6379
```

## CodeCrafters Entry Point

```bash
./spawn_redis_server.sh [--port N] [--replicaof host port] \
                        [--dir /path] [--dbfilename dump.rdb] \
                        [--appendonly yes] [--requirepass pass]
```
