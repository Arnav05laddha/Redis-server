# C++ Redis Implementation

A complete, high-performance C++ implementation of a Redis server from scratch. This project was built to successfully complete all 115 stages of the [CodeCrafters "Build Your Own Redis"](https://app.codecrafters.io/courses/redis/overview) challenge.

## What is Redis?

[Redis](https://redis.io/) (Remote Dictionary Server) is an open-source, in-memory data structure store. It can be used as a database, cache, and message broker. Unlike traditional databases that store data on disk, Redis keeps all data in main memory, which allows for exceptionally fast, sub-millisecond response times. It supports various complex data structures such as strings, lists, sets, sorted sets, streams, and geospatial indexes.

## How It Works

At its core, this custom Redis implementation operates as a TCP server that communicates with clients using the standard **RESP (REdis Serialization Protocol)**. 

When a client connects and sends a command:
1. **Connection Handling:** The server accepts the TCP connection and handles the client concurrently.
2. **Protocol Parsing:** The raw bytes are parsed using a custom RESP parser to extract the command and its arguments.
3. **Command Execution:** The command is routed to the appropriate handler, which interacts with the **In-Memory Store**. The store uses fine-grained locking (`std::mutex`) to ensure thread safety across concurrent operations.
4. **Persistence & Replication:** If configured, write operations are appended to an AOF (Append-Only File) or synced to connected replica servers. Data can also be periodically saved or loaded from disk using RDB snapshots.
5. **Response:** The result is serialized back into RESP format and sent to the client.

---

## Supported Features

This implementation covers a vast surface area of the Redis API, fully completing 115 distinct development stages:

### Basics & Core Data Types
- **Strings & Keys:** `SET`, `GET`, `PING`, `ECHO`, `INCR`, along with key expiry (`EX`, `PX`, `EXAT`, `PXAT`).
- **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, and blocking operations like `BLPOP`.
- **Streams:** `XADD`, `XRANGE`, `XREAD`, and `XREAD BLOCK` (with timeouts).
- **Sorted Sets:** `ZADD`, `ZRANGE`, `ZRANK`, `ZCARD`, `ZSCORE`, `ZREM`.
- **Geospatial:** `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH` (encoding coordinates using Geohash52).

### Advanced Mechanics
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`, and optimistic locking via `WATCH`/`UNWATCH`.
- **Pub/Sub:** `SUBSCRIBE`, `PUBLISH`, and `UNSUBSCRIBE` for real-time messaging channels.
- **Authentication:** `AUTH`, `ACL WHOAMI`, `ACL GETUSER`, and `--requirepass` enforcement.

### Persistence & High Availability
- **Replication:** Master/Replica handshakes, `INFO replication`, RDB transfer, command propagation, and `WAIT`.
- **RDB Persistence:** Parsing and loading RDB files on startup, reading key expiry and various data types.
- **AOF Persistence:** Writing commands to an Append-Only file, filtering read commands, and replaying the AOF on startup.

---

## Architecture & Design Decisions

### Thread Safety & Concurrency
- **`std::mutex`**: Protects all core store operations to prevent race conditions.
- **`std::condition_variable`**: Enables efficient blocking for commands like `BLPOP` and `XREAD BLOCK` without busy-waiting.
- **Sub-Registries**: Isolated mutexes for `WatchRegistry` (transactions), `PubSubRegistry` (messaging), and `AOFWriter` (disk I/O) to minimize lock contention.

### Component Structure
- **`Store`**: The pure data layer containing the dictionaries for all supported data types.
- **`CommandHandler`**: Stateless command dispatcher that receives a `ClientState` reference.
- **`ClientState`**: Per-connection state (e.g., transaction queue, watched keys, auth status) allocated on the stack to ensure clean and trivial resource cleanup.
- **`resp.hpp`**: Dedicated RESP protocol parser and serializer.

---

## Building and Running

### Build Instructions

This project uses CMake. To build the server:

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -- -j$(nproc)
```

### Running the Server

You can run the server in various modes depending on your needs:

```bash
# Standard Master (default port 6379)
./build/server

# With RDB Persistence (loads data from dump.rdb)
./build/server --dir /path/to/rdb --dbfilename dump.rdb

# With AOF Persistence
./build/server --appendonly yes --appenddirname /var/lib/redis --appendfilename appendonly.aof

# With Authentication Enforced
./build/server --requirepass mysecurepassword

# As a Replica (syncs from a master instance)
./build/server --port 6380 --replicaof localhost 6379
```

*(If testing through CodeCrafters, you can also use `./spawn_redis_server.sh` as the entry point.)*
