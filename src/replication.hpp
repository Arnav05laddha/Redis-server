/**
 * replication.hpp
 * 
 * Manages the Redis replication protocol (master-replica).
 * 
 * Features:
 * - Tracking replica connections.
 * - Propagating write commands from master to replicas.
 * - Managing replication offsets (for WAIT command synchronization).
 * - Empty RDB snapshot generation for initial full resync.
 */
#pragma once
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>
#include <random>
#include <sys/socket.h>

enum class Role { Master, Replica };

/**
 * ReplicaConn
 * Represents a connected replica node.
 * Stores its socket FD and how many bytes of the replication stream it has acknowledged.
 */
struct ReplicaConn {
    int fd;
    std::atomic<long long> offset{0};
    bool handshake_done = false;
    ReplicaConn() : fd(-1) {}
};

/**
 * ReplConfig
 * Global state managing whether this server is a Master or Replica,
 * its randomly generated replication ID, current offset, and the list of
 * attached replicas.
 */
struct ReplConfig {
    Role role = Role::Master;
    std::string master_host;
    int master_port = 0;
    std::string repl_id;
    std::atomic<long long> repl_offset{0};
    int master_fd = -1;

    std::mutex replicas_mu;
    std::vector<std::shared_ptr<ReplicaConn>> replica_list;

    ReplConfig() {
        static const char hex[] = "0123456789abcdef";
        std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> dist(0, 15);
        repl_id.resize(40);
        for (auto& c : repl_id) c = hex[dist(rng)];
    }

    bool is_master() const { return role == Role::Master; }

    void add_replica(int fd) {
        std::lock_guard<std::mutex> lk(replicas_mu);
        auto rc = std::make_shared<ReplicaConn>();
        rc->fd = fd;
        replica_list.push_back(rc);
    }

    std::shared_ptr<ReplicaConn> find_replica(int fd) {
        for (auto& rc : replica_list)
            if (rc->fd == fd) return rc;
        return nullptr;
    }

    /**
     * propagate
     * Sends a raw RESP-encoded command to all connected replicas that have
     * finished the initial handshake. Thread-safe.
     */
    void propagate(const std::string& raw) {
        std::lock_guard<std::mutex> lk(replicas_mu);
        for (auto& rc : replica_list) {
            if (rc->handshake_done) {
                send(rc->fd, raw.data(), raw.size(), MSG_NOSIGNAL);
                rc->offset += (long long)raw.size();
            }
        }
        repl_offset += (long long)raw.size();
    }

    int ack_count(long long offset) {
        std::lock_guard<std::mutex> lk(replicas_mu);
        int cnt = 0;
        for (auto& rc : replica_list)
            if (rc->offset >= offset) ++cnt;
        return cnt;
    }

    size_t replica_count() {
        std::lock_guard<std::mutex> lk(replicas_mu);
        return replica_list.size();
    }
};

/**
 * empty_rdb_bytes
 * Returns a hardcoded, valid Redis RDB file containing zero keys.
 * This is sent during the PSYNC full-resynchronization phase.
 */
inline std::string empty_rdb_bytes() {
    static const uint8_t rdb[] = {
        0x52,0x45,0x44,0x49,0x53,0x30,0x30,0x31,0x31,
        0xfa,0x09,0x72,0x65,0x64,0x69,0x73,0x2d,0x76,
        0x65,0x72,0x05,0x37,0x2e,0x32,0x2e,0x30,
        0xfa,0x0a,0x72,0x65,0x64,0x69,0x73,0x2d,0x62,
        0x69,0x74,0x73,0xc0,0x40,
        0xff,
        0x89,0x33,0x1a,0x14,0xce,0xa3,0x4c,0x43
    };
    return std::string((const char*)rdb, sizeof(rdb));
}
