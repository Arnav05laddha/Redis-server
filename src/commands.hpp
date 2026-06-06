/**
 * commands.hpp
 * 
 * Central command execution and routing logic.
 * 
 * This file contains the `CommandHandler` class, which takes parsed RESP arguments
 * and routes them to the appropriate handler function (e.g., `handle_set`, `handle_get`).
 * It also manages:
 * - Client authentication state (AUTH)
 * - Transaction queuing (MULTI / EXEC / DISCARD)
 * - Subscribed mode locking (SUBSCRIBE / UNSUBSCRIBE)
 */
#pragma once
#include "resp.hpp"
#include "store.hpp"
#include "replication.hpp"
#include "pubsub.hpp"

#include <string>
#include <vector>
#include <deque>
#include <unordered_set>
#include <algorithm>
#include <cctype>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <cmath>
#include <sys/socket.h>
#include <memory>

// ─── Utility ──────────────────────────────────────────────────────────────────
inline std::string to_upper(std::string s) {
    for (auto& c : s) c = (char)toupper((unsigned char)c);
    return s;
}

// ─── Server Config ────────────────────────────────────────────────────────────
struct ServerConfig {
    std::string dir;
    std::string dbfilename;
    int         port        = 6379;
    // AOF
    std::string appendonly     = "no";  // "yes"/"no"
    std::string appenddirname;
    std::string appendfilename = "appendonly.aof";
    // Auth
    std::string requirepass;
};

// ─── Per-connection Transaction State ────────────────────────────────────────
/**
 * TxState
 * 
 * Holds the transaction state for a single client connection.
 * When `in_multi` is true, commands are pushed to `queue` instead of being
 * executed immediately. They are executed sequentially when EXEC is called.
 */
struct TxState {
    bool in_multi    = false;
    bool tx_error    = false; // syntax error inside MULTI
    std::deque<std::vector<std::string>> queue;

    void reset() { in_multi = false; tx_error = false; queue.clear(); }
};

// ─── Per-connection Client State ──────────────────────────────────────────────
/**
 * ClientState
 * 
 * Maintains the session state for a client connection, persisting across commands.
 * - `tx`: The transaction queue.
 * - `watched_keys`: Keys monitored for optimistic concurrency (WATCH).
 * - `subscribed_mode`: If true, only Pub/Sub commands and PING are allowed.
 * - `authenticated`: Must be true to run standard commands (if a password is required).
 */
struct ClientState {
    TxState  tx;
    std::unordered_set<std::string> watched_keys;
    bool     subscribed_mode = false; // true → only SUBSCRIBE/UNSUBSCRIBE/PING allowed
    bool     authenticated   = false; // true if auth passed (or no password set)

    void reset_tx() {
        tx.reset();
        watched_keys.clear();
    }
};

// ─── Command Handler ──────────────────────────────────────────────────────────
/**
 * CommandHandler
 * 
 * The main dispatch router. It inspects the first argument (the command name),
 * checks client state (Auth, Subscribed, Multi), and delegates to specific private methods.
 */
class CommandHandler {
public:
    Store&        store;
    ReplConfig&   repl;
    ServerConfig& cfg;
    PubSubRegistry& pubsub;

    CommandHandler(Store& s, ReplConfig& r, ServerConfig& c, PubSubRegistry& ps)
        : store(s), repl(r), cfg(c), pubsub(ps) {}

    /**
     * handle
     * Main dispatch function.
     * @param args The tokenized command arguments.
     * @param client_fd The client socket descriptor.
     * @param should_propagate (Out) Set to true if the command mutated data (e.g. SET)
     *                         so that server.cpp knows to write it to AOF/Replicas.
     * @param cs The state of the client executing the command.
     */
    RespValue handle(const std::vector<std::string>& args, int client_fd,
                     bool& should_propagate, ClientState& cs) {
        should_propagate = false;
        if (args.empty()) return RespValue::error("ERR empty command");

        std::string cmd = to_upper(args[0]);

        // ── Authentication gate ───────────────────────────────────────────────
        if (!cfg.requirepass.empty() && !cs.authenticated) {
            if (cmd == "AUTH") return handle_auth(args, cs);
            // Allow HELLO, QUIT
            if (cmd != "QUIT" && cmd != "RESET")
                return RespValue::error("NOAUTH Authentication required.");
        }

        // ── Subscribed mode gate ──────────────────────────────────────────────
        if (cs.subscribed_mode) {
            if (cmd == "SUBSCRIBE")   return handle_subscribe(args, client_fd, cs);
            if (cmd == "UNSUBSCRIBE") return handle_unsubscribe(args, client_fd, cs);
            if (cmd == "PING") {
                std::string msg = (args.size() > 1) ? args[1] : "";
                std::vector<RespValue> rv = {
                    RespValue::bulk("pong"), RespValue::bulk(msg)
                };
                return RespValue::arr(rv);
            }
            return RespValue::error("ERR Command not allowed in subscribed state");
        }

        // ── Inside MULTI: queue commands ──────────────────────────────────────
        if (cs.tx.in_multi) {
            if (cmd == "MULTI")   return RespValue::error("ERR MULTI calls can not be nested");
            if (cmd == "EXEC")    return handle_exec(args, client_fd, should_propagate, cs);
            if (cmd == "DISCARD") return handle_discard(cs);
            // Queue the command
            cs.tx.queue.push_back(args);
            return RespValue::simple("QUEUED");
        }

        // ── Normal command dispatch ───────────────────────────────────────────
        if (cmd == "PING")      return handle_ping(args);
        if (cmd == "ECHO")      return handle_echo(args);
        

        if (cmd == "SET")  { should_propagate = true; return handle_set(args, cs); }
        if (cmd == "GET")       return handle_get(args);
        if (cmd == "DEL")  { should_propagate = true; return handle_del(args, cs); }
        if (cmd == "EXISTS")    return handle_exists(args);
        if (cmd == "INCR") { should_propagate = true; return handle_incr(args, cs); }
        if (cmd == "DECR") { should_propagate = true; return handle_decr(args, cs); }
        if (cmd == "MSET") { should_propagate = true; return handle_mset(args, cs); }
        if (cmd == "MGET")      return handle_mget(args);
        if (cmd == "APPEND") { should_propagate = true; return handle_append(args, cs); }
        if (cmd == "GETSET") { should_propagate = true; return handle_getset(args, cs); }

        if (cmd == "KEYS")      return handle_keys(args);
        if (cmd == "TYPE")      return handle_type(args);
        if (cmd == "TTL")       return handle_ttl(args);
        if (cmd == "PTTL")      return handle_pttl(args);
        if (cmd == "EXPIRE")  { should_propagate = true; return handle_expire(args, false, cs); }
        if (cmd == "PEXPIRE") { should_propagate = true; return handle_expire(args, true, cs);  }

        // Lists
        if (cmd == "RPUSH")  { should_propagate = true; return handle_rpush(args, cs); }
        if (cmd == "LPUSH")  { should_propagate = true; return handle_lpush(args, cs); }
        if (cmd == "RPUSHX") { should_propagate = true; return handle_rpushx(args, cs); }
        if (cmd == "LPUSHX") { should_propagate = true; return handle_lpushx(args, cs); }
        if (cmd == "LRANGE")    return handle_lrange(args);
        if (cmd == "LLEN")      return handle_llen(args);
        if (cmd == "LPOP")   { should_propagate = true; return handle_lpop(args, cs);  }
        if (cmd == "RPOP")   { should_propagate = true; return handle_rpop(args, cs);  }
        if (cmd == "BLPOP")     return handle_blpop(args, client_fd);
        if (cmd == "BRPOP")     return handle_blpop(args, client_fd); // alias

        // Transactions
        if (cmd == "MULTI")   return handle_multi(cs);
        if (cmd == "EXEC")    return handle_exec(args, client_fd, should_propagate, cs);
        if (cmd == "DISCARD") return handle_discard(cs);

        // WATCH
        if (cmd == "WATCH")   return handle_watch(args, client_fd, cs);
        if (cmd == "UNWATCH") return handle_unwatch(client_fd, cs);

        // Sorted Sets
        if (cmd == "ZADD")  { should_propagate = true; return handle_zadd(args, cs); }
        if (cmd == "ZRANK")     return handle_zrank(args);
        if (cmd == "ZRANGE")    return handle_zrange(args);
        if (cmd == "ZCARD")     return handle_zcard(args);
        if (cmd == "ZSCORE")    return handle_zscore(args);
        if (cmd == "ZREM")   { should_propagate = true; return handle_zrem(args, cs); }

        // Geo
        if (cmd == "GEOADD")    { should_propagate = true; return handle_geoadd(args, cs); }
        if (cmd == "GEOPOS")    return handle_geopos(args);
        if (cmd == "GEODIST")   return handle_geodist(args);
        if (cmd == "GEOSEARCH") return handle_geosearch(args);

        // Streams
        if (cmd == "XADD")  { should_propagate = true; return handle_xadd(args, cs); }
        if (cmd == "XRANGE")  return handle_xrange(args);
        if (cmd == "XLEN")    return handle_xlen(args);
        if (cmd == "XREAD")   return handle_xread(args);

        // Pub/Sub
        if (cmd == "SUBSCRIBE")   return handle_subscribe(args, client_fd, cs);
        if (cmd == "UNSUBSCRIBE") return handle_unsubscribe(args, client_fd, cs);
        if (cmd == "PUBLISH")     return handle_publish(args);

        // Config / Info
        if (cmd == "CONFIG")    return handle_config(args);
        if (cmd == "INFO")      return handle_info(args);
        if (cmd == "REPLCONF")  return handle_replconf(args, client_fd);
        if (cmd == "PSYNC")     return handle_psync(args, client_fd);
        if (cmd == "WAIT")      return handle_wait(args);

        // Auth / ACL
        if (cmd == "AUTH")      return handle_auth(args, cs);
        if (cmd == "ACL")       return handle_acl(args);

        // Misc
        if (cmd == "RESET") { cs.reset_tx(); return RespValue::simple("RESET"); }
        if (cmd == "QUIT")  return RespValue::simple("OK");

        return RespValue::error("ERR unknown command '" + args[0] + "'");
    }

private:
    // ── PING ──────────────────────────────────────────────────────────────────
    /** Handler for the Redis PING command. Returns PONG or the provided message. */
    RespValue handle_ping(const std::vector<std::string>& args) {
        if (args.size() > 1) return RespValue::bulk(args[1]);
        return RespValue::simple("PONG");
    }

    // ── ECHO ──────────────────────────────────────────────────────────────────
    /** Handler for the Redis ECHO command. Returns the first argument exactly as provided. */
    RespValue handle_echo(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'echo'");
        return RespValue::bulk(args[1]);
    }

    // ── SET ───────────────────────────────────────────────────────────────────
    RespValue handle_set(const std::vector<std::string> &args, ClientState &cs)
    {
        if (args.size() < 3)
            return RespValue::error("ERR wrong number of arguments for 'set'");
        const std::string &key = args[1];
        const std::string &val = args[2];

        std::optional<long long> px_ttl;
        bool nx = false, xx = false;

        for (size_t i = 3; i < args.size(); ++i)
        {
            std::string opt = to_upper(args[i]);
            if ((opt == "EX") && i + 1 < args.size())
            {
                px_ttl = std::stoll(args[++i]) * 1000;
            }
            else if ((opt == "PX") && i + 1 < args.size())
            {
                px_ttl = std::stoll(args[++i]);
            }
            else if ((opt == "EXAT") && i + 1 < args.size())
            {
                long long ts = std::stoll(args[++i]);
                auto now_s = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
                px_ttl = (ts - now_s) * 1000;
            }
            else if ((opt == "PXAT") && i + 1 < args.size())
            {
                long long ts = std::stoll(args[++i]);
                auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count();
                px_ttl = ts - now_ms;
            }
            else if (opt == "NX")
            {
                nx = true;
            }
            else if (opt == "XX")
            {
                xx = true;
            }
        }

        if (nx && store.get(key).has_value())
            return RespValue::null_bulk();
        if (xx && !store.get(key).has_value())
            return RespValue::null_bulk();

        store.set(key, val, px_ttl);
        // Dirty watched key
        if (!cs.watched_keys.empty())
            store.watch_reg.notify_write(key);
        return RespValue::simple("OK");
    }

    // ── GET ───────────────────────────────────────────────────────────────────
    /** Handler for the Redis GET command. Returns the value of a string key. */
    RespValue handle_get(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'get'");
        auto val = store.get(args[1]);
        return val ? RespValue::bulk(*val) : RespValue::null_bulk();
    }

    // ── DEL ───────────────────────────────────────────────────────────────────
    /** Handler for the Redis DEL command. Removes the specified keys and returns the count. */
    RespValue handle_del(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'del'");
        int count = 0;
        for (size_t i = 1; i < args.size(); ++i)
            if (store.del(args[i])) ++count;
        return RespValue::make_int(count);
    }

    // ── EXISTS ────────────────────────────────────────────────────────────────
    /** Handler for the Redis EXISTS command. Returns the number of keys that exist. */
    RespValue handle_exists(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        int count = 0;
        for (size_t i = 1; i < args.size(); ++i)
            if (store.get(args[i]).has_value()) ++count;
        return RespValue::make_int(count);
    }

    // ── INCR / DECR ───────────────────────────────────────────────────────────
    /** Handler for the Redis INCR command. Increments integer value by 1. */
    RespValue handle_incr(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return do_incr(args[1], 1);
    }
    /** Handler for the Redis DECR command. Decrements integer value by 1. */
    RespValue handle_decr(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return do_incr(args[1], -1);
    }
    /** Shared helper for INCR/DECR operations. */
    RespValue do_incr(const std::string& key, long long delta) {
        auto val = store.get(key);
        long long n = 0;
        if (val.has_value()) {
            try { n = std::stoll(*val); }
            catch (...) { return RespValue::error("ERR value is not an integer or out of range"); }
        }
        n += delta;
        store.set(key, std::to_string(n));
        return RespValue::make_int(n);
    }

    // ── MSET / MGET ──────────────────────────────────────────────────────────
    /** Handler for the Redis MSET command. Sets multiple keys to multiple values. */
    RespValue handle_mset(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3 || (args.size() % 2) == 0)
            return RespValue::error("ERR wrong number of arguments for 'mset'");
        for (size_t i = 1; i + 1 < args.size(); i += 2)
            store.set(args[i], args[i+1]);
        return RespValue::simple("OK");
    }
    /** Handler for the Redis MGET command. Gets values of all specified keys. */
    RespValue handle_mget(const std::vector<std::string>& args) {
        std::vector<RespValue> rv;
        for (size_t i = 1; i < args.size(); ++i) {
            auto v = store.get(args[i]);
            rv.push_back(v ? RespValue::bulk(*v) : RespValue::null_bulk());
        }
        return RespValue::arr(rv);
    }

    // ── APPEND ────────────────────────────────────────────────────────────────
    /** Handler for the Redis APPEND command. Appends a string to the value of a key. */
    RespValue handle_append(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        auto cur = store.get(args[1]).value_or("");
        cur += args[2];
        store.set(args[1], cur);
        return RespValue::make_int((long long)cur.size());
    }

    // ── GETSET ────────────────────────────────────────────────────────────────
    /** Handler for the Redis GETSET command. Sets the key and returns its old value. */
    RespValue handle_getset(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        auto old = store.get(args[1]);
        store.set(args[1], args[2]);
        return old ? RespValue::bulk(*old) : RespValue::null_bulk();
    }

    // ── KEYS ──────────────────────────────────────────────────────────────────
    /** Handler for the Redis KEYS command. Returns all keys matching a pattern. */
    RespValue handle_keys(const std::vector<std::string>& args) {
        std::string pat = (args.size() > 1) ? args[1] : "*";
        auto ks = store.keys(pat);
        std::vector<RespValue> rv;
        for (auto& k : ks) rv.push_back(RespValue::bulk(k));
        return RespValue::arr(rv);
    }

    // ── TYPE ──────────────────────────────────────────────────────────────────
    /** Handler for the Redis TYPE command. Returns the string representation of the type. */
    RespValue handle_type(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return RespValue::simple(store.type_of(args[1]));
    }

    // ── TTL / PTTL ────────────────────────────────────────────────────────────
    /** Handler for the Redis TTL command. Returns the remaining time to live in seconds. */
    RespValue handle_ttl(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return RespValue::make_int(store.ttl(args[1]));
    }
    /** Handler for the Redis PTTL command. Returns the remaining time to live in ms. */
    RespValue handle_pttl(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return RespValue::make_int(store.pttl(args[1]));
    }

    // ── EXPIRE / PEXPIRE ──────────────────────────────────────────────────────
    /** Shared handler for EXPIRE and PEXPIRE commands to set a key's time to live. */
    RespValue handle_expire(const std::vector<std::string>& args, bool ms, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        auto cur = store.get(args[1]);
        if (!cur) return RespValue::make_int(0);
        long long ttl = std::stoll(args[2]);
        long long px  = ms ? ttl : ttl * 1000;
        store.set(args[1], *cur, px);
        return RespValue::make_int(1);
    }

    // ── LIST commands ─────────────────────────────────────────────────────────
    /** Handler for the Redis RPUSH command. Appends elements to the tail of a list. */
    RespValue handle_rpush(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments for 'rpush'");
        std::vector<std::string> vals(args.begin()+2, args.end());
        long long len = store.list_push(args[1], vals, 1);
        if (len < 0) return RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value");
        return RespValue::make_int(len);
    }
    /** Handler for the Redis LPUSH command. Prepends elements to the head of a list. */
    RespValue handle_lpush(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments for 'lpush'");
        // LPUSH pushes in reverse order (last arg ends up at head)
        std::vector<std::string> vals(args.begin()+2, args.end());
        long long len = store.list_push(args[1], vals, -1);
        if (len < 0) return RespValue::error("WRONGTYPE Operation against a key holding the wrong kind of value");
        return RespValue::make_int(len);
    }
    /** Handler for the Redis RPUSHX command. Appends elements only if list exists. */
    RespValue handle_rpushx(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        if (store.llen(args[1]) == 0) return RespValue::make_int(0);
        std::vector<std::string> vals(args.begin()+2, args.end());
        return RespValue::make_int(store.list_push(args[1], vals, 1));
    }
    /** Handler for the Redis LPUSHX command. Prepends elements only if list exists. */
    RespValue handle_lpushx(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        if (store.llen(args[1]) == 0) return RespValue::make_int(0);
        std::vector<std::string> vals(args.begin()+2, args.end());
        return RespValue::make_int(store.list_push(args[1], vals, -1));
    }
    /** Handler for the Redis LRANGE command. Returns elements within index range. */
    RespValue handle_lrange(const std::vector<std::string>& args) {
        if (args.size() < 4) return RespValue::error("ERR wrong number of arguments for 'lrange'");
        long long start = std::stoll(args[2]);
        long long stop  = std::stoll(args[3]);
        auto elems = store.lrange(args[1], start, stop);
        std::vector<RespValue> rv;
        for (auto& e : elems) rv.push_back(RespValue::bulk(e));
        return RespValue::arr(rv);
    }
    /** Handler for the Redis LLEN command. Returns the length of a list. */
    RespValue handle_llen(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return RespValue::make_int(store.llen(args[1]));
    }
    /** Handler for the Redis LPOP command. Removes and returns the first element(s) of a list. */
    RespValue handle_lpop(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'lpop'");
        long long count = (args.size() >= 3) ? std::stoll(args[2]) : 1;
        bool multi = (args.size() >= 3);
        auto vals = store.list_pop(args[1], count, 1);
        if (vals.empty()) return RespValue::null_bulk();
        if (!multi) return RespValue::bulk(vals[0]);
        std::vector<RespValue> rv;
        for (auto& v : vals) rv.push_back(RespValue::bulk(v));
        return RespValue::arr(rv);
    }
    /** Handler for the Redis RPOP command. Removes and returns the last element(s) of a list. */
    RespValue handle_rpop(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'rpop'");
        long long count = (args.size() >= 3) ? std::stoll(args[2]) : 1;
        bool multi = (args.size() >= 3);
        auto vals = store.list_pop(args[1], count, -1);
        if (vals.empty()) return RespValue::null_bulk();
        if (!multi) return RespValue::bulk(vals[0]);
        std::vector<RespValue> rv;
        for (auto& v : vals) rv.push_back(RespValue::bulk(v));
        return RespValue::arr(rv);
    }
    /** Handler for the Redis BLPOP/BRPOP command. Blocks until elements are available. */
    RespValue handle_blpop(const std::vector<std::string>& args, int /*fd*/) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments for 'blpop'");
        std::vector<std::string> keys(args.begin()+1, args.end()-1);
        double timeout_s = std::stod(args.back());
        long long timeout_ms = (long long)(timeout_s * 1000.0);

        auto result = store.blpop(keys, timeout_ms);
        if (!result) return RespValue::null_bulk();
        std::vector<RespValue> rv = {
            RespValue::bulk(result->first),
            RespValue::bulk(result->second)
        };
        return RespValue::arr(rv);
    }

    // ── MULTI / EXEC / DISCARD ────────────────────────────────────────────────
    /** Handler for the Redis MULTI command. Starts a transaction block. */
    RespValue handle_multi(ClientState& cs) {
        if (cs.tx.in_multi) return RespValue::error("ERR MULTI calls can not be nested");
        cs.tx.in_multi = true;
        return RespValue::simple("OK");
    }

    /** Handler for the Redis EXEC command. Executes all previously queued commands in a transaction. */
    RespValue handle_exec(const std::vector<std::string>&, int client_fd,
                          bool& should_propagate, ClientState& cs) {
        if (!cs.tx.in_multi) return RespValue::error("ERR EXEC without MULTI");

        // Check watch dirty
        bool dirty = store.watch_reg.is_dirty(client_fd);
        store.watch_reg.unwatch(client_fd);

        if (dirty) {
            cs.reset_tx();
            return RespValue::null_bulk(); // nil array → watched key changed
        }

        if (cs.tx.tx_error) {
            cs.reset_tx();
            return RespValue::error("EXECABORT Transaction discarded because of previous errors.");
        }

        std::vector<RespValue> results;
        auto queued = std::move(cs.tx.queue);
        cs.reset_tx();

        for (auto& qargs : queued) {
            bool prop = false;
            auto resp = handle(qargs, client_fd, prop, cs);
            if (prop) should_propagate = true;
            results.push_back(resp);
        }
        return RespValue::arr(results);
    }

    /** Handler for the Redis DISCARD command. Flushes all previously queued commands. */
    RespValue handle_discard(ClientState& cs) {
        if (!cs.tx.in_multi) return RespValue::error("ERR DISCARD without MULTI");
        store.watch_reg.unwatch(-1); // clear watches (we'll use client_fd in server.cpp)
        cs.reset_tx();
        return RespValue::simple("OK");
    }

    // ── WATCH / UNWATCH ───────────────────────────────────────────────────────
    /** Handler for the Redis WATCH command. Marks the given keys to be watched for conditional execution of a transaction. */
    RespValue handle_watch(const std::vector<std::string>& args, int fd,
                           ClientState& cs) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'watch'");
        if (cs.tx.in_multi) return RespValue::error("ERR WATCH inside MULTI is not allowed");
        std::vector<std::string> keys(args.begin()+1, args.end());
        for (auto& k : keys) cs.watched_keys.insert(k);
        store.watch_reg.watch(fd, keys);
        return RespValue::simple("OK");
    }
    /** Handler for the Redis UNWATCH command. Flushes all the previously watched keys for a transaction. */
    RespValue handle_unwatch(int fd, ClientState& cs) {
        store.watch_reg.unwatch(fd);
        cs.watched_keys.clear();
        return RespValue::simple("OK");
    }

    // ── SORTED SETS ───────────────────────────────────────────────────────────
    /** Handler for the Redis ZADD command. Adds one or more members to a sorted set, or update its score if it already exists. */
    RespValue handle_zadd(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 4 || (args.size() % 2) != 0)
            return RespValue::error("ERR wrong number of arguments for 'zadd'");
        std::vector<std::pair<double,std::string>> members;
        for (size_t i = 2; i + 1 < args.size(); i += 2) {
            try {
                double score = std::stod(args[i]);
                members.push_back({score, args[i+1]});
            } catch (...) {
                return RespValue::error("ERR value is not a float or out of range");
            }
        }
        return RespValue::make_int(store.zadd(args[1], members));
    }
    /** Handler for the Redis ZRANK command. Determine the index of a member in a sorted set. */
    RespValue handle_zrank(const std::vector<std::string>& args) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        auto r = store.zrank(args[1], args[2]);
        return r ? RespValue::make_int(*r) : RespValue::null_bulk();
    }
    /** Handler for the Redis ZRANGE command. Return a range of members in a sorted set, by index. */
    RespValue handle_zrange(const std::vector<std::string>& args) {
        if (args.size() < 4) return RespValue::error("ERR wrong number of arguments for 'zrange'");
        long long start = std::stoll(args[2]);
        long long stop  = std::stoll(args[3]);
        auto members = store.zrange(args[1], start, stop);

        bool with_scores = false;
        for (size_t i = 4; i < args.size(); ++i)
            if (to_upper(args[i]) == "WITHSCORES") with_scores = true;

        std::vector<RespValue> rv;
        if (with_scores) {
            for (auto& m : members) {
                rv.push_back(RespValue::bulk(m));
                auto sc = store.zscore(args[1], m);
                std::ostringstream oss;
                oss << std::fixed << std::setprecision(17) << *sc;
                // Strip trailing zeros
                std::string s = oss.str();
                auto dot = s.find('.');
                if (dot != std::string::npos) {
                    s.erase(s.find_last_not_of('0') + 1);
                    if (s.back() == '.') s.pop_back();
                }
                rv.push_back(RespValue::bulk(s));
            }
        } else {
            for (auto& m : members) rv.push_back(RespValue::bulk(m));
        }
        return RespValue::arr(rv);
    }
    /** Handler for the Redis ZCARD command. Get the number of members in a sorted set. */
    RespValue handle_zcard(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return RespValue::make_int(store.zcard(args[1]));
    }
    /** Handler for the Redis ZSCORE command. Get the score associated with the given member in a sorted set. */
    RespValue handle_zscore(const std::vector<std::string>& args) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        auto sc = store.zscore(args[1], args[2]);
        if (!sc) return RespValue::null_bulk();
        std::ostringstream oss;
        oss << *sc;
        return RespValue::bulk(oss.str());
    }
    /** Handler for the Redis ZREM command. Remove one or more members from a sorted set. */
    RespValue handle_zrem(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
        std::vector<std::string> members(args.begin()+2, args.end());
        return RespValue::make_int(store.zrem(args[1], members));
    }

    // ── GEO commands ─────────────────────────────────────────────────────────
    /** Handler for the Redis GEOADD command. Add one or more geospatial items in the geospatial index. */
    RespValue handle_geoadd(const std::vector<std::string>& args, ClientState&) {
        // GEOADD key [lon lat member ...]
        if (args.size() < 5 || ((args.size() - 2) % 3) != 0)
            return RespValue::error("ERR wrong number of arguments for 'geoadd'");
        std::vector<std::tuple<double,double,std::string>> items;
        for (size_t i = 2; i + 2 < args.size(); i += 3) {
            try {
                double lon = std::stod(args[i]);
                double lat = std::stod(args[i+1]);
                items.emplace_back(lon, lat, args[i+2]);
            } catch (...) {
                return RespValue::error("ERR not a float");
            }
        }
        long long added = store.geoadd(args[1], items);
        if (added < 0) return RespValue::error("WRONGTYPE wrong kind of value");
        return RespValue::make_int(added);
    }

    /** Handler for the Redis GEOPOS command. Return longitude and latitude of members of a geospatial index. */
    RespValue handle_geopos(const std::vector<std::string>& args) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments for 'geopos'");
        std::vector<RespValue> rv;
        for (size_t i = 2; i < args.size(); ++i) {
            auto pos = store.geopos(args[1], args[i]);
            if (!pos) {
                rv.push_back(RespValue::null_bulk());
            } else {
                std::ostringstream lon_s, lat_s;
                lon_s << std::fixed << std::setprecision(4) << pos->first;
                lat_s << std::fixed << std::setprecision(4) << pos->second;
                rv.push_back(RespValue::arr({
                    RespValue::bulk(lon_s.str()),
                    RespValue::bulk(lat_s.str())
                }));
            }
        }
        return RespValue::arr(rv);
    }

    /** Handler for the Redis GEODIST command. Return the distance between two members in the geospatial index. */
    RespValue handle_geodist(const std::vector<std::string>& args) {
        if (args.size() < 4) return RespValue::error("ERR wrong number of arguments for 'geodist'");
        std::string unit = (args.size() >= 5) ? to_upper(args[4]) : "M";
        auto dist_m = store.geodist(args[1], args[2], args[3]);
        if (!dist_m) return RespValue::null_bulk();
        double d = *dist_m;
        if (unit == "KM") d /= 1000.0;
        else if (unit == "MI") d /= 1609.344;
        else if (unit == "FT") d *= 3.28084;
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(4) << d;
        return RespValue::bulk(oss.str());
    }

    /** Handler for the Redis GEOSEARCH command. Query a geospatial index for members inside a given area. */
    RespValue handle_geosearch(const std::vector<std::string>& args) {
        // GEOSEARCH key FROMMEMBER member BYRADIUS radius m|km|mi|ft ASC
        if (args.size() < 7) return RespValue::error("ERR syntax error");
        std::string from_member;
        double radius_m = 0;
        // Parse
        for (size_t i = 2; i < args.size(); ++i) {
            std::string opt = to_upper(args[i]);
            if (opt == "FROMMEMBER" && i+1 < args.size()) {
                from_member = args[++i];
            } else if (opt == "BYRADIUS" && i+2 < args.size()) {
                radius_m = std::stod(args[++i]);
                std::string unit = to_upper(args[++i]);
                if (unit == "KM")  radius_m *= 1000.0;
                else if (unit == "MI") radius_m *= 1609.344;
                else if (unit == "FT") radius_m /= 3.28084;
            }
        }
        auto members = store.geosearch_radius(args[1], from_member, radius_m);
        std::vector<RespValue> rv;
        for (auto& m : members) rv.push_back(RespValue::bulk(m));
        return RespValue::arr(rv);
    }

    // ── XADD ─────────────────────────────────────────────────────────────────
    /** Handler for the Redis XADD command. Appends a new entry to a stream. */
    RespValue handle_xadd(const std::vector<std::string>& args, ClientState&) {
        if (args.size() < 5)
            return RespValue::error("ERR wrong number of arguments for 'xadd'");
        const std::string& key = args[1];
        const std::string& id  = args[2];
        std::vector<std::pair<std::string,std::string>> fields;
        for (size_t i = 3; i + 1 < args.size(); i += 2)
            fields.push_back({args[i], args[i+1]});
        std::string assigned = store.xadd(key, id, fields);
        if (assigned.empty())
            return RespValue::error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item");
        return RespValue::bulk(assigned);
    }

    // ── XRANGE ───────────────────────────────────────────────────────────────
    /** Handler for the Redis XRANGE command. Return a range of elements in a stream, with IDs matching a given interval. */
    RespValue handle_xrange(const std::vector<std::string>& args) {
        if (args.size() < 4) return RespValue::error("ERR wrong number of arguments for 'xrange'");
        std::string start = args[2] == "-" ? "-" : args[2];
        std::string end   = args[3] == "+" ? "+" : args[3];
        if (start.find('-') == std::string::npos && start != "-") start += "-0";
        if (end.find('-') == std::string::npos && end != "+") end += "-18446744073709551615";
        auto entries = store.xrange(args[1], start, end);
        return stream_entries_to_resp(entries);
    }

    // ── XLEN ─────────────────────────────────────────────────────────────────
    /** Handler for the Redis XLEN command. Return the number of entries in a stream. */
    RespValue handle_xlen(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        return RespValue::make_int(store.xlen(args[1]));
    }

    // ── XREAD [BLOCK ms] STREAMS key [key ...] id [id ...] ───────────────────
    /** Handler for the Redis XREAD command. Return data from one or multiple streams, only returning entries with an ID greater than the last received ID. */
    RespValue handle_xread(const std::vector<std::string>& args) {
        size_t idx = 1;
        long long count = -1;
        long long block_ms = -1; // -1 = non-blocking

        while (idx < args.size()) {
            std::string opt = to_upper(args[idx]);
            if (opt == "COUNT" && idx+1 < args.size()) {
                count = std::stoll(args[++idx]);
            } else if (opt == "BLOCK" && idx+1 < args.size()) {
                block_ms = std::stoll(args[++idx]);
            } else if (opt == "STREAMS") {
                idx++; break;
            } else break;
            ++idx;
        }

        size_t remaining = args.size() - idx;
        size_t num_keys  = remaining / 2;
        std::vector<std::string> keys(args.begin()+idx, args.begin()+idx+num_keys);
        std::vector<std::string> ids (args.begin()+idx+num_keys, args.end());

        // Resolve $ IDs to last-known IDs
        for (size_t i = 0; i < ids.size(); ++i) {
            if (ids[i] == "$")
                ids[i] = store.xstream_last_id(keys[i]);
        }

        // Build result
        auto build_result = [&]() {
            std::vector<RespValue> result;
            for (size_t i = 0; i < keys.size(); ++i) {
                std::string after_id = ids[i];
                // Normalize
                if (after_id.find('-') == std::string::npos)
                    after_id += "-18446744073709551615";

                auto entries = store.xrange(keys[i], after_id, "+");
                // Filter strictly > after_id
                std::vector<StreamEntry> filtered;
                for (auto& e : entries)
                    if (e.id != ids[i]) filtered.push_back(e);
                if (count >= 0 && (long long)filtered.size() > count)
                    filtered.resize(count);
                if (filtered.empty()) continue;

                std::vector<RespValue> stream_result;
                for (auto& e : filtered) {
                    std::vector<RespValue> ea = {RespValue::bulk(e.id)};
                    std::vector<RespValue> fv;
                    for (auto& [k,v] : e.fields) {
                        fv.push_back(RespValue::bulk(k));
                        fv.push_back(RespValue::bulk(v));
                    }
                    ea.push_back(RespValue::arr(fv));
                    stream_result.push_back(RespValue::arr(ea));
                }
                result.push_back(RespValue::arr({
                    RespValue::bulk(keys[i]),
                    RespValue::arr(stream_result)
                }));
            }
            return result;
        };

        auto result = build_result();
        if (!result.empty()) return RespValue::arr(result);

        // Blocking mode
        if (block_ms >= 0 && keys.size() == 1) {
            // block_ms == 0 means block forever
            auto entries = store.xread_block(keys[0], ids[0], block_ms == 0 ? 0 : block_ms);
            if (entries.empty()) return RespValue::null_bulk();
            if (count >= 0 && (long long)entries.size() > count)
                entries.resize(count);
            return RespValue::arr({RespValue::arr({
                RespValue::bulk(keys[0]),
                stream_entries_to_resp(entries)
            })});
        }

        return RespValue::null_bulk();
    }

    // ── PUB/SUB ───────────────────────────────────────────────────────────────
    /** Handler for the Redis SUBSCRIBE command. Subscribes the client to one or more channels. */
    RespValue handle_subscribe(const std::vector<std::string>& args, int fd,
                               ClientState& cs) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'subscribe'");
        std::vector<std::string> channels(args.begin()+1, args.end());
        cs.subscribed_mode = true;

        // We need to send responses directly (one per channel)
        // Return a special null to prevent double-send in server.cpp
        // Server.cpp will call handle_subscribe_direct
        // For simplicity, we just return the first channel's response.
        // The server sends them all inline.
        pubsub.subscribe(fd, channels);
        // Build multi-bulk for first channel only; server.cpp handles rest
        int total = pubsub.subscribe(fd, {});  // doesn't add, just counts
        (void)total;
        return RespValue::null_bulk(); // server.cpp handles sending
    }

    /** Handler for the Redis UNSUBSCRIBE command. Unsubscribes the client from given channels, or from all of them if none is given. */
    RespValue handle_unsubscribe(const std::vector<std::string>& args, int fd,
                                 ClientState& cs) {
        std::vector<std::string> channels(args.begin()+1, args.end());
        pubsub.unsubscribe(fd, channels);
        if (!pubsub.is_subscribed(fd)) cs.subscribed_mode = false;
        return RespValue::null_bulk(); // server.cpp handles sending
    }

    /** Handler for the Redis PUBLISH command. Posts a message to the given channel. */
    RespValue handle_publish(const std::vector<std::string>& args) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments for 'publish'");
        // Build the message array: ["message", channel, msg]
        std::string serialized = serialize(RespValue::arr({
            RespValue::bulk("message"),
            RespValue::bulk(args[1]),
            RespValue::bulk(args[2])
        }));
        int count = pubsub.publish(args[1], args[2], serialized);
        return RespValue::make_int(count);
    }

    // ── CONFIG ────────────────────────────────────────────────────────────────
    /** Handler for the Redis CONFIG command. Retrieves or alters server configuration parameters. */
    RespValue handle_config(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments");
        std::string sub = to_upper(args[1]);
        if (sub == "GET") {
            if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
            std::string param = to_upper(args[2]);
            std::vector<RespValue> rv;
            auto add = [&](const std::string& k, const std::string& v) {
                rv.push_back(RespValue::bulk(k));
                rv.push_back(RespValue::bulk(v));
            };
            if (param == "DIR"          || param == "*") add("dir",             cfg.dir);
            if (param == "DBFILENAME"   || param == "*") add("dbfilename",      cfg.dbfilename);
            if (param == "SAVE"         || param == "*") add("save",            "");
            if (param == "APPENDONLY"   || param == "*") add("appendonly",      cfg.appendonly);
            if (param == "APPENDDIRNAME"|| param == "*") add("appenddirname",   cfg.appenddirname);
            if (param == "APPENDFILENAME"||param == "*") add("appendfilename",  cfg.appendfilename);
            if (param == "REQUIREPASS"  || param == "*") add("requirepass",     cfg.requirepass);
            return RespValue::arr(rv);
        }
        if (sub == "SET") {
            if (args.size() < 4) return RespValue::error("ERR wrong number of arguments");
            std::string k = to_upper(args[2]);
            if (k == "REQUIREPASS") cfg.requirepass = args[3];
            else if (k == "APPENDONLY") cfg.appendonly = args[3];
            return RespValue::simple("OK");
        }
        if (sub == "RESETSTAT") return RespValue::simple("OK");
        return RespValue::error("ERR unsupported CONFIG subcommand");
    }

    // ── INFO ──────────────────────────────────────────────────────────────────
    /** Handler for the Redis INFO command. Returns information and statistics about the server. */
    RespValue handle_info(const std::vector<std::string>& args) {
        std::string section = (args.size() > 1) ? to_upper(args[1]) : "ALL";
        std::ostringstream ss;
        if (section == "REPLICATION" || section == "ALL") {
            ss << "# Replication\r\n";
            ss << "role:" << (repl.is_master() ? "master" : "slave") << "\r\n";
            ss << "connected_slaves:" << repl.replica_count() << "\r\n";
            ss << "master_replid:"    << repl.repl_id << "\r\n";
            ss << "master_repl_offset:" << repl.repl_offset.load() << "\r\n";
        }
        if (section == "SERVER" || section == "ALL") {
            ss << "# Server\r\n";
            ss << "redis_version:7.2.0\r\n";
            ss << "tcp_port:" << cfg.port << "\r\n";
        }
        return RespValue::bulk(ss.str());
    }

    // ── REPLCONF ──────────────────────────────────────────────────────────────
    /** Handler for the internal REPLCONF command used during Master-Replica handshake. */
    RespValue handle_replconf(const std::vector<std::string>& args, int /*fd*/) {
        if (args.size() >= 3 && to_upper(args[1]) == "GETACK") {
            return RespValue::arr({
                RespValue::bulk("REPLCONF"),
                RespValue::bulk("ACK"),
                RespValue::bulk(std::to_string(repl.repl_offset.load()))
            });
        }
        return RespValue::simple("OK");
    }

    // ── PSYNC ─────────────────────────────────────────────────────────────────
    /** Handler for the internal PSYNC command used by replicas to initiate a replication stream. */
    RespValue handle_psync(const std::vector<std::string>&, int client_fd) {
        std::string resp = "+FULLRESYNC " + repl.repl_id + " 0\r\n";
        ::send(client_fd, resp.data(), resp.size(), MSG_NOSIGNAL);
        std::string rdb        = empty_rdb_bytes();
        std::string rdb_header = "$" + std::to_string(rdb.size()) + "\r\n";
        ::send(client_fd, rdb_header.data(), rdb_header.size(), MSG_NOSIGNAL);
        ::send(client_fd, rdb.data(), rdb.size(), MSG_NOSIGNAL);
        {
            std::lock_guard<std::mutex> lk(repl.replicas_mu);
            bool found = false;
            for (auto& rc : repl.replica_list)
                if (rc->fd == client_fd) { rc->handshake_done = true; found = true; break; }
            if (!found) {
                auto rc = std::make_shared<ReplicaConn>();
                rc->fd = client_fd; rc->handshake_done = true;
                repl.replica_list.push_back(rc);
            }
        }
        RespValue noop; noop.type = RespType::Null; return noop;
    }

    // ── WAIT ──────────────────────────────────────────────────────────────────
    /** Handler for the Redis WAIT command. Blocks until the previous write commands are successfully replicated to at least the specified number of replicas. */
    RespValue handle_wait(const std::vector<std::string>& args) {
        if (args.size() < 3) return RespValue::error("ERR wrong number of arguments for 'wait'");
        int num_replicas = std::stoi(args[1]);
        std::string getack = serialize(RespValue::arr({
            RespValue::bulk("REPLCONF"),
            RespValue::bulk("GETACK"),
            RespValue::bulk("*")
        }));
        {
            std::lock_guard<std::mutex> lk(repl.replicas_mu);
            for (auto& rc : repl.replica_list)
                if (rc->handshake_done)
                    ::send(rc->fd, getack.data(), getack.size(), MSG_NOSIGNAL);
        }
        long long offset = repl.repl_offset.load();
        int acked = repl.ack_count(offset);
        return RespValue::make_int(std::min(acked, num_replicas));
    }

    // ── AUTH ──────────────────────────────────────────────────────────────────
    /** Handler for the Redis AUTH command. Authenticates the client connection. */
    RespValue handle_auth(const std::vector<std::string>& args, ClientState& cs) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'auth'");
        std::string password = args.back(); // AUTH [username] password
        if (cfg.requirepass.empty()) {
            cs.authenticated = true;
            return RespValue::error("ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?");
        }
        if (password == cfg.requirepass) {
            cs.authenticated = true;
            return RespValue::simple("OK");
        }
        return RespValue::error("WRONGPASS invalid username-password pair or user is disabled.");
    }

    // ── ACL ───────────────────────────────────────────────────────────────────
    /** Handler for the Redis ACL command. Inspects and manages users and access control rules. */
    RespValue handle_acl(const std::vector<std::string>& args) {
        if (args.size() < 2) return RespValue::error("ERR wrong number of arguments for 'acl'");
        std::string sub = to_upper(args[1]);
        if (sub == "WHOAMI") return RespValue::bulk("default");
        if (sub == "GETUSER") {
            if (args.size() < 3) return RespValue::error("ERR wrong number of arguments");
            if (args[2] != "default") return RespValue::null_bulk();
            // Return user info map
            std::vector<RespValue> rv;
            rv.push_back(RespValue::bulk("flags"));
            std::vector<RespValue> flags;
            flags.push_back(RespValue::bulk("on"));
            if (cfg.requirepass.empty()) flags.push_back(RespValue::bulk("nopass"));
            rv.push_back(RespValue::arr(flags));
            rv.push_back(RespValue::bulk("passwords"));
            std::vector<RespValue> passwords;
            if (!cfg.requirepass.empty())
                passwords.push_back(RespValue::bulk("#" + cfg.requirepass));
            rv.push_back(RespValue::arr(passwords));
            rv.push_back(RespValue::bulk("commands"));
            rv.push_back(RespValue::bulk("+@all"));
            rv.push_back(RespValue::bulk("keys"));
            rv.push_back(RespValue::bulk("*"));
            rv.push_back(RespValue::bulk("channels"));
            rv.push_back(RespValue::bulk("*"));
            rv.push_back(RespValue::bulk("selectors"));
            rv.push_back(RespValue::arr({}));
            return RespValue::arr(rv);
        }
        return RespValue::error("ERR unknown ACL subcommand");
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    /** Helper function to serialize an array of StreamEntry structs into RESP format. */
    static RespValue stream_entries_to_resp(const std::vector<StreamEntry>& entries) {
        std::vector<RespValue> result;
        for (auto& e : entries) {
            std::vector<RespValue> ea = {RespValue::bulk(e.id)};
            std::vector<RespValue> fv;
            for (auto& [k,v] : e.fields) {
                fv.push_back(RespValue::bulk(k));
                fv.push_back(RespValue::bulk(v));
            }
            ea.push_back(RespValue::arr(fv));
            result.push_back(RespValue::arr(ea));
        }
        return RespValue::arr(result);
    }
};
