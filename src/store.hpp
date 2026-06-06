/**
 * store.hpp
 *
 * Core key-value store implementation.
 *
 * Features:
 * - Thread-safe access via a global mutex.
 * - Condition variables for blocking operations (BLPOP, XREAD BLOCK).
 * - Multi-type support: Strings, Lists, Streams, Sorted Sets (ZSet), Geo.
 * - Expiration and TTL tracking.
 * - Watch registry for optimistic concurrency control (Transactions).
 */
#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <set>
#include <deque>
#include <vector>
#include <optional>
#include <variant>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <cmath>
#include <cstdint>
#include <algorithm>

using Clock = std::chrono::steady_clock;
using TimePoint = std::chrono::time_point<Clock>;

// ─── Stream entry ─────────────────────────────────────────────────────────────
struct StreamEntry
{
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
};

// ─── Sorted-set member ────────────────────────────────────────────────────────
// Ordered first by score, then lexicographically
struct ZSetMember
{
    double score;
    std::string member;
    bool operator<(const ZSetMember &o) const
    {
        if (score != o.score)
            return score < o.score;
        return member < o.member;
    }
};

// ─── Value type aliases ───────────────────────────────────────────────────────
using ListData = std::deque<std::string>;
using StreamData = std::vector<StreamEntry>;

// Sorted set: ordered container + fast lookup by member name
struct ZSetData
{
    std::set<ZSetMember> ordered;                   // for rank / range
    std::unordered_map<std::string, double> scores; // member → score

    // Returns true if this is a new member
    bool zadd(const std::string &member, double score)
    {
        auto it = scores.find(member);
        if (it != scores.end())
        {
            ordered.erase({it->second, member});
            it->second = score;
        }
        else
        {
            scores[member] = score;
        }
        ordered.insert({score, member});
        return (it == scores.end());
    }

    bool zrem(const std::string &member)
    {
        auto it = scores.find(member);
        if (it == scores.end())
            return false;
        ordered.erase({it->second, member});
        scores.erase(it);
        return true;
    }

    std::optional<double> zscore(const std::string &member) const
    {
        auto it = scores.find(member);
        if (it == scores.end())
            return std::nullopt;
        return it->second;
    }

    // 0-based rank (ascending)
    std::optional<long long> zrank(const std::string &member) const
    {
        auto it = scores.find(member);
        if (it == scores.end())
            return std::nullopt;
        long long r = 0;
        for (auto &m : ordered)
        {
            if (m.member == member)
                return r;
            ++r;
        }
        return std::nullopt;
    }

    // zrange with raw indices (handles negatives)
    std::vector<std::string> zrange(long long start, long long stop) const
    {
        long long sz = (long long)ordered.size();
        if (start < 0)
            start = std::max(0LL, sz + start);
        if (stop < 0)
            stop = sz + stop;
        stop = std::min(stop, sz - 1);
        std::vector<std::string> result;
        long long idx = 0;
        for (auto &m : ordered)
        {
            if (idx >= start && idx <= stop)
                result.push_back(m.member);
            ++idx;
        }
        return result;
    }

    long long size() const { return (long long)scores.size(); }
};

// ─── Store entry ──────────────────────────────────────────────────────────────
struct StoreEntry
{
    enum class Type
    {
        String,
        List,
        Stream,
        ZSet
    } type = Type::String;
    std::string str_val;
    ListData list_val;
    StreamData stream_val;
    ZSetData zset_val;
    std::optional<TimePoint> expires_at;
};

// ─── Watch registry ───────────────────────────────────────────────────────────
/**
 * WatchRegistry
 *
 * Implements optimistic concurrency for Redis transactions (WATCH/MULTI/EXEC).
 *
 * - Maps client-fd → set of watched keys that have been dirtied.
 * - When a key is modified, all clients watching that key are marked as "dirty".
 * - If a client's transaction executes while it is dirty, the transaction fails.
 */
class WatchRegistry
{
public:
    // Register keys being watched by a client
    void watch(int fd, const std::vector<std::string> &keys)
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &k : keys)
            watchers_[k].insert(fd);
        // ensure fd entry exists
        dirty_.emplace(fd, std::unordered_set<std::string>{});
    }

    // Called on every write — mark dirty for any watcher
    void notify_write(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = watchers_.find(key);
        if (it == watchers_.end())
            return;
        for (int fd : it->second)
            dirty_[fd].insert(key);
    }

    // Returns true if fd has any dirty watched key
    bool is_dirty(int fd)
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = dirty_.find(fd);
        return it != dirty_.end() && !it->second.empty();
    }

    // Clear watches for a client (on EXEC/DISCARD/UNWATCH)
    void unwatch(int fd)
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto &[key, fds] : watchers_)
            fds.erase(fd);
        dirty_.erase(fd);
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::unordered_set<int>> watchers_;
    std::unordered_map<int, std::unordered_set<std::string>> dirty_;
};

// ─── Store ────────────────────────────────────────────────────────────────────
/**
 * Store
 *
 * The main thread-safe data structure holding all Redis keys and values.
 * All operations on the store are protected by `std::mutex mu`.
 */
class Store
{
public:
    std::mutex mu;
    std::condition_variable cv; // for BLPOP & blocking XREAD

    WatchRegistry watch_reg;

    // ── String ops ────────────────────────────────────────────────────────────
    /** Sets a string key with an optional millisecond TTL. */
    void set(const std::string &key, const std::string &val,
             std::optional<long long> px_ttl = std::nullopt)
    {
        {
            std::lock_guard<std::mutex> lk(mu);
            StoreEntry e;
            e.type = StoreEntry::Type::String;
            e.str_val = val;
            if (px_ttl.has_value() && *px_ttl > 0)
                e.expires_at = Clock::now() + std::chrono::milliseconds(*px_ttl);
            data_[key] = std::move(e);
        }
        watch_reg.notify_write(key);
    }

    /** Gets a string value, returning nullopt if expired or wrong type. */
    std::optional<std::string> get(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end())
            return std::nullopt;
        if (is_expired(it->second))
        {
            data_.erase(it);
            return std::nullopt;
        }
        if (it->second.type != StoreEntry::Type::String)
            return std::nullopt;
        return it->second.str_val;
    }

    /** Deletes a key from the store. Returns true if it existed. */
    bool del(const std::string &key)
    {
        bool erased;
        {
            std::lock_guard<std::mutex> lk(mu);
            erased = data_.erase(key) > 0;
        }
        if (erased)
            watch_reg.notify_write(key);
        return erased;
    }

    /** Returns time-to-live in milliseconds, or -1 if no TTL, -2 if not found. */
    long long pttl(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end())
            return -2;
        if (is_expired(it->second))
        {
            data_.erase(it);
            return -2;
        }
        if (!it->second.expires_at.has_value())
            return -1;
        auto rem = std::chrono::duration_cast<std::chrono::milliseconds>(
                       *it->second.expires_at - Clock::now())
                       .count();
        return rem > 0 ? rem : -2;
    }

    /** Returns time-to-live in seconds. */
    long long ttl(const std::string &key)
    {
        long long p = pttl(key);
        if (p < 0)
            return p;
        return (p + 999) / 1000;
    }

    /** Returns the type of the key as a string (e.g., "string", "list"). */
    std::string type_of(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end())
            return "none";
        if (is_expired(it->second))
        {
            data_.erase(it);
            return "none";
        }
        switch (it->second.type)
        {
        case StoreEntry::Type::String:
            return "string";
        case StoreEntry::Type::List:
            return "list";
        case StoreEntry::Type::Stream:
            return "stream";
        case StoreEntry::Type::ZSet:
            return "zset";
        }
        return "none";
    }

    /** Returns all keys matching a simple glob pattern. */
    std::vector<std::string> keys(const std::string &pattern)
    {
        std::lock_guard<std::mutex> lk(mu);
        std::vector<std::string> result;
        for (auto &[k, v] : data_)
            if (!is_expired(v) && matches(k, pattern))
                result.push_back(k);
        return result;
    }

    // ── List ops ──────────────────────────────────────────────────────────────
    /** Pushes elements to a list. dir=1 (right), dir=-1 (left). Returns new length. */
    long long list_push(const std::string &key,
                        const std::vector<std::string> &vals, int dir)
    {
        long long len;
        {
            std::lock_guard<std::mutex> lk(mu);
            auto &e = data_[key];
            if (e.type != StoreEntry::Type::List && !e.list_val.empty())
                return -1; // type error
            e.type = StoreEntry::Type::List;
            for (auto &v : vals)
            {
                if (dir >= 0)
                    e.list_val.push_back(v);
                else
                    e.list_val.push_front(v);
            }
            len = (long long)e.list_val.size();
        }
        watch_reg.notify_write(key);
        cv.notify_all(); // wake BLPOP waiters
        return len;
    }

    /** Pops up to `count` elements. dir=1 (left/LPOP), dir=-1 (right/RPOP). */
    std::vector<std::string> list_pop(const std::string &key,
                                      long long count, int dir)
    {
        std::vector<std::string> result;
        {
            std::lock_guard<std::mutex> lk(mu);
            auto it = data_.find(key);
            if (it == data_.end() || it->second.type != StoreEntry::Type::List)
                return result;
            auto &lst = it->second.list_val;
            while (count-- > 0 && !lst.empty())
            {
                if (dir >= 0)
                {
                    result.push_back(lst.front());
                    lst.pop_front();
                }
                else
                {
                    result.push_back(lst.back());
                    lst.pop_back();
                }
            }
            if (lst.empty())
                data_.erase(it);
        }
        if (!result.empty())
            watch_reg.notify_write(key);
        return result;
    }

    /** Returns a range of elements from a list. Supports negative indices. */
    std::vector<std::string> lrange(const std::string &key,
                                    long long start, long long stop)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::List)
            return {};
        auto &lst = it->second.list_val;
        long long sz = (long long)lst.size();
        if (start < 0)
            start = std::max(0LL, sz + start);
        if (stop < 0)
            stop = sz + stop;
        stop = std::min(stop, sz - 1);
        std::vector<std::string> result;
        for (long long i = start; i <= stop; ++i)
            result.push_back(lst[i]);
        return result;
    }

    /** Returns the length of a list. */
    long long llen(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::List)
            return 0;
        return (long long)it->second.list_val.size();
    }

    /**
     * BLPOP
     * Blocks until an element is available on any of the provided keys, or timeout expires.
     * timeout_ms == 0 blocks forever.
     */
    std::optional<std::pair<std::string, std::string>>
    blpop(const std::vector<std::string> &keys_list, long long timeout_ms)
    {
        auto deadline = (timeout_ms > 0)
                            ? std::optional<TimePoint>(Clock::now() + std::chrono::milliseconds(timeout_ms))
                            : std::nullopt;

        std::unique_lock<std::mutex> lk(mu);
        while (true)
        {
            // Try each key
            for (auto &k : keys_list)
            {
                auto it = data_.find(k);
                if (it != data_.end() &&
                    it->second.type == StoreEntry::Type::List &&
                    !it->second.list_val.empty())
                {
                    auto val = it->second.list_val.front();
                    it->second.list_val.pop_front();
                    if (it->second.list_val.empty())
                        data_.erase(it);
                    return std::make_pair(k, val);
                }
            }
            // Wait for a push event
            if (deadline)
            {
                auto status = cv.wait_until(lk, *deadline);
                if (status == std::cv_status::timeout)
                    return std::nullopt;
            }
            else
            {
                cv.wait(lk);
            }
        }
    }

    // ── Stream ops ────────────────────────────────────────────────────────────
    /**
     * XADD
     * Adds an entry to a stream. Auto-generates ID if id is "*" or "ms-*".
     * Validates that the new ID is strictly greater than the last added ID.
     */
    std::string xadd(const std::string &key, std::string id,
                     const std::vector<std::pair<std::string, std::string>> &fields)
    {
        std::string final_id;
        {
            std::lock_guard<std::mutex> lk(mu);
            auto &e = data_[key];
            if (e.type != StoreEntry::Type::Stream && !e.stream_val.empty())
                return "";
            e.type = StoreEntry::Type::Stream;

            uint64_t last_ms = 0, last_seq = 0;
            if (!e.stream_val.empty())
            {
                auto &last = e.stream_val.back().id;
                auto dash = last.find('-');
                last_ms = std::stoull(last.substr(0, dash));
                last_seq = std::stoull(last.substr(dash + 1));
            }

            uint64_t ms, seq;
            if (id == "*")
            {
                ms = now_ms();
                seq = (ms == last_ms) ? last_seq + 1 : 0;
            }
            else
            {
                auto dash = id.find('-');
                ms = std::stoull(id.substr(0, dash));
                std::string seq_part = id.substr(dash + 1);
                if (seq_part == "*")
                    seq = (ms == last_ms) ? last_seq + 1 : (ms == 0 ? 1 : 0);
                else
                    seq = std::stoull(seq_part);
            }

            // Validate monotonically increasing
            if (ms < last_ms || (ms == last_ms && seq <= last_seq))
            {
                if (ms == 0 && seq == 0)
                    return "";
                if (!(ms == 0 && seq == 0 && last_ms == 0 && last_seq == 0))
                    return "";
            }

            final_id = std::to_string(ms) + "-" + std::to_string(seq);
            StreamEntry se;
            se.id = final_id;
            se.fields = fields;
            e.stream_val.push_back(se);
        }
        watch_reg.notify_write(key);
        cv.notify_all(); // wake blocking XREAD
        return final_id;
    }

    /** Snapshot of the last known stream ID for a key (used by XREAD $). */
    std::string xstream_last_id(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::Stream || it->second.stream_val.empty())
            return "0-0";
        return it->second.stream_val.back().id;
    }

    /** Returns a range of stream entries between start and end IDs (inclusive). */
    std::vector<StreamEntry> xrange(const std::string &key,
                                    const std::string &start,
                                    const std::string &end)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::Stream)
            return {};
        std::vector<StreamEntry> result;
        for (auto &e : it->second.stream_val)
            if (cmp_id(e.id, start) >= 0 && (end == "+" || cmp_id(e.id, end) <= 0))
                result.push_back(e);
        return result;
    }

    /**
     * Blocking XREAD
     * Blocks until new entries appear in the stream strictly after `after_id`,
     * or until `timeout_ms` expires.
     */
    std::vector<StreamEntry> xread_block(const std::string &key,
                                         const std::string &after_id,
                                         long long timeout_ms)
    {
        auto deadline = (timeout_ms > 0)
                            ? std::optional<TimePoint>(Clock::now() + std::chrono::milliseconds(timeout_ms))
                            : std::nullopt;

        std::unique_lock<std::mutex> lk(mu);
        while (true)
        {
            auto it = data_.find(key);
            if (it != data_.end() && it->second.type == StoreEntry::Type::Stream)
            {
                std::vector<StreamEntry> result;
                for (auto &e : it->second.stream_val)
                    if (cmp_id(e.id, after_id) > 0)
                        result.push_back(e);
                if (!result.empty())
                    return result;
            }
            if (deadline)
            {
                auto status = cv.wait_until(lk, *deadline);
                if (status == std::cv_status::timeout)
                    return {};
            }
            else
            {
                cv.wait(lk);
            }
        }
    }

    /** Returns the number of entries in a stream. */
    long long xlen(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::Stream)
            return 0;
        return (long long)it->second.stream_val.size();
    }

    // ── Sorted set ops ────────────────────────────────────────────────────────
    /** Adds members with scores to a sorted set. Returns count of new members. */
    long long zadd(const std::string &key,
                   const std::vector<std::pair<double, std::string>> &members)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto &e = data_[key];
        if (e.type != StoreEntry::Type::ZSet && e.type != StoreEntry::Type::String)
            return 0;
        if (e.type == StoreEntry::Type::String && !e.str_val.empty())
            return 0;
        e.type = StoreEntry::Type::ZSet;
        long long added = 0;
        for (auto &[score, member] : members)
            if (e.zset_val.zadd(member, score))
                ++added;
        return added;
    }

    /** Returns the 0-based rank of a member in a sorted set (ordered by score). */
    std::optional<long long> zrank(const std::string &key,
                                   const std::string &member)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return std::nullopt;
        return it->second.zset_val.zrank(member);
    }

    /** Returns a range of members from a sorted set by rank. */
    std::vector<std::string> zrange(const std::string &key,
                                    long long start, long long stop)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return {};
        return it->second.zset_val.zrange(start, stop);
    }

    /** Returns the number of members in a sorted set. */
    long long zcard(const std::string &key)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return 0;
        return it->second.zset_val.size();
    }

    /** Returns the score of a member in a sorted set. */
    std::optional<double> zscore(const std::string &key,
                                 const std::string &member)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return std::nullopt;
        return it->second.zset_val.zscore(member);
    }

    /** Removes members from a sorted set. Returns number of members removed. */
    long long zrem(const std::string &key,
                   const std::vector<std::string> &members)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return 0;
        long long cnt = 0;
        for (auto &m : members)
            if (it->second.zset_val.zrem(m))
                ++cnt;
        return cnt;
    }

    // ── Geo ops (stored inside ZSetData via geohash score) ───────────────────
    /**
     * GEOADD
     * Stores geographic coordinates (longitude, latitude) by encoding them into
     * a 52-bit geohash, which is then stored as the score in a Sorted Set.
     */
    long long geoadd(const std::string &key,
                     const std::vector<std::tuple<double, double, std::string>> &items)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto &e = data_[key];
        if (e.type != StoreEntry::Type::ZSet && e.type != StoreEntry::Type::String)
            return -1;
        if (e.type == StoreEntry::Type::String && !e.str_val.empty())
            return -1;
        e.type = StoreEntry::Type::ZSet;
        long long added = 0;
        for (auto &[lon, lat, member] : items)
        {
            // Validate
            if (lat < -85.05112878 || lat > 85.05112878 ||
                lon < -180.0 || lon > 180.0)
                continue;
            double score = encode_geohash(lon, lat);
            if (e.zset_val.zadd(member, score))
                ++added;
        }
        return added;
    }

    /**
     * GEOPOS
     * Decodes the lon/lat coordinates from the geohash score of a member.
     */
    std::optional<std::pair<double, double>> geopos(const std::string &key,
                                                    const std::string &member)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return std::nullopt;
        auto sc = it->second.zset_val.zscore(member);
        if (!sc)
            return std::nullopt;
        return decode_geohash(*sc);
    }

    /**
     * GEODIST
     * Computes the distance between two members using the Haversine formula.
     */
    std::optional<double> geodist(const std::string &key,
                                  const std::string &m1, const std::string &m2)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return std::nullopt;
        auto s1 = it->second.zset_val.zscore(m1);
        auto s2 = it->second.zset_val.zscore(m2);
        if (!s1 || !s2)
            return std::nullopt;
        auto [lon1, lat1] = decode_geohash(*s1);
        auto [lon2, lat2] = decode_geohash(*s2);
        return haversine_m(lat1, lon1, lat2, lon2);
    }

    /**
     * GEOSEARCH FROMMEMBER BYRADIUS
     * Returns members that reside within `radius_m` meters of `from_member`.
     */
    std::vector<std::string> geosearch_radius(const std::string &key,
                                              const std::string &from_member,
                                              double radius_m)
    {
        std::lock_guard<std::mutex> lk(mu);
        auto it = data_.find(key);
        if (it == data_.end() || it->second.type != StoreEntry::Type::ZSet)
            return {};
        auto sc0 = it->second.zset_val.zscore(from_member);
        if (!sc0)
            return {};
        auto [clon, clat] = decode_geohash(*sc0);

        std::vector<std::string> result;
        for (auto &[m, score] : it->second.zset_val.scores)
        {
            auto [lon, lat] = decode_geohash(score);
            double dist = haversine_m(clat, clon, lat, lon);
            if (dist <= radius_m)
                result.push_back(m);
        }
        return result;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    static uint64_t now_ms()
    {
        return (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }

private:
    std::unordered_map<std::string, StoreEntry> data_;

    static bool is_expired(const StoreEntry &e)
    {
        return e.expires_at.has_value() && Clock::now() > *e.expires_at;
    }

    // Simple glob (* and ?)
    static bool matches(const std::string &str, const std::string &pat)
    {
        size_t s = 0, p = 0, star_p = std::string::npos, star_s = 0;
        while (s < str.size())
        {
            if (p < pat.size() && (pat[p] == '?' || pat[p] == str[s]))
            {
                ++s;
                ++p;
            }
            else if (p < pat.size() && pat[p] == '*')
            {
                star_p = p++;
                star_s = s;
            }
            else if (star_p != std::string::npos)
            {
                p = star_p + 1;
                s = ++star_s;
            }
            else
                return false;
        }
        while (p < pat.size() && pat[p] == '*')
            ++p;
        return p == pat.size();
    }

    // Compare stream IDs (ms-seq)
    static int cmp_id(const std::string &a, const std::string &b)
    {
        auto parse_id = [](const std::string &s, uint64_t &ms, uint64_t &seq)
        {
            if (s == "-")
            {
                ms = 0;
                seq = 0;
                return;
            }
            if (s == "+")
            {
                ms = UINT64_MAX;
                seq = UINT64_MAX;
                return;
            }
            auto dash = s.find('-');
            ms = std::stoull(s.substr(0, dash));
            seq = std::stoull(s.substr(dash + 1));
        };
        uint64_t a_ms, a_seq, b_ms, b_seq;
        parse_id(a, a_ms, a_seq);
        parse_id(b, b_ms, b_seq);
        if (a_ms != b_ms)
            return a_ms < b_ms ? -1 : 1;
        if (a_seq != b_seq)
            return a_seq < b_seq ? -1 : 1;
        return 0;
    }

    // ── Geohash52 encode / decode ─────────────────────────────────────────────
    // Redis uses a 52-bit integer geohash stored as a float64 score.
    static double encode_geohash(double lon, double lat)
    {
        // Normalize to [0, 1)
        double norm_lon = (lon + 180.0) / 360.0;
        double norm_lat = (lat + 85.05112878) / 170.10225756;
        // Interleave 26 bits each → 52-bit integer
        uint64_t x = (uint64_t)(norm_lon * (1ULL << 26));
        uint64_t y = (uint64_t)(norm_lat * (1ULL << 26));
        uint64_t hash = 0;
        for (int i = 25; i >= 0; --i)
        {
            hash = (hash << 1) | ((x >> i) & 1);
            hash = (hash << 1) | ((y >> i) & 1);
        }
        return (double)hash;
    }

    static std::pair<double, double> decode_geohash(double score)
    {
        uint64_t hash = (uint64_t)score;
        uint64_t x = 0, y = 0;
        for (int i = 0; i < 26; ++i)
        {
            x = (x << 1) | ((hash >> (51 - 2 * i)) & 1);
            y = (y << 1) | ((hash >> (50 - 2 * i)) & 1);
        }
        double norm_lon = (double)x / (double)(1ULL << 26);
        double norm_lat = (double)y / (double)(1ULL << 26);
        double lon = norm_lon * 360.0 - 180.0;
        double lat = norm_lat * 170.10225756 - 85.05112878;
        return {lon, lat};
    }

    // Haversine formula — returns distance in metres
    static double haversine_m(double lat1, double lon1,
                              double lat2, double lon2)
    {
        const double R = 6372797.560856;
        const double pi = 3.14159265358979323846;
        auto to_rad = [&](double d)
        { return d * pi / 180.0; };
        double dlat = to_rad(lat2 - lat1);
        double dlon = to_rad(lon2 - lon1);
        double a = std::sin(dlat / 2) * std::sin(dlat / 2) + std::cos(to_rad(lat1)) * std::cos(to_rad(lat2)) * std::sin(dlon / 2) * std::sin(dlon / 2);
        return R * 2 * std::asin(std::sqrt(a));
    }
};
