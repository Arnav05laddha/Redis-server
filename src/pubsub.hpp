/**
 * pubsub.hpp
 * 
 * Manages the Redis Publish/Subscribe (Pub/Sub) messaging paradigm.
 * 
 * Clients can subscribe to one or more channels. When another client publishes
 * a message to a channel, it is broadcasted (pushed) to all connected clients
 * listening to that channel.
 */
#pragma once
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <sys/socket.h>

// ─── Pub/Sub Registry ─────────────────────────────────────────────────────────
/**
 * PubSubRegistry
 * 
 * Thread-safe global channel registry.
 * Maps channel names to sets of client FDs (socket file descriptors), and vice versa.
 * PUBLISH commands serialize the message once and push it directly to all
 * subscriber sockets (fire-and-forget).
 */
class PubSubRegistry {
public:
    /**
     * subscribe
     * Subscribes a client to a list of channels.
     * Returns the total number of channels the client is now subscribed to.
     */
    int subscribe(int fd, const std::vector<std::string>& channels) {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto& ch : channels) {
            channel_fds_[ch].insert(fd);
            fd_channels_[fd].insert(ch);
        }
        return (int)fd_channels_[fd].size();
    }

    /**
     * unsubscribe
     * Unsubscribes a client from specific channels (or all channels if the list is empty).
     * Returns the remaining number of subscriptions for the client.
     */
    int unsubscribe(int fd, const std::vector<std::string>& channels) {
        std::lock_guard<std::mutex> lk(mu_);
        if (channels.empty()) {
            // Unsubscribe all
            for (auto& ch : fd_channels_[fd])
                channel_fds_[ch].erase(fd);
            fd_channels_.erase(fd);
            return 0;
        }
        for (auto& ch : channels) {
            channel_fds_[ch].erase(fd);
            fd_channels_[fd].erase(ch);
        }
        auto it = fd_channels_.find(fd);
        return (it == fd_channels_.end()) ? 0 : (int)it->second.size();
    }

    /**
     * remove_client
     * Purges all subscriptions for a client. Called when the TCP socket is closed.
     */
    void remove_client(int fd) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_channels_.find(fd);
        if (it == fd_channels_.end()) return;
        for (auto& ch : it->second)
            channel_fds_[ch].erase(fd);
        fd_channels_.erase(fd);
    }

    /**
     * publish
     * Broadcasts a serialized RESP message directly to all connected sockets
     * that are subscribed to the specified channel.
     * Returns the number of clients that received the message.
     */
    int publish(const std::string& channel, const std::string& /*msg*/,
                const std::string& serialized_msg) {
        std::unordered_set<int> fds;
        {
            std::lock_guard<std::mutex> lk(mu_);
            auto it = channel_fds_.find(channel);
            if (it == channel_fds_.end()) return 0;
            fds = it->second;
        }
        for (int fd : fds)
            send_all(fd, serialized_msg);
        return (int)fds.size();
    }

    /** Returns true if the client has at least one active subscription. */
    bool is_subscribed(int fd) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_channels_.find(fd);
        return it != fd_channels_.end() && !it->second.empty();
    }

    /** Returns a set of all channels a specific client is subscribed to. */
    std::unordered_set<std::string> get_channels(int fd) {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = fd_channels_.find(fd);
        if (it == fd_channels_.end()) return {};
        return it->second;
    }

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::unordered_set<int>> channel_fds_;
    std::unordered_map<int, std::unordered_set<std::string>> fd_channels_;

    /** Private helper to robustly send a string payload over a raw socket. */
    static void send_all(int fd, const std::string& s) {
        size_t total = 0;
        while (total < s.size()) {
            ssize_t n = ::send(fd, s.data() + total, s.size() - total, MSG_NOSIGNAL);
            if (n <= 0) break;
            total += n;
        }
    }
};
