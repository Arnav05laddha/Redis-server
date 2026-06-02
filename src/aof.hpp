#pragma once
#include <string>
#include <fstream>
#include <mutex>
#include <vector>
#include <functional>
#include <filesystem>
#include <iostream>

// ─── AOF Writer ───────────────────────────────────────────────────────────────
// Appends RESP-encoded write commands to the append-only file.
// Thread-safe — all writes are guarded by a mutex.
class AOFWriter {
public:
    AOFWriter() = default;

    // Open (or create) the AOF file.
    bool open(const std::string& dir, const std::string& filename) {
        std::lock_guard<std::mutex> lk(mu_);
        try {
            std::filesystem::create_directories(dir);
        } catch (...) {}

        path_ = dir + "/" + filename;
        file_.open(path_, std::ios::app | std::ios::binary);
        enabled_ = file_.is_open();

        // Create manifest file alongside AOF
        try {
            std::string manifest_path = dir + "/appendonly.aof.manifest";
            if (!std::filesystem::exists(manifest_path)) {
                std::ofstream mf(manifest_path);
                mf << "file " << filename << " seq 1 type b\n";
            }
        } catch (...) {}

        return enabled_;
    }

    bool is_enabled() const { return enabled_; }

    // Write a RESP-serialized command to the AOF
    void append(const std::string& resp_cmd) {
        if (!enabled_) return;
        std::lock_guard<std::mutex> lk(mu_);
        if (file_.is_open()) {
            file_ << resp_cmd;
            file_.flush();
        }
    }

    std::string path() const { return path_; }

private:
    std::mutex   mu_;
    std::ofstream file_;
    std::string  path_;
    bool         enabled_ = false;
};

// ─── AOF Replayer ─────────────────────────────────────────────────────────────
// Reads an AOF file and invokes a callback for each command.
class AOFReplayer {
public:
    // callback receives the parsed args vector
    static bool replay(const std::string& path,
                       std::function<void(const std::vector<std::string>&)> cb) {
        std::ifstream f(path, std::ios::binary);
        if (!f) return false;

        std::string buf((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
        size_t pos = 0;

        while (pos < buf.size()) {
            // Expect *N\r\n
            if (buf[pos] != '*') { ++pos; continue; }
            auto cr = buf.find("\r\n", pos);
            if (cr == std::string::npos) break;
            long long count = std::stoll(buf.substr(pos + 1, cr - pos - 1));
            pos = cr + 2;

            std::vector<std::string> args;
            bool ok = true;
            for (long long i = 0; i < count; ++i) {
                if (pos >= buf.size() || buf[pos] != '$') { ok = false; break; }
                auto cr2 = buf.find("\r\n", pos);
                if (cr2 == std::string::npos) { ok = false; break; }
                long long len = std::stoll(buf.substr(pos + 1, cr2 - pos - 1));
                pos = cr2 + 2;
                if (pos + len + 2 > buf.size()) { ok = false; break; }
                args.push_back(buf.substr(pos, len));
                pos += len + 2;
            }
            if (ok && !args.empty())
                cb(args);
        }
        return true;
    }
};
