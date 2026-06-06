/**
 * aof.hpp
 * 
 * Implements the Append Only File (AOF) persistence mechanism.
 * When enabled via `--appendonly yes`, every mutating command (SET, DEL, etc.)
 * is written to an append-only log file in RESP format before returning to the client.
 * 
 * The AOFReplayer is used at startup to read this file and reconstruct the state
 * of the database by sequentially executing the logged commands.
 */
#pragma once
#include <string>
#include <fstream>
#include <mutex>
#include <vector>
#include <functional>
#include <filesystem>
#include <iostream>

// ─── AOF Writer ───────────────────────────────────────────────────────────────
/**
 * AOFWriter
 * 
 * Appends RESP-encoded write commands to the append-only file.
 * Thread-safe — all writes are guarded by a mutex, ensuring concurrent clients
 * write commands to the log atomically.
 */
class AOFWriter {
public:
    AOFWriter() = default;

    /**
     * open
     * Opens (or creates) the AOF file at the given directory and filename.
     * Also writes a simple manifest file alongside it if one doesn't exist.
     * Returns true if successfully opened.
     */
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

    /** Returns true if the AOF file is successfully opened and enabled for writing. */
    bool is_enabled() const { return enabled_; }

    /**
     * append
     * Thread-safely appends a RESP-serialized command string to the AOF.
     */
    void append(const std::string& resp_cmd) {
        if (!enabled_) return;
        std::lock_guard<std::mutex> lk(mu_);
        if (file_.is_open()) {
            file_ << resp_cmd;
            file_.flush();
        }
    }

    /** Returns the absolute or relative path to the active AOF file. */
    std::string path() const { return path_; }

private:
    std::mutex   mu_;
    std::ofstream file_;
    std::string  path_;
    bool         enabled_ = false;
};

// ─── AOF Replayer ─────────────────────────────────────────────────────────────
/**
 * AOFReplayer
 * 
 * Reads an AOF file into memory, parses the RESP arrays, and invokes a callback 
 * for each command. The callback typically runs the command against a temporary 
 * ClientState, thereby restoring the database state.
 */
class AOFReplayer {
public:
    /**
     * replay
     * Opens an AOF file, parses the binary RESP stream, and invokes the callback
     * for each extracted command (represented as a vector of strings).
     */
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
