#pragma once
#include "store.hpp"
#include <fstream>
#include <string>
#include <cstdint>
#include <iostream>

// Minimal RDB v9 parser (enough for CodeCrafters stages)
// Supports string keys with optional expiry

class RdbLoader {
public:
    static bool load(Store& store, const std::string& path) {
        std::ifstream f(path, std::ios::binary);
        if (!f) return false;

        std::string magic(5, '\0');
        f.read(magic.data(), 5);
        if (magic != "REDIS") return false;

        // Skip version (4 bytes)
        f.seekg(4, std::ios::cur);

        while (f.good()) {
            uint8_t op = read_byte(f);

            if (op == 0xFF) break; // EOF marker

            if (op == 0xFE) {       // DB selector
                read_length(f);     // db index
                continue;
            }

            if (op == 0xFB) {       // Resize DB (RDB v7+)
                read_length(f);     // hash table size
                read_length(f);     // expiry hash table size
                continue;
            }

            if (op == 0xFA) {       // Auxiliary field
                read_string(f);     // key
                read_string(f);     // value
                continue;
            }

            // Expiry prefix
            std::optional<long long> px_ttl;
            if (op == 0xFC) {       // Expiry ms
                uint64_t exp_ms = read_uint64_le(f);
                auto now_ms = (uint64_t)std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                if (exp_ms > now_ms)
                    px_ttl = (long long)(exp_ms - now_ms);
                else
                    px_ttl = 0; // already expired
                op = read_byte(f); // type byte
            } else if (op == 0xFD) { // Expiry seconds
                uint32_t exp_s = read_uint32_le(f);
                auto now_s = (uint64_t)std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                if (exp_s > now_s)
                    px_ttl = (long long)(exp_s - now_s) * 1000;
                else
                    px_ttl = 0;
                op = read_byte(f);
            }

            // op is now the value type
            if (op == 0) { // String type
                std::string key = read_string(f);
                std::string val = read_string(f);
                if (!px_ttl.has_value() || *px_ttl > 0)
                    store.set(key, val, px_ttl);
                // if px_ttl == 0 → expired, don't load
            } else {
                // Skip unsupported types (list, set, zset, hash)
                // For CodeCrafters we only need strings
                break;
            }
        }
        return true;
    }

private:
    static uint8_t read_byte(std::ifstream& f) {
        uint8_t b; f.read((char*)&b, 1); return b;
    }

    static uint32_t read_uint32_le(std::ifstream& f) {
        uint8_t buf[4]; f.read((char*)buf, 4);
        return (uint32_t)buf[0] | ((uint32_t)buf[1]<<8) |
               ((uint32_t)buf[2]<<16) | ((uint32_t)buf[3]<<24);
    }

    static uint64_t read_uint64_le(std::ifstream& f) {
        uint8_t buf[8]; f.read((char*)buf, 8);
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= ((uint64_t)buf[i] << (8*i));
        return v;
    }

    static uint64_t read_length(std::ifstream& f) {
        uint8_t b = read_byte(f);
        uint8_t enc = (b & 0xC0) >> 6;
        if (enc == 0) return b & 0x3F;
        if (enc == 1) {
            uint8_t b2 = read_byte(f);
            return ((uint64_t)(b & 0x3F) << 8) | b2;
        }
        if (enc == 2) {
            uint8_t buf[4]; f.read((char*)buf, 4);
            return ((uint64_t)buf[0]<<24)|((uint64_t)buf[1]<<16)|
                   ((uint64_t)buf[2]<<8)|buf[3];
        }
        // Special encoding (enc == 3)
        uint8_t special = b & 0x3F;
        if (special == 0) { read_byte(f); return 0; }   // 8-bit int
        if (special == 1) { f.seekg(2, std::ios::cur); return 0; }  // 16-bit int
        if (special == 2) { f.seekg(4, std::ios::cur); return 0; }  // 32-bit int
        return 0;
    }

    static std::string read_string(std::ifstream& f) {
        uint8_t b = read_byte(f);
        uint8_t enc = (b & 0xC0) >> 6;
        if (enc == 3) {
            // Integer encoded
            uint8_t special = b & 0x3F;
            long long val = 0;
            if (special == 0) {
                val = (int8_t)read_byte(f);
            } else if (special == 1) {
                uint8_t lo = read_byte(f), hi = read_byte(f);
                val = (int16_t)((hi << 8) | lo);
            } else if (special == 2) {
                uint8_t buf[4]; f.read((char*)buf, 4);
                val = (int32_t)((buf[3]<<24)|(buf[2]<<16)|(buf[1]<<8)|buf[0]);
            }
            return std::to_string(val);
        }
        // Normal length-prefixed string
        f.seekg(-1, std::ios::cur); // put back byte
        uint64_t len = read_length(f);
        std::string s(len, '\0');
        f.read(s.data(), len);
        return s;
    }
};
