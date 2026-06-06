/**
 * resp.hpp
 * 
 * Implements the REdis Serialization Protocol (RESP) parser and serializer.
 * Supports RESP2 data types:
 * - Simple Strings (+)
 * - Errors (-)
 * - Integers (:)
 * - Bulk Strings ($)
 * - Arrays (*)
 * 
 * Also handles fallback parsing for inline commands (used by redis-benchmark).
 */
#pragma once
#include <string>
#include <vector>
#include <stdexcept>
#include <sstream>

enum class RespType { SimpleString, Error, Integer, BulkString, Array, Null };

struct RespValue {
    RespType type = RespType::Null;
    std::string str;
    long long num = 0;
    std::vector<RespValue> array;
    bool is_null = false;

    /** Creates a Simple String RESP value (e.g. +OK\r\n) */
    static RespValue simple(const std::string& s) {
        RespValue v; v.type = RespType::SimpleString; v.str = s; return v;
    }
    /** Creates an Error RESP value (e.g. -ERR\r\n) */
    static RespValue error(const std::string& s) {
        RespValue v; v.type = RespType::Error; v.str = s; return v;
    }
    /** Creates an Integer RESP value (e.g. :1000\r\n) */
    static RespValue make_int(long long n) {
        RespValue v; v.type = RespType::Integer; v.num = n; return v;
    }
    /** Creates a Bulk String RESP value (e.g. $5\r\nhello\r\n) */
    static RespValue bulk(const std::string& s) {
        RespValue v; v.type = RespType::BulkString; v.str = s; return v;
    }
    /** Creates a Null Bulk String RESP value (e.g. $-1\r\n) */
    static RespValue null_bulk() {
        RespValue v; v.type = RespType::BulkString; v.is_null = true; return v;
    }
    /** Creates an Array RESP value containing nested RespValue elements */
    static RespValue arr(const std::vector<RespValue>& a) {
        RespValue v; v.type = RespType::Array; v.array = a; return v;
    }
};

/**
 * serialize
 * 
 * Converts a memory-resident `RespValue` tree back into a raw RESP2-encoded
 * byte string, ready to be transmitted over a TCP socket.
 */
inline std::string serialize(const RespValue& v) {
    std::ostringstream out;
    switch (v.type) {
        case RespType::SimpleString: out << "+" << v.str << "\r\n"; break;
        case RespType::Error:        out << "-" << v.str << "\r\n"; break;
        case RespType::Integer:      out << ":" << v.num  << "\r\n"; break;
        case RespType::BulkString:
            if (v.is_null) out << "$-1\r\n";
            else out << "$" << v.str.size() << "\r\n" << v.str << "\r\n";
            break;
        case RespType::Array:
            if (v.is_null) out << "*-1\r\n";
            else { out << "*" << v.array.size() << "\r\n"; for (auto& e : v.array) out << serialize(e); }
            break;
        default: break;
    }
    return out.str();
}

struct ParseResult {
    RespValue value;
    size_t consumed = 0;
    bool ok = false;
};

/**
 * parse
 * 
 * Core RESP parser function. Recursively parses a raw byte buffer starting at `pos`
 * and extracts exactly one complete RESP value.
 * 
 * @param buf The raw byte buffer received from the socket.
 * @param pos The offset at which to start parsing.
 * @return ParseResult containing the extracted value, the number of bytes consumed,
 *         and a boolean indicating if a complete value was successfully parsed.
 */
inline ParseResult parse(const std::string& buf, size_t pos = 0) {
    if (pos >= buf.size()) return {};

    auto find_crlf = [&](size_t from) -> size_t {
        for (size_t i = from; i + 1 < buf.size(); ++i)
            if (buf[i] == '\r' && buf[i+1] == '\n') return i;
        return std::string::npos;
    };

    char prefix = buf[pos];
    size_t line_end = find_crlf(pos + 1);
    if (line_end == std::string::npos) return {};
    std::string line = buf.substr(pos + 1, line_end - pos - 1);
    size_t after_line = line_end + 2;

    ParseResult res;
    res.ok = true;

    if (prefix == '+') { res.value = RespValue::simple(line); res.consumed = after_line - pos; }
    else if (prefix == '-') { res.value = RespValue::error(line); res.consumed = after_line - pos; }
    else if (prefix == ':') { res.value = RespValue::make_int(std::stoll(line)); res.consumed = after_line - pos; }
    else if (prefix == '$') {
        long long len = std::stoll(line);
        if (len == -1) { res.value = RespValue::null_bulk(); res.consumed = after_line - pos; }
        else {
            size_t data_end = after_line + len + 2;
            if (data_end > buf.size()) { res.ok = false; return res; }
            res.value = RespValue::bulk(buf.substr(after_line, len));
            res.consumed = data_end - pos;
        }
    } else if (prefix == '*') {
        long long count = std::stoll(line);
        if (count == -1) {
            RespValue v; v.type = RespType::Array; v.is_null = true;
            res.value = v; res.consumed = after_line - pos;
        } else {
            std::vector<RespValue> arr;
            size_t cur = after_line;
            for (long long i = 0; i < count; ++i) {
                auto elem = parse(buf, cur);
                if (!elem.ok) { res.ok = false; return res; }
                arr.push_back(elem.value);
                cur += elem.consumed;
            }
            res.value = RespValue::arr(arr);
            res.consumed = cur - pos;
        }
    } else {
        // Handle inline commands (e.g. from redis-benchmark or telnet)
        // An inline command is just text followed by \r\n.
        // The prefix is the first character of the text.
        res.value = RespValue::simple(buf.substr(pos, line_end - pos));
        res.consumed = after_line - pos;
        res.ok = true;
    }
    return res;
}

/**
 * parse_inline
 * 
 * Tokenizes a space-separated inline command (like 'PING' or 'SET key val')
 * into an array of argument strings. Used as a fallback for non-RESP requests.
 */
inline std::vector<std::string> parse_inline(const std::string& line) {
    std::vector<std::string> tokens;
    std::istringstream ss(line);
    std::string tok;
    while (ss >> tok) tokens.push_back(tok);
    return tokens;
}
