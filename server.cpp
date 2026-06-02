#include "resp.hpp"
#include "store.hpp"
#include "replication.hpp"
#include "commands.hpp"
#include "rdb.hpp"
#include "aof.hpp"
#include "pubsub.hpp"

#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <cstring>
#include <csignal>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>

// ─── Global state ─────────────────────────────────────────────────────────────
Store           g_store;
ReplConfig      g_repl;
ServerConfig    g_cfg;
AOFWriter       g_aof;
PubSubRegistry  g_pubsub;

// ─── Send raw bytes ───────────────────────────────────────────────────────────
static void send_all(int fd, const std::string& s) {
    size_t total = 0;
    while (total < s.size()) {
        ssize_t n = send(fd, s.data() + total, s.size() - total, MSG_NOSIGNAL);
        if (n <= 0) break;
        total += n;
    }
}

// ─── Directly send SUBSCRIBE/UNSUBSCRIBE confirmations ───────────────────────
// Redis sends one response array per channel on SUBSCRIBE/UNSUBSCRIBE
static void send_sub_response(int fd, const std::string& kind,
                               const std::string& channel, int count) {
    auto msg = serialize(RespValue::arr({
        RespValue::bulk(kind),
        RespValue::bulk(channel),
        RespValue::make_int(count)
    }));
    send_all(fd, msg);
}


static void handle_client(int fd, bool is_replica_client = false) {
    CommandHandler handler(g_store, g_repl, g_cfg, g_pubsub);
    ClientState cs;

    // If no password required, clients start authenticated
    cs.authenticated = g_cfg.requirepass.empty();

    std::string buf;
    char tmp[4096];

    while (true) {
        ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) break;
        buf.append(tmp, n);

        while (!buf.empty()) {
            ParseResult pr = parse(buf);
            if (!pr.ok) break;

            std::vector<std::string> args;
            if (pr.value.type == RespType::Array) {
                for (auto& e : pr.value.array) {
                    if (e.type == RespType::BulkString && !e.is_null)
                        args.push_back(e.str);
                    else if (e.type == RespType::SimpleString)
                        args.push_back(e.str);
                }
            } else if (pr.value.type == RespType::BulkString && !pr.value.is_null) {
                args = parse_inline(pr.value.str);
            } else if (pr.value.type == RespType::SimpleString) {
                args = parse_inline(pr.value.str);
            }

            buf.erase(0, pr.consumed);
            if (args.empty()) continue;

            std::string cmd = to_upper(args[0]);

            // ── Handle SUBSCRIBE/UNSUBSCRIBE specially (multi-reply) ──────────
            if (cmd == "SUBSCRIBE") {
                cs.subscribed_mode = true;
                for (size_t i = 1; i < args.size(); ++i) {
                    int cnt = g_pubsub.subscribe(fd, {args[i]});
                    send_sub_response(fd, "subscribe", args[i], cnt);
                }
                continue;
            }
            if (cmd == "UNSUBSCRIBE") {
                std::vector<std::string> channels(args.begin()+1, args.end());
                if (channels.empty()) {
                    // Unsubscribe all
                    auto active = g_pubsub.get_channels(fd);
                    if (active.empty()) {
                        send_sub_response(fd, "unsubscribe", "", 0);
                    } else {
                        for (auto& ch : active) {
                            int cnt = g_pubsub.unsubscribe(fd, {ch});
                            send_sub_response(fd, "unsubscribe", ch, cnt);
                        }
                    }
                } else {
                    for (auto& ch : channels) {
                        int cnt = g_pubsub.unsubscribe(fd, {ch});
                        send_sub_response(fd, "unsubscribe", ch, cnt);
                    }
                }
                if (!g_pubsub.is_subscribed(fd)) cs.subscribed_mode = false;
                continue;
            }

            bool propagate = false;
            RespValue resp = handler.handle(args, fd, propagate, cs);

            // ── PSYNC is handled manually in handle_psync ──────────────────
            bool skip_send = (cmd == "PSYNC") ||
                             (is_replica_client && cmd == "REPLCONF");

            if (!skip_send && resp.type != RespType::Null)
                send_all(fd, serialize(resp));

            // ── DISCARD must also unwatch ─────────────────────────────────
            if (cmd == "DISCARD") g_store.watch_reg.unwatch(fd);
            if (cmd == "EXEC")    g_store.watch_reg.unwatch(fd);
            if (cmd == "UNWATCH") g_store.watch_reg.unwatch(fd);

            // ── AOF append for write commands ─────────────────────────────
            if (propagate && g_aof.is_enabled()) {
                std::vector<RespValue> arr;
                for (auto& a : args) arr.push_back(RespValue::bulk(a));
                g_aof.append(serialize(RespValue::arr(arr)));
            }

            // ── Propagate write commands to replicas ──────────────────────
            if (propagate && g_repl.is_master()) {
                std::vector<RespValue> arr;
                for (auto& a : args) arr.push_back(RespValue::bulk(a));
                g_repl.propagate(serialize(RespValue::arr(arr)));
            }
        }
    }

    // Cleanup
    g_pubsub.remove_client(fd);
    g_store.watch_reg.unwatch(fd);
    close(fd);
}

// ─── Replica handshake thread ─────────────────────────────────────────────────
static void run_replica() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return; }

    struct sockaddr_in addr{};
    struct hostent* he = gethostbyname(g_repl.master_host.c_str());
    if (!he) { std::cerr << "Cannot resolve master host\n"; return; }
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(g_repl.master_port);
    memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect to master"); return;
    }
    g_repl.master_fd = fd;

    auto send_cmd = [&](const std::vector<std::string>& a) {
        std::vector<RespValue> arr;
        for (auto& x : a) arr.push_back(RespValue::bulk(x));
        send_all(fd, serialize(RespValue::arr(arr)));
    };

    auto recv_line = [&]() -> std::string {
        std::string s;
        char c;
        while (recv(fd, &c, 1, 0) == 1) {
            s += c;
            if (s.size() >= 2 && s.substr(s.size()-2) == "\r\n")
                return s.substr(0, s.size()-2);
        }
        return s;
    };

    send_cmd({"PING"});                          recv_line();
    send_cmd({"REPLCONF","listening-port", std::to_string(g_cfg.port)}); recv_line();
    send_cmd({"REPLCONF","capa","psync2"});       recv_line();
    send_cmd({"PSYNC","?","-1"});                recv_line();

    // Read RDB bulk
    std::string rdb_header;
    char c;
    while (recv(fd, &c, 1, 0) == 1) {
        rdb_header += c;
        if (!rdb_header.empty() && rdb_header[0] == '$')
            if (rdb_header.size() >= 2 && rdb_header.substr(rdb_header.size()-2) == "\r\n")
                break;
    }
    if (!rdb_header.empty() && rdb_header[0] == '$') {
        long long rdb_len = std::stoll(rdb_header.substr(1));
        std::string rdb_data(rdb_len, '\0');
        size_t got = 0;
        while ((long long)got < rdb_len) {
            ssize_t n = recv(fd, rdb_data.data() + got, rdb_len - got, 0);
            if (n <= 0) break;
            got += n;
        }
    }

    // Listen for propagated commands
    CommandHandler handler(g_store, g_repl, g_cfg, g_pubsub);
    ClientState cs;
    cs.authenticated = true;
    std::string buf;
    char tmp[4096];

    while (true) {
        ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) break;
        buf.append(tmp, n);

        while (!buf.empty()) {
            ParseResult pr = parse(buf);
            if (!pr.ok) break;

            std::vector<std::string> args;
            if (pr.value.type == RespType::Array)
                for (auto& e : pr.value.array)
                    if (e.type == RespType::BulkString && !e.is_null)
                        args.push_back(e.str);

            size_t consumed = pr.consumed;
            buf.erase(0, consumed);
            g_repl.repl_offset += (long long)consumed;

            if (args.empty()) continue;

            std::string cmd = to_upper(args[0]);
            if (cmd == "REPLCONF" && args.size() >= 2 &&
                to_upper(args[1]) == "GETACK") {
                auto ack = serialize(RespValue::arr({
                    RespValue::bulk("REPLCONF"),
                    RespValue::bulk("ACK"),
                    RespValue::bulk(std::to_string(g_repl.repl_offset.load()))
                }));
                send_all(fd, ack);
                continue;
            }

            bool propagate = false;
            handler.handle(args, fd, propagate, cs);
        }
    }
    close(fd);
}

// ─── Parse CLI args ───────────────────────────────────────────────────────────
static void parse_args(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port"           && i+1 < argc) { g_cfg.port = std::stoi(argv[++i]); }
        else if (arg == "--replicaof" && i+2 < argc) {
            g_repl.role        = Role::Replica;
            g_repl.master_host = argv[++i];
            g_repl.master_port = std::stoi(argv[++i]);
        }
        else if (arg == "--dir"            && i+1 < argc) { g_cfg.dir             = argv[++i]; }
        else if (arg == "--dbfilename"     && i+1 < argc) { g_cfg.dbfilename      = argv[++i]; }
        else if (arg == "--appendonly"     && i+1 < argc) { g_cfg.appendonly      = argv[++i]; }
        else if (arg == "--appenddirname"  && i+1 < argc) { g_cfg.appenddirname   = argv[++i]; }
        else if (arg == "--appendfilename" && i+1 < argc) { g_cfg.appendfilename  = argv[++i]; }
        else if (arg == "--requirepass"    && i+1 < argc) { g_cfg.requirepass     = argv[++i]; }
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);
    parse_args(argc, argv);

    // Load RDB if configured
    if (!g_cfg.dir.empty() && !g_cfg.dbfilename.empty()) {
        std::string path = g_cfg.dir + "/" + g_cfg.dbfilename;
        if (RdbLoader::load(g_store, path))
            std::cerr << "[RDB] Loaded: " << path << "\n";
    }

    // Setup AOF
    if (g_cfg.appendonly == "yes") {
        std::string aof_dir  = g_cfg.appenddirname.empty()  ? "." : g_cfg.appenddirname;
        std::string aof_file = g_cfg.appendfilename.empty() ? "appendonly.aof" : g_cfg.appendfilename;
        if (g_aof.open(aof_dir, aof_file)) {
            std::cerr << "[AOF] Enabled: " << g_aof.path() << "\n";
            // Replay existing AOF
            CommandHandler temp_handler(g_store, g_repl, g_cfg, g_pubsub);
            ClientState    temp_cs;
            temp_cs.authenticated = true;
            AOFReplayer::replay(g_aof.path(), [&](const std::vector<std::string>& args) {
                bool prop = false;
                temp_handler.handle(args, -1, prop, temp_cs);
            });
        }
    }

    // Start replica handshake if needed
    if (!g_repl.is_master())
        std::thread(run_replica).detach();

    // Create TCP server socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(g_cfg.port);

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(server_fd, 128) < 0) { perror("listen"); return 1; }

    std::cerr << "[Redis] Listening on port " << g_cfg.port
              << " role=" << (g_repl.is_master() ? "master" : "replica") << "\n";

    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) continue;

        std::thread([client_fd]() {
            handle_client(client_fd);
        }).detach();
    }

    close(server_fd);
    return 0;
}
