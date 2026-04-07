#include "thread_pool.h"
#include "executor.h"
#include "storage_engine.h"
#include "buffered_socket.h"

#include <filesystem>
#include <iostream>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <thread>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <csignal>
#include <atomic>

std::atomic<bool> g_running{true};
int g_server_fd = -1;

void handle_signal(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        g_running = false;
        if (g_server_fd >= 0) {
            ::shutdown(g_server_fd, SHUT_RDWR);
            ::close(g_server_fd);
            g_server_fd = -1;
        }
    }
}

namespace fs = std::filesystem;

namespace {

constexpr int kPort = 9000;
constexpr int kBacklog = 64;

bool send_all(int sock, const std::string &data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t rc = ::send(sock, data.data() + sent, data.size() - sent, 0);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

bool recv_sql_request(BufferedSocket &socket, std::string &sql) {
    std::string header;
    if (!socket.read_line(header)) {
        return false;
    }
    if (header.rfind("EXEC ", 0) != 0) {
        return false;
    }
    size_t length = static_cast<size_t>(std::stoull(header.substr(5)));
    return socket.read_exact(sql, length);
}

bool send_error(int sock, const std::string &message) {
    std::string out = "ERROR " + std::to_string(message.size()) + "\n";
    out += message;
    out += "\nEND\n";
    return send_all(sock, out);
}

bool send_ok(int sock, const QueryResult &result) {
    // Pre-compute total buffer size to avoid reallocations
    size_t est = 32;
    for (const auto &c : result.columns) est += 12 + c.size();
    for (const auto &row : result.rows)
        for (const auto &v : row) est += 12 + v.size();

    std::string out;
    out.reserve(est);
    out += "OK\n";
    out += "META "; out += std::to_string(result.columns.size()); out += "\n";
    for (const auto &column : result.columns) {
        out += "COL "; out += std::to_string(column.size()); out += "\n";
        out += column; out += "\n";
    }
    for (const auto &row : result.rows) {
        out += "ROW "; out += std::to_string(row.size()); out += "\n";
        for (const auto &value : row) {
            out += "VAL "; out += std::to_string(value.size()); out += "\n";
            out += value; out += "\n";
        }
    }
    out += "END\n";
    return send_all(sock, out);
}

void handle_client(int client_socket, Database &database) {
    BufferedSocket socket(client_socket);
    while (true) {
        std::string sql;
        if (!recv_sql_request(socket, sql)) {
            break;
        }

        ExecResult result = execute_sql(database, sql);
        bool ok = result.ok ? send_ok(client_socket, result.query_result) : send_error(client_socket, result.error);
        if (!ok) {
            break;
        }
    }
    ::close(client_socket);
}

}  // namespace

int main(int argc, char **argv) {
    bool fresh_start = false;
    size_t sync_every_batches = 1;
    bool use_wal = false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--fresh") {
            fresh_start = true;
            continue;
        }
        if (arg == "--wal") {
            use_wal = true;
            continue;
        }
        const std::string prefix = "--sync-every-batches=";
        if (arg.rfind(prefix, 0) == 0) {
            sync_every_batches = static_cast<size_t>(std::stoull(arg.substr(prefix.size())));
            if (sync_every_batches == 0) {
                std::cerr << "sync-every-batches must be >= 1" << std::endl;
                return 1;
            }
            continue;
        }
        std::cerr << "Unknown argument: " << arg << std::endl;
        return 1;
    }

    if (fresh_start) {
        std::error_code ec;
        fs::remove_all(storage_root_dir(), ec);
    }

    Database database;
    database.sync_every_batches = sync_every_batches;
    database.use_wal = use_wal;
    std::string error;
    if (!load_database(database, error)) {
        std::cerr << "Startup failed: " << error << std::endl;
        return 1;
    }

    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket() failed" << std::endl;
        return 1;
    }
    g_server_fd = server_fd;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(kPort);

    if (::bind(server_fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
        std::cerr << "bind() failed" << std::endl;
        ::close(server_fd);
        return 1;
    }

    if (::listen(server_fd, kBacklog) < 0) {
        std::cerr << "listen() failed" << std::endl;
        ::close(server_fd);
        return 1;
    }

    std::cout << "FlexQL Server running on port " << kPort
              << (use_wal ? " [WAL]" : " [direct]")
              << " (sync every " << sync_every_batches << " batch"
              << (sync_every_batches == 1 ? "" : "es") << ")" << std::endl;

    ThreadPool pool;
    size_t worker_count = std::thread::hardware_concurrency();
    pool.start(worker_count == 0 ? 4 : worker_count);

    while (g_running) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_socket = ::accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &addr_len);
        if (client_socket < 0) {
            if (!g_running) break;
            continue;
        }
        int nodelay = 1;
        setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        pool.submit([client_socket, &database]() {
            handle_client(client_socket, database);
        });
    }

    std::cout << "Shutting down..." << std::endl;
    pool.stop();
    if (server_fd >= 0) {
        ::close(server_fd);
    }

    // Final fsync on all open table files to ensure durability
    {
        std::shared_lock<std::shared_mutex> lock(database.mutex);
        for (auto& [name, tbl] : database.tables) {
            if (tbl->row_fd >= 0) ::fsync(tbl->row_fd);
        }
    }
    return 0;
}
