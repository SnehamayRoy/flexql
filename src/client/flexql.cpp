#include "flexql.h"
#include "buffered_socket.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <netdb.h>
#include <netinet/tcp.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

struct FlexQL {
    int socket_fd;
    BufferedSocket socket;

    explicit FlexQL(int fd) : socket_fd(fd), socket(fd) {}
};

namespace {

bool send_all(int fd, const std::string &data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t rc = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

char *dup_cstr(const std::string &value) {
    char *out = static_cast<char *>(std::malloc(value.size() + 1));
    if (!out) {
        return nullptr;
    }
    std::memcpy(out, value.c_str(), value.size() + 1);
    return out;
}

int set_error(char **errmsg, const std::string &message) {
    if (errmsg) {
        *errmsg = dup_cstr(message);
    }
    return FLEXQL_ERROR;
}

}  // namespace

extern "C" {

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) {
        return FLEXQL_ERROR;
    }

    *db = nullptr;

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo *result = nullptr;
    std::string port_str = std::to_string(port);
    if (::getaddrinfo(host, port_str.c_str(), &hints, &result) != 0) {
        return FLEXQL_ERROR;
    }

    int sock = -1;
    for (addrinfo *rp = result; rp != nullptr; rp = rp->ai_next) {
        sock = ::socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (sock < 0) {
            continue;
        }
        if (::connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) {
            break;
        }
        ::close(sock);
        sock = -1;
    }
    ::freeaddrinfo(result);

    if (sock < 0) {
        return FLEXQL_ERROR;
    }

    int nodelay = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

    *db = new FlexQL(sock);
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) {
        return FLEXQL_ERROR;
    }
    ::close(db->socket_fd);
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg) {
    if (errmsg) {
        *errmsg = nullptr;
    }
    if (!db || !sql) {
        return set_error(errmsg, "invalid database handle or SQL");
    }

    std::string sql_text(sql);
    std::string request = "EXEC " + std::to_string(sql_text.size()) + "\n" + sql_text;
    if (!send_all(db->socket_fd, request)) {
        return set_error(errmsg, "failed to send request to server");
    }

    std::string line;
    if (!db->socket.read_line(line)) {
        return set_error(errmsg, "failed to read server response");
    }

    if (line.rfind("ERROR ", 0) == 0) {
        size_t length = static_cast<size_t>(std::stoull(line.substr(6)));
        std::string message;
        if (!db->socket.read_exact(message, length)) {
            return set_error(errmsg, "failed to read error body");
        }
        std::string newline;
        if (!db->socket.read_line(newline)) {
            return set_error(errmsg, "failed to finish error body");
        }
        if (!db->socket.read_line(line) || line != "END") {
            return set_error(errmsg, "protocol error after error response");
        }
        return set_error(errmsg, message);
    }

    if (line != "OK") {
        return set_error(errmsg, "invalid server response");
    }

    if (!db->socket.read_line(line) || line.rfind("META ", 0) != 0) {
        return set_error(errmsg, "missing result metadata");
    }
    int column_count = std::stoi(line.substr(5));

    std::vector<std::string> columns(static_cast<size_t>(column_count));
    std::vector<char *> column_names(static_cast<size_t>(column_count), nullptr);
    std::vector<std::string> row_values;
    std::vector<char *> row_ptrs;

    for (int i = 0; i < column_count; ++i) {
        if (!db->socket.read_line(line) || line.rfind("COL ", 0) != 0) {
            return set_error(errmsg, "invalid column metadata");
        }
        size_t length = static_cast<size_t>(std::stoull(line.substr(4)));
        if (!db->socket.read_exact(columns[static_cast<size_t>(i)], length)) {
            return set_error(errmsg, "failed to read column name");
        }
        std::string newline;
        if (!db->socket.read_line(newline)) {
            return set_error(errmsg, "failed to terminate column name");
        }
        column_names[static_cast<size_t>(i)] = columns[static_cast<size_t>(i)].data();
    }

    while (true) {
        if (!db->socket.read_line(line)) {
            return set_error(errmsg, "unexpected end of response");
        }
        if (line == "END") {
            return FLEXQL_OK;
        }
        if (line.rfind("ROW ", 0) != 0) {
            return set_error(errmsg, "invalid row header");
        }

        int field_count = std::stoi(line.substr(4));
        row_values.assign(static_cast<size_t>(field_count), std::string{});
        row_ptrs.assign(static_cast<size_t>(field_count), nullptr);

        for (int i = 0; i < field_count; ++i) {
            if (!db->socket.read_line(line) || line.rfind("VAL ", 0) != 0) {
                return set_error(errmsg, "invalid field header");
            }
            size_t length = static_cast<size_t>(std::stoull(line.substr(4)));
            if (!db->socket.read_exact(row_values[static_cast<size_t>(i)], length)) {
                return set_error(errmsg, "failed to read field value");
            }
            std::string newline;
            if (!db->socket.read_line(newline)) {
                return set_error(errmsg, "failed to terminate field value");
            }
            row_ptrs[static_cast<size_t>(i)] = row_values[static_cast<size_t>(i)].data();
        }

        if (callback) {
            int cb_rc = callback(arg, field_count, row_ptrs.data(), column_names.data());
            if (cb_rc != 0) {
                while (true) {
                    if (!db->socket.read_line(line)) {
                        return set_error(errmsg, "failed to discard aborted response");
                    }
                    if (line == "END") {
                        break;
                    }
                    if (line.rfind("ROW ", 0) != 0) {
                        continue;
                    }
                    int discard_count = std::stoi(line.substr(4));
                    for (int i = 0; i < discard_count; ++i) {
                        if (!db->socket.read_line(line) || line.rfind("VAL ", 0) != 0) {
                            return set_error(errmsg, "protocol error while aborting");
                        }
                        size_t length = static_cast<size_t>(std::stoull(line.substr(4)));
                        std::string discard;
                        if (!db->socket.read_exact(discard, length)) {
                            return set_error(errmsg, "protocol error while aborting");
                        }
                        std::string newline;
                        if (!db->socket.read_line(newline)) {
                            return set_error(errmsg, "protocol error while aborting");
                        }
                    }
                }
                return FLEXQL_OK;
            }
        }
    }
}

void flexql_free(void *ptr) {
    std::free(ptr);
}

}  // extern "C"
