#include "flexql.h"

#include <iostream>
#include <string>
#include <vector>

namespace {

int print_row_callback(void *, int argc, char **argv, char **azColName) {
    for (int i = 0; i < argc; ++i) {
        if (i > 0) {
            std::cout << " | ";
        }
        const char *name = (azColName && azColName[i]) ? azColName[i] : "";
        const char *value = (argv && argv[i]) ? argv[i] : "NULL";
        std::cout << name << "=" << value;
    }
    std::cout << "\n";
    return 0;
}

bool is_exit_command(const std::string &line) {
    return line == "exit" || line == "quit";
}

}  // namespace

int main(int argc, char **argv) {
    const char *host = "127.0.0.1";
    int port = 9000;

    if (argc >= 2) {
        host = argv[1];
    }
    if (argc >= 3) {
        port = std::stoi(argv[2]);
    }

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot open FlexQL at " << host << ":" << port << "\n";
        return 1;
    }

    std::cout << "Connected to FlexQL at " << host << ":" << port << "\n";
    std::cout << "Enter SQL ending with ';'. Type 'exit' or 'quit' to stop.\n";

    while (true) {
        std::string sql;
        std::string line;

        std::cout << "flexql> " << std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (is_exit_command(line)) {
            break;
        }

        sql += line;
        while (sql.find(';') == std::string::npos) {
            std::cout << "     -> " << std::flush;
            if (!std::getline(std::cin, line)) {
                break;
            }
            if (is_exit_command(line) && sql.empty()) {
                break;
            }
            sql += "\n";
            sql += line;
        }

        if (sql.empty()) {
            continue;
        }
        if (is_exit_command(sql)) {
            break;
        }

        char *errmsg = nullptr;
        int rc = flexql_exec(db, sql.c_str(), print_row_callback, nullptr, &errmsg);
        if (rc != FLEXQL_OK) {
            std::cerr << "ERROR: " << (errmsg ? errmsg : "unknown error") << "\n";
            if (errmsg) {
                flexql_free(errmsg);
            }
            continue;
        }
        std::cout << "OK\n";
    }

    flexql_close(db);
    return 0;
}
