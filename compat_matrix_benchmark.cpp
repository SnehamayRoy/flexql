#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <sstream>
#include <string>
#include <vector>
#include "flexql.h"

using clk = std::chrono::steady_clock;

static long long ms_now() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        clk::now().time_since_epoch()).count();
}
static long long us_now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        clk::now().time_since_epoch()).count();
}

struct Config {
    const char *host = "127.0.0.1";
    int port = 9000;
    std::vector<long long> scales{100, 200000, 1000000, 10000000};
    int insert_batch = 2000;
    int lookup_samples = 200;
    int join_cap = 20000;
    long long concurrency_cap = 40000;
    int concurrency_threads = 4;
    int concurrency_batch = 100;
    bool strict = false;
    bool run_checks = true;
    bool run_reads = true;
    bool run_joins = true;
    bool run_concurrency = true;
};

struct Measure {
    std::string label;
    long long rows = 0;
    long long ms = 0;
    long long per_sec = 0;
};

struct CheckResult {
    std::string label;
    bool ok = false;
};

static int cb_count(void *d, int, char **, char **) {
    ++*(long long *)d;
    return 0;
}

static int cb_collect_first(void *d, int argc, char **argv, char **) {
    std::string &out = *(std::string *)d;
    if (out.empty() && argc > 0 && argv[0]) out = argv[0];
    return 0;
}

static bool exec_ok(FlexQL *db, const std::string &sql, std::string *errmsg = nullptr) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (rc != FLEXQL_OK) {
        if (errmsg) *errmsg = err ? err : "unknown error";
        if (err) flexql_free(err);
        return false;
    }
    if (err) flexql_free(err);
    return true;
}

static bool exec_expect_fail(FlexQL *db, const std::string &sql) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &err);
    if (err) flexql_free(err);
    return rc != FLEXQL_OK;
}

static Measure run_count_query(FlexQL *db, const std::string &label, const std::string &sql) {
    Measure m;
    m.label = label;
    char *err = nullptr;
    long long rows = 0;
    auto t0 = ms_now();
    int rc = flexql_exec(db, sql.c_str(), cb_count, &rows, &err);
    m.ms = ms_now() - t0;
    m.rows = (rc == FLEXQL_OK) ? rows : -1;
    m.per_sec = (m.ms > 0 && rows >= 0) ? rows * 1000LL / m.ms : rows;
    if (err) flexql_free(err);
    return m;
}

static long long run_lookup_avg_us(FlexQL *db, const std::string &table, long long rows, int samples) {
    long long total = 0;
    for (int i = 0; i < samples; ++i) {
        long long id = 1 + ((long long)i * 7919 % std::max(1LL, rows));
        std::string sql = "SELECT NAME FROM " + table + " WHERE ID = " + std::to_string(id) + ";";
        auto t0 = us_now();
        std::string first;
        char *err = nullptr;
        flexql_exec(db, sql.c_str(), cb_collect_first, &first, &err);
        if (err) flexql_free(err);
        total += us_now() - t0;
    }
    return total / std::max(1, samples);
}

static std::string upper(std::string s) {
    for (char &c : s) c = (char)toupper((unsigned char)c);
    return s;
}

static std::string prefix_for(long long scale) {
    static std::atomic<long long> seq{0};
    long long id = ++seq;
    return upper("BM" + std::to_string(ms_now()) + "_" + std::to_string(scale) + "_" + std::to_string(id));
}

static bool run_correctness_suite(FlexQL *db, const std::string &pfx) {
    std::string users = pfx + "_USERS";
    std::string orders = pfx + "_ORDERS";
    std::string types = pfx + "_TYPES";

    if (!exec_ok(db, "CREATE TABLE " + users + "(ID DECIMAL, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);")) return false;
    if (!exec_ok(db, "INSERT INTO " + users + " VALUES "
                     "(1,'Alice',1200,1893456000),"
                     "(2,'Bob',450,1893456000),"
                     "(3,'Carol',2200,1893456000),"
                     "(4,'Dave',800,1893456000);")) return false;
    if (run_count_query(db, "users_count", "SELECT * FROM " + users + ";").rows != 4) return false;
    if (run_count_query(db, "where_exact", "SELECT * FROM " + users + " WHERE ID = 2;").rows != 1) return false;
    if (!exec_ok(db, "CREATE TABLE " + orders + "(OID DECIMAL, UID DECIMAL, AMT DECIMAL, EXPIRES_AT DECIMAL);")) return false;
    if (!exec_ok(db, "INSERT INTO " + orders + " VALUES "
                     "(101,1,50,1893456000),"
                     "(102,1,150,1893456000),"
                     "(103,3,500,1893456000);")) return false;
    if (run_count_query(db, "join_count",
                        "SELECT " + users + ".NAME, " + orders + ".AMT FROM " + users +
                        " INNER JOIN " + orders + " ON " + users + ".ID = " + orders + ".UID;").rows != 3) return false;
    if (!exec_ok(db, "CREATE TABLE " + types + "(A INT, B DECIMAL, C VARCHAR(64), D DATETIME);")) return false;
    if (!exec_ok(db, "INSERT INTO " + types + " VALUES (42, 3.14, 'hello', 1893456000);")) return false;
    if (!exec_expect_fail(db, "SELECT NOPE FROM " + users + ";")) return false;
    if (!exec_expect_fail(db, "SELECT * FROM " + pfx + "_MISSING;")) return false;
    if (!exec_expect_fail(db, "INSERT INTO " + users + " VALUES (1,2);")) return false;
    return true;
}

static std::vector<CheckResult> run_edge_error_suite(FlexQL *db, const std::string &pfx) {
    std::vector<CheckResult> out;
    std::string edge = pfx + "_EDGE";

    bool setup = exec_ok(db, "CREATE TABLE " + edge + "(ID DECIMAL, NAME VARCHAR(32), SCORE DECIMAL, EXPIRES_AT DECIMAL);") &&
                 exec_ok(db, "INSERT INTO " + edge + " VALUES "
                             "(1,'lo',0,1893456000),"
                             "(2,'mid',500,1893456000),"
                             "(3,'hi',999999,1893456000);");
    out.push_back({"edge_setup", setup});
    if (!setup) return out;

    out.push_back({"edge_empty_result",
                   run_count_query(db, "edge_empty_result", "SELECT * FROM " + edge + " WHERE ID = 9999;").rows == 0});
    out.push_back({"edge_lower_bound",
                   run_count_query(db, "edge_lower_bound", "SELECT * FROM " + edge + " WHERE ID <= 1;").rows == 1});
    out.push_back({"edge_upper_bound",
                   run_count_query(db, "edge_upper_bound", "SELECT * FROM " + edge + " WHERE SCORE >= 999999;").rows == 1});
    out.push_back({"error_missing_table", exec_expect_fail(db, "SELECT * FROM " + pfx + "_NO_TABLE;")});
    out.push_back({"error_missing_column", exec_expect_fail(db, "SELECT NOPE FROM " + edge + ";")});
    out.push_back({"error_insert_arity", exec_expect_fail(db, "INSERT INTO " + edge + " VALUES (1,2);")});
    return out;
}

static bool populate_main_table(FlexQL *db, const std::string &table, long long n, int batch, Measure &out) {
    if (!exec_ok(db, "CREATE TABLE " + table + "(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), SCORE DECIMAL, EXPIRES_AT DECIMAL);"))
        return false;

    auto t0 = ms_now();
    long long ins = 0;
    while (ins < n) {
        std::ostringstream ss;
        ss << "INSERT INTO " << table << " VALUES ";
        int b = 0;
        while (b < batch && ins < n) {
            long long id = ins + 1;
            if (b) ss << ",";
            ss << "(" << id << ",'user" << id << "','u" << id << "@test.com',"
               << (1000 + id % 10000) << ",1893456000)";
            ++ins;
            ++b;
        }
        ss << ";";
        if (!exec_ok(db, ss.str())) return false;
    }

    out.label = "write";
    out.rows = n;
    out.ms = ms_now() - t0;
    out.per_sec = out.ms ? n * 1000LL / out.ms : n;
    return true;
}

static void run_read_suite(FlexQL *db, const std::string &table, long long rows,
                           int lookup_samples, std::vector<Measure> &out) {
    std::cout << "    [read] full_scan\n" << std::flush;
    out.push_back(run_count_query(db, "read_full_scan", "SELECT * FROM " + table + ";"));
    std::cout << "    [read] projected\n" << std::flush;
    out.push_back(run_count_query(db, "read_projected", "SELECT ID, NAME FROM " + table + ";"));
    std::cout << "    [read] filter_selective\n" << std::flush;
    out.push_back(run_count_query(db, "read_filter_selective", "SELECT ID, NAME FROM " + table + " WHERE SCORE <= 1500;"));
    std::cout << "    [read] filter_broad\n" << std::flush;
    out.push_back(run_count_query(db, "read_filter_broad", "SELECT * FROM " + table + " WHERE SCORE > 5000;"));
    std::cout << "    [read] pk_range\n" << std::flush;
    out.push_back(run_count_query(db, "read_pk_range", "SELECT ID FROM " + table + " WHERE ID >= " + std::to_string(std::max(1LL, rows / 2)) + ";"));
    std::cout << "    [read] empty_exact\n" << std::flush;
    out.push_back(run_count_query(db, "read_empty_exact", "SELECT ID FROM " + table + " WHERE ID = " + std::to_string(rows + 1) + ";"));

    Measure pk;
    pk.label = "read_pk_lookup_avg_us";
    pk.rows = lookup_samples;
    std::cout << "    [read] pk_lookup_avg\n" << std::flush;
    pk.ms = run_lookup_avg_us(db, table, rows, lookup_samples);
    pk.per_sec = 0;
    out.push_back(pk);
}

static bool populate_join_tables(FlexQL *db, const std::string &users, const std::string &orders, long long n) {
    if (!exec_ok(db, "CREATE TABLE " + users + "(UID DECIMAL, NAME VARCHAR(32), TIER DECIMAL);")) return false;
    if (!exec_ok(db, "CREATE TABLE " + orders + "(OID DECIMAL, UID DECIMAL, TOTAL DECIMAL, EXPIRES_AT DECIMAL);")) return false;

    const int user_batch = 1000;
    for (long long base = 1; base <= n; base += user_batch) {
        std::ostringstream ss;
        ss << "INSERT INTO " << users << " VALUES ";
        bool first = true;
        for (long long i = base; i < base + user_batch && i <= n; ++i) {
            if (!first) ss << ",";
            first = false;
            ss << "(" << i << ",'u" << i << "'," << (i % 5) << ")";
        }
        ss << ";";
        if (!exec_ok(db, ss.str())) return false;
    }

    long long oid = 1;
    const int order_batch_users = 300;
    for (long long base = 1; base <= n; base += order_batch_users) {
        std::ostringstream ss;
        ss << "INSERT INTO " << orders << " VALUES ";
        bool first = true;
        for (long long i = base; i < base + order_batch_users && i <= n; ++i) {
            for (int j = 0; j < 3; ++j) {
                if (!first) ss << ",";
                first = false;
                ss << "(" << oid++ << "," << i << "," << (50 + (i + j) % 950) << ",1893456000)";
            }
        }
        ss << ";";
        if (!exec_ok(db, ss.str())) return false;
    }
    return true;
}

static void run_join_suite(FlexQL *db, const std::string &users, const std::string &orders,
                           std::vector<Measure> &out) {
    std::cout << "    [join] point\n" << std::flush;
    out.push_back(run_count_query(db, "join_point",
                                  "SELECT " + users + ".NAME, " + orders + ".OID FROM " + users +
                                  " INNER JOIN " + orders + " ON " + users + ".UID = " + orders + ".UID WHERE " +
                                  users + ".UID = 1;"));
    std::cout << "    [join] selective\n" << std::flush;
    out.push_back(run_count_query(db, "join_selective",
                                  "SELECT " + users + ".NAME, " + orders + ".TOTAL FROM " + users +
                                  " INNER JOIN " + orders + " ON " + users + ".UID = " + orders + ".UID WHERE " +
                                  orders + ".TOTAL > 900;"));
    std::cout << "    [join] broad\n" << std::flush;
    out.push_back(run_count_query(db, "join_broad",
                                  "SELECT " + users + ".NAME, " + orders + ".TOTAL FROM " + users +
                                  " INNER JOIN " + orders + " ON " + users + ".UID = " + orders + ".UID;"));
}

struct CArg {
    const char *host;
    int port;
    int tid;
    long long rows_each;
    int batch_sz;
    std::string table_name;
    std::atomic<long long> *total;
    std::atomic<int> *errs;
};

static void *cworker(void *varg) {
    auto *a = (CArg *)varg;
    FlexQL *db = nullptr;
    if (flexql_open(a->host, a->port, &db) != FLEXQL_OK) { ++(*a->errs); return nullptr; }
    long long base = (long long)a->tid * a->rows_each;
    long long ins = 0;
    while (ins < a->rows_each) {
        std::ostringstream ss;
        ss << "INSERT INTO " << a->table_name << " VALUES ";
        int b = 0;
        while (b < a->batch_sz && ins < a->rows_each) {
            long long id = base + ins + 1;
            if (b) ss << ",";
            ss << "(" << id << ",'t" << a->tid << "r" << ins << "'," << (id % 10000) << ",1893456000)";
            ++ins;
            ++b;
        }
        ss << ";";
        char *err = nullptr;
        if (flexql_exec(db, ss.str().c_str(), nullptr, nullptr, &err) != FLEXQL_OK) {
            ++(*a->errs);
            if (err) flexql_free(err);
            break;
        }
        (*a->total) += b;
    }
    flexql_close(db);
    return nullptr;
}

static Measure run_concurrency_suite(const Config &cfg, const std::string &table, long long total_rows) {
    Measure m;
    m.label = "concurrency_write";
    FlexQL *setup = nullptr;
    if (flexql_open(cfg.host, cfg.port, &setup) != FLEXQL_OK) {
        m.rows = -1;
        return m;
    }
    exec_ok(setup, "CREATE TABLE " + table + "(ID DECIMAL, NAME VARCHAR(64), VAL DECIMAL, EXPIRES_AT DECIMAL);");

    long long per_thread = total_rows / std::max(1, cfg.concurrency_threads);
    std::atomic<long long> total{0};
    std::atomic<int> errs{0};
    std::vector<CArg> args(cfg.concurrency_threads);
    std::vector<pthread_t> threads(cfg.concurrency_threads);
    auto t0 = ms_now();
    for (int i = 0; i < cfg.concurrency_threads; ++i) {
        args[i] = {cfg.host, cfg.port, i, per_thread, cfg.concurrency_batch, table, &total, &errs};
        pthread_create(&threads[i], nullptr, cworker, &args[i]);
    }
    for (int i = 0; i < cfg.concurrency_threads; ++i) pthread_join(threads[i], nullptr);
    m.ms = ms_now() - t0;
    m.rows = total.load();
    m.per_sec = m.ms ? m.rows * 1000LL / m.ms : m.rows;
    long long counted = run_count_query(setup, "concurrency_validate", "SELECT * FROM " + table + ";").rows;
    if (errs.load() != 0 || counted != m.rows) m.rows = -1;
    flexql_close(setup);
    return m;
}

static std::vector<long long> parse_scales(const std::string &s) {
    std::vector<long long> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        out.push_back(atoll(item.c_str()));
    }
    if (out.empty()) out = {100, 200000, 1000000, 10000000};
    return out;
}

static void print_measure(const Measure &m) {
    std::cout << "  " << std::left << std::setw(24) << m.label
              << " rows=" << m.rows
              << " ms=" << m.ms;
    if (m.per_sec > 0) std::cout << " rate=" << m.per_sec << "/s";
    std::cout << "\n" << std::flush;
}

static void print_checks(const std::vector<CheckResult> &checks) {
    for (const auto &c : checks) {
        std::cout << "  " << std::left << std::setw(24) << c.label
                  << (c.ok ? "PASS" : "FAIL") << "\n";
    }
    std::cout << std::flush;
}

int main(int argc, char **argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--host" && i + 1 < argc) cfg.host = argv[++i];
        else if (a == "--port" && i + 1 < argc) cfg.port = atoi(argv[++i]);
        else if (a == "--matrix" && i + 1 < argc) cfg.scales = parse_scales(argv[++i]);
        else if (a == "--batch" && i + 1 < argc) cfg.insert_batch = atoi(argv[++i]);
        else if (a == "--lookup-samples" && i + 1 < argc) cfg.lookup_samples = atoi(argv[++i]);
        else if (a == "--join-cap" && i + 1 < argc) cfg.join_cap = atoi(argv[++i]);
        else if (a == "--concurrency-cap" && i + 1 < argc) cfg.concurrency_cap = atoll(argv[++i]);
        else if (a == "--concurrency-threads" && i + 1 < argc) cfg.concurrency_threads = atoi(argv[++i]);
        else if (a == "--concurrency-batch" && i + 1 < argc) cfg.concurrency_batch = atoi(argv[++i]);
        else if (a == "--skip-checks") cfg.run_checks = false;
        else if (a == "--skip-reads") cfg.run_reads = false;
        else if (a == "--skip-joins") cfg.run_joins = false;
        else if (a == "--skip-concurrency") cfg.run_concurrency = false;
        else if (a == "--strict") cfg.strict = true;
    }

    FlexQL *db = nullptr;
    if (flexql_open(cfg.host, cfg.port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL at " << cfg.host << ":" << cfg.port << "\n";
        return 1;
    }

    std::string smoke_prefix = prefix_for(0);
    if (cfg.run_checks) {
        bool ok = run_correctness_suite(db, smoke_prefix);
        std::cout << "[correctness] " << (ok ? "PASS" : "FAIL") << "\n";
        if (!ok) {
            flexql_close(db);
            return 1;
        }
        print_checks(run_edge_error_suite(db, smoke_prefix + "_CHK"));
    } else {
        std::cout << "[correctness] skipped\n";
    }

    for (long long scale : cfg.scales) {
        std::string pfx = prefix_for(scale);
        std::cout << "\n[matrix] scale=" << scale << "\n";

        Measure write;
        if (!populate_main_table(db, pfx + "_DATA", scale, cfg.insert_batch, write)) {
            std::cout << "  write FAILED\n";
            continue;
        }
        print_measure(write);

        if (cfg.run_reads) {
            std::vector<Measure> reads;
            std::cout << "  [phase] reads\n" << std::flush;
            run_read_suite(db, pfx + "_DATA", scale, cfg.lookup_samples, reads);
            for (const auto &m : reads) print_measure(m);
        }

        long long join_rows = cfg.strict ? scale : std::min<long long>(scale, cfg.join_cap);
        if (cfg.run_joins && join_rows > 0 && populate_join_tables(db, pfx + "_JU", pfx + "_JO", join_rows)) {
            std::vector<Measure> joins;
            std::cout << "  [phase] joins n=" << join_rows << "\n" << std::flush;
            run_join_suite(db, pfx + "_JU", pfx + "_JO", joins);
            for (const auto &m : joins) {
                Measure tagged = m;
                tagged.label += "(n=" + std::to_string(join_rows) + ")";
                print_measure(tagged);
            }
        }

        long long conc_rows = cfg.strict ? scale : std::min<long long>(scale, cfg.concurrency_cap);
        if (cfg.run_concurrency && conc_rows > 0) {
            std::cout << "  [phase] concurrency rows=" << conc_rows << "\n" << std::flush;
            Measure conc = run_concurrency_suite(cfg, pfx + "_CP", conc_rows);
            conc.label += "(rows=" + std::to_string(conc_rows) + ")";
            print_measure(conc);
        }
    }

    flexql_close(db);
    return 0;
}
