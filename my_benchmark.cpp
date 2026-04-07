/*
 * FlexQL Extended Benchmark Suite
 *
 * Covers:
 *   1. Correctness / unit tests   (all types, all WHERE ops, JOIN, errors)
 *   2. Bulk write throughput       (batch INSERT, configurable N and batch size)
 *   3. Read performance            (full scan, index lookup, scan+filter)
 *   4. Cache warmup curve          (latency of repeated identical queries)
 *   5. WHERE operator exhaustive   (=, >, <, >=, <=, boundary values)
 *   6. JOIN throughput             (index-accelerated, non-index, JOIN+WHERE)
 *   7. Concurrent write stress     (T threads, each with N rows, own connection)
 *   8. Mixed read/write            (2:1 write/read ratio, interleaved)
 *   9. Large batch INSERT          (1000-row batch, measures WAL batching)
 *  10. Expiry column queries       (EXPIRES_AT pattern used in spec)
 *
 * Usage:
 *   ./my_benchmark                     full suite, 100 000 rows
 *   ./my_benchmark N                   full suite, N rows
 *   ./my_benchmark --unit-test
 *   ./my_benchmark --write N [batch]
 *   ./my_benchmark --read
 *   ./my_benchmark --where
 *   ./my_benchmark --join [N]
 *   ./my_benchmark --concurrent T N
 *   ./my_benchmark --mixed N
 *   ./my_benchmark --cache
 *   ./my_benchmark --expiry N
 *   ./my_benchmark --large-batch N
 *   ./my_benchmark --read-profile N
 *   ./my_benchmark --join-profile N
 *   ./my_benchmark --pk-profile N [lookups]
 *   ./my_benchmark --tiny-query-profile N [lookups]
 *   ./my_benchmark --concurrency-profile T N [batch]
 */

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
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

/* ── global pass/fail counters (protected by a mutex in threaded sections) */
static std::atomic<int> g_pass{0}, g_fail{0};
static std::mutex        g_print_mu;

static void pr(const std::string &s) {
    std::lock_guard<std::mutex> lk(g_print_mu);
    puts(s.c_str());
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  Helper wrappers                                                             */
/* ─────────────────────────────────────────────────────────────────────────── */

struct RowColl { std::vector<std::string> rows; };

static int cb_count(void *d, int, char **, char **) {
    ++*(long long *)d; return 0;
}
static int cb_coll(void *d, int argc, char **argv, char **) {
    std::string r;
    for (int i = 0; i < argc; ++i) { if (i) r += "|"; r += argv[i] ? argv[i] : "NULL"; }
    ((RowColl *)d)->rows.push_back(r);
    return 0;
}
static int cb_noop(void *, int, char **, char **) { return 0; }

/* Execute SQL, return true on success. Prints [PASS/FAIL] if label given. */
static bool xok(FlexQL *db, const std::string &sql, const std::string &label = "") {
    char *err = nullptr;
    auto t0 = ms_now();
    int rc = flexql_exec(db, sql.c_str(), cb_noop, nullptr, &err);
    long long dt = ms_now() - t0;
    bool ok = (rc == FLEXQL_OK);
    if (!label.empty()) {
        std::string pfx = ok ? "[PASS] " : "[FAIL] ";
        pr(pfx + label + " (" + std::to_string(dt) + " ms)");
        if (!ok && err) pr("       " + std::string(err));
        (ok ? g_pass : g_fail)++;
    }
    if (err) flexql_free(err);
    return ok;
}

/* Execute and expect failure. */
static bool xfail(FlexQL *db, const std::string &sql, const std::string &label) {
    char *err = nullptr;
    int rc = flexql_exec(db, sql.c_str(), cb_noop, nullptr, &err);
    if (err) flexql_free(err);
    bool ok = (rc != FLEXQL_OK);
    pr(std::string(ok ? "[PASS] " : "[FAIL] ") + label + (ok ? "" : " (expected error, got OK)"));
    (ok ? g_pass : g_fail)++;
    return ok;
}

static std::vector<std::string> qrows(FlexQL *db, const std::string &sql) {
    RowColl c; char *err = nullptr;
    flexql_exec(db, sql.c_str(), cb_coll, &c, &err);
    if (err) flexql_free(err);
    return c.rows;
}
static long long qcount(FlexQL *db, const std::string &sql) {
    long long n = 0; char *err = nullptr;
    flexql_exec(db, sql.c_str(), cb_count, &n, &err);
    if (err) flexql_free(err);
    return n;
}

static bool check_eq(const std::string &lbl, std::vector<std::string> got,
                      std::vector<std::string> exp) {
    std::sort(got.begin(), got.end());
    std::sort(exp.begin(), exp.end());
    bool ok = (got == exp);
    pr(std::string(ok ? "[PASS] " : "[FAIL] ") + lbl);
    if (!ok) {
        std::string e, g;
        for (auto &s : exp) e += "{" + s + "} ";
        for (auto &s : got) g += "{" + s + "} ";
        pr("       exp: " + e); pr("       got: " + g);
    }
    (ok ? g_pass : g_fail)++;
    return ok;
}

static bool check_cnt(const std::string &lbl, long long got, long long exp) {
    bool ok = (got == exp);
    pr(std::string(ok ? "[PASS] " : "[FAIL] ") + lbl +
       " (got=" + std::to_string(got) + " exp=" + std::to_string(exp) + ")");
    (ok ? g_pass : g_fail)++;
    return ok;
}

static bool check_nonempty(const std::string &lbl, const std::vector<std::string> &rows) {
    bool ok = !rows.empty();
    pr(std::string(ok ? "[PASS] " : "[FAIL] ") + lbl);
    (ok ? g_pass : g_fail)++;
    return ok;
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  1. Unit / correctness tests                                                 */
/* ─────────────────────────────────────────────────────────────────────────── */

static void unit_tests(FlexQL *db) {
    pr("\n=== 1. Unit / Correctness Tests ===\n");

    /* --- CREATE TABLE --------------------------------------------------- */
    xok(db, "CREATE TABLE USERS(ID DECIMAL, NAME VARCHAR(64), "
            "BALANCE DECIMAL, EXPIRES_AT DECIMAL);", "CREATE USERS");

    /* --- INSERT (single) ----------------------------------------------- */
    xok(db, "INSERT INTO USERS VALUES (1,'Alice',1200,9999999999);",  "INSERT Alice");
    xok(db, "INSERT INTO USERS VALUES (2,'Bob',450,9999999999);",     "INSERT Bob");
    xok(db, "INSERT INTO USERS VALUES (3,'Carol',2200,9999999999);",  "INSERT Carol");
    xok(db, "INSERT INTO USERS VALUES (4,'Dave',800,9999999999);",    "INSERT Dave");
    xok(db, "INSERT INTO USERS VALUES (5,'Eve',5000,9999999999);",    "INSERT Eve");

    /* --- SELECT * -------------------------------------------------------- */
    check_cnt("Row count = 5", qcount(db, "SELECT * FROM USERS;"), 5);

    /* --- WHERE = (on non-PK, scan path) --------------------------------- */
    auto r = qrows(db, "SELECT NAME, BALANCE FROM USERS WHERE ID = 2;");
    check_eq("WHERE = exact", r, {"Bob|450"});

    /* --- WHERE operators ------------------------------------------------ */
    check_cnt("WHERE >  1000", qcount(db, "SELECT * FROM USERS WHERE BALANCE > 1000;"),  3);
    check_cnt("WHERE >= 1200", qcount(db, "SELECT * FROM USERS WHERE BALANCE >= 1200;"), 3);
    check_cnt("WHERE <  1000", qcount(db, "SELECT * FROM USERS WHERE BALANCE < 1000;"),  2);
    check_cnt("WHERE <= 800",  qcount(db, "SELECT * FROM USERS WHERE BALANCE <= 800;"),  2);
    check_cnt("Empty result",  qcount(db, "SELECT * FROM USERS WHERE BALANCE > 99999;"), 0);

    /* --- Specific column SELECT ----------------------------------------- */
    r = qrows(db, "SELECT NAME FROM USERS WHERE BALANCE > 1000;");
    check_cnt("SELECT col, 3 rows", (long long)r.size(), 3);

    /* --- Batch INSERT ---------------------------------------------------- */
    xok(db, "INSERT INTO USERS VALUES "
            "(10,'Frank',300,9999999999),"
            "(11,'Grace',700,9999999999),"
            "(12,'Hank',1500,9999999999);",
        "Batch INSERT 3 rows");
    r = qrows(db, "SELECT NAME FROM USERS WHERE ID = 11;");
    check_eq("Batch row readable", r, {"Grace"});
    check_cnt("Total rows = 8", qcount(db, "SELECT * FROM USERS;"), 8);

    /* --- ORDERS table for JOIN ------------------------------------------ */
    xok(db, "CREATE TABLE ORDERS(OID DECIMAL, UID DECIMAL, AMT DECIMAL, "
            "EXPIRES_AT DECIMAL);", "CREATE ORDERS");
    xok(db, "INSERT INTO ORDERS VALUES (101,1,50,9999999999);",   "INSERT O101");
    xok(db, "INSERT INTO ORDERS VALUES (102,1,150,9999999999);",  "INSERT O102");
    xok(db, "INSERT INTO ORDERS VALUES (103,3,500,9999999999);",  "INSERT O103");
    xok(db, "INSERT INTO ORDERS VALUES (104,5,9999,9999999999);", "INSERT O104");

    /* --- INNER JOIN ----------------------------------------------------- */
    r = qrows(db, "SELECT USERS.NAME, ORDERS.AMT FROM USERS "
                  "INNER JOIN ORDERS ON USERS.ID = ORDERS.UID "
                  "WHERE ORDERS.AMT > 900;");
    check_cnt("JOIN+WHERE>900 (Eve,9999)", (long long)r.size(), 1);

    r = qrows(db, "SELECT ORDERS.OID FROM USERS "
                  "INNER JOIN ORDERS ON USERS.ID = ORDERS.UID "
                  "WHERE USERS.ID = 1;");
    check_cnt("JOIN user1 has 2 orders", (long long)r.size(), 2);

    r = qrows(db, "SELECT USERS.NAME, ORDERS.AMT FROM USERS "
                  "INNER JOIN ORDERS ON USERS.ID = ORDERS.UID "
                  "WHERE ORDERS.AMT > 99999;");
    check_cnt("JOIN no matches", (long long)r.size(), 0);

    /* --- Type tests ------------------------------------------------------ */
    xok(db, "CREATE TABLE TYPES_TEST(A INT, B DECIMAL, C VARCHAR(64), D DATETIME);",
        "CREATE TYPES_TEST");
    xok(db, "INSERT INTO TYPES_TEST VALUES (42, 3.14, 'hello world', 1893456000);",
        "INSERT all types");
    r = qrows(db, "SELECT A, B, C, D FROM TYPES_TEST WHERE A = 42;");
    check_nonempty("All-types roundtrip", r);

    /* --- INT roundtrip --------------------------------------------------- */
    xok(db, "CREATE TABLE INTT(ID INT, V INT);", "CREATE INTT");
    xok(db, "INSERT INTO INTT VALUES (1, 2147483647);", "INSERT max INT");
    r = qrows(db, "SELECT V FROM INTT WHERE ID = 1;");
    check_eq("INT max value", r, {"2147483647"});

    /* --- VARCHAR boundary ----------------------------------------------- */
    std::string long_str(200, 'X');
    xok(db, "CREATE TABLE VCT(ID INT, D VARCHAR(255));", "CREATE VCT");
    xok(db, "INSERT INTO VCT VALUES (1, '" + long_str + "');", "INSERT 200-char VARCHAR");
    r = qrows(db, "SELECT D FROM VCT WHERE ID = 1;");
    check_eq("VARCHAR 200-char roundtrip", r, {long_str});

    /* --- DATETIME roundtrip --------------------------------------------- */
    xok(db, "CREATE TABLE DTT(ID INT, TS DATETIME);", "CREATE DTT");
    xok(db, "INSERT INTO DTT VALUES (1, 1893456000);", "INSERT DATETIME");
    r = qrows(db, "SELECT TS FROM DTT WHERE ID = 1;");
    check_eq("DATETIME roundtrip", r, {"1893456000"});

    /* --- Error rejection ------------------------------------------------- */
    xfail(db, "SELECT NOPE FROM USERS;",        "Unknown column rejected");
    xfail(db, "SELECT * FROM GHOST_TABLE;",     "Missing table rejected");
    xfail(db, "INSERT INTO USERS VALUES (1,2);","Column count mismatch rejected");

    pr("\n--- Unit Tests done ---\n");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  2. Bulk write throughput                                                    */
/* ─────────────────────────────────────────────────────────────────────────── */

static bool write_bench(FlexQL *db, long long n, int batch = 100) {
    pr("\n=== 2. Write Throughput (n=" + std::to_string(n) +
       " batch=" + std::to_string(batch) + ") ===\n");

    if (!xok(db, "CREATE TABLE BIG(ID DECIMAL, NAME VARCHAR(64), "
                 "EMAIL VARCHAR(64), SCORE DECIMAL, EXPIRES_AT DECIMAL);",
             "CREATE BIG")) return false;

    long long t0 = ms_now(), ins = 0;
    long long prog_step = std::max(1LL, n / 10), next_prog = prog_step;

    while (ins < n) {
        std::ostringstream ss; ss << "INSERT INTO BIG VALUES ";
        int b = 0;
        while (b < batch && ins < n) {
            long long id = ins + 1;
            if (b) ss << ",";
            ss << "(" << id << ",'user" << id << "','u" << id
               << "@test.com'," << (1000 + id % 10000) << ",9999999999)";
            ++ins; ++b;
        }
        ss << ";";
        if (!xok(db, ss.str())) { pr("[FAIL] batch insert"); return false; }
        if (ins >= next_prog || ins == n) {
            pr("  progress: " + std::to_string(ins) + "/" + std::to_string(n));
            next_prog += prog_step;
        }
    }

    long long dt = ms_now() - t0;
    long long tput = dt ? n * 1000LL / dt : n;
    pr("[INFO] Rows=" + std::to_string(n) + " elapsed=" + std::to_string(dt) +
       "ms throughput=" + std::to_string(tput) + " rows/sec");
    return true;
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  3. Read performance                                                         */
/* ─────────────────────────────────────────────────────────────────────────── */

static void read_bench(FlexQL *db) {
    pr("\n=== 3. Read / SELECT Performance ===\n");

    auto bench = [&](const std::string &label, const std::string &sql) {
        long long n = 0; char *err = nullptr;
        auto t0 = ms_now();
        flexql_exec(db, sql.c_str(), cb_count, &n, &err);
        long long dt = ms_now() - t0;
        if (err) flexql_free(err);
        pr("[INFO] " + label + ": " + std::to_string(n) + " rows in " +
           std::to_string(dt) + " ms (" +
           std::to_string(dt ? n * 1000LL / dt : n) + " r/s)");
    };

    bench("Full scan SELECT *",              "SELECT * FROM BIG;");
    bench("WHERE SCORE>5000 (scan)",         "SELECT * FROM BIG WHERE SCORE > 5000;");
    bench("WHERE SCORE<=2000 (scan)",        "SELECT ID,NAME FROM BIG WHERE SCORE <= 2000;");
    bench("SELECT two cols, no filter",      "SELECT ID, NAME FROM BIG;");

    /* Primary key point lookup (index path) */
    {
        auto t0 = us_now();
        auto r = qrows(db, "SELECT NAME FROM BIG WHERE ID = 500;");
        long long dt = us_now() - t0;
        pr("[INFO] PK lookup ID=500: " + (r.empty() ? "NOT FOUND" : r[0]) +
           " in " + std::to_string(dt) + " µs");
    }

    /* 100 random PK lookups */
    {
        long long total_us = 0;
        for (int i = 1; i <= 100; ++i) {
            auto t0 = us_now();
            qrows(db, "SELECT NAME FROM BIG WHERE ID = " + std::to_string(i * 37) + ";");
            total_us += us_now() - t0;
        }
        pr("[INFO] 100 PK lookups: avg " + std::to_string(total_us / 100) + " µs each");
    }
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  4. Cache warmup curve                                                       */
/* ─────────────────────────────────────────────────────────────────────────── */

static void cache_bench(FlexQL *db) {
    pr("\n=== 4. Cache Warmup Curve ===\n");
    const std::string sql = "SELECT * FROM BIG WHERE SCORE = 1337;";
    std::vector<long long> lat;
    for (int i = 0; i < 20; ++i) {
        long long n = 0; char *err = nullptr;
        auto t0 = ms_now();
        flexql_exec(db, sql.c_str(), cb_count, &n, &err);
        lat.push_back(ms_now() - t0);
        if (err) flexql_free(err);
    }
    pr("  run[1-5]:  " + std::to_string(lat[0]) + " " + std::to_string(lat[1]) +
       " " + std::to_string(lat[2]) + " " + std::to_string(lat[3]) + " " +
       std::to_string(lat[4]) + " ms");
    long long avg_last = 0;
    for (int i = 10; i < 20; ++i) avg_last += lat[i];
    avg_last /= 10;
    pr("  avg(runs 11-20): " + std::to_string(avg_last) + " ms (first: " +
       std::to_string(lat[0]) + " ms)");
    if (avg_last <= lat[0])
        pr("[INFO] Cache speedup observed ✓");
    else
        pr("[INFO] Already hot after first run (buffer pool warm from prior tests)");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  5. WHERE operator exhaustive                                                */
/* ─────────────────────────────────────────────────────────────────────────── */

static void where_bench(FlexQL *db) {
    pr("\n=== 5. WHERE Operator Exhaustive ===\n");

    xok(db, "CREATE TABLE WTEST(ID DECIMAL, VAL DECIMAL, TAG VARCHAR(16));",
        "CREATE WTEST");

    /* Insert 1000 rows in one big batch */
    std::ostringstream ss; ss << "INSERT INTO WTEST VALUES ";
    for (int i = 1; i <= 1000; ++i) {
        if (i > 1) ss << ",";
        ss << "(" << i << "," << i * 10 << ",'t" << i << "')";
    }
    ss << ";";
    xok(db, ss.str(), "INSERT 1000 rows (batch)");

    struct Case { std::string sql, lbl; long long exp; };
    std::vector<Case> cases = {
        {"SELECT * FROM WTEST WHERE VAL = 5000;",   "= 5000  (1 row)",  1},
        {"SELECT * FROM WTEST WHERE VAL = 1;",      "= 1 (no match)",   0},
        {"SELECT * FROM WTEST WHERE VAL > 9000;",   "> 9000  (100)",    100},
        {"SELECT * FROM WTEST WHERE VAL < 100;",    "< 100   (9)",      9},
        {"SELECT * FROM WTEST WHERE VAL >= 9990;",  ">= 9990 (2)",      2},
        {"SELECT * FROM WTEST WHERE VAL <= 50;",    "<= 50   (5)",      5},
        {"SELECT * FROM WTEST WHERE VAL >= 1;",     ">= 1 (all 1000)",  1000},
        {"SELECT * FROM WTEST WHERE VAL <= 10000;", "<= 10000 (all)",   1000},
        {"SELECT * FROM WTEST WHERE VAL > 10000;",  "> 10000 (0)",      0},
        {"SELECT * FROM WTEST WHERE VAL < 10;",     "< 10  (0)",        0},
        /* string comparison */
        {"SELECT * FROM WTEST WHERE TAG = 't500';", "TAG = t500",       1},
    };
    for (auto &c : cases)
        check_cnt(c.lbl, qcount(db, c.sql), c.exp);
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  6. JOIN benchmark                                                           */
/* ─────────────────────────────────────────────────────────────────────────── */

static void join_bench(FlexQL *db, long long n) {
    pr("\n=== 6. JOIN Benchmark (n=" + std::to_string(n) + " users) ===\n");

    xok(db, "CREATE TABLE JUSER(UID DECIMAL, UNAME VARCHAR(32), CITY VARCHAR(32));",
        "CREATE JUSER");
    xok(db, "CREATE TABLE JORDER(OID DECIMAL, UID DECIMAL, TOTAL DECIMAL, "
            "EXPIRES_AT DECIMAL);", "CREATE JORDER");

    /* Batch-insert users */
    {
        long long chunk = 500;
        for (long long base = 1; base <= n; base += chunk) {
            std::ostringstream s; s << "INSERT INTO JUSER VALUES ";
            for (long long i = base; i < base + chunk && i <= n; ++i) {
                if (i > base) s << ",";
                s << "(" << i << ",'juser" << i << "','city" << (i % 10) << "')";
            }
            s << ";"; xok(db, s.str());
        }
    }

    /* 3 orders per user */
    {
        long long oid = 1, chunk = 300;
        for (long long base = 1; base <= n; base += chunk) {
            std::ostringstream s; s << "INSERT INTO JORDER VALUES ";
            bool first = true;
            for (long long i = base; i < base + chunk && i <= n; ++i)
                for (int j = 0; j < 3; ++j) {
                    if (!first) s << ",";
                    first = false;
                    s << "(" << oid++ << "," << i << "," << (50 + i % 950) << ",9999999999)";
                }
            s << ";"; xok(db, s.str());
        }
    }

    auto bench_join = [&](const std::string &lbl, const std::string &sql) {
        long long cnt = 0; char *err = nullptr;
        auto t0 = ms_now();
        flexql_exec(db, sql.c_str(), cb_count, &cnt, &err);
        long long dt = ms_now() - t0;
        if (err) flexql_free(err);
        pr("[INFO] " + lbl + ": " + std::to_string(cnt) + " rows in " + std::to_string(dt) + " ms");
    };

    bench_join("JOIN all (n*3 expected)",
               "SELECT JUSER.UNAME, JORDER.TOTAL FROM JUSER "
               "INNER JOIN JORDER ON JUSER.UID = JORDER.UID;");

    bench_join("JOIN+WHERE TOTAL>500",
               "SELECT JUSER.UNAME, JORDER.TOTAL FROM JUSER "
               "INNER JOIN JORDER ON JUSER.UID = JORDER.UID "
               "WHERE JORDER.TOTAL > 500;");

    bench_join("JOIN single user (uid=1)",
               "SELECT JUSER.UNAME, JORDER.OID FROM JUSER "
               "INNER JOIN JORDER ON JUSER.UID = JORDER.UID "
               "WHERE JUSER.UID = 1;");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  7. Concurrent write stress                                                  */
/* ─────────────────────────────────────────────────────────────────────────── */

struct CArg {
    const char     *host;
    int             port, tid, rows_each, batch_sz;
    const char     *table_name;
    std::atomic<long long> *total;
    std::atomic<int>       *errs;
};

static void *cworker(void *varg) {
    auto *a = (CArg *)varg;
    FlexQL *db = nullptr;
    if (flexql_open(a->host, a->port, &db) != FLEXQL_OK) { ++(*a->errs); return nullptr; }
    long long base = (long long)a->tid * a->rows_each, ins = 0;
    while (ins < a->rows_each) {
        std::ostringstream ss; ss << "INSERT INTO " << a->table_name << " VALUES ";
        int b = 0;
        while (b < a->batch_sz && ins < a->rows_each) {
            long long id = base + ins + 1;
            if (b) ss << ",";
            ss << "(" << id << ",'t" << a->tid << "r" << ins << "',"
               << id % 10000 << ",9999999999)";
            ++ins; ++b;
        }
        ss << ";";
        char *err = nullptr;
        if (flexql_exec(db, ss.str().c_str(), cb_noop, nullptr, &err) != FLEXQL_OK) {
            ++(*a->errs); if (err) flexql_free(err); break;
        }
        (*a->total) += b;
    }
    flexql_close(db); return nullptr;
}

static void concurrent_bench(const char *host, int port, int threads, long long per_thread, int batch = 50) {
    pr("\n=== 7. Concurrent Write Stress (" + std::to_string(threads) +
       " threads x " + std::to_string(per_thread) + " rows) ===\n");

    /* Create table */
    FlexQL *setup = nullptr;
    if (flexql_open(host, port, &setup) != FLEXQL_OK) { pr("[FAIL] connect"); return; }
    xok(setup, "CREATE TABLE CTEST(ID DECIMAL, NAME VARCHAR(64), "
               "VAL DECIMAL, EXPIRES_AT DECIMAL);", "CREATE CTEST");
        flexql_close(setup);

    std::atomic<long long> total{0};
    std::atomic<int>       errs{0};
    std::vector<CArg>      args(threads);
    std::vector<pthread_t> ts(threads);

    auto t0 = ms_now();
    for (int i = 0; i < threads; ++i) {
        args[i] = {host, port, i, (int)per_thread, batch, "CTEST", &total, &errs};
        pthread_create(&ts[i], nullptr, cworker, &args[i]);
    }
    for (int i = 0; i < threads; ++i) pthread_join(ts[i], nullptr);
    long long dt = ms_now() - t0;

    long long tot = total.load();
    pr("[INFO] Concurrent: threads=" + std::to_string(threads) +
       " rows=" + std::to_string(tot) +
       " errs=" + std::to_string(errs.load()) +
       " elapsed=" + std::to_string(dt) + "ms" +
       " throughput=" + std::to_string(dt ? tot * 1000LL / dt : tot) + " r/s");

    /* Verify row count */
    FlexQL *v = nullptr;
    if (flexql_open(host, port, &v) == FLEXQL_OK) {
        check_cnt("Concurrent row count correct",
                  qcount(v, "SELECT * FROM CTEST;"), tot);
        flexql_close(v);
    }
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  8. Mixed read/write                                                         */
/* ─────────────────────────────────────────────────────────────────────────── */

static void mixed_bench(FlexQL *db, long long n) {
    pr("\n=== 8. Mixed Read/Write (n=" + std::to_string(n) + " ops) ===\n");

    xok(db, "CREATE TABLE MIXED(ID DECIMAL, VAL VARCHAR(64), EXPIRES_AT DECIMAL);",
        "CREATE MIXED");

    long long writes = 0, reads = 0, wms = 0, rms = 0;
    for (long long i = 1; i <= n; ++i) {
        if (i % 3 == 0 && writes > 0) {
            long long rid = 1 + i % writes;
            auto t0 = ms_now();
            qrows(db, "SELECT VAL FROM MIXED WHERE ID = " + std::to_string(rid) + ";");
            rms += ms_now() - t0; ++reads;
        } else {
            ++writes;
            std::ostringstream ss;
            ss << "INSERT INTO MIXED VALUES (" << writes << ",'v" << writes << "',9999999999);";
            auto t0 = ms_now(); xok(db, ss.str()); wms += ms_now() - t0;
        }
    }
    pr("[INFO] Writes=" + std::to_string(writes) + " avg=" +
       std::to_string(writes ? wms / writes : 0) + "ms | Reads=" +
       std::to_string(reads)  + " avg=" +
       std::to_string(reads  ? rms / reads  : 0) + "ms");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  9. Large-batch INSERT (WAL coalescing effect)                               */
/* ─────────────────────────────────────────────────────────────────────────── */

static void large_batch_bench(FlexQL *db, long long n) {
    pr("\n=== 9. Large-Batch INSERT (WAL coalescing) ===\n");

    xok(db, "CREATE TABLE LBATCH(ID DECIMAL, A VARCHAR(32), B DECIMAL, "
            "EXPIRES_AT DECIMAL);", "CREATE LBATCH");

    /* Baseline: 1 row per INSERT */
    {
        auto t0 = ms_now();
        for (long long i = 1; i <= std::min(n, 1000LL); ++i) {
            std::ostringstream ss;
            ss << "INSERT INTO LBATCH VALUES (" << i << ",'x" << i << "'," << i << ",9999999999);";
            char *err = nullptr;
            flexql_exec(db, ss.str().c_str(), cb_noop, nullptr, &err);
            if (err) flexql_free(err);
        }
        long long dt = ms_now() - t0;
        pr("[INFO] Single-row inserts (1000): " + std::to_string(dt) + "ms");
    }

    /* Comparison: 1000-row batch */
    xok(db, "CREATE TABLE LBATCH2(ID DECIMAL, A VARCHAR(32), B DECIMAL, "
            "EXPIRES_AT DECIMAL);", "CREATE LBATCH2");
    {
        std::ostringstream ss; ss << "INSERT INTO LBATCH2 VALUES ";
        for (long long i = 1; i <= 1000; ++i) {
            if (i > 1) ss << ",";
            ss << "(" << i << ",'x" << i << "'," << i << ",9999999999)";
        }
        ss << ";";
        auto t0 = ms_now();
        xok(db, ss.str());
        long long dt = ms_now() - t0;
        pr("[INFO] 1000-row single batch: " + std::to_string(dt) + "ms");
    }

    /* Now full n rows in 1000-row batches */
    {
        long long ins = 0; int batch = 1000;
        auto t0 = ms_now();
        while (ins < n) {
            std::ostringstream ss; ss << "INSERT INTO LBATCH VALUES ";
            int b = 0;
            while (b < batch && ins < n) {
                long long id = ins + 1001;
                if (b) ss << ",";
                ss << "(" << id << ",'y" << id << "'," << id << ",9999999999)";
                ++ins; ++b;
            }
            ss << ";"; xok(db, ss.str());
        }
        long long dt = ms_now() - t0;
        long long tput = dt ? n * 1000LL / dt : n;
        pr("[INFO] " + std::to_string(n) + " rows in 1000-row batches: " +
           std::to_string(dt) + "ms, " + std::to_string(tput) + " r/s");
    }
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  10. EXPIRES_AT / expiry pattern                                             */
/* ─────────────────────────────────────────────────────────────────────────── */

static void expiry_bench(FlexQL *db, long long n) {
    pr("\n=== 10. Expiry Column Pattern ===\n");

    xok(db, "CREATE TABLE EXP_T(ID DECIMAL, NAME VARCHAR(64), EXPIRES_AT DECIMAL);",
        "CREATE EXP_T");

    long long now_ts   = 1893456000LL;
    long long future   = now_ts + 86400;
    long long past     = now_ts - 86400;

    /* half expired, half valid */
    {
        std::ostringstream ss; ss << "INSERT INTO EXP_T VALUES ";
        for (long long i = 1; i <= n; ++i) {
            if (i > 1) ss << ",";
            long long exp = (i % 2 == 0) ? future : past;
            ss << "(" << i << ",'u" << i << "'," << exp << ")";
        }
        ss << ";"; xok(db, ss.str(), "INSERT " + std::to_string(n) + " rows mixed expiry");
    }

    long long t0 = ms_now();
    long long valid = qcount(db, "SELECT * FROM EXP_T WHERE EXPIRES_AT > " +
                                 std::to_string(now_ts) + ";");
    long long dt = ms_now() - t0;
    check_cnt("Valid (not expired) rows = n/2", valid, n / 2);
    pr("[INFO] Expiry filter query: " + std::to_string(dt) + "ms for " +
       std::to_string(n) + " rows");

    t0 = ms_now();
    long long expired = qcount(db, "SELECT * FROM EXP_T WHERE EXPIRES_AT <= " +
                                   std::to_string(now_ts) + ";");
    dt = ms_now() - t0;
    check_cnt("Expired rows = n/2", expired, n / 2);
    pr("[INFO] Expired filter query: " + std::to_string(dt) + "ms");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  11. Standalone read-path profile                                            */
/* ─────────────────────────────────────────────────────────────────────────── */

static void read_profile_bench(FlexQL *db, long long n) {
    pr("\n=== 11. Standalone Read Profile (n=" + std::to_string(n) + ") ===\n");

    xok(db, "CREATE TABLE RPROF(ID DECIMAL, NAME VARCHAR(64), SCORE DECIMAL, "
            "CITY VARCHAR(32), EXPIRES_AT DECIMAL);", "CREATE RPROF");

    long long ins = 0;
    const int batch = 2000;
    while (ins < n) {
        std::ostringstream ss; ss << "INSERT INTO RPROF VALUES ";
        int b = 0;
        while (b < batch && ins < n) {
            long long id = ins + 1;
            if (b) ss << ",";
            ss << "(" << id << ",'user" << id << "'," << (1000 + id % 10000)
               << ",'city" << (id % 20) << "',9999999999)";
            ++ins; ++b;
        }
        ss << ";";
        if (!xok(db, ss.str())) return;
    }

    auto bench = [&](const std::string &label, const std::string &sql) {
        long long rows = 0; char *err = nullptr;
        auto t0 = ms_now();
        flexql_exec(db, sql.c_str(), cb_count, &rows, &err);
        long long dt = ms_now() - t0;
        if (err) flexql_free(err);
        pr("[INFO] " + label + ": rows=" + std::to_string(rows) +
           " time=" + std::to_string(dt) + "ms");
    };

    bench("Full scan SELECT *", "SELECT * FROM RPROF;");
    bench("Projected scan", "SELECT ID, NAME FROM RPROF;");
    bench("Non-PK selective filter", "SELECT ID, NAME FROM RPROF WHERE SCORE <= 1500;");
    bench("Non-PK broad filter", "SELECT * FROM RPROF WHERE SCORE > 5000;");
    bench("PK range >= midpoint", "SELECT ID FROM RPROF WHERE ID >= " + std::to_string(std::max(1LL, n / 2)) + ";");

    {
        long long total_us = 0;
        for (int i = 0; i < 200; ++i) {
            long long id = 1 + ((long long)i * 7919 % std::max(1LL, n));
            auto t0 = us_now();
            qrows(db, "SELECT NAME FROM RPROF WHERE ID = " + std::to_string(id) + ";");
            total_us += us_now() - t0;
        }
        pr("[INFO] 200 PK point lookups avg=" + std::to_string(total_us / 200) + " µs");
    }
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  12. Standalone join profile                                                 */
/* ─────────────────────────────────────────────────────────────────────────── */

static void join_profile_bench(FlexQL *db, long long n) {
    pr("\n=== 12. Standalone Join Profile (n=" + std::to_string(n) + ") ===\n");

    xok(db, "CREATE TABLE JPU(UID DECIMAL, NAME VARCHAR(32), TIER DECIMAL);",
        "CREATE JPU");
    xok(db, "CREATE TABLE JPO(OID DECIMAL, UID DECIMAL, TOTAL DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE JPO");

    const int user_batch = 1000;
    for (long long base = 1; base <= n; base += user_batch) {
        std::ostringstream ss; ss << "INSERT INTO JPU VALUES ";
        bool first = true;
        for (long long i = base; i < base + user_batch && i <= n; ++i) {
            if (!first) ss << ",";
            first = false;
            ss << "(" << i << ",'u" << i << "'," << (i % 5) << ")";
        }
        ss << ";";
        if (!xok(db, ss.str())) return;
    }

    long long oid = 1;
    const int order_batch_users = 300;
    for (long long base = 1; base <= n; base += order_batch_users) {
        std::ostringstream ss; ss << "INSERT INTO JPO VALUES ";
        bool first = true;
        for (long long i = base; i < base + order_batch_users && i <= n; ++i) {
            for (int j = 0; j < 3; ++j) {
                if (!first) ss << ",";
                first = false;
                ss << "(" << oid++ << "," << i << "," << (50 + (i + j) % 950) << ",9999999999)";
            }
        }
        ss << ";";
        if (!xok(db, ss.str())) return;
    }

    auto bench = [&](const std::string &label, const std::string &sql) {
        long long rows = 0; char *err = nullptr;
        auto t0 = ms_now();
        flexql_exec(db, sql.c_str(), cb_count, &rows, &err);
        long long dt = ms_now() - t0;
        if (err) flexql_free(err);
        pr("[INFO] " + label + ": rows=" + std::to_string(rows) +
           " time=" + std::to_string(dt) + "ms");
    };

    bench("Point join on single UID",
          "SELECT JPU.NAME, JPO.OID FROM JPU "
          "INNER JOIN JPO ON JPU.UID = JPO.UID "
          "WHERE JPU.UID = 1;");

    bench("Selective join filter",
          "SELECT JPU.NAME, JPO.TOTAL FROM JPU "
          "INNER JOIN JPO ON JPU.UID = JPO.UID "
          "WHERE JPO.TOTAL > 900;");

    bench("Broad join",
          "SELECT JPU.NAME, JPO.TOTAL FROM JPU "
          "INNER JOIN JPO ON JPU.UID = JPO.UID;");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  13. Standalone PK lookup profile                                            */
/* ─────────────────────────────────────────────────────────────────────────── */

static void pk_profile_bench(FlexQL *db, long long n, int lookups) {
    pr("\n=== 13. Standalone PK Lookup Profile (n=" + std::to_string(n) +
       " lookups=" + std::to_string(lookups) + ") ===\n");

    xok(db, "CREATE TABLE PKPROF(ID DECIMAL, NAME VARCHAR(64), SCORE DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE PKPROF");

    long long ins = 0;
    const int batch = 2000;
    while (ins < n) {
        std::ostringstream ss; ss << "INSERT INTO PKPROF VALUES ";
        int b = 0;
        while (b < batch && ins < n) {
            long long id = ins + 1;
            if (b) ss << ",";
            ss << "(" << id << ",'user" << id << "'," << (1000 + id % 10000) << ",9999999999)";
            ++ins; ++b;
        }
        ss << ";";
        if (!xok(db, ss.str())) return;
    }

    auto single_lookup_us = [&](long long id) -> long long {
        auto t0 = us_now();
        qrows(db, "SELECT NAME FROM PKPROF WHERE ID = " + std::to_string(id) + ";");
        return us_now() - t0;
    };

    long long first = single_lookup_us(1);
    long long middle = single_lookup_us(std::max(1LL, n / 2));
    long long last = single_lookup_us(n);

    long long total_us = 0;
    for (int i = 0; i < lookups; ++i) {
        long long id = 1 + ((long long)i * 104729 % std::max(1LL, n));
        total_us += single_lookup_us(id);
    }

    pr("[INFO] PK lookup first row: " + std::to_string(first) + " µs");
    pr("[INFO] PK lookup middle row: " + std::to_string(middle) + " µs");
    pr("[INFO] PK lookup last row: " + std::to_string(last) + " µs");
    pr("[INFO] PK lookup avg over " + std::to_string(lookups) + ": " +
       std::to_string(total_us / std::max(1, lookups)) + " µs");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  14. Standalone tiny-query profile                                           */
/* ─────────────────────────────────────────────────────────────────────────── */

static void tiny_query_profile_bench(FlexQL *db, long long n, int lookups) {
    pr("\n=== 14. Standalone Tiny Query Profile (n=" + std::to_string(n) +
       " lookups=" + std::to_string(lookups) + ") ===\n");

    xok(db, "CREATE TABLE TQPROF(ID DECIMAL, NAME VARCHAR(64), SCORE DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE TQPROF");

    long long ins = 0;
    const int insert_batch = 2000;
    while (ins < n) {
        std::ostringstream ss; ss << "INSERT INTO TQPROF VALUES ";
        int b = 0;
        while (b < insert_batch && ins < n) {
            long long id = ins + 1;
            if (b) ss << ",";
            ss << "(" << id << ",'user" << id << "'," << (1000 + id % 10000) << ",9999999999)";
            ++ins; ++b;
        }
        ss << ";";
        if (!xok(db, ss.str())) return;
    }

    auto avg_us = [&](const std::function<std::string(int)> &sql_for) -> long long {
        long long total = 0;
        for (int i = 0; i < lookups; ++i) {
            auto t0 = us_now();
            qrows(db, sql_for(i));
            total += us_now() - t0;
        }
        return total / std::max(1, lookups);
    };

    long long same_exact = avg_us([&](int) {
        return "SELECT NAME FROM TQPROF WHERE ID = 1;";
    });

    long long varying_exact = avg_us([&](int i) {
        long long id = 1 + ((long long)i * 104729 % std::max(1LL, n));
        return "SELECT NAME FROM TQPROF WHERE ID = " + std::to_string(id) + ";";
    });

    long long same_projected = avg_us([&](int) {
        return "SELECT ID, NAME FROM TQPROF WHERE ID = 1;";
    });

    pr("[INFO] Repeated exact same PK query avg=" + std::to_string(same_exact) + " µs");
    pr("[INFO] Repeated varying PK query avg=" + std::to_string(varying_exact) + " µs");
    pr("[INFO] Repeated projected same PK query avg=" + std::to_string(same_projected) + " µs");
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  15. Standalone concurrency profile                                          */
/* ─────────────────────────────────────────────────────────────────────────── */

static void concurrency_profile_bench(const char *host, int port, int threads,
                                      long long total_rows, int batch) {
    pr("\n=== 15. Standalone Concurrency Profile (" + std::to_string(threads) +
       " threads total_rows=" + std::to_string(total_rows) +
       " batch=" + std::to_string(batch) + ") ===\n");

    FlexQL *setup = nullptr;
    if (flexql_open(host, port, &setup) != FLEXQL_OK) { pr("[FAIL] connect"); return; }
    xok(setup, "CREATE TABLE CPROF(ID DECIMAL, NAME VARCHAR(64), VAL DECIMAL, EXPIRES_AT DECIMAL);",
        "CREATE CPROF");
    flexql_close(setup);

    long long per_thread = total_rows / std::max(1, threads);
    std::atomic<long long> total{0};
    std::atomic<int> errs{0};
    std::vector<CArg> args(threads);
    std::vector<pthread_t> ts(threads);

    auto t0 = ms_now();
    for (int i = 0; i < threads; ++i) {
        args[i] = {host, port, i, (int)per_thread, batch, "CPROF", &total, &errs};
        pthread_create(&ts[i], nullptr, cworker, &args[i]);
    }
    for (int i = 0; i < threads; ++i) pthread_join(ts[i], nullptr);
    long long dt = ms_now() - t0;

    long long tot = total.load();
    pr("[INFO] Concurrent profile: rows=" + std::to_string(tot) +
       " errs=" + std::to_string(errs.load()) +
       " elapsed=" + std::to_string(dt) + "ms" +
       " throughput=" + std::to_string(dt ? tot * 1000LL / dt : tot) + " r/s");

    FlexQL *v = nullptr;
        if (flexql_open(host, port, &v) == FLEXQL_OK) {
        check_cnt("Concurrency profile row count correct",
                  qcount(v, "SELECT * FROM CPROF;"), tot);
        flexql_close(v);
    }
}

/* ─────────────────────────────────────────────────────────────────────────── */
/*  main                                                                        */
/* ─────────────────────────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    const char *host = "127.0.0.1"; int port = 9000;
    long long n = 100000;
    std::string mode;
    int conc_threads = 4;
    int extra = 200;
    int batch = 100;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if      (a == "--unit-test")   mode = "unit";
        else if (a == "--write")       { mode = "write";  if (i+1 < argc) n = atoll(argv[++i]); }
        else if (a == "--read")        mode = "read";
        else if (a == "--cache")       mode = "cache";
        else if (a == "--where")       mode = "where";
        else if (a == "--join")        { mode = "join";   if (i+1 < argc) n = atoll(argv[++i]); }
        else if (a == "--mixed")       { mode = "mixed";  if (i+1 < argc) n = atoll(argv[++i]); }
        else if (a == "--large-batch") { mode = "lbatch"; if (i+1 < argc) n = atoll(argv[++i]); }
        else if (a == "--expiry")      { mode = "expiry"; if (i+1 < argc) n = atoll(argv[++i]); }
        else if (a == "--read-profile"){ mode = "rprof"; if (i+1 < argc) n = atoll(argv[++i]); }
        else if (a == "--join-profile"){ mode = "jprof"; if (i+1 < argc) n = atoll(argv[++i]); }
        else if (a == "--pk-profile")  {
            mode = "pkprof";
            if (i+1 < argc) n = atoll(argv[++i]);
            if (i+1 < argc) extra = atoi(argv[++i]);
        }
        else if (a == "--tiny-query-profile") {
            mode = "tqprof";
            if (i+1 < argc) n = atoll(argv[++i]);
            if (i+1 < argc) extra = atoi(argv[++i]);
        }
        else if (a == "--concurrency-profile") {
            mode = "cprof";
            if (i+1 < argc) conc_threads = atoi(argv[++i]);
            if (i+1 < argc) n = atoll(argv[++i]);
            if (i+1 < argc) batch = atoi(argv[++i]);
        }
        else if (a == "--concurrent")  {
            mode = "conc";
            if (i+1 < argc) conc_threads = atoi(argv[++i]);
            if (i+1 < argc) n = atoll(argv[++i]);
        }
        else n = atoll(a.c_str());
    }

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        fprintf(stderr, "Cannot connect to FlexQL at %s:%d\n", host, port);
        return 1;
    }
    printf("Connected to FlexQL at %s:%d\n", host, port);

    /* Single-mode runs */
    if (mode == "unit")   { unit_tests(db); }
    else if (mode == "write")  { write_bench(db, n, 100); }
    else if (mode == "read")   { read_bench(db); }
    else if (mode == "cache")  { cache_bench(db); }
    else if (mode == "where")  { where_bench(db); }
    else if (mode == "join")   { join_bench(db, std::min(n, 50000LL)); }
    else if (mode == "mixed")  { mixed_bench(db, n); }
    else if (mode == "lbatch") { large_batch_bench(db, n); }
    else if (mode == "expiry") { expiry_bench(db, n); }
    else if (mode == "rprof")  { read_profile_bench(db, n); }
    else if (mode == "jprof")  { join_profile_bench(db, std::min(n, 50000LL)); }
    else if (mode == "pkprof") { pk_profile_bench(db, n, extra); }
    else if (mode == "tqprof") { tiny_query_profile_bench(db, n, extra); }
    else if (mode == "conc")   {
        flexql_close(db);
        concurrent_bench(host, port, conc_threads, n / conc_threads, 100);
        db = nullptr;
    }
    else if (mode == "cprof")  {
        flexql_close(db);
        concurrency_profile_bench(host, port, conc_threads, n, batch);
        db = nullptr;
    }
    else {
        /* Full suite */
        printf("Full benchmark suite. n=%lld\n", n);
        unit_tests(db);
        write_bench(db, n, 100);
        read_bench(db);
        cache_bench(db);
        where_bench(db);
        join_bench(db, std::min(n / 5, 20000LL));
        large_batch_bench(db, std::min(n, 50000LL));
        expiry_bench(db, std::min(n / 2, 50000LL));
        mixed_bench(db, std::min(n / 10, 10000LL));
        flexql_close(db); db = nullptr;
        concurrent_bench(host, port, 4, n / 4, 100);
    }

    if (db) flexql_close(db);

    int p = g_pass.load(), f = g_fail.load();
    printf("\n=== Summary: %d passed, %d failed ===\n", p, f);
    return f ? 1 : 0;
}
