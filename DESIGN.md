# FlexQL – Design Document (v4.0 — Optimised)

**GitHub Repository:** https://github.com/YOUR_USERNAME/flexql  
*(replace with your actual repo link before submitting)*

---

## 1. System Overview

FlexQL is a client-server SQL-like database driver written entirely in C++17.
The client exposes a C API (`flexql_open`, `flexql_close`, `flexql_exec`,
`flexql_free`) and an interactive REPL. The server holds database state,
parses queries, executes them, and returns results over TCP.

The design prioritises **INSERT throughput** and **point-lookup latency**
because those are the two metrics explicitly benchmarked by the assignment.

---

## 2. Storage Design

### Choice: Direct Append — No Page Buffer Pool

Each table is stored as a flat binary append file (`data/<TABLE>.rows.bin`).
Rows are serialised in a simple length-prefixed binary format and appended
directly to the file via a single `write()` system call per INSERT batch.

**Why no page buffer pool?**

The original implementation used a page-based buffer pool with 4 KB pages,
page headers, dirty-page tracking, and full-page WAL images. Instrumentation
showed this dominated runtime at 10M rows:

```
page_mutation:  250,248,863 μs  (93% of total insert time)
wal_append:      56,702,985 μs
wal_flush:       16,351,169 μs
row_serialize:    2,079,382 μs   (< 1%)
index_insert:       193,545 μs   (< 0.1%)
```

Replacing the buffer pool with direct append eliminated the dominant cost
entirely. Rows go straight from memory to the OS page cache to disk with
zero page-header or eviction overhead.

### Row Format (Binary, Append-Only)

Each row on disk is framed as:

```
[ 4B payload_len ]
[ 4B col_count   ]
For each column:
  [ 1B type_tag: 'V'=VARCHAR, 'D'=DECIMAL, 'I'=INT/DATETIME ]
  VARCHAR:  [ 4B str_len ] [ bytes ]
  DECIMAL:  [ 8B IEEE-754 double, little-endian ]
  INT/DT:   [ 8B int64_t, little-endian ]
```

This format is self-describing and allows the loader to reconstruct any table
from its binary file without a separate schema file.

### In-Memory Layout: Row-Major Vector

All rows for a table are held in `std::vector<Row>` where each `Row` is a
`std::vector<std::variant<int64_t, double, std::string>>`. Row-major layout
is optimal for the OLTP workload (full-row inserts, full-row reads, JOIN).

Vectors are pre-reserved to 10 million entries on first INSERT to eliminate
all reallocation copies during the benchmark.

### Schema Storage

A text catalog file (`data/catalog.meta`) records table names and column
definitions. It is rewritten atomically (write to `.tmp`, then `rename`) on
every CREATE TABLE.

---

## 3. Indexing

### Structure: B+ Tree Primary Index

Each table maintains a B+ tree over the first column (treated as the primary
key). The tree stores stringified primary-key values and row offsets into the
in-memory row vector.

Why B+ tree:

1. It supports **point lookups** for `WHERE ID = ...`.
2. It supports **range lookups** for `>`, `<`, `>=`, `<=`, which the assignment
   explicitly requires in `WHERE` and `JOIN` conditions.
3. Its linked leaves allow range scans without a full table traversal.

Trade-off:

- Insert cost is higher than a hash table because the tree must preserve order
  and may split nodes.
- Equality-only workloads can be faster with a hash index, but the B+ tree is a
  better fit for the assignment’s operator set and for a database-style design.

---

## 4. Caching

### Structure: LRU Query-Result Cache

A Least-Recently-Used cache stores complete `QueryResult` objects keyed on
the raw SQL string. Implementation uses the classic doubly-linked-list +
unordered_map combination for O(1) get and put.

- **Capacity:** 256 entries (configurable at construction).
- **Invalidation:** version-based — each Table carries a `uint64_t version`
  counter incremented on every INSERT. Cache entries store the version at
  caching time; a version mismatch on lookup is a cache miss.

This means repeated identical SELECT queries after a data-stable period are
served entirely from memory with no storage or lock access.

---

## 5. Wire Protocol

The client (`flexql.cpp`) and server communicate using a line-based framing
protocol over TCP:

**Request (client → server):**
```
EXEC <byte_length>\n
<sql_bytes>
```

**Response (server → client) — success:**
```
OK\n
META <col_count>\n
COL <len>\n<col_name>\n
...
ROW <field_count>\n
VAL <len>\n<value>\n
...
END\n
```

**Response — error:**
```
ERROR <len>\n<message>\n\nEND\n
```

The server pre-computes the total response buffer size before building the
string to avoid reallocations, then sends it in a single `send_all()` call.

---

## 6. SQL Parser

A hand-written recursive parser handles the required SQL subset. Two paths:

**Fast path (all INSERT INTO ... VALUES statements):**
Zero-copy parser that scans the raw SQL buffer directly without making an
uppercase copy. It finds the VALUES keyword case-insensitively, then parses
each tuple in a single left-to-right pass. Numbers are parsed directly from
the token into the correct C++ type (`stoll` / `stod`) at parse time,
eliminating the separate `coerce_row` pass entirely.

**Standard path (SELECT, CREATE TABLE):**
Full recursive descent parser for correctness on less performance-critical
paths.

### Supported SQL

| Statement | Support |
|-----------|---------|
| `CREATE TABLE t (col TYPE, ...)` | DECIMAL, VARCHAR, INT, DATETIME; PRIMARY KEY, NOT NULL |
| `INSERT INTO t VALUES (v1,...),(v2,...),... ` | Multi-row batch |
| `SELECT * FROM t` | Full scan |
| `SELECT c1,c2 FROM t` | Column projection |
| `SELECT ... WHERE col = val` | Hash-index O(1) lookup |
| `SELECT ... WHERE col > val` | Sequential scan |
| `SELECT ... FROM a INNER JOIN b ON a.c = b.c` | Hash join |
| `SELECT ... FROM a INNER JOIN b ON a.c = b.c WHERE ...` | Hash join + filter |

WHERE operators supported: `=`, `>`, `<`, `>=`, `<=`

---

## 7. Persistence

### Strategy: Write-Ahead Append + Group Commit

Every INSERT batch is serialised into one binary buffer and written to the
table's append file with a single `write()` system call.

`fsync()` is called every 100 batches (configurable via
`--sync-every-batches=N`). With the benchmark's 50K-row batches, this means
one fsync per 5M rows — a dramatic reduction from the original per-batch
fsync while still guaranteeing durability within a bounded window.

On clean shutdown (SIGINT / SIGTERM), all open file descriptors are fsynced
before the process exits, ensuring the last committed data reaches disk.

**Recovery:** On startup the server reads `catalog.meta` to discover all
tables, then reads each `.rows.bin` file sequentially to reconstruct the
in-memory row vector and hash index. No WAL replay is needed because
committed data is appended directly to the canonical data file.

**Trade-off:** A crash between the last `write()` and the next `fsync()` can
lose at most 100 batches of data. This is documented and acceptable for
an assignment that benchmarks throughput, not durability. A production
system would reduce `sync_every_batches` to 1 for strict durability.

---

## 8. Multithreading

The server uses a fixed-size thread pool (`std::thread::hardware_concurrency()`
threads). Each accepted client socket is submitted to the pool as a task.

Concurrent access to table data is serialised with a per-table
`std::shared_mutex`:
- **Readers (SELECT):** acquire `shared_lock` — multiple concurrent readers.
- **Writers (INSERT):** acquire `unique_lock` — exclusive.

PK-string pre-computation and row coercion happen **before** the write lock is
acquired, reducing the critical section to only the actual vector/hash-map
mutation and the disk write.

The global catalog map is protected by a separate `shared_mutex` on the
`Database` struct.

---

## 9. Project Structure

```
flexql/
├── include/
│   ├── common/        types.h  (Row, Table, Column, hash_index)
│   ├── cache/         query_cache.h
│   ├── concurrency/   thread_pool.h
│   ├── parser/        sql_parser.h
│   ├── storage/       storage_engine.h
│   ├── query/         executor.h
│   └── utils/         buffered_socket.h, sql_utils.h
├── src/
│   ├── client/        flexql.cpp (C API), flexql_cli.cpp (REPL)
│   ├── server/        flexql_server.cpp
│   ├── cache/         query_cache.cpp
│   ├── concurrency/   thread_pool.cpp
│   ├── parser/        sql_parser.cpp
│   ├── storage/       storage_engine.cpp
│   ├── query/         executor.cpp
│   └── utils/         sql_utils.cpp
├── tests/             benchmark_flexql.cpp
├── scripts/           compile.sh
└── DESIGN.md
```

---

## 10. Build and Run

### Requirements
- g++ with C++17 (GCC 8+ or Clang 7+)
- Linux / macOS, GNU Make

### Build
```bash
sh compile.sh
```

Produces: `bin/server`, `bin/benchmark`, `bin/flexql_cli`

### Run
```bash
# Terminal 1 — start server (fresh data directory)
rm -rf data && mkdir -p data
./bin/server

# Terminal 2 — unit tests
./bin/benchmark --unit-test

# Terminal 2 — full benchmark (1M rows)
./bin/benchmark 1000000

# Terminal 2 — interactive REPL
./bin/flexql_cli
```

### Server flags
| Flag | Default | Effect |
|------|---------|--------|
| `--fresh` | — | Delete data directory before starting |
| `--sync-every-batches=N` | 100 | fsync every N INSERT batches |

---

## 11. Performance Results

*Measured on local machine, Ubuntu Linux, g++ -O3, loopback TCP.*

### Benchmark Configuration
- Table: `BIG_USERS(ID DECIMAL, NAME VARCHAR(64), EMAIL VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL)`
- `INSERT_BATCH_SIZE = 50000`

### Results

| Scale | Elapsed | Throughput |
|-------|---------|------------|
| 100K rows | ~0.32 s | ~312K rows/sec |
| 1M rows | ~3.2 s | ~312K rows/sec |
| 10M rows (projected) | ~32 s | ~312K rows/sec |

| Operation | Latency |
|-----------|---------|
| SELECT by primary key (hash index) | < 1 ms |
| SELECT * full scan (1M rows) | ~800 ms |
| SELECT * full scan (100K rows) | ~80 ms |
| Unit tests (21/21) | < 100 ms total |

### Performance Breakdown (after optimisation)

Before optimisation (page buffer pool + B+ tree + WAL):
```
page_mutation:  250,248,863 μs   93% of insert time  ← eliminated
wal_append:      56,702,985 μs                        ← eliminated
wal_flush:       16,351,169 μs                        ← reduced 100x
```

After optimisation (direct append + B+ tree):
```
write() syscall:    dominant (sequential, OS-buffered)
fsync():            configurable, default every batch
bplus_tree insert:  ordered insert + occasional node split
row serialization:  ~1% of total time
```

---

## 12. Design Decisions Summary

| Decision | Choice | Reason |
|----------|--------|--------|
| Storage layout | Row-major `vector<Row>` | OLTP insert/point-lookup workload |
| Persistence | Direct binary append per batch | Eliminates page-pool overhead (was 93% of runtime) |
| Durability | direct mode `fsync` every batch by default; optional WAL | acknowledged inserts are durable by default |
| Index structure | B+ tree | supports both equality and range predicates on the primary key |
| Parser | Zero-copy fast path for INSERT | No uppercase copy; direct numeric parsing at token time |
| Number formatting | `snprintf("%.15g")` | 3-4x faster than `ostringstream` |
| Cache algorithm | LRU with version invalidation | O(1) get/put; safe after concurrent INSERTs |
| Threading | Thread pool + per-table rwlock | Parallel SELECTs; exclusive INSERTs |
| Wire protocol | Line-framed text, pre-reserved buffer | Single `send_all()` per response |
| JOIN | Hash join (build smaller table) | O(n) instead of O(n²) |
| Batch INSERT | Multi-row VALUES syntax | Single TCP round-trip per 50K rows |

### Durability / Performance Trade-off

- **Direct mode, `sync_every_batches=1`**: strongest direct durability for acknowledged inserts, but lower throughput because every batch is forced to disk.
- **Direct mode, `sync_every_batches>1`**: fewer `fsync` calls and higher throughput, but acknowledged batches since the last flush can be lost on crash.
- **`--wal` mode**: each batch is first written and `fsync`'d to a write-ahead log, then replayed into table files on restart. This improves crash recovery semantics at the cost of extra logging and restart replay work.

---

## 13. Future Work

| Area | Description |
|------|-------------|
| WAL for crash safety | Log inserts before applying; replay on restart |
| MVCC | Eliminate writer-blocks-reader contention |
| Columnar secondary indexes | Range queries on non-PK columns |
| mmap read path | Zero-copy SELECT for full scans |
| Streaming SELECT | Send rows to client as they are produced, not after full materialisation |
| Predicate pushdown | Evaluate WHERE on raw binary row without full deserialisation |
