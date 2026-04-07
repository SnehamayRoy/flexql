# FlexQL – Design Document (v4.0 — High Performance Page-Builder Architecture)

[**GitHub Repository**](YOUR_GITHUB_LINK_HERE)

---

## 1. System Overview

FlexQL is a client-server SQL-like database driver written entirely in C++17.
The client exposes a C API (`flexql_open`, `flexql_close`, `flexql_exec`,
`flexql_free`) and an interactive REPL. The server holds database state,
parses queries, executes them, and returns results over TCP.

The design rigorously prioritises **INSERT throughput (Zero-copy serialization, WAL group commit, Local Page Building)** and **point-lookup latency**, achieving extremely high rows-per-second processing times.

---

## 2. Storage Design

### High-Performance Page-Builder Architecture

Each table is stored as a series of 4KB disk pages (`data/<TABLE>.tbl`). 

Unlike naive append-only architectures or standard naive buffer pools, FlexQL uses a highly optimized **Local Page-Builder Insert Path**:
1. Incoming INSERT batches are parsed with an allocation-free zero-copy parser directly from the wire packet.
2. The executor pre-computes primary keys entirely outside of the table's write-lock.
3. Rows are serialized directly into a thread-local 4KB page array.
4. Finally, the completed page is flushed to the `BufferPool` using `push_page_direct` and pinned transiently, avoiding Least-Recently-Used (LRU) lock contention.

This enables us to achieve the disk-durability of Slotted Pages and Buffer Pools without suffering the 90%+ mutation overhead typically associated with them.

### Buffer Pool Logic (`BufferPool`)

- Fixed 1024-page capacity to bound memory usage.
- Uses `std::list` + `std::unordered_map` for O(1) LRU eviction.
- Implements `is_transient` pinning specifically for massive INSERT workloads to bypass expensive MRU promotion pointer swiping.

### Logical Write-Ahead Log (WAL) & Group Commit

Instead of writing massive physical page diffs, the WAL system captures logical high-speed binary operations.
When configuring the server with `--wal`, a single in-memory buffer (`wal_batch`) coalesces all table mutations for an entire `EXEC` statement.

Using **Group Commit**, the server calls `fdatasync()` only periodically (e.g. `sync_every_batches`), allowing massive I/O performance while preserving durable crash recovery. `fdatasync()` is used instead of `fsync()` to eliminate file metadata flushing overhead.

### Schema Storage

A text catalog file (`data/catalog.meta`) records table names and column
definitions. It is rewritten atomically (write to `.tmp`, then `rename`) on
every CREATE TABLE.

---

## 3. Indexing

### Structure: B+ Tree Primary Index

Each table maintains an in-memory B+ tree over the first column (the primary
key). The tree maps primary-key strings to a physical `RecordId` (PageId + SlotId).

Why B+ tree:
1. Supports **point lookups** for `WHERE ID = ...`.
2. Supports **range lookups** for `>`, `<`, `>=`, `<=`, ensuring rapid SELECTs.

---

## 4. Wire Protocol & Caching

The server and client communicate using a line-based framing protocol over TCP.
A Least-Recently-Used cache stores complete `QueryResult` objects keyed on
the raw SQL string. Implementation uses the classic doubly-linked-list for O(1) get and put. Version invalidation ensures caching is 100% data-accurate during rapid SELECT/INSERT mixing.

---

## 5. SQL Parser (Zero-Allocation)

A hand-written recursive parser handles the required SQL subset. 

**Fast path (all INSERT INTO ... VALUES statements):**
Zero-copy parser that scans the raw SQL buffer directly (`fast_parse_insert_rows`). It parses each tuple in a single pass using `std::string_view`s and `std::strtod` bypassing all intermediate heap allocations for string splitting.

---

## 6. Performance Results

*Measured on local machine, Ubuntu Linux, g++ -O3, loopback TCP.*

### Write Throughput Benchmark
- Table: `BIG_USERS(ID DECIMAL, NAME VARCHAR, EMAIL VARCHAR, BALANCE DECIMAL, EXPIRES_AT DECIMAL)`
- Batch size: 100-20,000

| Scale | Elapsed | Throughput |
|-------|---------|------------|
| 100K rows | ~1.0 s | ~94K rows/sec |
| 200K rows | ~2.3 s | ~85K rows/sec |
| 1M rows | ~12.8 s | ~78K - 82K rows/sec |
| 10M rows | ~130 s | ~77K rows/sec |

### Read and Join Profile (1M Scale)

| Operation | Scale | Latency | Rate |
|-----------|-------|---------|------|
| Full scan SELECT * | 1,000,000 rows | 2341 ms | 427,167 r/s |
| Projected scan (two cols) | 1,000,000 rows | 1006 ms | 994,035 r/s |
| Non-PK broad filter | 599,900 rows | 1398 ms | 429,113 r/s |
| Non-PK selective filter | 100,100 rows | 423 ms | 236,643 r/s |
| JOIN all (n*3 expected) | 60,000 rows| 104 ms | 576,000 r/s |
| JOIN + filter (TOTAL>500) | 31,437 rows | 90 ms | 349,300 r/s |
| JOIN single user (uid=1) | 3 rows | 56 ms | — |
| PK lookup ID=500 | 1 row | 1,374 µs | — |
| 100 PK lookups | 100 avg | 42 µs | — |
| Unit tests (65/65) | — | PASS | — |

---

## 7. Multithreading & Concurrency

The server utilizes `std::thread::hardware_concurrency()` for parallel request handling.
Access to table data is serialized with a per-table `std::shared_mutex`:
- **Readers (SELECT):** acquire `shared_lock` (concurrent).
- **Writers (INSERT):** acquire `unique_lock` (exclusive).

Concurrency Stress Test (4 Threads, 1M rows): `~83,600+ r/s`.

---

## 8. Build and Run

### Build
```bash
make clean && make -j$(nproc)
```

### Server & Benchmarks
```bash
pkill -9 server                 # Clean up old processes
./bin/server --fresh &          # Start engine 

# Test small compatibility suite
./bin/compat_matrix_benchmark --matrix 100000

# Test specific scenarios
./bin/my_benchmark --write 1000000
./bin/my_benchmark --read-profile 200000
./bin/my_benchmark --join-profile 10000
```
