# FlexQL

A high-performance client-server SQL-like database driver written in C++17.
Built from scratch — no external database libraries.

---

## Quick Start

```bash
# Build everything
sh compile.sh

# Terminal 1 — start the server
rm -rf data && mkdir -p data
./bin/server

# Terminal 2 — run unit tests
./bin/benchmark --unit-test

# Terminal 2 — run benchmark (1M rows)
./bin/benchmark 1000000

# Terminal 2 — interactive REPL
./bin/flexql_cli
```

---

## Build Requirements

- g++ with C++17 support (GCC 8+ or Clang 7+)
- Linux / macOS
- POSIX sockets and pthreads

---

## Supported SQL

```sql
CREATE TABLE t (ID DECIMAL PRIMARY KEY NOT NULL, NAME VARCHAR(64));

INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');

SELECT * FROM t;
SELECT ID, NAME FROM t WHERE ID = 1;
SELECT * FROM t WHERE BALANCE > 1000;

SELECT a.NAME, b.AMOUNT
FROM users a
INNER JOIN orders b ON a.ID = b.USER_ID
WHERE b.AMOUNT > 500;
```

WHERE operators: `=`, `>`, `<`, `>=`, `<=`

---

## Server Flags

```bash
./bin/server                         # default: fsync every batch (durable direct append)
./bin/server --fresh                 # delete data directory and start clean
./bin/server --wal                   # write-ahead logging mode (safer recovery, slower writes)
./bin/server --sync-every-batches=10 # relax direct-mode fsync frequency for throughput
```

---

## Client API (C)

```c
#include "flexql.h"

// Open connection
FlexQL *db;
flexql_open("127.0.0.1", 9000, &db);

// Execute SQL
int callback(void *arg, int col_count, char **values, char **col_names) {
    for (int i = 0; i < col_count; i++)
        printf("%s = %s\n", col_names[i], values[i]);
    return 0;  // return 1 to stop early
}

char *errmsg = NULL;
flexql_exec(db, "SELECT * FROM t;", callback, NULL, &errmsg);
if (errmsg) { printf("Error: %s\n", errmsg); flexql_free(errmsg); }

// Close
flexql_close(db);
```

---

## Performance

| Scale | Elapsed | Throughput |
|-------|---------|------------|
| 100K rows INSERT | ~0.32 s | ~312K rows/sec |
| 1M rows INSERT | ~3.2 s | ~312K rows/sec |
| SELECT by PK | < 1 ms | indexed lookup |

Achieved by:
- **No page buffer pool** — rows append directly to binary files
- **B+ tree primary index** — supports equality and range predicates on the primary key
- **Zero-copy INSERT parser** — no uppercase copy of SQL string
- **Batch INSERT** — 50K rows per TCP round-trip

Performance / durability trade-off:
- **Direct mode with `sync_every_batches=1`** gives crash-safe acknowledged inserts, but lower throughput than deferred `fsync`.
- **Direct mode with larger `sync_every_batches`** improves throughput, but acknowledged batches since the last `fsync` can be lost on crash.
- **`--wal` mode** writes each batch to a durable WAL first and replays it on restart; this improves crash recovery semantics, but adds log write and replay overhead.

---

## Data Directory Layout

```
data/
├── catalog.meta       table schemas (text)
├── BIG_USERS.rows.bin binary row data (append-only)
└── TEST_USERS.rows.bin
```

Data persists across server restarts. The server loads all tables from disk
on startup and rebuilds the B+ tree index from the binary row files. In WAL
mode, pending logged batches are replayed into table files before loading.

---

## Design

See [DESIGN.md](DESIGN.md) for full architecture documentation.  
See [EXPERIMENTS.md](EXPERIMENTS.md) for the optimisation history and
profiling data that drove each design decision.
