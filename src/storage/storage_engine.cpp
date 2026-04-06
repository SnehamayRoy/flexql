#include "storage_engine.h"
#include "sql_parser.h"
#include "sql_utils.h"

#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <filesystem>
#include <sstream>

namespace fs = std::filesystem;

static fs::path project_root() {
    std::vector<char> buf(4096, '\0');
    ssize_t n = ::readlink("/proc/self/exe", buf.data(), buf.size() - 1);
    if (n <= 0) return fs::current_path();
    buf[n] = '\0';
    return fs::path(buf.data()).parent_path().parent_path();
}

fs::path storage_root_dir()                        { return project_root() / "data"; }
fs::path table_data_path(const std::string& name)  { return storage_root_dir() / (name + ".rows.bin"); }
fs::path wal_file_path()                           { return storage_root_dir() / "flexql.wal"; }
static fs::path catalog_file_path()               { return storage_root_dir() / "catalog.meta"; }

static BPlusTree::Mode index_mode_for_column(ColumnType type) {
    switch (type) {
        case ColumnType::Int:
        case ColumnType::Decimal:
        case ColumnType::DateTime:
            return BPlusTree::Mode::Numeric;
        case ColumnType::Varchar:
            return BPlusTree::Mode::Lexicographic;
    }
    return BPlusTree::Mode::Lexicographic;
}

static std::unique_ptr<BPlusTree> make_primary_index(const std::vector<Column>& columns) {
    if (columns.empty()) {
        return std::make_unique<BPlusTree>();
    }
    return std::make_unique<BPlusTree>(16, index_mode_for_column(columns.front().type));
}

static size_t estimate_row_bytes(const Table& tbl);

bool ensure_store_dir() {
    std::error_code ec;
    fs::create_directories(storage_root_dir(), ec);
    return !ec;
}

// ── binary write helpers ──────────────────────────────────────────────────────
static void w32(std::string& b, uint32_t v) { b.append(reinterpret_cast<char*>(&v), 4); }
static void w64(std::string& b, uint64_t v) { b.append(reinterpret_cast<char*>(&v), 8); }
static bool r32(const char* p, size_t& o, size_t e, uint32_t& v) {
    if (o+4>e) return false; memcpy(&v,p+o,4); o+=4; return true; }
static bool r64(const char* p, size_t& o, size_t e, uint64_t& v) {
    if (o+8>e) return false; memcpy(&v,p+o,8); o+=8; return true; }

static void serialize_row(const Table& tbl, const Row& row, std::string& out) {
    const size_t nc = tbl.columns.size();
    w32(out, static_cast<uint32_t>(nc));
    for (size_t i = 0; i < nc; ++i) {
        if (tbl.columns[i].type == ColumnType::Varchar) {
            const auto& s = std::get<std::string>(row.values[i]);
            out += 'V'; w32(out, static_cast<uint32_t>(s.size())); out.append(s);
        } else if (tbl.columns[i].type == ColumnType::Decimal) {
            double d = std::holds_alternative<double>(row.values[i])
                ? std::get<double>(row.values[i])
                : static_cast<double>(std::get<int64_t>(row.values[i]));
            uint64_t raw; memcpy(&raw,&d,8); out += 'D'; w64(out,raw);
        } else {
            int64_t iv = std::holds_alternative<int64_t>(row.values[i])
                ? std::get<int64_t>(row.values[i])
                : static_cast<int64_t>(std::get<double>(row.values[i]));
            uint64_t raw; memcpy(&raw,&iv,8); out += 'I'; w64(out,raw);
        }
    }
}

static std::string build_row_batch_payload(const Table& tbl, const std::vector<Row>& rows) {
    std::string buf;
    buf.reserve(rows.size() * estimate_row_bytes(tbl));

    for (const Row& row : rows) {
        const size_t off0 = buf.size();
        w32(buf, 0);
        const size_t start = buf.size();
        serialize_row(tbl, row, buf);
        uint32_t plen = static_cast<uint32_t>(buf.size() - start);
        memcpy(&buf[off0], &plen, 4);
    }
    return buf;
}

static bool write_fully(int fd, const std::string& buf, std::string& error, const char* what) {
    const char* ptr = buf.data();
    size_t remaining = buf.size();
    while (remaining > 0) {
        ssize_t rc = ::write(fd, ptr, remaining);
        if (rc <= 0) {
            error = std::string(what) + " write error";
            return false;
        }
        ptr += rc;
        remaining -= static_cast<size_t>(rc);
    }
    return true;
}

static bool open_wal_fd(Database& db, std::string& error) {
    if (db.wal_fd >= 0) {
        return true;
    }
    db.wal_fd = ::open(wal_file_path().c_str(), O_RDWR | O_CREAT, 0644);
    if (db.wal_fd < 0) {
        error = "cannot open wal file";
        return false;
    }
    return true;
}

static bool replay_wal(Database& db, std::string& error) {
    std::ifstream in(wal_file_path(), std::ios::binary | std::ios::ate);
    if (!in) {
        return true;
    }
    const size_t file_size = static_cast<size_t>(in.tellg());
    if (file_size == 0) {
        return true;
    }
    in.seekg(0);
    std::string raw(file_size, '\0');
    in.read(raw.data(), static_cast<std::streamsize>(file_size));
    if (in.gcount() != static_cast<std::streamsize>(file_size)) {
        error = "failed to read wal";
        return false;
    }

    size_t off = 0;
    while (off < file_size) {
        uint32_t table_name_len = 0;
        uint32_t payload_len = 0;
        if (!r32(raw.data(), off, file_size, table_name_len)) {
            error = "truncated wal record";
            return false;
        }
        if (off + table_name_len > file_size) {
            error = "corrupt wal table name";
            return false;
        }
        std::string table_name(raw.data() + off, table_name_len);
        off += table_name_len;
        if (!r32(raw.data(), off, file_size, payload_len) || off + payload_len > file_size) {
            error = "corrupt wal payload";
            return false;
        }
        std::string payload(raw.data() + off, payload_len);
        off += payload_len;

        auto table = find_table(db, table_name);
        if (!table) {
            error = "wal references missing table: " + table_name;
            return false;
        }
        if (!open_table_fd(*table, error)) {
            return false;
        }
        if (!write_fully(table->row_fd, payload, error, "table replay")) {
            return false;
        }
        if (::fsync(table->row_fd) != 0) {
            error = "failed to fsync replayed table data";
            return false;
        }
    }

    int wal_fd = ::open(wal_file_path().c_str(), O_RDWR | O_CREAT, 0644);
    if (wal_fd < 0) {
        error = "cannot reopen wal for truncate";
        return false;
    }
    bool ok = (::ftruncate(wal_fd, 0) == 0) && (::fsync(wal_fd) == 0);
    ::close(wal_fd);
    if (!ok) {
        error = "failed to truncate wal after replay";
        return false;
    }
    return true;
}

// ── catalog ───────────────────────────────────────────────────────────────────
bool rewrite_catalog(const Database& db, std::string& error) {
    fs::path tmp = catalog_file_path(); tmp += ".tmp";
    std::ofstream out(tmp);
    if (!out) { error = "cannot write catalog"; return false; }
    for (auto& [name, tbl] : db.tables) {
        out << tbl->name;
        for (auto& col : tbl->columns)
            out << '|' << col.name << ':' << column_type_name(col.type);
        out << '\n';
    }
    out.close();
    std::error_code ec;
    fs::rename(tmp, catalog_file_path(), ec);
    if (ec) { error = "catalog rename failed"; return false; }
    return true;
}

bool open_table_fd(Table& tbl, std::string& error) {
    if (tbl.row_fd >= 0) return true;
    tbl.row_fd = ::open(table_data_path(tbl.name).c_str(),
                        O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (tbl.row_fd < 0) { error = "cannot open table file"; return false; }
    return true;
}
void close_table_fd(Table& tbl) {
    if (tbl.row_fd >= 0) { ::close(tbl.row_fd); tbl.row_fd = -1; }
}

// ── direct append — NO page buffer pool ──────────────────────────────────────
// Estimate bytes per row to avoid buffer reallocation
static size_t estimate_row_bytes(const Table& tbl) {
    size_t est = 4 + 4; // frame len + col count
    for (const auto& col : tbl.columns) {
        if (col.type == ColumnType::Varchar) est += 1 + 4 + 32; // tag+len+avg
        else                                 est += 1 + 8;       // tag+8B
    }
    return est;
}

bool append_rows_to_disk(Table& tbl, const std::vector<Row>& rows, std::string& error) {
    if (!open_table_fd(tbl, error)) return false;
    std::string buf = build_row_batch_payload(tbl, rows);
    if (!write_fully(tbl.row_fd, buf, error, "table")) {
        return false;
    }

    ++tbl.batches_since_sync;
    if (tbl.batches_since_sync >= tbl.sync_every_batches) {
        if (::fsync(tbl.row_fd) != 0) {
            error = "failed to fsync table data";
            return false;
        }
        tbl.batches_since_sync = 0;
    }
    return true;
}

bool persist_rows(Database& db, Table& tbl, const std::vector<Row>& rows, std::string& error) {
    if (!db.use_wal) {
        return append_rows_to_disk(tbl, rows, error);
    }

    if (!open_wal_fd(db, error)) {
        return false;
    }

    std::string payload = build_row_batch_payload(tbl, rows);
    std::string wal_record;
    w32(wal_record, static_cast<uint32_t>(tbl.name.size()));
    wal_record.append(tbl.name);
    w32(wal_record, static_cast<uint32_t>(payload.size()));
    wal_record.append(payload);

    if (!write_fully(db.wal_fd, wal_record, error, "wal")) {
        return false;
    }
    if (::fsync(db.wal_fd) != 0) {
        error = "failed to fsync wal";
        return false;
    }
    return true;
}

// ── load rows from disk + rebuild primary index ───────────────────────────────
static bool load_table_rows(Table& tbl, std::string& error) {
    std::ifstream in(table_data_path(tbl.name), std::ios::binary | std::ios::ate);
    if (!in) return true;
    size_t fsize = static_cast<size_t>(in.tellg());
    in.seekg(0);
    std::string raw(fsize, '\0');
    in.read(&raw[0], static_cast<std::streamsize>(fsize));

    const char* data = raw.data();
    size_t off = 0;
    const size_t nc = tbl.columns.size();

    while (off < fsize) {
        uint32_t plen;
        if (!r32(data, off, fsize, plen)) {
            error = "truncated row frame header in " + tbl.name;
            return false;
        }
        const size_t pend = off + plen;
        if (pend > fsize) { error = "truncated row in "+tbl.name; return false; }

        uint32_t col_count;
        if (!r32(data, off, pend, col_count)) {
            error = "truncated column-count header in " + tbl.name;
            return false;
        }
        if (col_count != nc) { error = "col mismatch in "+tbl.name; return false; }

        Row row; row.values.reserve(nc);
        bool ok = true;
        for (size_t c = 0; c < nc && ok; ++c) {
            if (off >= pend) { ok=false; break; }
            char tag = data[off++];
            if (tag == 'V') {
                uint32_t sl; if (!r32(data,off,pend,sl)||off+sl>pend){ok=false;break;}
                row.values.emplace_back(std::string(data+off,sl)); off+=sl;
            } else if (tag == 'D') {
                uint64_t r64v; if (!r64(data,off,pend,r64v)){ok=false;break;}
                double d; memcpy(&d,&r64v,8); row.values.emplace_back(d);
            } else {
                uint64_t r64v; if (!r64(data,off,pend,r64v)){ok=false;break;}
                int64_t iv; memcpy(&iv,&r64v,8); row.values.emplace_back(iv);
            }
        }
        if (!ok) { error = "corrupt row in "+tbl.name; return false; }
        off = pend;

        const std::string pk = value_to_string(row.values[0]);
        if (!tbl.primary_index->insert(pk, tbl.rows.size())) {
            error = "duplicate primary key while loading " + tbl.name;
            return false;
        }
        tbl.rows.push_back(std::move(row));
    }
    return true;
}

bool load_database(Database& db, std::string& error) {
    if (!ensure_store_dir()) { error = "cannot create data dir"; return false; }
    std::ifstream cat(catalog_file_path());
    if (!cat) return true;

    std::string line;
    while (std::getline(cat, line)) {
        line = trim(line); if (line.empty()) continue;
        std::vector<std::string> parts;
        std::stringstream ss(line); std::string p;
        while (std::getline(ss,p,'|')) parts.push_back(p);
        if (parts.size()<2){error="corrupt catalog";return false;}

        auto tbl = std::make_shared<Table>();
        tbl->name = parts[0];
        tbl->sync_every_batches = db.sync_every_batches;
        for (size_t i=1;i<parts.size();++i){
            size_t colon=parts[i].find(':');
            if (colon==std::string::npos){error="corrupt catalog col";return false;}
            Column col{parts[i].substr(0,colon), parse_column_type(parts[i].substr(colon+1))};
            tbl->column_index[col.name]=tbl->columns.size();
            tbl->columns.push_back(col);
        }
        tbl->primary_index = make_primary_index(tbl->columns);
        if (!open_table_fd(*tbl,error)) return false;
        db.tables[tbl->name]=tbl;
    }
    if (!replay_wal(db, error)) return false;
    for (auto& [name, tbl] : db.tables) {
        tbl->rows.reserve(10'000'000);
        if (!load_table_rows(*tbl, error)) return false;
    }
    if (db.use_wal && !open_wal_fd(db, error)) return false;
    return true;
}

std::shared_ptr<Table> find_table(const Database& db, const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(db.mutex);
    auto it = db.tables.find(name);
    return it==db.tables.end()?nullptr:it->second;
}
