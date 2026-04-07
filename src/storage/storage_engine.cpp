#include "storage_engine.h"
#include "sql_parser.h"
#include "sql_utils.h"

#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <sys/uio.h>
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
fs::path table_data_path(const std::string& name)  { return storage_root_dir() / (name + ".tbl"); }
fs::path wal_file_path()                           { return storage_root_dir() / "flexql_logical.wal"; }
static fs::path catalog_file_path()               { return storage_root_dir() / "catalog.meta"; }

bool ensure_store_dir() {
    std::error_code ec;
    fs::create_directories(storage_root_dir(), ec);
    return !ec;
}

size_t encode_record_id(PageId pid, SlotId sid) { return ((size_t)pid << 32) | sid; }
void decode_record_id(size_t val, PageId& pid, SlotId& sid) { pid = val >> 32; sid = val & 0xFFFFFFFF; }

static BPlusTree::Mode index_mode_for_column(ColumnType type) {
    if (type == ColumnType::Varchar) return BPlusTree::Mode::Lexicographic;
    return BPlusTree::Mode::Numeric;
}

static std::unique_ptr<BPlusTree> make_primary_index(const std::vector<Column>& columns) {
    if (columns.empty()) return std::make_unique<BPlusTree>();
    return std::make_unique<BPlusTree>(64, index_mode_for_column(columns.front().type));
}

// ── binary write helpers ──────────────────────────────────────────────────────
static void w32(std::string& b, uint32_t v) { b.append(reinterpret_cast<const char*>(&v), 4); }
static void w64(std::string& b, uint64_t v) { b.append(reinterpret_cast<const char*>(&v), 8); }
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

void deserialize_row(const char* data, size_t& off, size_t end, const std::vector<Column>& columns, Row& row) {
    uint32_t col_count;
    if (!r32(data, off, end, col_count)) return;
    const size_t nc = columns.size();
    row.values.reserve(nc);
    for (size_t c = 0; c < nc; ++c) {
        if (off >= end) break;
        char tag = data[off++];
        if (tag == 'V') {
            uint32_t sl; if (!r32(data,off,end,sl)||off+sl>end) break;
            row.values.emplace_back(std::string(data+off,sl)); off+=sl;
        } else if (tag == 'D') {
            uint64_t r64v; if (!r64(data,off,end,r64v)) break;
            double d; memcpy(&d,&r64v,8); row.values.emplace_back(d);
        } else {
            uint64_t r64v; if (!r64(data,off,end,r64v)) break;
            int64_t iv; memcpy(&iv,&r64v,8); row.values.emplace_back(iv);
        }
    }
}

static bool flush_fd(int fd, std::string& error, const char* what) {
#if defined(__linux__)
    if (::fdatasync(fd) == 0) return true;
#endif
    if (::fsync(fd) == 0) return true;
    error = std::string("failed to flush ") + what;
    return false;
}

bool open_table_fd(Table& tbl, std::string& error) {
    if (tbl.row_fd >= 0) return true;
    tbl.row_fd = ::open(table_data_path(tbl.name).c_str(), O_RDWR | O_CREAT, 0644);
    if (tbl.row_fd < 0) { error = "cannot open table file"; return false; }
    off_t file_size = lseek(tbl.row_fd, 0, SEEK_END);
    if (file_size == 0) {
        // Init page 0 metadata, set next_page_id = 1
        char zero[PAGE_SIZE] = {0};
        ::pwrite(tbl.row_fd, zero, PAGE_SIZE, 0); // Metadata page
        tbl.next_page_id = 1;
        tbl.next_slot_id = 0;
    } else {
        tbl.next_page_id = file_size / PAGE_SIZE;
        if (tbl.next_page_id == 0) tbl.next_page_id = 1;
    }
    return true;
}

void close_table_fd(Table& tbl) {
    if (tbl.row_fd >= 0) { ::close(tbl.row_fd); tbl.row_fd = -1; }
}

bool rewrite_catalog(const Database& db, std::string& error) {
    fs::path tmp = catalog_file_path(); tmp += ".tmp";
    std::ofstream out(tmp);
    if (!out) { error = "cannot write catalog"; return false; }
    for (auto& [name, tbl] : db.tables) {
        out << tbl->name;
        for (auto& col : tbl->columns) out << '|' << col.name << ':' << column_type_name(col.type);
        out << '\n';
    }
    out.close();
    std::error_code ec;
    fs::rename(tmp, catalog_file_path(), ec);
    if (ec) { error = "catalog rename failed"; return false; }
    return true;
}

static bool open_wal_fd(Database& db, std::string& error) {
    if (db.wal_fd >= 0) return true;
    db.wal_fd = ::open(wal_file_path().c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (db.wal_fd < 0) { error = "cannot open wal file"; return false; }
    return true;
}

static size_t serialize_row_to_buffer(const Table& tbl, const Row& row, char* out) {
    size_t off = 0;
    const size_t nc = tbl.columns.size();
    uint32_t unc = static_cast<uint32_t>(nc);
    memcpy(out + off, &unc, 4); off += 4;
    for (size_t i = 0; i < nc; ++i) {
        if (tbl.columns[i].type == ColumnType::Varchar) {
            const auto& s = std::get<std::string>(row.values[i]);
            out[off++] = 'V';
            uint32_t len = static_cast<uint32_t>(s.size());
            memcpy(out + off, &len, 4); off += 4;
            memcpy(out + off, s.data(), len); off += len;
        } else if (tbl.columns[i].type == ColumnType::Decimal) {
            double d = std::holds_alternative<double>(row.values[i])
                ? std::get<double>(row.values[i])
                : static_cast<double>(std::get<int64_t>(row.values[i]));
            out[off++] = 'D';
            memcpy(out + off, &d, 8); off += 8;
        } else {
            int64_t iv = std::holds_alternative<int64_t>(row.values[i])
                ? std::get<int64_t>(row.values[i])
                : static_cast<int64_t>(std::get<double>(row.values[i]));
            out[off++] = 'I';
            memcpy(out + off, &iv, 8); off += 8;
        }
    }
    return off;
}

struct PageHeader {
    uint32_t record_count;
    uint32_t free_offset;
};

// Insert batch using local page builder design + Zero Copy + Group Commit
bool persist_rows_batch(Database& db, Table& tbl, const std::vector<Row>& rows, const std::vector<std::string>& pk_strings, std::string& error) {
    if (!open_table_fd(tbl, error)) return false;
    
    PageId current_pid = tbl.next_page_id;
    if (current_pid > 1 && tbl.next_slot_id == 0) {
        // Last page was full, start a new one
    } else if (current_pid > 1) {
        current_pid--; // Append to tail page
    }
    
    char local_page[PAGE_SIZE];
    PageHeader header;
    uint32_t* offsets = (uint32_t*)(local_page + 8);
    
    auto load_page = [&]() {
        Page* p = db.pool.fetch_page(tbl.row_fd, current_pid, true);
        if (p) {
            memcpy(local_page, p->data, PAGE_SIZE);
            memcpy(&header, local_page, 8);
            db.pool.unpin_page(tbl.row_fd, current_pid, false);
        } else {
            memset(local_page, 0, PAGE_SIZE);
            header.record_count = 0;
            header.free_offset = PAGE_SIZE;
            memcpy(local_page, &header, 8);
        }
    };
    
    auto pub_page = [&]() {
        memcpy(local_page, &header, 8);
        db.pool.push_page_direct(tbl.row_fd, current_pid, local_page);
        if (current_pid >= tbl.next_page_id) {
            tbl.next_page_id = current_pid + 1;
        }
        tbl.next_slot_id = header.record_count;
    };

    if (current_pid < tbl.next_page_id) {
        load_page();
    } else {
        memset(local_page, 0, PAGE_SIZE);
        header.record_count = 0;
        header.free_offset = PAGE_SIZE;
        tbl.next_slot_id = 0;
    }
    
    std::string wal_batch;
    size_t wal_offset = 0;
    if (db.use_wal) {
        // Pre-calculate exact wal buffer size to avoid all reallocations
        size_t total_wal_size = 0;
        uint32_t name_len = static_cast<uint32_t>(tbl.name.size());
        for (const auto& row : rows) {
            size_t row_size = 4; // record_count col
            for (size_t i = 0; i < tbl.columns.size(); ++i) {
                if (tbl.columns[i].type == ColumnType::Varchar) {
                    row_size += 1 + 4 + std::get<std::string>(row.values[i]).size();
                } else if (tbl.columns[i].type == ColumnType::Decimal) {
                    row_size += 1 + 8;
                } else {
                    row_size += 1 + 8;
                }
            }
            total_wal_size += 4 + name_len + 4 + 4 + 4 + row_size;
        }
        wal_batch.resize(total_wal_size);
    }
    
    char temp_row_buf[PAGE_SIZE];
    uint32_t name_len = static_cast<uint32_t>(tbl.name.size());
    
    for (size_t i = 0; i < rows.size(); ++i) {
        const auto& row = rows[i];
        size_t row_size = serialize_row_to_buffer(tbl, row, temp_row_buf);
        
        size_t required = row_size + 4; // payload + offset entry
        size_t available = header.free_offset - (8 + header.record_count * 4);
        
        if (required > available) {
            pub_page();
            current_pid++;
            memset(local_page, 0, PAGE_SIZE);
            header.record_count = 0;
            header.free_offset = PAGE_SIZE;
            tbl.next_slot_id = 0;
        }
        
        header.free_offset -= static_cast<uint32_t>(row_size);
        memcpy(local_page + header.free_offset, temp_row_buf, row_size);
        offsets[header.record_count] = header.free_offset;
        
        if (db.use_wal) {
            char* ptr = wal_batch.data() + wal_offset;
            memcpy(ptr, &name_len, 4); ptr += 4;
            memcpy(ptr, tbl.name.data(), name_len); ptr += name_len;
            memcpy(ptr, &current_pid, 4); ptr += 4;
            uint32_t slot = header.record_count;
            memcpy(ptr, &slot, 4); ptr += 4;
            uint32_t rs = static_cast<uint32_t>(row_size);
            memcpy(ptr, &rs, 4); ptr += 4;
            memcpy(ptr, temp_row_buf, row_size); ptr += row_size;
            wal_offset = ptr - wal_batch.data();
        }
        
        tbl.primary_index->insert(pk_strings[i], encode_record_id(current_pid, header.record_count));
        
        header.record_count++;
    }
    
    pub_page();
    
    if (db.use_wal && !wal_batch.empty()) {
        if (open_wal_fd(db, error)) {
            const char* ptr = wal_batch.data();
            size_t rem = wal_batch.size();
            while (rem > 0) {
                ssize_t rc = ::write(db.wal_fd, ptr, rem);
                if (rc <= 0) break;
                ptr += rc; rem -= rc;
            }
        }
        
        ++tbl.batches_since_sync;
        if (tbl.batches_since_sync >= tbl.sync_every_batches) {
            flush_fd(db.wal_fd, error, "wal");
            db.pool.flush_all(); 
            tbl.batches_since_sync = 0;
        }
    }
    
    return true;
}

void scan_table(Database& db, Table& tbl, std::function<void(const Row&)> callback) {
    for (PageId pid = 1; pid < tbl.next_page_id; ++pid) {
        Page* page = db.pool.fetch_page(tbl.row_fd, pid);
        if (!page) continue;
        
        uint32_t record_count;
        memcpy(&record_count, page->data, 4);
        uint32_t* offsets = (uint32_t*)(page->data + 8);
        
        for (uint32_t sid = 0; sid < record_count; ++sid) {
            Row row;
            size_t off = offsets[sid];
            deserialize_row(page->data, off, PAGE_SIZE, tbl.columns, row);
            if (!row.values.empty()) {
                callback(row);
            }
        }
        db.pool.unpin_page(tbl.row_fd, pid, false);
    }
}

bool fetch_row_by_id(Database& db, Table& tbl, size_t index_val, Row& row) {
    PageId pid; SlotId sid;
    decode_record_id(index_val, pid, sid);
    Page* page = db.pool.fetch_page(tbl.row_fd, pid);
    if (!page) return false;
    
    uint32_t record_count;
    memcpy(&record_count, page->data, 4);
    if (sid >= record_count) {
        db.pool.unpin_page(tbl.row_fd, pid, false);
        return false;
    }
    
    uint32_t* offsets = (uint32_t*)(page->data + 8);
    size_t off = offsets[sid];
    deserialize_row(page->data, off, PAGE_SIZE, tbl.columns, row);
    
    db.pool.unpin_page(tbl.row_fd, pid, false);
    return !row.values.empty();
}

static bool replay_wal(Database& db, std::string& error) {
    std::ifstream in(wal_file_path(), std::ios::binary | std::ios::ate);
    if (!in) return true;
    size_t file_size = in.tellg();
    if (file_size == 0) return true;
    in.seekg(0);
    std::string raw(file_size, '\0');
    in.read(raw.data(), file_size);
    
    size_t off = 0;
    while (off < file_size) {
        uint32_t tlen; if (!r32(raw.data(), off, file_size, tlen)) break;
        std::string tname(raw.data() + off, tlen); off += tlen;
        uint32_t pid; if (!r32(raw.data(), off, file_size, pid)) break;
        uint32_t sid; if (!r32(raw.data(), off, file_size, sid)) break;
        uint32_t plen; if (!r32(raw.data(), off, file_size, plen)) break;
        std::string payload(raw.data() + off, plen); off += plen;
        
        auto tbl = find_table(db, tname);
        if (!tbl) continue;
        
        Page* page = db.pool.fetch_page(tbl->row_fd, pid, false);
        if (!page) {
            page = db.pool.new_page(tbl->row_fd, pid);
        }
        if (page) {
            uint32_t rec_count; memcpy(&rec_count, page->data, 4);
            uint32_t free_off; memcpy(&free_off, page->data + 4, 4);
            if (rec_count == 0) free_off = PAGE_SIZE;
            
            if (sid >= rec_count) {
                // assume appending
                free_off -= payload.size();
                memcpy(page->data + free_off, payload.data(), payload.size());
                uint32_t* offsets = (uint32_t*)(page->data + 8);
                offsets[sid] = free_off;
                rec_count = sid + 1;
                memcpy(page->data, &rec_count, 4);
                memcpy(page->data + 4, &free_off, 4);
                page->is_dirty = true;
                
                if (pid >= tbl->next_page_id) tbl->next_page_id = pid + 1;
                tbl->next_slot_id = rec_count;
            }
            db.pool.unpin_page(tbl->row_fd, pid, true);
        }
    }
    
    int wal_fd = ::open(wal_file_path().c_str(), O_RDWR | O_CREAT, 0644);
    if (wal_fd >= 0) {
        ::ftruncate(wal_fd, 0);
        ::close(wal_fd);
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
    
    // Build primary index
    for (auto& [name, tbl] : db.tables) {
        for (PageId pid = 1; pid < tbl->next_page_id; ++pid) {
            Page* page = db.pool.fetch_page(tbl->row_fd, pid);
            if (!page) continue;
            uint32_t record_count;
            memcpy(&record_count, page->data, 4);
            uint32_t* offsets = (uint32_t*)(page->data + 8);
            
            for (uint32_t sid = 0; sid < record_count; ++sid) {
                Row row;
                size_t off = offsets[sid];
                deserialize_row(page->data, off, PAGE_SIZE, tbl->columns, row);
                if (!row.values.empty()) {
                    std::string pk = value_to_string(row.values[0]);
                    tbl->primary_index->insert(pk, encode_record_id(pid, sid));
                }
            }
            db.pool.unpin_page(tbl->row_fd, pid, false);
        }
    }
    
    return true;
}

std::shared_ptr<Table> find_table(const Database& db, const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(db.mutex);
    auto it = db.tables.find(name);
    return it==db.tables.end()?nullptr:it->second;
}
