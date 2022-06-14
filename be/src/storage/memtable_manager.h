// This file is licensed under the Elastic License 2.0. Copyright 2021-present,
// StarRocks Limited.

#pragma once

#include <unordered_map>

#include "column/field.h"
#include "column/schema.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/memtable.h"
#include "storage/tablet_schema.h"

namespace starrocks {

namespace vectorized {

static const string LOAD_OP_COLUMN = "__op";

class MemTableManager;

class MemTableContext {
public:
    MemTableContext(std::unique_ptr<MemTable> memtable)
            : _memtable(std::move(memtable)), _last_used_time(time(nullptr)), _in_use(true) {}

    MemTable* memtable_reallocate(int64_t tablet_id, const std::vector<SlotDescriptor*>* slot_descs, MemTableSink* sink,
                                  MemTracker* mem_tracker) {
        _in_use = true;
        _last_used_time = time(nullptr);
        _memtable->reallocate(tablet_id, slot_descs, sink, mem_tracker);

        return _memtable.get();
    }

    bool in_use() const { return _in_use; }

    void set_in_use(bool flag) { _in_use = flag; }

    time_t last_used_time() const { return _last_used_time; }

    void set_last_used_time(time_t time) { _last_used_time = time; }

    int64_t memtable_id() { return _memtable->memtable_id(); }

    MemTable* memtable() { return _memtable.get(); }

private:
    std::unique_ptr<MemTable> _memtable;
    time_t _last_used_time;
    bool _in_use;
};

class MemTableGroup {
public:
    MemTableGroup(MemTableManager* manager) : _memtable_id_generator(0), _memtable_manager(manager) {}

    MemTable* allocate_memtable(int64_t tablet_id, Schema* schema, const std::vector<SlotDescriptor*>* slot_descs,
                                MemTableSink* sink, MemTracker* mem_tracker, bool need_op);

    void free_memtable(MemTable* memtable);

    uint32_t recycle(bool aggressive, time_t cur_time, int64_t max_time_elapsed);

private:
    std::list<MemTableContext> _memtable_group;
    std::list<MemTableContext> _memtable_with_op_group;
    std::atomic<int64_t> _memtable_id_generator;
    MemTableManager* _memtable_manager;
    std::mutex _mutex;
};

class MemTableManager {
public:
    MemTableManager() : _memtable_count(0) {}

    ~MemTableManager() {}

    MemTable* allocate_memtable(int64_t index_id, int64_t tablet_id, const TabletSchema* tablet_schema,
                                const std::vector<SlotDescriptor*>* slot_descs, MemTableSink* sink,
                                MemTracker* mem_tracker);

    void free_memtable(int64_t index_id, MemTable* memtable);

    void increase_memtable_count(int64_t count) { _memtable_count.fetch_add(count, std::memory_order_release); }

    int64_t memtable_count() { return _memtable_count.load(std::memory_order_acquire); }

    // Todo: add aggresively memory recycle when memory is used too much
    void garbage_collection();

private:
    bool check_need_op(KeysType keys_type, const std::vector<SlotDescriptor*>* slot_descs) {
        if (keys_type == KeysType::PRIMARY_KEYS && slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
            return true;
        }
        return false;
    }

    Schema* get_schema(int64_t index_id, const TabletSchema* tablet_schema);

    Schema* get_schema_with_op(int64_t index_id, const TabletSchema* tablet_schema);

    std::unordered_map<uint64_t, MemTableGroup> _memtable_pool;                 // index_id -> MemTable_group
    std::unordered_map<uint64_t, std::unique_ptr<Schema>> _schema_pool;         // index_id -> schema
    std::unordered_map<uint64_t, std::unique_ptr<Schema>> _schema_with_op_pool; // index_id -> schema_with_op
    std::mutex _mutex;
    std::atomic<uint64_t> _memtable_count;
};

} // namespace vectorized
} // namespace starrocks