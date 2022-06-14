// This file is licensed under the Elastic License 2.0. Copyright 2021-present,
// StarRocks Limited.
#include "storage/memtable_manager.h"

namespace starrocks {

namespace vectorized {

MemTable* MemTableGroup::allocate_memtable(int64_t tablet_id, Schema* schema,
                                           const std::vector<SlotDescriptor*>* slot_descs, MemTableSink* sink,
                                           MemTracker* mem_tracker, bool need_op) {
    std::lock_guard<std::mutex> lock(_mutex);
    if (!need_op) {
        for (auto it = _memtable_group.begin(); it != _memtable_group.end(); ++it) {
            if (!it->in_use()) {
                return it->memtable_reallocate(tablet_id, slot_descs, sink, mem_tracker);
            }
        }
    } else {
        for (auto it = _memtable_with_op_group.begin(); it != _memtable_with_op_group.end(); ++it) {
            if (!it->in_use()) {
                return it->memtable_reallocate(tablet_id, slot_descs, sink, mem_tracker);
            }
        }
    }
    // no free memtable here, we need to allocate new memtable
    int64_t memtable_id = _memtable_id_generator.load(std::memory_order_acquire);
    std::unique_ptr<MemTable> memtable =
            std::make_unique<MemTable>(memtable_id, tablet_id, schema, slot_descs, sink, mem_tracker);
    // LOG(WARNING) << "new memtable_id" << memtable_id;

    _memtable_id_generator.fetch_add(1, std::memory_order_release);
    _memtable_manager->increase_memtable_count(1);
    MemTable* result = memtable.get();
    if (!need_op) {
        _memtable_group.emplace_back(std::move(memtable));
    } else {
        _memtable_with_op_group.emplace_back(std::move(memtable));
    }
    return result;
}

void MemTableGroup::free_memtable(MemTable* memtable) {
    std::lock_guard<std::mutex> lock(_mutex);
    int64_t memtable_id = memtable->memtable_id();
    // LOG(WARNING) << "free memtable_id" << memtable_id;
    bool need_op = memtable->has_op_slot();
    bool free_flag = false;
    if (!need_op) {
        for (auto it = _memtable_group.begin(); it != _memtable_group.end(); ++it) {
            if (it->memtable_id() == memtable_id) {
                DCHECK(it->in_use());
                it->set_in_use(false);
                free_flag = true;
                break;
            }
        }
    } else {
        for (auto it = _memtable_with_op_group.begin(); it != _memtable_with_op_group.end(); ++it) {
            if (it->memtable_id() == memtable_id) {
                DCHECK(it->in_use());
                it->set_in_use(false);
                free_flag = true;
                break;
            }
        }
    }
    DCHECK(free_flag);
}

uint32_t MemTableGroup::recycle(bool aggressive, time_t cur_time, int64_t max_time_elapsed) {
    std::lock_guard<std::mutex> lock(_mutex);
    int64_t count_before_recycle = _memtable_group.size() + _memtable_with_op_group.size();
    // LOG(WARNING) << "cur_time: " << cur_time;
    // for (auto it = _memtable_group.begin(); it != _memtable_group.end(); ++it) {
    // LOG(WARNING) << "memtable_id: " << it->memtable_id() << ", memtable_time: " << it->last_used_time();
    // }
    if (aggressive) {
        _memtable_group.remove_if([](const MemTableContext& context) { return !context.in_use(); });
        _memtable_with_op_group.remove_if([](const MemTableContext& context) { return !context.in_use(); });
    } else {
        _memtable_group.remove_if([&cur_time, &max_time_elapsed](const MemTableContext& context) {
            return !context.in_use() && (cur_time - context.last_used_time()) >= max_time_elapsed;
        });
        _memtable_with_op_group.remove_if([&cur_time, &max_time_elapsed](const MemTableContext& context) {
            return !context.in_use() && (cur_time - context.last_used_time()) >= max_time_elapsed;
        });
    }
    int64_t count_after_recycle = _memtable_group.size() + _memtable_with_op_group.size();
    _memtable_manager->increase_memtable_count(count_after_recycle - count_before_recycle);
    return count_after_recycle;
}

MemTable* MemTableManager::MemTableManager::allocate_memtable(int64_t index_id, int64_t tablet_id,
                                                              const TabletSchema* tablet_schema,
                                                              const std::vector<SlotDescriptor*>* slot_descs,
                                                              MemTableSink* sink, MemTracker* mem_tracker) {
    std::unique_lock<std::mutex> lock(_mutex);
    auto iter = _memtable_pool.find(index_id);
    if (iter == _memtable_pool.end()) {
        auto [it, success] = _memtable_pool.emplace(index_id, this);
        iter = it;
    }
    bool need_op = check_need_op(tablet_schema->keys_type(), slot_descs);
    Schema* schema;
    if (!need_op) {
        schema = get_schema(index_id, tablet_schema);
    } else {
        schema = get_schema_with_op(index_id, tablet_schema);
    }
    lock.unlock();
    return iter->second.allocate_memtable(tablet_id, schema, slot_descs, sink, mem_tracker, need_op);
}

void MemTableManager::free_memtable(int64_t index_id, MemTable* memtable) {
    std::unique_lock<std::mutex> lock(_mutex);
    // Todo: if it is still in use, which means it hasn't been reuse_finalize
    // before, then we need to mimic reuse_finalize
    auto iter = _memtable_pool.find(index_id);
    DCHECK(iter != _memtable_pool.end());
    lock.unlock();
    iter->second.free_memtable(memtable);
}

// Todo: add aggresively memory recycle when memory is used too much
void MemTableManager::garbage_collection() {
    std::lock_guard<std::mutex> lock(_mutex);
    time_t now = time(nullptr);
    std::vector<int64_t> recycle_index_id;
    for (auto it = _memtable_pool.begin(); it != _memtable_pool.end(); it++) {
        if (it->second.recycle(false, now, config::memtable_pool_recycle_interval_sec) == 0) {
            recycle_index_id.push_back(it->first);
        }
    }

    for (size_t i = 0; i < recycle_index_id.size(); i++) {
        int64_t index_id = recycle_index_id[i];
        _memtable_pool.erase(index_id);
        _schema_pool.erase(index_id);
        _schema_with_op_pool.erase(index_id);
    }
    // todo: remove following warning log
    // LOG(WARNING) << "memtable count: "
    //              << _memtable_count.load(std::memory_order_acquire);
}

Schema* MemTableManager::get_schema(int64_t index_id, const TabletSchema* tablet_schema) {
    auto iter = _schema_pool.find(index_id);
    if (iter != _schema_pool.end()) {
        return iter->second.get();
    }
    Fields fields;
    for (ColumnId cid = 0; cid < tablet_schema->num_columns(); ++cid) {
        auto f = ChunkHelper::convert_field_to_format_v2(cid, tablet_schema->column(cid));
        fields.emplace_back(std::make_shared<Field>(std::move(f)));
    }
    std::unique_ptr<Schema> schema = std::make_unique<Schema>(std::move(fields), tablet_schema->keys_type());
    Schema* result = schema.get();
    _schema_pool.insert(std::make_pair(index_id, std::move(schema)));
    return result;
}

Schema* MemTableManager::get_schema_with_op(int64_t index_id, const TabletSchema* tablet_schema) {
    auto iter = _schema_with_op_pool.find(index_id);
    if (iter != _schema_with_op_pool.end()) {
        return iter->second.get();
    }
    Fields fields;
    for (ColumnId cid = 0; cid < tablet_schema->num_columns(); ++cid) {
        auto f = ChunkHelper::convert_field_to_format_v2(cid, tablet_schema->column(cid));
        fields.emplace_back(std::make_shared<Field>(std::move(f)));
    }
    std::unique_ptr<Schema> schema_with_op = std::make_unique<Schema>(std::move(fields), tablet_schema->keys_type());
    auto op_column = std::make_shared<Field>((ColumnId)-1, LOAD_OP_COLUMN, FieldType::OLAP_FIELD_TYPE_TINYINT, false);
    op_column->set_aggregate_method(OLAP_FIELD_AGGREGATION_REPLACE);
    schema_with_op->append(op_column);
    Schema* result = schema_with_op.get();
    _schema_with_op_pool.insert(std::make_pair(index_id, std::move(schema_with_op)));
    return result;
}

} // namespace vectorized
} // namespace starrocks