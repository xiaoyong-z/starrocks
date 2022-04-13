// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "storage/rowset/column_reader_cache.h"

namespace starrocks {
    ColumnReaderCache::ColumnReaderCache(uint64_t capacity): _capactiy(capacity) {
        // Make empty circular linked list
        _lru.next = &_lru;
        _lru.prev = &_lru;
    }

    ColumnReaderCache::~ColumnReaderCache() {
        // prune();
    }

    bool look_up_column_readers(vector<CacheKey> cacheKeys, std::vector<ColumnReader*>& readers, std::vector<uint32_t>& unload_indexes) {
        std::lock_guard l(_mutex);
        bool all_column_loaded = true;  
        for (usize_t i = 0; i < cacheKeys.size(); i++) {
            if (!cacheKeys[i].empty()) {
                LRUHandle* e = _table.lookup(key, hash);
                if (e != nullptr) {
                    // we get it from _table, so in_cache must be true
                    DCHECK(e->in_cache);
                    if (e->refs == 1) {
                        // only in LRU free list, remove it from list
                        _lru_remove(e);
                    }
                    e->refs++;
                    ++_hit_count;
                    readers[i] = reinterpret_cast<ColumnReader*>(e);
                } else {
                    unload_indexes.push_back(i);
                    all_column_loaded = false;
                }
            }            
        }
        return all_column_loaded;
    }

    // std::vector<ColumnReader*> insert_column_readers(vector<CacheKey> cacheKeys, Segment* segment) {
    //     for (auto i = 0; i < cacheKeys.size(); i++) {
    //         segment->load_column_reader(i);
    //     }
    //     return vectors;
    // }

}