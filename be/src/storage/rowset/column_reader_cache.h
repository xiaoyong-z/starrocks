// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include "storage/lru_cache.h"

namespace starrocks {
    // Modified from LRUCache
    class ColumnReaderCache : public Cache {
    public:
        explicit ColumnReaderCache(uint64_t capacity);
        ~ColumnReaderCache() override = default;
        Handle* insert(const CacheKey& key, void* value, size_t charge, void (*deleter)(const CacheKey& key, void* value), CachePriority priority = CachePriority::NORMAL) override;
        
        void release(Handle* handle) override;
        void erase(const CacheKey& key) override;
        void* value(Handle* handle) override;
        Slice value_slice(Handle* handle) override;
        uint64_t new_id() override;
        void prune() override;
        size_t get_memory_usage() override;
        void get_cache_status(rapidjson::Document* document) override;
        
        bool look_up_column_readers(vector<CacheKey> cacheKeys, std::vector<ColumnReader*>& readers, std::vector<uint32_t>& unload_indexes);

         load_column_readers(vector<CacheKey> cacheKeys, Segment* segment) {
            for (auto i = 0; i < cacheKeys.size(); i++) {
                segment->load_column_reader(i);
            }
            return vectors;
        }

    private:
        Handle* _lookup(const CacheKey& key) override;

        // Initialized before use.
        const uint64_t _capacity;

         // _mutex protects the following state.
        std::mutex _mutex;
        std::atomic<uint64_t> _mem_counter{0};

        // Dummy head of LRU list.
        // lru.prev is newest entry, lru.next is oldest entry.
        // Entries have refs==1 and in_cache==true.
        LRUHandle _lru;

        HandleTable _table; 
    };
}