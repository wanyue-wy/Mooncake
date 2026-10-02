# Add allocator benchmark executable
add_executable(allocator_bench allocator_bench.cpp)
target_link_libraries(allocator_bench PRIVATE cachelib_memory_allocator ${MOONCAKE_STORE_LIBS})

# The benchmark uses the centralized Master RPC client in either Store build.
add_executable(master_bench
    master_bench.cpp
    ../src/master_client.cpp
)
target_link_libraries(master_bench PRIVATE cachelib_memory_allocator mooncake_master_core)

# Add storage backend benchmark executable
# This benchmark tests all storage backends (OffsetAllocator, Bucket, FilePerKey)
# with realistic KV cache workloads for LLM inference scenarios
add_executable(storage_backend_bench storage_backend_bench.cpp)
target_link_libraries(storage_backend_bench PRIVATE
    ${MOONCAKE_STORE_LIBS}
    cachelib_memory_allocator
    gflags::gflags
    glog::glog
    pthread
)
