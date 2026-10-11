set(MOONCAKE_E2E_TEST_SOURCES process_handler.cpp)
if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "p2p")
    list(APPEND MOONCAKE_E2E_TEST_SOURCES p2p_client_wrapper.cpp)
else()
    list(APPEND MOONCAKE_E2E_TEST_SOURCES client_wrapper.cpp)
endif()

function(add_store_e2e_executable name)
    add_executable(${name} ${ARGN} ${MOONCAKE_E2E_TEST_SOURCES})
    target_link_libraries(${name} PRIVATE
        ${MOONCAKE_STORE_LIBS} transfer_engine cachelib_memory_allocator
        glog gflags gtest pthread ${ETCD_WRAPPER_LIB})
endfunction()

add_store_e2e_executable(clientctl clientctl.cpp)

# These workloads require centralized segment mounting and etcd discovery.
if(MOONCAKE_STORE_CLIENT_ARCH STREQUAL "centralized")
    add_store_e2e_executable(e2e_rand_test e2e_rand_test.cpp)
    add_store_e2e_executable(chaosctl chaosctl.cpp)
    add_store_e2e_executable(client_runner client_runner.cpp)
    add_store_e2e_executable(chaos_test chaos_test.cpp)
    add_store_e2e_executable(chaos_rand_test chaos_rand_test.cpp)
    if(STORE_USE_ETCD)
        add_test(NAME chaos_test COMMAND chaos_test
            --master_path=$<TARGET_FILE:mooncake_master>
            --client_path=$<TARGET_FILE:client_runner>
            --engine_meta_url=P2PHANDSHAKE
            --out_dir=${CMAKE_CURRENT_BINARY_DIR}/chaos_output)
        set_tests_properties(chaos_test PROPERTIES
            LABELS "centralized;ha;etcd" RUN_SERIAL TRUE TIMEOUT 300)
    endif()
elseif(STORE_USE_REDIS)
    add_store_e2e_executable(redis_chaos_test redis_chaos_test.cpp)
    add_test(NAME redis_chaos_test COMMAND redis_chaos_test
        --master_path=$<TARGET_FILE:mooncake_master>
        --clientctl_path=$<TARGET_FILE:clientctl>
        --out_dir=${CMAKE_CURRENT_BINARY_DIR}/redis_chaos_output)
    set_tests_properties(redis_chaos_test PROPERTIES
        LABELS "p2p;ha;redis_ha" RUN_SERIAL TRUE TIMEOUT 180)
endif()
