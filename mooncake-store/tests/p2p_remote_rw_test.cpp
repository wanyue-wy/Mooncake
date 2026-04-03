/**
 * @file p2p_remote_rw_test.cpp
 * @brief Integration tests for P2P remote read/write data correctness.
 *
 * Launches an in-process P2P master and TWO P2PClientService instances.
 * Uses Query to confirm where data actually lives, then reads from the
 * OTHER client to force the remote-read path (RPC + TransferEngine).
 *
 * NOTE: both clients are in the same process, so TE TCP goes through
 * loopback. This validates store-layer correctness but cannot catch
 * cross-host TE bugs. For cross-host testing, use the QA stress scripts
 * with the DATA_MISMATCH diagnostics.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "p2p_client_service.h"
#include "test_p2p_server_helpers.h"
#include "types.h"

namespace mooncake {
namespace testing {

// ============================================================================
// Fixture
// ============================================================================

class P2PRemoteRWTest : public ::testing::Test {
   protected:
    static std::shared_ptr<P2PClientService> CreateClient(
        const std::string& host, uint32_t rpc_port = 0) {
        if (rpc_port == 0) rpc_port = getFreeTcpPort();

        auto config = ClientConfigBuilder::build_p2p_real_client(
            host, "P2PHANDSHAKE", "tcp", std::nullopt, master_address_,
            R"({"tiers":[{"type":"DRAM","capacity":1073741824,"priority":100}]})",
            /*local_buffer_size=*/0, nullptr, "", rpc_port);

        config.route_cache_max_memory_bytes = 0;
        config.route_cache_ttl_ms = 0;

        auto client = std::make_shared<P2PClientService>(
            config.local_ip, config.te_port, config.metadata_connstring,
            config.labels);
        auto err = client->Init(config);
        EXPECT_EQ(err, ErrorCode::OK);
        return client;
    }

    static void SetUpTestSuite() {
        ASSERT_TRUE(master_.Start());
        master_address_ = master_.master_address();

        client_a_ = CreateClient("localhost:19901");
        ASSERT_NE(client_a_, nullptr);
        client_b_ = CreateClient("localhost:19902");
        ASSERT_NE(client_b_, nullptr);

        // Register shared buffers for both clients
        buf_a_.resize(kMaxBuf);
        buf_b_.resize(kMaxBuf);
        ASSERT_TRUE(client_a_->RegisterLocalMemory(
            buf_a_.data(), buf_a_.size(), kWildcardLocation, false, true)
            .has_value());
        ASSERT_TRUE(client_b_->RegisterLocalMemory(
            buf_b_.data(), buf_b_.size(), kWildcardLocation, false, true)
            .has_value());
    }

    static void TearDownTestSuite() {
        if (client_a_) client_a_->unregisterLocalMemory(buf_a_.data(), true);
        if (client_b_) client_b_->unregisterLocalMemory(buf_b_.data(), true);
        client_b_.reset();
        client_a_.reset();
        master_.Stop();
    }

    // ------ helpers ------

    /// Build a WriteRouteRequestConfig that guarantees local write.
    /// Uses max_candidates=0 (RETURN_ALL_CANDIDATES) so PutViaRoute
    /// always sees the local candidate in the list and picks it.
    static WriteRouteRequestConfig LocalWriteConfig() {
        WriteRouteRequestConfig cfg;
        cfg.max_candidates = WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;
        cfg.allow_local = true;
        cfg.prefer_local = true;
        return cfg;
    }

    /// Write data on `writer` locally, then Query to confirm it lives on
    /// `writer`, then read it from `reader` (which must go remote).
    static void PutAndRemoteGet(
        P2PClientService& writer, P2PClientService& reader,
        const std::string& key, size_t size, uint8_t fill_byte) {
        ASSERT_LE(size, kMaxBuf);

        // --- Write ---
        std::memset(buf_a_.data(), fill_byte, size);
        std::vector<Slice> slices = {{buf_a_.data(), size}};
        auto put = writer.Put(key, slices, LocalWriteConfig());
        ASSERT_TRUE(put.has_value())
            << "Put failed: " << static_cast<int>(put.error());

        // --- Query: confirm data is on the writer ---
        auto query = writer.Query(key);
        ASSERT_TRUE(query.has_value())
            << "Query failed after Put: " << static_cast<int>(query.error());
        auto& replicas = query.value()->replicas;
        ASSERT_GE(replicas.size(), 1u) << "No replicas for key=" << key;

        // Verify at least one replica belongs to the writer
        bool found_on_writer = false;
        for (auto& r : replicas) {
            if (r.is_p2p_proxy_replica() &&
                r.get_p2p_proxy_descriptor().client_id ==
                    writer.GetClientID()) {
                found_on_writer = true;
                break;
            }
        }
        EXPECT_TRUE(found_on_writer)
            << "Replica for key=" << key << " not found on writer client";

        // --- Read from the other client (remote path) ---
        std::memset(buf_b_.data(), 0, size);
        auto get = reader.Get(key,
                              {static_cast<void*>(buf_b_.data())}, {size});
        ASSERT_TRUE(get.has_value())
            << "Remote Get failed: " << static_cast<int>(get.error());
        ASSERT_EQ(static_cast<size_t>(get.value()), size);

        // Byte-level verification with diagnostic output
        size_t first_wrong = size;
        size_t first_zero = size;
        size_t wrong_count = 0;
        for (size_t j = 0; j < size; ++j) {
            if (buf_b_[j] != fill_byte) {
                wrong_count++;
                if (first_wrong == size) first_wrong = j;
            }
            if (buf_b_[j] == 0 && first_zero == size) first_zero = j;
        }
        ASSERT_EQ(wrong_count, 0u)
            << "DATA_MISMATCH key=" << key
            << " expected=0x" << std::hex << (int)fill_byte << std::dec
            << " first_wrong_at=" << first_wrong
            << " actual=0x" << std::hex << (int)buf_b_[first_wrong] << std::dec
            << " first_zero_at=" << first_zero
            << " wrong_bytes=" << wrong_count << "/" << size;
    }

    static constexpr size_t kMaxBuf = 4 * 1024 * 1024;
    static InProcP2PMaster master_;
    static std::string master_address_;
    static std::shared_ptr<P2PClientService> client_a_;
    static std::shared_ptr<P2PClientService> client_b_;
    static std::vector<uint8_t> buf_a_;
    static std::vector<uint8_t> buf_b_;
};

InProcP2PMaster P2PRemoteRWTest::master_;
std::string P2PRemoteRWTest::master_address_;
std::shared_ptr<P2PClientService> P2PRemoteRWTest::client_a_;
std::shared_ptr<P2PClientService> P2PRemoteRWTest::client_b_;
std::vector<uint8_t> P2PRemoteRWTest::buf_a_;
std::vector<uint8_t> P2PRemoteRWTest::buf_b_;

// ============================================================================
// Tests
// ============================================================================

// 1. A writes, B reads (various sizes)
TEST_F(P2PRemoteRWTest, RemoteRead_VariousSizes) {
    const std::vector<size_t> sizes = {
        1, 127, 4096, 65536, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024,
    };
    for (size_t sz : sizes) {
        SCOPED_TRACE("size=" + std::to_string(sz));
        uint8_t pattern = static_cast<uint8_t>((sz % 251) + 1);
        PutAndRemoteGet(*client_a_, *client_b_,
                        "a2b_sz_" + std::to_string(sz), sz, pattern);
    }
}

// 2. B writes, A reads (bidirectional)
TEST_F(P2PRemoteRWTest, RemoteRead_Bidirectional) {
    PutAndRemoteGet(*client_a_, *client_b_, "bidir_a2b", 1024 * 1024, 0xAA);
    PutAndRemoteGet(*client_b_, *client_a_, "bidir_b2a", 1024 * 1024, 0xBB);
}

// 3. Multiple keys
TEST_F(P2PRemoteRWTest, RemoteRead_PreloadPattern) {
    constexpr int kNumKeys = 20;
    constexpr size_t kSize = 1024 * 1024;

    for (int i = 0; i < kNumKeys; ++i) {
        uint8_t pattern = static_cast<uint8_t>('A' + (i % 26));
        PutAndRemoteGet(*client_a_, *client_b_,
                        "preload_" + std::to_string(i), kSize, pattern);
    }
}

// 4. Batch write + BatchQuery verification + individual remote get
TEST_F(P2PRemoteRWTest, BatchWriteQueryGet) {
    constexpr int kNumKeys = 10;
    constexpr size_t kSize = 256 * 1024;

    // Write all keys from A
    for (int i = 0; i < kNumKeys; ++i) {
        uint8_t pattern = static_cast<uint8_t>('A' + (i % 26));
        std::memset(buf_a_.data(), pattern, kSize);
        std::vector<Slice> slices = {{buf_a_.data(), kSize}};
        ASSERT_TRUE(
            client_a_
                ->Put("bq_" + std::to_string(i), slices, LocalWriteConfig())
                .has_value());
    }

    // BatchQuery: verify all replicas exist and belong to A
    std::vector<std::string> keys;
    keys.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; ++i) {
        keys.push_back("bq_" + std::to_string(i));
    }
    auto batch_query = client_b_->BatchQuery(keys);
    ASSERT_EQ(batch_query.size(), static_cast<size_t>(kNumKeys));
    for (int i = 0; i < kNumKeys; ++i) {
        ASSERT_TRUE(batch_query[i].has_value())
            << "BatchQuery failed for key=" << keys[i];
        auto& replicas = batch_query[i].value()->replicas;
        ASSERT_GE(replicas.size(), 1u);
        bool found = false;
        for (auto& r : replicas) {
            if (r.is_p2p_proxy_replica() &&
                r.get_p2p_proxy_descriptor().client_id ==
                    client_a_->GetClientID()) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Replica for key=" << keys[i]
                           << " not found on client_a";
    }

    // Remote Get each key from B and verify data
    for (int i = 0; i < kNumKeys; ++i) {
        uint8_t pattern = static_cast<uint8_t>('A' + (i % 26));
        SCOPED_TRACE("key=bq_" + std::to_string(i));
        std::memset(buf_b_.data(), 0, kSize);
        auto get = client_b_->Get("bq_" + std::to_string(i),
                                  {static_cast<void*>(buf_b_.data())}, {kSize});
        ASSERT_TRUE(get.has_value());
        ASSERT_EQ(static_cast<size_t>(get.value()), kSize);
        EXPECT_TRUE(std::all_of(buf_b_.begin(), buf_b_.begin() + kSize,
                                [pattern](uint8_t b) { return b == pattern; }))
            << "Data mismatch for key=bq_" << i;
    }
}

// 5. Concurrent remote reads from multiple threads
TEST_F(P2PRemoteRWTest, ConcurrentRemoteReads) {
    constexpr int kNumKeys = 10;
    constexpr size_t kSize = 256 * 1024;

    // Write all keys on A
    for (int i = 0; i < kNumKeys; ++i) {
        uint8_t pat = static_cast<uint8_t>((i % 251) + 1);
        std::memset(buf_a_.data(), pat, kSize);
        std::vector<Slice> slices = {{buf_a_.data(), kSize}};
        ASSERT_TRUE(
            client_a_
                ->Put("conc_" + std::to_string(i), slices, LocalWriteConfig())
                .has_value());
    }

    // Read concurrently from B (each thread has its own buffer)
    constexpr int kThreads = 4;
    std::atomic<int> failures{0};

    // Pre-register per-thread buffers
    std::vector<std::vector<uint8_t>> thread_bufs(kThreads,
        std::vector<uint8_t>(kSize, 0));
    for (int t = 0; t < kThreads; ++t) {
        ASSERT_TRUE(client_b_->RegisterLocalMemory(
            thread_bufs[t].data(), kSize, kWildcardLocation, false, true)
            .has_value());
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            ReadRouteConfig read_cfg;
            for (int i = t; i < kNumKeys; i += kThreads) {
                uint8_t pat = static_cast<uint8_t>((i % 251) + 1);
                auto& tbuf = thread_bufs[t];
                std::memset(tbuf.data(), 0, kSize);
                auto r = client_b_->Get(
                    "conc_" + std::to_string(i),
                    {static_cast<void*>(tbuf.data())}, {kSize}, read_cfg);
                if (!r || static_cast<size_t>(r.value()) != kSize) {
                    failures.fetch_add(1);
                    continue;
                }
                for (size_t j = 0; j < kSize; ++j) {
                    if (tbuf[j] != pat) {
                        LOG(ERROR) << "Concurrent mismatch: key=conc_" << i
                                   << " byte=" << j
                                   << " expected=" << (int)pat
                                   << " got=" << (int)tbuf[j];
                        failures.fetch_add(1);
                        break;
                    }
                }
            }
        });
    }
    for (auto& th : threads) th.join();

    for (int t = 0; t < kThreads; ++t) {
        client_b_->unregisterLocalMemory(thread_bufs[t].data(), true);
    }
    EXPECT_EQ(failures.load(), 0);
}

// 6. Concurrent writes from multiple threads, then verify via Query + Get
TEST_F(P2PRemoteRWTest, ConcurrentWrites) {
    constexpr int kNumKeys = 12;
    constexpr size_t kSize = 128 * 1024;
    constexpr int kThreads = 4;

    // Each thread gets its own write buffer registered on A
    std::vector<std::vector<uint8_t>> write_bufs(
        kThreads, std::vector<uint8_t>(kSize, 0));
    for (int t = 0; t < kThreads; ++t) {
        ASSERT_TRUE(client_a_
                        ->RegisterLocalMemory(write_bufs[t].data(), kSize,
                                              kWildcardLocation, false, true)
                        .has_value());
    }

    std::atomic<int> write_failures{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = t; i < kNumKeys; i += kThreads) {
                uint8_t pat = static_cast<uint8_t>((i % 251) + 1);
                auto& wbuf = write_bufs[t];
                std::memset(wbuf.data(), pat, kSize);
                std::vector<Slice> slices = {{wbuf.data(), kSize}};
                auto put = client_a_->Put("conc_w_" + std::to_string(i), slices,
                                          LocalWriteConfig());
                if (!put.has_value()) {
                    LOG(ERROR) << "Concurrent write failed: key=conc_w_" << i
                               << " error=" << static_cast<int>(put.error());
                    write_failures.fetch_add(1);
                }
            }
        });
    }
    for (auto& th : threads) th.join();

    for (int t = 0; t < kThreads; ++t) {
        client_a_->unregisterLocalMemory(write_bufs[t].data(), true);
    }
    ASSERT_EQ(write_failures.load(), 0) << "Some concurrent writes failed";

    // Verify all keys: Query confirms on A, then Remote Get from B
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = "conc_w_" + std::to_string(i);
        uint8_t pat = static_cast<uint8_t>((i % 251) + 1);
        SCOPED_TRACE("key=" + key);

        auto query = client_b_->Query(key);
        ASSERT_TRUE(query.has_value()) << "Query failed for key=" << key;
        auto& replicas = query.value()->replicas;
        ASSERT_GE(replicas.size(), 1u);
        bool found = false;
        for (auto& r : replicas) {
            if (r.is_p2p_proxy_replica() &&
                r.get_p2p_proxy_descriptor().client_id ==
                    client_a_->GetClientID()) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Replica not on client_a";

        std::memset(buf_b_.data(), 0, kSize);
        auto get =
            client_b_->Get(key, {static_cast<void*>(buf_b_.data())}, {kSize});
        ASSERT_TRUE(get.has_value()) << "Remote Get failed for key=" << key;
        ASSERT_EQ(static_cast<size_t>(get.value()), kSize);
        EXPECT_TRUE(std::all_of(buf_b_.begin(), buf_b_.begin() + kSize,
                                [pat](uint8_t b) { return b == pat; }))
            << "Data mismatch for key=" << key;
    }
}

// 7. Concurrent read-write mix: writers on A, readers on B simultaneously
TEST_F(P2PRemoteRWTest, ConcurrentReadWrite) {
    constexpr int kNumKeys = 16;
    constexpr size_t kSize = 64 * 1024;
    constexpr int kWriterThreads = 2;
    constexpr int kReaderThreads = 2;

    // Per-key ready flags
    std::unique_ptr<std::atomic<bool>[]> key_ready(
        new std::atomic<bool>[kNumKeys]);
    for (int i = 0; i < kNumKeys; ++i) key_ready[i].store(false);

    std::atomic<int> read_failures{0};
    std::atomic<int> write_failures{0};
    std::atomic<bool> writers_done{false};

    // Per-writer buffers registered on A
    std::vector<std::vector<uint8_t>> write_bufs(
        kWriterThreads, std::vector<uint8_t>(kSize, 0));
    for (int t = 0; t < kWriterThreads; ++t) {
        ASSERT_TRUE(client_a_
                        ->RegisterLocalMemory(write_bufs[t].data(), kSize,
                                              kWildcardLocation, false, true)
                        .has_value());
    }

    // Per-reader buffers registered on B
    std::vector<std::vector<uint8_t>> read_bufs(kReaderThreads,
                                                std::vector<uint8_t>(kSize, 0));
    for (int t = 0; t < kReaderThreads; ++t) {
        ASSERT_TRUE(client_b_
                        ->RegisterLocalMemory(read_bufs[t].data(), kSize,
                                              kWildcardLocation, false, true)
                        .has_value());
    }

    // Writer threads
    std::vector<std::thread> writers;
    for (int t = 0; t < kWriterThreads; ++t) {
        writers.emplace_back([&, t]() {
            for (int i = t; i < kNumKeys; i += kWriterThreads) {
                uint8_t pat = static_cast<uint8_t>((i % 251) + 1);
                auto& wbuf = write_bufs[t];
                std::memset(wbuf.data(), pat, kSize);
                std::vector<Slice> slices = {{wbuf.data(), kSize}};
                auto put = client_a_->Put("crw_" + std::to_string(i), slices,
                                          LocalWriteConfig());
                if (!put.has_value()) {
                    write_failures.fetch_add(1);
                }
                key_ready[i].store(true, std::memory_order_release);
            }
        });
    }

    // Reader threads: wait for writers, then read all ready keys
    std::vector<std::thread> readers;
    for (int t = 0; t < kReaderThreads; ++t) {
        readers.emplace_back([&, t]() {
            // Wait until writers are done
            while (!writers_done.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            for (int i = t; i < kNumKeys; i += kReaderThreads) {
                if (!key_ready[i].load(std::memory_order_acquire)) continue;
                uint8_t pat = static_cast<uint8_t>((i % 251) + 1);
                auto& rbuf = read_bufs[t];
                std::memset(rbuf.data(), 0, kSize);
                auto r =
                    client_b_->Get("crw_" + std::to_string(i),
                                   {static_cast<void*>(rbuf.data())}, {kSize});
                if (!r || static_cast<size_t>(r.value()) != kSize) {
                    read_failures.fetch_add(1);
                    continue;
                }
                for (size_t j = 0; j < kSize; ++j) {
                    if (rbuf[j] != pat) {
                        LOG(ERROR) << "CRW mismatch: key=crw_" << i
                                   << " byte=" << j << " expected=" << (int)pat
                                   << " got=" << (int)rbuf[j];
                        read_failures.fetch_add(1);
                        break;
                    }
                }
            }
        });
    }

    for (auto& th : writers) th.join();
    writers_done.store(true, std::memory_order_release);
    for (auto& th : readers) th.join();

    // Cleanup
    for (int t = 0; t < kWriterThreads; ++t) {
        client_a_->unregisterLocalMemory(write_bufs[t].data(), true);
    }
    for (int t = 0; t < kReaderThreads; ++t) {
        client_b_->unregisterLocalMemory(read_bufs[t].data(), true);
    }

    EXPECT_EQ(write_failures.load(), 0) << "Writer thread failures";
    EXPECT_EQ(read_failures.load(), 0) << "Reader thread failures";

    // Final comprehensive verification
    for (int i = 0; i < kNumKeys; ++i) {
        uint8_t pat = static_cast<uint8_t>((i % 251) + 1);
        std::memset(buf_b_.data(), 0, kSize);
        auto get = client_b_->Get("crw_" + std::to_string(i),
                                  {static_cast<void*>(buf_b_.data())}, {kSize});
        ASSERT_TRUE(get.has_value());
        EXPECT_TRUE(std::all_of(buf_b_.begin(), buf_b_.begin() + kSize,
                                [pat](uint8_t b) { return b == pat; }))
            << "Final verify failed for key=crw_" << i;
    }
}

// 8. Route cache path: enable cache, read twice (miss then hit)
TEST_F(P2PRemoteRWTest, RemoteRead_WithRouteCache) {
    // Create a third client with route cache enabled
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "localhost:19903", "P2PHANDSHAKE", "tcp", std::nullopt,
        master_address_,
        R"({"tiers":[{"type":"DRAM","capacity":67108864,"priority":100}]})",
        0, nullptr, "", getFreeTcpPort());
    config.route_cache_max_memory_bytes = 10 * 1024 * 1024;
    config.route_cache_ttl_ms = 60000;

    auto cached = std::make_shared<P2PClientService>(
        config.local_ip, config.te_port, config.metadata_connstring,
        config.labels);
    ASSERT_EQ(cached->Init(config), ErrorCode::OK);

    std::vector<uint8_t> cbuf(1024 * 1024, 0);
    ASSERT_TRUE(cached->RegisterLocalMemory(
        cbuf.data(), cbuf.size(), kWildcardLocation, false, true).has_value());

    constexpr size_t kSize = 1024 * 1024;
    constexpr uint8_t kPat = 0x57;

    // Write on client_a
    std::memset(buf_a_.data(), kPat, kSize);
    std::vector<Slice> slices = {{buf_a_.data(), kSize}};
    ASSERT_TRUE(
        client_a_->Put("cache_test", slices, LocalWriteConfig()).has_value());

    // First read (cache miss)
    std::memset(cbuf.data(), 0, kSize);
    auto r1 = cached->Get("cache_test",
                          {static_cast<void*>(cbuf.data())}, {kSize});
    ASSERT_TRUE(r1.has_value());
    EXPECT_TRUE(std::all_of(cbuf.begin(), cbuf.end(),
                            [](uint8_t b) { return b == kPat; }))
        << "Cache miss read: data mismatch";

    // Second read (cache hit)
    std::memset(cbuf.data(), 0, kSize);
    auto r2 = cached->Get("cache_test",
                          {static_cast<void*>(cbuf.data())}, {kSize});
    ASSERT_TRUE(r2.has_value());

    size_t mismatch = kSize;
    for (size_t j = 0; j < kSize; ++j) {
        if (cbuf[j] != kPat) { mismatch = j; break; }
    }
    EXPECT_EQ(mismatch, kSize)
        << "Cache hit read: mismatch at byte " << mismatch
        << " got=0x" << std::hex << (int)cbuf[mismatch] << std::dec;

    cached->unregisterLocalMemory(cbuf.data(), true);
}

}  // namespace testing
}  // namespace mooncake
