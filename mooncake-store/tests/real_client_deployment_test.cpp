#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>

#include "real_client_test_helper.h"
#if defined(MOONCAKE_STORE_CLIENT_P2P)
#include "p2p/client/p2p_client_config_builder.h"
#include "p2p/test_p2p_server_helpers.h"
#else
#include "centralized_client_config_builder.h"
#include "test_server_helpers.h"
#endif

namespace mooncake::testing {
namespace {

class SharedFile {
   public:
    void Init() {
        file_.reset(std::tmpfile());
        ASSERT_NE(file_, nullptr);
        ASSERT_EQ(ftruncate(fileno(file_.get()), kSize), 0);
        data = mmap(nullptr, kSize, PROT_READ | PROT_WRITE, MAP_SHARED,
                    fileno(file_.get()), 0);
        ASSERT_NE(data, MAP_FAILED);
        std::memset(data, 0x5a, kSize);
    }
    ~SharedFile() {
        if (data != MAP_FAILED) EXPECT_EQ(munmap(data, kSize), 0);
    }
    int fd() const { return fileno(file_.get()); }
    static constexpr size_t kSize = 4096;
    void* data = MAP_FAILED;

   private:
    std::unique_ptr<std::FILE, decltype(&std::fclose)> file_{nullptr,
                                                             std::fclose};
};

struct Socket {
    explicit Socket(int value) : fd(value) {}
    ~Socket() {
        if (fd >= 0 && close(fd) != 0) {
            LOG(ERROR) << "Cannot close deployment test socket: "
                       << strerror(errno);
        }
    }
    int fd;
};

struct ClientOwner {
    std::shared_ptr<RealClient> client = RealClient::create();
    ~ClientOwner() {
        EXPECT_EQ(client->tearDownAll(), 0);
        client.reset();
    }
};

inline int ConnectIpc(const std::string& path) {
    if (path.size() > sizeof(sockaddr_un{}.sun_path) - 2) {
        LOG(ERROR) << "Deployment IPC name is too long";
        return -1;
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
        const int fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            LOG(ERROR) << "Cannot create deployment test socket: "
                       << strerror(errno);
            return -1;
        }
        sockaddr_un address{};
        address.sun_family = AF_UNIX;
        std::memcpy(address.sun_path + 1, path.data(), path.size());
        if (connect(fd, reinterpret_cast<sockaddr*>(&address),
                    sizeof(sa_family_t) + 1 + path.size()) == 0) {
            timeval timeout{5, 0};
            if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                           sizeof(timeout)) == 0)
                return fd;
            LOG(ERROR) << "Cannot set deployment receive timeout: "
                       << strerror(errno);
            close(fd);
            return -1;
        }
        close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    LOG(ERROR) << "Deployment IPC listener did not become ready: " << path;
    return -1;
}

inline void SendRegistrationRequest(int socket_fd, int memory_fd,
                                    const ShmRegisterRequest& request,
                                    size_t request_size) {
    iovec payload{const_cast<ShmRegisterRequest*>(&request), request_size};
    alignas(cmsghdr) char control[CMSG_SPACE(sizeof(int))]{};
    msghdr message{};
    message.msg_iov = &payload;
    message.msg_iovlen = 1;
    message.msg_control = control;
    message.msg_controllen = sizeof(control);
    auto* header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(sizeof(int));
    std::memcpy(CMSG_DATA(header), &memory_fd, sizeof(memory_fd));
    ASSERT_EQ(sendmsg(socket_fd, &message, MSG_NOSIGNAL), request_size);
}

inline void SendRegistration(int socket_fd, int memory_fd,
                             const ShmRegisterRequest& request,
                             size_t request_size, int& response) {
    ASSERT_NO_FATAL_FAILURE(
        SendRegistrationRequest(socket_fd, memory_fd, request, request_size));
    // EOF makes a short request deterministic without extending the protocol.
    ASSERT_EQ(shutdown(socket_fd, SHUT_WR), 0);
    ASSERT_EQ(recv(socket_fd, &response, sizeof(response), MSG_WAITALL),
              sizeof(response));
}

template <typename Config>
void CheckIpcRecovery(Config config, const WriteConfig& write_config) {
    SharedFile file;
    ASSERT_NO_FATAL_FAILURE(file.Init());
    const auto id = generate_uuid();
    config.ipc_socket_path = "real_deployment_" + std::to_string(getpid()) +
                             "_" + std::to_string(id.first);
    ClientOwner owner;
    ASSERT_EQ(owner.client->setup(config), 0);
    ShmRegisterRequest request{id.first, id.second, 0x100000, SharedFile::kSize,
                               false};
    {
        Socket peer(ConnectIpc(config.ipc_socket_path));
        ASSERT_GE(peer.fd, 0);
        int response = 0;
        ASSERT_NO_FATAL_FAILURE(SendRegistration(
            peer.fd, file.fd(), request, sizeof(request) / 2, response));
        EXPECT_NE(response, 0);
        EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id),
                  nullptr);
    }
    {
        Socket peer(ConnectIpc(config.ipc_socket_path));
        ASSERT_GE(peer.fd, 0);
        auto invalid = request;
        invalid.shm_size = 0;
        int response = 0;
        ASSERT_NO_FATAL_FAILURE(SendRegistration(peer.fd, file.fd(), invalid,
                                                 sizeof(invalid), response));
        EXPECT_NE(response, 0);
        EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id),
                  nullptr);
    }
    Socket peer(ConnectIpc(config.ipc_socket_path));
    ASSERT_GE(peer.fd, 0);
    int response = -1;
    ASSERT_NO_FATAL_FAILURE(SendRegistration(peer.fd, file.fd(), request,
                                             sizeof(request), response));
    ASSERT_EQ(response, 0);
    const std::string first("ipc\0first", 9);
    const std::string second("ipc\0second", 10);
    ASSERT_EQ(owner.client->put_batch({"ipc_a", "ipc_b"}, {first, second},
                                      write_config),
              0);
    auto reads = owner.client->batch_get_into_dummy_helper(
        {"ipc_a", "ipc_b"},
        {request.dummy_base_addr, request.dummy_base_addr + 128}, {128, 128},
        std::nullopt, id);
    ASSERT_EQ(reads.size(), 2);
    ASSERT_TRUE(reads[0]);
    ASSERT_TRUE(reads[1]);
    EXPECT_EQ(*reads[0], first.size());
    EXPECT_EQ(*reads[1], second.size());
    EXPECT_EQ(std::string(static_cast<char*>(file.data), first.size()), first);
    EXPECT_EQ(std::string(static_cast<char*>(file.data) + 128, second.size()),
              second);
    ASSERT_TRUE(owner.client->unmap_shm_internal(id));
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), nullptr);
}

template <typename Config>
void CheckUnmapWaitsForContext(Config config, const WriteConfig& write_config) {
    SharedFile file;
    ASSERT_NO_FATAL_FAILURE(file.Init());
    ClientOwner owner;
    ASSERT_EQ(owner.client->setup(config), 0);
    const auto id = generate_uuid();
    const int fd = dup(file.fd());
    ASSERT_GE(fd, 0);
    ASSERT_TRUE(RealClientTestHelper::MapShm(
        *owner.client, fd, 0x100000, SharedFile::kSize, false, id));
    const std::string value = "read while SHM is being unmapped";
    ASSERT_EQ(owner.client->put("unmap_key", value, write_config), 0);
    auto context = RealClientTestHelper::FindShmContext(*owner.client, id);
    ASSERT_NE(context, nullptr);
    std::unique_lock<SharedMutex> held(context->mutex_);
    std::promise<void> started;
    auto calling = started.get_future();
    auto read = std::async(std::launch::async, [&] {
        started.set_value();
        return owner.client->batch_get_into_dummy_helper(
            {"unmap_key"}, {0x100000}, {SharedFile::kSize}, std::nullopt, id);
    });
    calling.wait();
    auto unmap = std::async(std::launch::async, [&] {
        return owner.client->unmap_shm_internal(id);
    });
    // Detachment proves unmap has reached the context lock, which is held here.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (RealClientTestHelper::FindShmContext(*owner.client, id) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), nullptr);
    EXPECT_EQ(unmap.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    held.unlock();
    ASSERT_TRUE(unmap.get());
    auto results = read.get();
    ASSERT_EQ(results.size(), 1);
    if (results[0]) {
        EXPECT_EQ(*results[0], value.size());
        EXPECT_EQ(std::string(static_cast<char*>(file.data), value.size()),
                  value);
    } else {
        // Unmap may win admission; it must reject the read without touching
        // SHM.
        EXPECT_EQ(results[0].error(), ErrorCode::INVALID_PARAMS);
        EXPECT_EQ(std::string(static_cast<char*>(file.data), SharedFile::kSize),
                  std::string(SharedFile::kSize, 0x5a));
    }
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), nullptr);
}

}  // namespace

class RealClientDeploymentTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RealClientDeploymentTest");
        FLAGS_logtostderr = 1;
        ResourceTracker::getInstance();
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
#if defined(MOONCAKE_STORE_CLIENT_P2P)
        ASSERT_TRUE(master_.Start());
#else
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
#endif
    }

    void TearDown() override { master_.Stop(); }

    auto DeploymentConfig() {
#if defined(MOONCAKE_STORE_CLIENT_P2P)
        auto config = P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1", "P2PHANDSHAKE", "tcp", std::nullopt,
            master_.master_address(),
            R"({"tiers":[{"type":"DRAM","capacity":67108864,"priority":100}]})",
            4 * 1024 * 1024, nullptr, "", 0);
        config.enable_http_server = false;
        config.enable_metric_collection = false;
        config.async_sender_thread_count = 0;
        config.te_async_poll_worker_num = 2;
        return config;
#else
        return CentralizedClientConfigBuilder::build_centralized_real_client(
            "127.0.0.1:0", "P2PHANDSHAKE", "tcp", std::nullopt,
            master_.master_address(), 16 * 1024 * 1024, 4 * 1024 * 1024);
#endif
    }

    WriteConfig LocalWriteConfig() {
        WriteConfig config;
#if defined(MOONCAKE_STORE_CLIENT_P2P)
        config.remote_weight = 0.0;
#endif
        return config;
    }

#if defined(MOONCAKE_STORE_CLIENT_P2P)
    InProcP2PMaster master_;
#else
    InProcMaster master_;
#endif
};

TEST_F(RealClientDeploymentTest, IpcRecoveryAndClose) {
    CheckIpcRecovery(DeploymentConfig(), LocalWriteConfig());
}

TEST_F(RealClientDeploymentTest, UnmapWaitsForContext) {
    CheckUnmapWaitsForContext(DeploymentConfig(), LocalWriteConfig());
}

TEST_F(RealClientDeploymentTest, DeploymentModeIsIndependentOfReadiness) {
#if defined(MOONCAKE_STORE_CLIENT_P2P)
    constexpr auto expected_mode = DeploymentMode::P2P;
#else
    constexpr auto expected_mode = DeploymentMode::CENTRALIZATION;
#endif
    ClientOwner owner;
    EXPECT_EQ(owner.client->deployment_mode(), expected_mode);
    EXPECT_FALSE(owner.client->is_initialized());
    auto readiness = owner.client->service_ready_internal();
    ASSERT_FALSE(readiness);
    EXPECT_EQ(readiness.error(), ErrorCode::INTERNAL_ERROR);

    ASSERT_EQ(owner.client->setup(DeploymentConfig()), 0);
    EXPECT_EQ(owner.client->deployment_mode(), expected_mode);
    EXPECT_TRUE(owner.client->is_initialized());
    readiness = owner.client->service_ready_internal();
    ASSERT_TRUE(readiness);
    EXPECT_EQ(readiness.value(), expected_mode);

    EXPECT_EQ(owner.client->tearDownAll(), 0);
    EXPECT_EQ(owner.client->deployment_mode(), expected_mode);
    EXPECT_FALSE(owner.client->is_initialized());
    readiness = owner.client->service_ready_internal();
    ASSERT_FALSE(readiness);
    EXPECT_EQ(readiness.error(), ErrorCode::SHUTTING_DOWN);
}

TEST_F(RealClientDeploymentTest, DummyRequestsDoNotWaitForLifecycleWriter) {
    SharedFile file;
    ASSERT_NO_FATAL_FAILURE(file.Init());
    ClientOwner owner;
    ASSERT_EQ(owner.client->setup(DeploymentConfig()), 0);
    const auto id = generate_uuid();
    const int fd = dup(file.fd());
    ASSERT_GE(fd, 0);
    ASSERT_TRUE(RealClientTestHelper::MapShm(
        *owner.client, fd, 0x100000, SharedFile::kSize, false, id));
    auto context = RealClientTestHelper::FindShmContext(*owner.client, id);
    ASSERT_NE(context, nullptr);
    const int rejected_fd = dup(file.fd());
    ASSERT_GE(rejected_fd, 0);

    std::unique_lock writer(
        RealClientTestHelper::LifecycleMutex(*owner.client));
    auto ping_while_locked = std::async(std::launch::async, [&] {
        return owner.client->ping(id);
    });
    const auto ping_status = ping_while_locked.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(ping_status, std::future_status::ready);
    if (ping_status != std::future_status::ready) writer.unlock();
    EXPECT_TRUE(ping_while_locked.get());
    if (!writer.owns_lock()) writer.lock();
    RealClientTestHelper::CloseAdmission(*owner.client);
    auto requests = std::async(std::launch::async, [&] {
        EXPECT_NE(owner.client->deployment_mode(), DeploymentMode::UNKNOWN);
        EXPECT_FALSE(owner.client->is_initialized());
        auto readiness = owner.client->service_ready_internal();
        EXPECT_FALSE(readiness);
        if (!readiness) {
            EXPECT_EQ(readiness.error(), ErrorCode::SHUTTING_DOWN);
        }
        auto reads = owner.client->batch_get_into_dummy_helper(
            {"blocked"}, {0x100000}, {SharedFile::kSize}, std::nullopt, id);
        EXPECT_EQ(reads.size(), 1);
        if (reads.size() == 1) {
            EXPECT_FALSE(reads.front());
            if (!reads.front()) {
                EXPECT_EQ(reads.front().error(),
                          ErrorCode::SHUTTING_DOWN);
            }
        }
        auto unmap = owner.client->unmap_shm_internal(id);
        EXPECT_FALSE(unmap);
        if (!unmap) {
            EXPECT_EQ(unmap.error(), ErrorCode::SHUTTING_DOWN);
        }
        auto map = RealClientTestHelper::MapShm(
            *owner.client, rejected_fd, 0x100000, SharedFile::kSize, false, id);
        EXPECT_FALSE(map);
        if (!map) {
            EXPECT_EQ(map.error(), ErrorCode::SHUTTING_DOWN);
        }
        EXPECT_EQ(fcntl(rejected_fd, F_GETFD), -1);
        EXPECT_EQ(errno, EBADF);
    });
    EXPECT_EQ(requests.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    // A broken blocking implementation must still be able to finish the test.
    writer.unlock();
    requests.get();
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), context);
    EXPECT_EQ(owner.client->tearDownAll(), 0);
    EXPECT_TRUE(context->mapped_shms_.empty());
    auto ping = owner.client->ping(id);
    ASSERT_FALSE(ping);
    EXPECT_EQ(ping.error(), ErrorCode::SHUTTING_DOWN);
}

namespace {

#if defined(MOONCAKE_STORE_CLIENT_P2P)
void CheckIpcStop(P2PClientConfig config, bool connect_peer,
                  bool partial_request) {
    const auto id = generate_uuid();
    config.ipc_socket_path = "ipc_stop_" + std::to_string(getpid()) + "_" +
                             std::to_string(id.first);
    ClientOwner owner;
    ASSERT_EQ(owner.client->setup(config), 0);
    Socket peer(connect_peer ? ConnectIpc(config.ipc_socket_path) : -1);
    if (connect_peer) ASSERT_GE(peer.fd, 0);
    if (partial_request) {
        // No SCM_RIGHTS: ancillary data can make MSG_WAITALL return early.
        const char prefix = 'x';
        ASSERT_EQ(send(peer.fd, &prefix, sizeof(prefix), MSG_NOSIGNAL), 1);
    }
    auto closed = std::async(std::launch::async, [&] {
        return owner.client->tearDownAll();
    });
    const auto status = closed.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(status, std::future_status::ready);
    if (status != std::future_status::ready && peer.fd >= 0) {
        // Let a broken implementation finish so the test can report failure.
        EXPECT_EQ(shutdown(peer.fd, SHUT_RDWR), 0);
    }
    EXPECT_EQ(closed.get(), 0);
    EXPECT_FALSE(RealClientTestHelper::IpcThreadJoinable(*owner.client));
    EXPECT_FALSE(owner.client->is_initialized());
    EXPECT_EQ(owner.client->tearDownAll(), 0);
}

#endif

class PausedDummyRead {
   public:
    PausedDummyRead(RealClient& client, UUID id, bool after_native)
        : client_(client), id_(id), after_native_(after_native),
          released_(release.get_future()) {}

    int64_t Read() {
        if (!after_native_) Pause();
        auto result = client_.batch_get_into_dummy_helper(
            {"drain_key"}, {0x100000}, {SharedFile::kSize}, std::nullopt, id_);
        if (after_native_) Pause();
        const auto value =
            result.front() ? *result.front() : toInt(result.front().error());
        finished.set_value(value);
        return value;
    }

    std::promise<void> reached;
    std::promise<void> release;
    std::promise<int64_t> finished;

   private:
    void Pause() {
        reached.set_value();
        released_.wait();
    }
    RealClient& client_;
    UUID id_;
    bool after_native_;
    std::future<void> released_;
};

void CheckDummyRpcDrain(ClientConfig config, const WriteConfig& write_config,
                        bool after_native) {
    SharedFile file;
    ASSERT_NO_FATAL_FAILURE(file.Init());
    ClientOwner owner;
    ASSERT_EQ(owner.client->setup(config), 0);
    const auto id = generate_uuid();
    const int fd = dup(file.fd());
    ASSERT_GE(fd, 0);
    ASSERT_TRUE(RealClientTestHelper::MapShm(
        *owner.client, fd, 0x100000, SharedFile::kSize, false, id));
    const std::string value = "drain the complete Dummy handler";
    ASSERT_EQ(owner.client->put("drain_key", value, write_config), 0);
    auto context = RealClientTestHelper::FindShmContext(*owner.client, id);
    // Retain the native object: close must call Stop(), not rely on destruction.
    ASSERT_NE(context, nullptr);
    auto native = RealClientTestHelper::NativeClient(*owner.client);
    PausedDummyRead handler(*owner.client, id, after_native);
    auto reached = handler.reached.get_future();
    auto finished = handler.finished.get_future();
    RealClientTestHelper::DummyRpcServer(*owner.client) =
        std::make_unique<coro_rpc::coro_rpc_server>(2, 0, "127.0.0.1");
    auto& server = *RealClientTestHelper::DummyRpcServer(*owner.client);
    server.register_handler<&PausedDummyRead::Read>(&handler);
    auto exited = server.async_start();
    ASSERT_FALSE(exited.hasResult());
    auto response = std::async(std::launch::async, [&] {
        coro_rpc::coro_rpc_client peer;
        auto connected = async_simple::coro::syncAwait(
            peer.connect("127.0.0.1", std::to_string(server.port())));
        EXPECT_FALSE(connected);
        return async_simple::coro::syncAwait(peer.call<&PausedDummyRead::Read>());
    });
    const auto entered = reached.wait_for(std::chrono::seconds(5));
    if (entered != std::future_status::ready) {
        handler.release.set_value();
        owner.client->tearDownAll();
        response.get();
        FAIL() << "Dummy handler did not reach its pause point";
    }
    auto closed = std::async(std::launch::async, [&] {
        return owner.client->tearDownAll();
    });
    EXPECT_EQ(closed.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    // Acceptor shutdown happens after native Stop has completed.
    const auto stop_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!exited.hasResult() &&
           std::chrono::steady_clock::now() < stop_deadline) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(exited.hasResult());
#if defined(MOONCAKE_STORE_CLIENT_P2P)
    EXPECT_EQ(native->GetHealthStatus(), "STOPPED");
    if (native->GetHealthStatus() == "STOPPED") {
        EXPECT_EQ(native->GetBufferAllocator(), nullptr);
    }
#else
    EXPECT_NE(native->GetBufferAllocator(), nullptr);
#endif
    auto late = native->IsExist("late");
    EXPECT_FALSE(late);
    if (!late) EXPECT_EQ(late.error(), ErrorCode::SHUTTING_DOWN);
    // Dummy SHM must survive native Stop until the complete handler exits.
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), context);
    {
        SharedMutexLocker lock(&context->mutex_, shared_lock);
        EXPECT_FALSE(context->mapped_shms_.empty());
    }
    handler.release.set_value();
    // stop() may close the response connection; server-side completion matters.
    response.get();
    EXPECT_EQ(closed.get(), 0);
    if (after_native) {
        EXPECT_EQ(finished.get(), static_cast<int64_t>(value.size()));
        EXPECT_EQ(std::string(static_cast<char*>(file.data), value.size()), value);
    } else {
        EXPECT_EQ(finished.get(), toInt(ErrorCode::SHUTTING_DOWN));
        EXPECT_EQ(std::string(static_cast<char*>(file.data), SharedFile::kSize),
                  std::string(SharedFile::kSize, 0x5a));
    }
#if defined(MOONCAKE_STORE_CLIENT_P2P)
    EXPECT_EQ(native->GetHealthStatus(), "STOPPED");
    EXPECT_EQ(native->GetBufferAllocator(), nullptr);
#endif
    EXPECT_TRUE(context->mapped_shms_.empty());
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), nullptr);
    EXPECT_EQ(owner.client->tearDownAll(), 0);
}

}  // namespace

TEST_F(RealClientDeploymentTest, DummyRpcStopWaitsBeforeNativeEntry) {
    CheckDummyRpcDrain(DeploymentConfig(), LocalWriteConfig(), false);
}

TEST_F(RealClientDeploymentTest, DummyRpcStopWaitsAfterNativeReturn) {
    CheckDummyRpcDrain(DeploymentConfig(), LocalWriteConfig(), true);
}

#if defined(MOONCAKE_STORE_CLIENT_P2P)
TEST_F(RealClientDeploymentTest, P2PReadinessDoesNotWaitForLifecycleWriter) {
    ClientOwner owner;
    ASSERT_EQ(owner.client->setup(DeploymentConfig()), 0);
    std::unique_lock writer(
        RealClientTestHelper::LifecycleMutex(*owner.client));
    auto readiness = std::async(std::launch::async, [&] {
        EXPECT_TRUE(owner.client->is_initialized());
        return owner.client->service_ready_internal();
    });
    EXPECT_EQ(readiness.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    // Release the writer even on failure so a blocking reader can finish.
    writer.unlock();
    auto result = readiness.get();
    ASSERT_TRUE(result);
    EXPECT_EQ(result.value(), DeploymentMode::P2P);
    EXPECT_TRUE(owner.client->service_ready_internal());
}

TEST_F(RealClientDeploymentTest, P2PIpcStopWithoutConnections) {
    CheckIpcStop(DeploymentConfig(), false, false);
}

TEST_F(RealClientDeploymentTest, P2PIpcStopWithEmptyRequest) {
    CheckIpcStop(DeploymentConfig(), true, false);
}

TEST_F(RealClientDeploymentTest, P2PIpcStopWithPartialRequest) {
    CheckIpcStop(DeploymentConfig(), true, true);
}

TEST_F(RealClientDeploymentTest, P2PCloseWaitsForRegisteredShmPublication) {
    SharedFile file;
    ASSERT_NO_FATAL_FAILURE(file.Init());
    ClientResources transport;
    ASSERT_EQ(transport.InitTransferEngine("127.0.0.1", "P2PHANDSHAKE", "tcp",
                                           std::nullopt),
              ErrorCode::OK);
    auto config = DeploymentConfig();
    config.transfer_engine = transport.GetTransferEngine();
    config.local_hostname = transport.GetLocalEndpoint();
    const auto id = generate_uuid();
    config.ipc_socket_path = "ipc_publish_" + std::to_string(getpid()) + "_" +
                             std::to_string(id.first);
    ClientOwner owner;
    ASSERT_EQ(owner.client->setup(config), 0);
    auto native = RealClientTestHelper::NativeClient(*owner.client);
    auto metadata = config.transfer_engine->getMetadata();
    ASSERT_NE(metadata, nullptr);
    auto before = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(before, nullptr);
    Socket peer(ConnectIpc(config.ipc_socket_path));
    ASSERT_GE(peer.fd, 0);

    // Shared access lets the initial context lookup finish but blocks insertion
    // after mmap and TE registration. No production test hook is required.
    std::shared_lock<SharedMutex> pause_publish(
        RealClientTestHelper::ShmMutex(*owner.client));
    ShmRegisterRequest request{id.first, id.second, 0x100000, SharedFile::kSize,
                               false};
    ASSERT_NO_FATAL_FAILURE(
        SendRegistrationRequest(peer.fd, file.fd(), request, sizeof(request)));
    uint64_t registered_addr = 0;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (registered_addr == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        // TCP registration publishes an immutable descriptor snapshot.
        auto current = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (current) {
            for (const auto& buffer : current->buffers) {
                if (buffer.length == SharedFile::kSize &&
                    std::none_of(before->buffers.begin(), before->buffers.end(),
                                 [&](const auto& old) {
                                     return old.addr == buffer.addr;
                                 })) {
                    registered_addr = buffer.addr;
                    break;
                }
            }
        }
        if (registered_addr == 0) std::this_thread::yield();
    }
    if (registered_addr == 0) {
        pause_publish.unlock();
        EXPECT_EQ(owner.client->tearDownAll(), 0);
        FAIL() << "IPC registration did not become visible in TE metadata";
    }
    EXPECT_TRUE(RealClientTestHelper::ShmContexts(*owner.client).empty());
    auto closed = std::async(std::launch::async, [&] {
        return owner.client->tearDownAll();
    });
    EXPECT_EQ(closed.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    const auto stop_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (native->GetHealthStatus() != "STOPPED" &&
           std::chrono::steady_clock::now() < stop_deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(native->GetHealthStatus(), "STOPPED");
    if (native->GetHealthStatus() == "STOPPED") {
        EXPECT_EQ(native->GetBufferAllocator(), nullptr);
    }
    pause_publish.unlock();
    EXPECT_EQ(closed.get(), 0);
    EXPECT_FALSE(RealClientTestHelper::IpcThreadJoinable(*owner.client));
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), nullptr);
    EXPECT_EQ(native->GetHealthStatus(), "STOPPED");
    auto after = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(after, nullptr);
    EXPECT_TRUE(std::none_of(after->buffers.begin(), after->buffers.end(),
                             [&](const auto& buffer) {
                                 return buffer.addr == registered_addr;
                             }));
}

TEST_F(RealClientDeploymentTest, P2PRpcWaitReturnsAfterShmCleanup) {
    SharedFile file;
    ASSERT_NO_FATAL_FAILURE(file.Init());
    ClientOwner owner;
    ASSERT_TRUE(owner.client->setup_internal(
        DeploymentConfig(), /*start_dummy_rpc=*/true, 2, 0));
    ASSERT_NE(RealClientTestHelper::DummyRpcServer(*owner.client), nullptr);
    EXPECT_NE(RealClientTestHelper::DummyRpcServer(*owner.client)->port(), 0);
    const auto id = generate_uuid();
    const int fd = dup(file.fd());
    ASSERT_GE(fd, 0);
    ASSERT_TRUE(RealClientTestHelper::MapShm(
        *owner.client, fd, 0x100000, SharedFile::kSize, false, id));
    auto context = RealClientTestHelper::FindShmContext(*owner.client, id);
    ASSERT_NE(context, nullptr);
    std::unique_lock<SharedMutex> held(context->mutex_);
    auto serving = std::async(std::launch::async, [&] {
        return owner.client->wait_dummy_rpc_server();
    });
    auto closed = std::async(std::launch::async, [&] {
        return owner.client->tearDownAll();
    });
    const auto close_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    // The listener is already stopped when close detaches this context.
    while (RealClientTestHelper::FindShmContext(*owner.client, id) &&
           std::chrono::steady_clock::now() < close_deadline) {
        std::this_thread::yield();
    }
    EXPECT_EQ(RealClientTestHelper::FindShmContext(*owner.client, id), nullptr);
    EXPECT_EQ(serving.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    held.unlock();
    EXPECT_EQ(closed.get(), 0);
    EXPECT_EQ(serving.get(), 0);
    EXPECT_TRUE(context->mapped_shms_.empty());
}

TEST_F(RealClientDeploymentTest, P2PRpcBindFailureClosesClient) {
    coro_rpc::coro_rpc_server occupied(1, 0, "127.0.0.1");
    auto exited = occupied.async_start();
    ASSERT_FALSE(exited.hasResult());
    ClientOwner owner;
    EXPECT_EQ(RealClientTestHelper::DummyRpcServer(*owner.client), nullptr);
    auto result = owner.client->setup_internal(
        DeploymentConfig(), /*start_dummy_rpc=*/true, 1, occupied.port());
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
    EXPECT_FALSE(owner.client->is_initialized());
    EXPECT_EQ(RealClientTestHelper::NativeClient(*owner.client), nullptr);
    EXPECT_EQ(owner.client->tearDownAll(), 0);
}
#endif

}  // namespace mooncake::testing
