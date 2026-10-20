#include <glog/logging.h>
#include <gtest/gtest.h>

#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
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

inline void SendRegistration(int socket_fd, int memory_fd,
                             const ShmRegisterRequest& request,
                             size_t request_size, int& response) {
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


#if defined(MOONCAKE_STORE_CLIENT_P2P)
namespace {

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

}  // namespace

TEST_F(RealClientDeploymentTest, P2PIpcStopWithoutConnections) {
    CheckIpcStop(DeploymentConfig(), false, false);
}

TEST_F(RealClientDeploymentTest, P2PIpcStopWithEmptyRequest) {
    CheckIpcStop(DeploymentConfig(), true, false);
}

TEST_F(RealClientDeploymentTest, P2PIpcStopWithPartialRequest) {
    CheckIpcStop(DeploymentConfig(), true, true);
}
#endif

}  // namespace mooncake::testing
