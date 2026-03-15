#include <gtest/gtest.h>
#include <glog/logging.h>
#include "client_config_builder.h"

namespace mooncake {

class ClientConfigBuilderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Save and clear env to avoid interference
        saved_env_ = nullptr;
        if (const char* env = std::getenv("MOONCAKE_TIERED_CONFIG")) {
            saved_env_ = strdup(env);
        }
        unsetenv("MOONCAKE_TIERED_CONFIG");
    }

    void TearDown() override {
        if (saved_env_) {
            setenv("MOONCAKE_TIERED_CONFIG", saved_env_, 1);
            free(saved_env_);
        } else {
            unsetenv("MOONCAKE_TIERED_CONFIG");
        }
    }

    char* saved_env_ = nullptr;
};

// ============================================================
// build_dummy
// ============================================================

TEST_F(ClientConfigBuilderTest, BuildDummyBasic) {
    auto config = ClientConfigBuilder::build_dummy(
        1024 * 1024,      // mem_pool_size
        512 * 1024,        // local_buffer_size
        "192.168.1.1:50051",  // real_client_addr
        "/tmp/ipc.sock"   // ipc_socket_path
    );

    EXPECT_EQ(config.mem_pool_size, 1024 * 1024);
    EXPECT_EQ(config.local_buffer_size, 512 * 1024);
    EXPECT_EQ(config.real_client_addr, "192.168.1.1:50051");
    EXPECT_EQ(config.ipc_socket_path, "/tmp/ipc.sock");
}

// ============================================================
// build_centralized_real_client
// ============================================================

TEST_F(ClientConfigBuilderTest, BuildCentralizedBasic) {
    auto config = ClientConfigBuilder::build_centralized_real_client(
        "192.168.1.1",    // local_hostname
        "redis://localhost:6379",  // metadata_connstring
        "tcp",             // protocol
        std::nullopt,      // rdma_devices
        "127.0.0.1:50051"  // master_server_entry
    );

    EXPECT_EQ(config.local_ip, "192.168.1.1");
    EXPECT_EQ(config.te_port, 0);  // no port specified → default 0
    EXPECT_EQ(config.metadata_connstring, "redis://localhost:6379");
    EXPECT_EQ(config.protocol, "tcp");
    EXPECT_EQ(config.master_server_entry, "127.0.0.1:50051");
}

TEST_F(ClientConfigBuilderTest, BuildCentralizedWithIPv4Port) {
    auto config = ClientConfigBuilder::build_centralized_real_client(
        "192.168.1.1:12345",  // local_hostname with port
        "redis://localhost"
    );

    EXPECT_EQ(config.local_ip, "192.168.1.1");
    EXPECT_EQ(config.te_port, 12345);
}

TEST_F(ClientConfigBuilderTest, BuildCentralizedWithIPv6Brackets) {
    auto config = ClientConfigBuilder::build_centralized_real_client(
        "[2001:db8::1]:8080",
        "redis://localhost"
    );

    EXPECT_EQ(config.local_ip, "2001:db8::1");
    EXPECT_EQ(config.te_port, 8080);
}

TEST_F(ClientConfigBuilderTest, BuildCentralizedWithIPv6NoBrackets) {
    auto config = ClientConfigBuilder::build_centralized_real_client(
        "[2001:db8::1]",
        "redis://localhost"
    );

    EXPECT_EQ(config.local_ip, "2001:db8::1");
    EXPECT_EQ(config.te_port, 0);  // no port
}

TEST_F(ClientConfigBuilderTest, BuildCentralizedWithSegmentAndOffload) {
    auto config = ClientConfigBuilder::build_centralized_real_client(
        "10.0.0.1",
        "redis://localhost",
        "tcp",
        std::nullopt,
        "127.0.0.1:50051",
        1024 * 1024 * 100,  // global_segment_size
        1024 * 1024,         // local_buffer_size
        nullptr,             // transfer_engine
        "",                  // ipc_socket_path
        true                 // enable_offload
    );

    EXPECT_EQ(config.global_segment_size, 1024ULL * 1024 * 100);
    EXPECT_EQ(config.local_buffer_size, 1024ULL * 1024);
    EXPECT_TRUE(config.enable_offload);
}

TEST_F(ClientConfigBuilderTest, BuildCentralizedWithLabels) {
    std::map<std::string, std::string> labels = {
        {"region", "us-west"},
        {"tier", "gpu"},
    };

    auto config = ClientConfigBuilder::build_centralized_real_client(
        "10.0.0.1",
        "redis://localhost",
        "tcp", std::nullopt, "127.0.0.1:50051",
        0, 0, nullptr, "", false,
        labels
    );

    EXPECT_EQ(config.labels.size(), 2);
    EXPECT_EQ(config.labels.at("region"), "us-west");
    EXPECT_EQ(config.labels.at("tier"), "gpu");
}

// ============================================================
// build_p2p_real_client
// ============================================================

TEST_F(ClientConfigBuilderTest, BuildP2PWithValidConfig) {
    std::string tiered_json = R"({
        "tiers": [
            {"type": "DRAM", "capacity": 1048576, "priority": 100}
        ]
    })";

    auto config = ClientConfigBuilder::build_p2p_real_client(
        "10.0.0.1",
        "redis://localhost",
        "tcp",
        std::nullopt,
        "127.0.0.1:50051",
        tiered_json,
        0,         // local_buffer_size
        nullptr,   // transfer_engine
        "",        // ipc_socket_path
        12345,     // client_rpc_port
        4          // rpc_thread_num
    );

    EXPECT_EQ(config.local_ip, "10.0.0.1");
    EXPECT_EQ(config.client_rpc_port, 12345);
    EXPECT_EQ(config.rpc_thread_num, 4);
    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
}

TEST_F(ClientConfigBuilderTest, BuildP2PEmptyConfigThrows) {
    // Empty tiered config and no env variable → should throw
    EXPECT_THROW(
        ClientConfigBuilder::build_p2p_real_client(
            "10.0.0.1",
            "redis://localhost",
            "tcp", std::nullopt, "127.0.0.1:50051",
            ""  // empty config
        ),
        std::runtime_error
    );
}

TEST_F(ClientConfigBuilderTest, BuildP2PFromEnvVariable) {
    std::string tiered_json = R"({
        "tiers": [
            {"type": "DRAM", "capacity": 2097152, "priority": 50}
        ]
    })";
    setenv("MOONCAKE_TIERED_CONFIG", tiered_json.c_str(), 1);

    auto config = ClientConfigBuilder::build_p2p_real_client(
        "10.0.0.1",
        "redis://localhost",
        "tcp", std::nullopt, "127.0.0.1:50051",
        ""  // empty json, should fallback to env
    );

    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1);
}

TEST_F(ClientConfigBuilderTest, BuildP2PInvalidJsonLogsError) {
    // Invalid JSON should throw because tiers will be missing
    EXPECT_THROW(
        ClientConfigBuilder::build_p2p_real_client(
            "10.0.0.1",
            "redis://localhost",
            "tcp", std::nullopt, "127.0.0.1:50051",
            "not valid json{{"  // invalid JSON
        ),
        std::runtime_error
    );
}

}  // namespace mooncake
