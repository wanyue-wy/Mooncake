#include <gtest/gtest.h>
#include <stdexcept>
#include <cstdlib>
#include <optional>
#include <fstream>
#include <filesystem>
#include <unordered_map>

#include "centralized_client_config_builder.h"
#include "p2p/client/p2p_client_config_builder.h"
#include "client_service.h"

namespace mooncake {
namespace {

const char* kTieredConfigJson = R"({
  "tiers": [
    {
      "type": "DRAM",
      "capacity": 1048576,
      "priority": 10
    }
  ]
})";

// Architecture-separation contract, separate from the A00 reconnect suite.
TEST(ClientConfigBuilderTest, CentralizedClientRejectsRedisMasterDiscovery) {
    EXPECT_FALSE(Client::Create("127.0.0.1:18000", "P2PHANDSHAKE", "tcp",
                                std::nullopt, "redis://127.0.0.1:6379")
                     .has_value());
}

TEST(ClientConfigBuilderTest, CentralizedRejectsExplicitRuntimeConfig) {
    std::unordered_map<std::string, std::string> config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"runtime_config", R"({"write":{"replica_num":3}})"},
    };
    EXPECT_THROW(CentralizedClientConfigBuilder::build_centralized_real_client(config),
                 std::invalid_argument);
}

TEST(ClientConfigBuilderTest, RuntimeEnvironmentBelongsToP2P) {
    const char* old = std::getenv("MC_RUNTIME_CONFIG");
    struct RestoreRuntimeEnv {
        std::optional<std::string> value;
        ~RestoreRuntimeEnv() {
            if (value) {
                setenv("MC_RUNTIME_CONFIG", value->c_str(), 1);
            } else {
                unsetenv("MC_RUNTIME_CONFIG");
            }
        }
    } restore{old ? std::optional<std::string>(old) : std::nullopt};
    ASSERT_EQ(setenv("MC_RUNTIME_CONFIG",
                     R"({"write":{"remote_weight":0.8}})", 1), 0);

    auto p2p = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "P2PHANDSHAKE", "tcp", std::nullopt,
        "127.0.0.1:50051", kTieredConfigJson);
    EXPECT_DOUBLE_EQ(p2p.runtime_config_json["write"]["remote_weight"].asDouble(),
                     0.8);

    ASSERT_EQ(setenv("MC_RUNTIME_CONFIG", "{invalid json", 1), 0);
    EXPECT_NO_THROW(CentralizedClientConfigBuilder::build_centralized_real_client(
        "127.0.0.1:12345", "P2PHANDSHAKE"));
    EXPECT_THROW(P2PClientConfigBuilder::build_p2p_real_client(
                     "127.0.0.1:12345", "P2PHANDSHAKE", "tcp", std::nullopt,
                     "127.0.0.1:50051", kTieredConfigJson),
                 std::runtime_error);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigUsesDefaults) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson);

    EXPECT_FALSE(config.start_local_only);
    EXPECT_FALSE(config.tiered_backend_config.isNull());
    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);
    EXPECT_EQ(config.local_memcpy_async_worker_num, 32u);
    EXPECT_EQ(config.te_async_poll_worker_num, 32u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::TE);
    EXPECT_EQ(config.p2p_key_lease_duration_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseDurationMs);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms,
              P2PClientConfig::kP2pDefaultKeyLeaseScanIntervalMs);
    EXPECT_EQ(config.transfer_direction_mode, TransferDirectionMode::REVERSE);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 4);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 1);
}

TEST(ClientConfigBuilderTest, P2PLocalOnlyStartupIsExplicit) {
    std::unordered_map<std::string, std::string> values = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"tiered_backend_config", kTieredConfigJson},
        {"start_local_only", "true"},
    };
    EXPECT_TRUE(P2PClientConfigBuilder::build_p2p_real_client(values).start_local_only);
    values["start_local_only"] = "false";
    EXPECT_FALSE(P2PClientConfigBuilder::build_p2p_real_client(values).start_local_only);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigUsesRedisDiscoveryDefaults) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
        {"tiered_backend_config", kTieredConfigJson},
    };

    auto config = P2PClientConfigBuilder::build_p2p_real_client(raw_config);

    EXPECT_EQ(config.redis_master_view_ttl_sec, 4);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 1);
}

TEST(ClientConfigBuilderTest, CentralizedConfigKeepsDiscoveryForBackendValidation) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
    };
    auto config = CentralizedClientConfigBuilder::build_centralized_real_client(raw_config);
    EXPECT_EQ(config.master_server_entry, "redis://127.0.0.1:6379");
    EXPECT_EQ(config.offload_rpc_port, 0);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigKeyLeaseOverrides) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 8, 2048, 512 * 1024 * 1024, 120000, "te", 32, 9003, true, {}, 0,
        2000, 0, 3333, 444);

    EXPECT_EQ(config.p2p_key_lease_duration_ms, 3333u);
    EXPECT_EQ(config.p2p_key_lease_scan_interval_ms, 444u);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigReadsRedisDiscoveryConfig) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
        {"tiered_backend_config", kTieredConfigJson},
        {"redis_cluster_id", "test-cluster"},
        {"redis_password", "test-password"},
        {"redis_db_index", "3"},
        {"redis_master_view_ttl_sec", "9"},
        {"redis_heartbeat_interval_sec", "4"},
    };

    auto config = P2PClientConfigBuilder::build_p2p_real_client(raw_config);

    EXPECT_EQ(config.redis_cluster_id, "test-cluster");
    EXPECT_EQ(config.redis_password, "test-password");
    EXPECT_EQ(config.redis_db_index, 3);
    EXPECT_EQ(config.redis_master_view_ttl_sec, 9);
    EXPECT_EQ(config.redis_heartbeat_interval_sec, 4);
}

TEST(ClientConfigBuilderTest, CentralizedConfigRejectsRemovedParameters) {
    for (const auto* key : {"redis_cluster_id", "http_port", "enable_http_server",
                            "enable_metric_collection", "metric_report_interval_seconds",
                            "local_rpc_port"}) {
        std::unordered_map<std::string, std::string> config = {
            {"local_hostname", "127.0.0.1"}, {"metadata_server", "P2PHANDSHAKE"},
            {key, "1"},
        };
        EXPECT_THROW(CentralizedClientConfigBuilder::build_centralized_real_client(config),
                     std::invalid_argument) << key;
    }
}

TEST(ClientConfigBuilderTest, CentralizedOffloadPortIsIndependent) {
    std::unordered_map<std::string, std::string> config = {
        {"local_hostname", "127.0.0.1:12345"}, {"metadata_server", "P2PHANDSHAKE"},
        {"enable_offload", "true"}, {"offload_rpc_port", "12346"},
    };
    auto parsed = CentralizedClientConfigBuilder::build_centralized_real_client(config);
    EXPECT_EQ(parsed.te_port, 12345);
    EXPECT_TRUE(parsed.enable_offload);
    EXPECT_EQ(parsed.offload_rpc_port, 12346);
    config["offload_rpc_port"] = "65536";
    EXPECT_THROW(CentralizedClientConfigBuilder::build_centralized_real_client(config), std::invalid_argument);
}

TEST(ClientConfigBuilderTest,
     BuildP2PClientConfigAcceptsCustomAsyncCopyConfig) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 8, 2048, 512 * 1024 * 1024, 120000, "memcpy", 3);

    EXPECT_EQ(config.local_memcpy_async_worker_num, 3u);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::MEMCPY);
    EXPECT_EQ(config.te_async_poll_worker_num, 32u);
}

TEST(ClientConfigBuilderTest, BuildP2PTeModePassesTeAsyncPollWorkerArg) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 2, 1024, 100 * 1024 * 1024, 60 * 1000, "te", 5, 9003, true, {},
        0, 2000, 0, 0, 0, "reverse", "", true, 60, DEFAULT_CLUSTER_ID, "", 0, 5,
        2, "", 0, 18, true);
    EXPECT_TRUE(config.start_local_only);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::TE);
    EXPECT_EQ(config.te_async_poll_worker_num, 18u);
    EXPECT_EQ(config.local_memcpy_async_worker_num, 32u);
}

TEST(ClientConfigBuilderTest, BuildP2PMemcpyModePassesTeAsyncPollWorkerArg) {
    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 2, 1024, 100 * 1024 * 1024, 60 * 1000, "memcpy", 5, 9003, true,
        {}, 0, 2000, 0, 0, 0, "reverse", "", true, 60, DEFAULT_CLUSTER_ID, "",
        0, 5, 2, "", 0, 99);
    EXPECT_EQ(config.local_transfer_mode, LocalTransferMode::MEMCPY);
    EXPECT_EQ(config.local_memcpy_async_worker_num, 5u);
    EXPECT_EQ(config.te_async_poll_worker_num, 99u);
}

TEST(ClientConfigBuilderTest, BuildP2PDictConfigPassesTeAsyncPollWorkerNum) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "http://127.0.0.1:8080/metadata"},
        {"master_server_addr", "127.0.0.1:50051"},
        {"tiered_backend_config", kTieredConfigJson},
        {"local_transfer_mode", "te"},
        {"te_async_poll_worker_num", "7"},
    };
    auto config = P2PClientConfigBuilder::build_p2p_real_client(raw_config);
    EXPECT_EQ(config.te_async_poll_worker_num, 7u);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigParsesTransferDirectionMode) {
    auto forward = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
        12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32, 9003, true,
        {}, 0, 2000, 0, 0, 0, "forward");
    EXPECT_EQ(forward.transfer_direction_mode, TransferDirectionMode::FORWARD);
}

TEST(ClientConfigBuilderTest,
     BuildP2PClientConfigRejectsInvalidTransferDirectionMode) {
    EXPECT_THROW(
        P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
            12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32, 9003,
            true, {}, 0, 2000, 0, 0, 0, "invalid"),
        std::runtime_error);
}

TEST(ClientConfigBuilderTest, BuildP2PClientConfigRejectsInvalidTransferMode) {
    EXPECT_THROW(
        P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", kTieredConfigJson, 0, nullptr, "",
            12345, 8, 2048, 512 * 1024 * 1024, 120000, "invalid_mode"),
        std::runtime_error);
}

// ---- LoadTieredConfig: file path ----

TEST(ClientConfigBuilderTest, LoadFromFilePath) {
    const std::string tmp_path = "/tmp/mc_test_tiered_cfg.json";
    {
        std::ofstream f(tmp_path);
        f << kTieredConfigJson;
    }

    auto config = P2PClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
        std::nullopt, "127.0.0.1:50051", tmp_path);

    EXPECT_FALSE(config.tiered_backend_config.isNull());
    EXPECT_TRUE(config.tiered_backend_config.isMember("tiers"));
    EXPECT_EQ(config.tiered_backend_config["tiers"].size(), 1u);

    std::filesystem::remove(tmp_path);
}

// ---- LoadTieredConfig: invalid file path → throws ----

TEST(ClientConfigBuilderTest, InvalidFilePathThrows) {
    EXPECT_THROW(
        P2PClientConfigBuilder::build_p2p_real_client(
            "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
            std::nullopt, "127.0.0.1:50051", "/nonexistent/path/tiered.json"),
        std::runtime_error);
}

// ---- LoadTieredConfig: malformed JSON string → throws ----

TEST(ClientConfigBuilderTest, MalformedJsonStringThrows) {
    // Starts with '{' so LoadTieredConfig treats it as inline JSON, but it is
    // syntactically invalid and will fail to parse.
    std::string bad_json = "{ not valid json";
    EXPECT_THROW(P2PClientConfigBuilder::build_p2p_real_client(
                     "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
                     std::nullopt, "127.0.0.1:50051", bad_json),
                 std::runtime_error);
}

// ---- LoadTieredConfig: empty string → throws (tries to open file named "")
// ----

TEST(ClientConfigBuilderTest, EmptyStringThrows) {
    EXPECT_THROW(P2PClientConfigBuilder::build_p2p_real_client(
                     "127.0.0.1:12345", "http://127.0.0.1:8080/metadata", "tcp",
                     std::nullopt, "127.0.0.1:50051", ""),
                 std::runtime_error);
}

}  // namespace
}  // namespace mooncake
