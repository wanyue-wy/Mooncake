#include <gtest/gtest.h>
#include <stdexcept>
#include <cstdlib>
#include <optional>
#include <fstream>
#include <filesystem>
#include <unordered_map>

#include "centralized_client_config_builder.h"
#include "client_service.h"

namespace mooncake {
namespace {

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

    ASSERT_EQ(setenv("MC_RUNTIME_CONFIG", "{invalid json", 1), 0);
    EXPECT_NO_THROW(CentralizedClientConfigBuilder::build_centralized_real_client(
        "127.0.0.1:12345", "P2PHANDSHAKE"));
}

TEST(ClientConfigBuilderTest, CentralizedConfigKeepsDiscoveryForBackendValidation) {
    std::unordered_map<std::string, std::string> raw_config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"master_server_addr", "redis://127.0.0.1:6379"},
    };
    auto config = CentralizedClientConfigBuilder::build_centralized_real_client(raw_config);
    EXPECT_EQ(config.master_server_entry, "redis://127.0.0.1:6379");
    EXPECT_EQ(config.client_rpc_port, 0);
}

TEST(ClientConfigBuilderTest, CentralizedClientRpcPortIsIndependentOfOffload) {
    std::unordered_map<std::string, std::string> config = {
        {"local_hostname", "127.0.0.1:12345"}, {"metadata_server", "P2PHANDSHAKE"},
        {"client_rpc_port", "12346"},
    };
    auto parsed = CentralizedClientConfigBuilder::build_centralized_real_client(config);
    EXPECT_EQ(parsed.te_port, 12345);
    EXPECT_FALSE(parsed.enable_offload);
    EXPECT_EQ(parsed.client_rpc_port, 12346);
    config["enable_offload"] = "true";
    auto with_offload = CentralizedClientConfigBuilder::build_centralized_real_client(config);
    EXPECT_TRUE(with_offload.enable_offload);
    EXPECT_EQ(with_offload.client_rpc_port, parsed.client_rpc_port);
    config["client_rpc_port"] = "65536";
    EXPECT_THROW(CentralizedClientConfigBuilder::build_centralized_real_client(config), std::invalid_argument);
}

}  // namespace
}  // namespace mooncake
