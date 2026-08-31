#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "p2p/master/p2p_master_config_loader.h"

namespace mooncake {
namespace testing {

TEST(P2PMasterConfigTest, UsesArchitectureDefaults) {
    google::FlagSaver flag_saver;
    auto config = LoadP2PMasterConfig();
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->rpc.port, 50051u);
    EXPECT_EQ(config->rpc.thread_num, 4u);
    EXPECT_EQ(config->client_lifecycle.crashed_ttl_seconds,
              config->client_lifecycle.live_ttl_seconds * 3);
}

TEST(P2PMasterConfigTest, CliOverridesFileAndNormalizesRedisEndpoint) {
    google::FlagSaver flag_saver;
    const auto path = std::filesystem::temp_directory_path() /
                      "mooncake_p2p_master_config_test.json";
    {
        std::ofstream output(path);
        output << R"({
  "rpc_port": 51051,
  "redis_endpoint": "redis.example.com"
})";
    }
    ASSERT_NE(google::SetCommandLineOption("config_path", path.c_str()), "");
    ASSERT_NE(google::SetCommandLineOption("rpc_port", "52051"), "");

    auto config = LoadP2PMasterConfig();
    std::filesystem::remove(path);
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->rpc.port, 52051u);
    EXPECT_EQ(config->redis.endpoint, "redis.example.com:6379");
}

}  // namespace testing
}  // namespace mooncake
