#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <unistd.h>

#include "p2p/master/p2p_master_config_loader.h"

namespace mooncake {
namespace {

class P2PMasterConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        directory_ = std::filesystem::temp_directory_path() /
                     ("p2p-master-config-" + std::to_string(::getpid()));
        std::filesystem::create_directories(directory_);
    }
    void TearDown() override { std::filesystem::remove_all(directory_); }

    std::filesystem::path Write(std::string name, std::string content) {
        auto path = directory_ / std::move(name);
        std::ofstream output(path);
        output << content;
        output.close();
        return path;
    }

    std::filesystem::path directory_;
};

TEST_F(P2PMasterConfigTest, LoadsYamlAndDerivesCrashedTtl) {
    auto path = Write("master.yaml", R"(
rpc_port: 51001
client_live_ttl_sec: 7
redis_endpoint: cache.local
max_client_per_key: 3
)");
    auto config = LoadP2PMasterConfig(path.string());
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->rpc.port, 51001);
    EXPECT_EQ(config->client_lifecycle.live_ttl_seconds, 7);
    EXPECT_EQ(config->client_lifecycle.crashed_ttl_seconds, 21);
    EXPECT_EQ(config->redis.endpoint, "cache.local:6379");
    EXPECT_EQ(config->routes.max_clients_per_key, 3);
}

TEST_F(P2PMasterConfigTest, LoadsJsonAndExplicitOverridesWin) {
    auto path = Write("master.json", R"({
      "rpc_port": 51001,
      "metrics_port": 9100,
      "client_live_ttl_sec": 8,
      "client_crashed_ttl_sec": 30
    })");
    P2PMasterConfigOverrides overrides;
    overrides.rpc_port = 52001;
    overrides.client_ttl = 9;
    auto config = LoadP2PMasterConfig(path.string(), overrides);
    ASSERT_TRUE(config.has_value()) << config.error();
    EXPECT_EQ(config->rpc.port, 52001);
    EXPECT_EQ(config->metrics.http_port, 9100);
    EXPECT_EQ(config->client_lifecycle.live_ttl_seconds, 9);
    EXPECT_EQ(config->client_lifecycle.crashed_ttl_seconds, 30);
}

TEST_F(P2PMasterConfigTest, RejectsInvalidEndpointTtlAndSnapshot) {
    P2PMasterConfigOverrides endpoint;
    endpoint.rpc_port = 70000;
    EXPECT_FALSE(LoadP2PMasterConfig({}, endpoint).has_value());

    P2PMasterConfigOverrides ttl;
    ttl.client_ttl = 10;
    ttl.client_crashed_ttl = 9;
    EXPECT_FALSE(LoadP2PMasterConfig({}, ttl).has_value());

    P2PMasterConfigOverrides snapshot;
    snapshot.standby_snapshot_chunk_size = 0;
    EXPECT_FALSE(LoadP2PMasterConfig({}, snapshot).has_value());
}

TEST_F(P2PMasterConfigTest, ValidatesHaBackendRequirements) {
    P2PMasterConfigOverrides etcd;
    etcd.enable_ha = true;
    etcd.election_backend = "etcd";
    EXPECT_FALSE(LoadP2PMasterConfig({}, etcd).has_value());
    etcd.etcd_endpoints = "127.0.0.1:2379";
    auto valid = LoadP2PMasterConfig({}, etcd);
    ASSERT_TRUE(valid.has_value()) << valid.error();
    EXPECT_EQ(valid->ha.election_backend, ElectionBackend::ETCD);

    P2PMasterConfigOverrides invalid;
    invalid.enable_ha = true;
    invalid.election_backend = "unknown";
    EXPECT_FALSE(LoadP2PMasterConfig({}, invalid).has_value());
}

}  // namespace
}  // namespace mooncake
