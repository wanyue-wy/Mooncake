#pragma once

#include "client_config_builder.h"

namespace mooncake {

struct CentralizedClientConfig : RealClientConfigBase {
    // Compatibility input; centralized setup only accepts zero.
    uint16_t heartbeat_rpc_port = 0;

    // Size of global segment to mount (0 to skip)
    uint64_t global_segment_size = 0;

    // Whether to enable file storage offloading.
    bool enable_offload = false;

    // Native offload listener; 0 requests an available port.
    uint16_t offload_rpc_port = 0;
};

class CentralizedClientConfigBuilder : private ClientConfigBuilder {
   public:
    static CentralizedClientConfig build_centralized_real_client(
        const std::string& local_hostname,
        const std::string& metadata_connstring,
        const std::string& protocol = "tcp",
        const std::optional<std::string>& rdma_devices = std::nullopt,
        const std::string& master_server_entry = "127.0.0.1:50051",
        uint64_t global_segment_size = 0, uint64_t local_buffer_size = 0,
        const std::shared_ptr<TransferEngine>& transfer_engine = nullptr,
        const std::string& ipc_socket_path = "", bool enable_offload = false,
        const std::map<std::string, std::string>& labels = {},
        const std::string& runtime_config = "", uint16_t heartbeat_rpc_port = 0,
        uint16_t offload_rpc_port = 0) {
        if (!runtime_config.empty()) {
            LOG(ERROR) << "Centralized clients do not support runtime_config";
            throw std::invalid_argument(
                "Centralized clients do not support runtime_config");
        }
        CentralizedClientConfig config;
        fill_real_client_config_base(
            config, local_hostname, metadata_connstring, protocol, rdma_devices,
            master_server_entry, local_buffer_size, transfer_engine,
            ipc_socket_path, labels);
        config.global_segment_size = global_segment_size;
        config.enable_offload = enable_offload;
        config.heartbeat_rpc_port = heartbeat_rpc_port;
        config.offload_rpc_port = offload_rpc_port;
        return config;
    }

    static CentralizedClientConfig build_centralized_real_client(
        const std::unordered_map<std::string, std::string>& config) {
        for (const auto& [key, value] : config) {
            if (key == "http_port" || key == "enable_http_server" ||
                key == "enable_metric_collection" ||
                key == "metric_report_interval_seconds" ||
                key == "local_rpc_port" || key.rfind("redis_", 0) == 0) {
                LOG(ERROR) << "Unsupported centralized setup parameter: "
                           << key;
                throw std::invalid_argument(
                    "Unsupported centralized setup parameter: " + key);
            }
        }
        auto devices = get_config_str(config, DictCommon::kRdmaDevices);
        auto offload_port = get_config_port(config, "offload_rpc_port");
        auto heartbeat_port = static_cast<uint16_t>(
            get_config_size(config, DictCentralized::kHeartbeatRpcPort,
                            DictCentralized::kDefaultHeartbeatRpcPort));
        return build_centralized_real_client(
            get_config_str(config, DictCommon::kLocalHostname),
            get_config_str(config, DictCommon::kMetadataServer),
            get_config_str(config, DictCommon::kProtocol,
                           DictCommon::kDefaultProtocol),
            devices.empty() ? std::nullopt
                            : std::optional<std::string>(devices),
            get_config_str(config, DictCommon::kMasterServerAddr,
                           DictCommon::kDefaultMasterServerAddr),
            get_config_size(config, DictCentralized::kGlobalSegmentSize,
                            DictCentralized::kDefaultGlobalSegmentSize),
            get_config_size(config, DictCommon::kLocalBufferSize,
                            DictCommon::kDefaultLocalBufferSize),
            nullptr, get_config_str(config, DictCommon::kIpcSocketPath),
            get_config_bool(config, "enable_offload", false), {},
            get_config_str(config, DictCentralized::kRuntimeConfig),
            heartbeat_port, offload_port);
    }

   private:
    struct DictCentralized {
        // Keys
        static constexpr const char* kGlobalSegmentSize = "global_segment_size";
        // Defaults
        static constexpr size_t kDefaultGlobalSegmentSize = 1024 * 1024 * 16;

        static constexpr const char* kRuntimeConfig = "runtime_config";
        static constexpr const char* kHeartbeatRpcPort = "heartbeat_rpc_port";
        static constexpr uint16_t kDefaultHeartbeatRpcPort = 0;
    };

    static uint16_t get_config_port(
        const std::unordered_map<std::string, std::string>& config,
        const std::string& key) {
        auto it = config.find(key);
        if (it == config.end()) return 0;
        const auto& value = it->second;
        try {
            size_t parsed = 0;
            auto port = std::stoul(value, &parsed);
            if (value.empty() || value.front() == '-' ||
                parsed != value.size() || port > UINT16_MAX) {
                throw std::invalid_argument("out of range or malformed port");
            }
            return static_cast<uint16_t>(port);
        } catch (const std::exception& error) {
            LOG(ERROR) << "Invalid " << key << ": " << error.what();
            throw std::invalid_argument("Invalid " + key);
        }
    }
};

}  // namespace mooncake
