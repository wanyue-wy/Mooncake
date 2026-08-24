#include "centralized_master_rpc_adapter.h"

#include <algorithm>
#include <string>

#include <glog/logging.h>

namespace mooncake {

tl::expected<void, ErrorCode>
CentralizedMasterRpcAdapter::ApplyReadRouteConfig(
    GetReplicaListResponse& response, const ReadRouteConfig& config) {
    if (config.p2p_config.has_value()) {
        LOG(ERROR) << "P2P read-route filters are invalid in centralized mode";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (config.max_candidates ==
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES ||
        config.max_candidates >= response.replicas.size()) {
        return {};
    }

    auto priority = [](ReplicaType type) {
        if (type == ReplicaType::MEMORY) return 2;
        if (type == ReplicaType::LOCAL_DISK) return 1;
        return 0;
    };
    std::stable_sort(response.replicas.begin(), response.replicas.end(),
                     [&](const auto& lhs, const auto& rhs) {
                         return priority(lhs.type()) > priority(rhs.type());
                     });
    response.replicas.resize(config.max_candidates);
    return {};
}

tl::expected<GetReplicaListResponse, ErrorCode>
CentralizedMasterRpcAdapter::GetReplicaList(const std::string& key,
                                            const ReadRouteConfig& config) {
    auto response = MasterClient::GetReplicaList(key);
    if (!response) return response;
    auto configured = ApplyReadRouteConfig(response.value(), config);
    if (!configured) return tl::make_unexpected(configured.error());
    return response;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
CentralizedMasterRpcAdapter::BatchGetReplicaList(
    const std::vector<std::string_view>& keys, const ReadRouteConfig& config) {
    if (config.p2p_config.has_value()) {
        LOG(ERROR) << "P2P read-route filters are invalid in centralized mode";
        return std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<std::string> owned_keys;
    owned_keys.reserve(keys.size());
    for (auto key : keys) owned_keys.emplace_back(key);

    auto responses = MasterClient::BatchGetReplicaList(owned_keys);
    for (auto& response : responses) {
        if (!response) continue;
        auto configured = ApplyReadRouteConfig(response.value(), config);
        if (!configured) response = tl::make_unexpected(configured.error());
    }
    return responses;
}

}  // namespace mooncake
