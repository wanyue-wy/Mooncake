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
    if (!response) {
        return tl::make_unexpected(response.error());
    }

    GetReplicaListResponse adapted;
    adapted.replicas = std::move(response->replicas);
    adapted.centralized_extra.emplace(response->lease_ttl_ms);
    auto configured = ApplyReadRouteConfig(adapted, config);
    if (!configured) return tl::make_unexpected(configured.error());
    return adapted;
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

    auto wire_responses = MasterClient::BatchGetReplicaList(owned_keys);
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> responses;
    responses.reserve(wire_responses.size());
    for (auto& response : wire_responses) {
        if (!response) {
            responses.emplace_back(tl::make_unexpected(response.error()));
            continue;
        }

        GetReplicaListResponse adapted;
        adapted.replicas = std::move(response->replicas);
        adapted.centralized_extra.emplace(response->lease_ttl_ms);
        auto configured = ApplyReadRouteConfig(adapted, config);
        if (!configured) {
            responses.emplace_back(
                tl::make_unexpected(configured.error()));
        } else {
            responses.emplace_back(std::move(adapted));
        }
    }
    return responses;
}

}  // namespace mooncake
