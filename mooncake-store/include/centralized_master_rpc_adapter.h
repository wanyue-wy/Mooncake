#pragma once

#include <string_view>
#include <vector>

#include "master_client.h"

namespace mooncake {

/**
 * @brief Adapts the stable client API to the a00f757 centralized RPC protocol.
 */
class CentralizedMasterRpcAdapter final : public MasterClient {
   public:
    CentralizedMasterRpcAdapter(const UUID& client_id,
                                MasterClientMetric* metrics = nullptr)
        : MasterClient(client_id, metrics) {}

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key, const ReadRouteConfig& config);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string_view>& keys,
                        const ReadRouteConfig& config);

   private:
    static tl::expected<void, ErrorCode> ApplyReadRouteConfig(
        GetReplicaListResponse& response, const ReadRouteConfig& config);
};

}  // namespace mooncake
