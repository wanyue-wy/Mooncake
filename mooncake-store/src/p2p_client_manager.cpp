#include "p2p_client_manager.h"
#include "p2p_client_meta.h"
#include <glog/logging.h>

namespace mooncake {

P2PClientManager::P2PClientManager(const int64_t disconnect_timeout_sec,
                                   const int64_t crash_timeout_sec,
                                   const ViewVersionId view_version)
    : ClientManager(disconnect_timeout_sec, crash_timeout_sec, view_version) {}

std::shared_ptr<ClientMeta> P2PClientManager::CreateClientMeta(
    const RegisterClientRequest& req) {
    auto meta = std::make_shared<P2PClientMeta>(
        req.client_id, req.ip_address.value_or(""), req.rpc_port.value_or(0));
    return meta;
}

HeartbeatTaskResult P2PClientManager::ProcessTask(const UUID& client_id,
                                                  const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type_;

    switch (task.type_) {
        case HeartbeatTaskType::SYNC_SEGMENT_META: {
            // TODO: wanyue-wy
            // P2P client reports its own segment status
            result.error = ErrorCode::OK;
            break;
        }
        default:
            result.error = ErrorCode::NOT_IMPLEMENTED;
            break;
    }
    return result;
}

}  // namespace mooncake
