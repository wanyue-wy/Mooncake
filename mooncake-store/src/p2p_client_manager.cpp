#include "p2p_client_manager.h"
#include <glog/logging.h>
#include <unordered_set>

#include "p2p_types.h"

namespace mooncake {

P2PClientManager::P2PClientManager(const int64_t disconnect_timeout_sec,
                                   const int64_t crash_timeout_sec,
                                   const ViewVersionId view_version)
    : ClientManager(disconnect_timeout_sec, crash_timeout_sec, view_version) {
    p2p_segment_manager_ = std::make_shared<P2PSegmentManager>();
    segment_manager_ = p2p_segment_manager_;
}

auto P2PClientManager::UnmountSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    // Check client is registered
    if (!IsClientRegistered(client_id)) {
        LOG(ERROR) << "UnmountSegment: client not registered"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    ErrorCode err = segment_manager_->UnmountSegment(segment_id, client_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to unmount segment"
                   << "segment_id=" << segment_id << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

// ===== Virtual Factory =====

std::unique_ptr<ClientMeta> P2PClientManager::CreateClientMeta(
    const RegisterClientRequest& req) {
    auto meta = std::make_unique<P2PClientMeta>();
    meta->health_state.status = ClientStatus::HEALTH;
    meta->health_state.last_heartbeat = std::chrono::steady_clock::now();
    meta->ip_address = req.ip_address.value_or("");
    meta->rpc_port = req.rpc_port.value_or(0);
    return meta;
}

// ===== Hook Functions =====

void P2PClientManager::OnClientDisconnected(const UUID& client_id) {
    LOG(INFO) << "client_id=" << client_id
              << ", action=p2p_client_disconnected";
    // In P2P, we might want to notify other peers or update routing table
    // For now, just logging.
}

void P2PClientManager::OnClientCrashed(const UUID& client_id) {
    LOG(WARNING) << "client_id=" << client_id << ", action=p2p_client_crashed";

    // We need to unmount segments for the crashed client.
    // Since ClientMonitorFunc holds the lock when calling this hook,
    // we can access client_metas_ directly without re-locking.
    // However, ClientMonitorFunc loop iterates client_metas_.
    // Erasing from it while iterating is handled by ClientManager's loop (Phase
    // 3). Here we just handle cleanup logic (unmount).

    auto it = client_metas_.find(client_id);
    if (it != client_metas_.end()) {
        auto& meta = it->second;
        // Copy segments to avoid iterator invalidation if unmount modifies
        // list? UnmountSegment calls SegmentManager::UnmountSegment. It does
        // NOT modify ClientMeta's segment list directly (unless we do it). But
        // ClientManager::UnRegisterClient logic calls Unmount then erase. Here
        // we just want to ensure Backend SegmentManager cleans up.

        for (const auto& seg : meta->segments) {
            segment_manager_->UnmountSegment(seg->id, client_id);
        }
    }
}

void P2PClientManager::OnClientRecovered(const UUID& client_id) {
    LOG(INFO) << "client_id=" << client_id << ", action=p2p_client_recovered";
    // Resume participation in P2P network
}

HeartbeatTaskResult P2PClientManager::ProcessTask(const UUID& client_id,
                                                  const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type();

    switch (task.type()) {
        case HeartbeatTaskType::SYNC_SEGMENT_META: {
            // P2P clients might report their own segment status
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
