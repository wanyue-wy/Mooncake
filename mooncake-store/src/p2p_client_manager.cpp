#include "p2p_client_manager.h"
#include <glog/logging.h>
#include <unordered_set>

#include "p2p_types.h"

namespace mooncake {

P2PClientManager::P2PClientManager(const int64_t disconnect_timeout_sec,
                                   const int64_t crash_timeout_sec,
                                   const ViewVersionId view_version)
    : ClientManager(disconnect_timeout_sec, crash_timeout_sec, view_version) {
    segment_manager_ = std::make_shared<P2PSegmentManager>();
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

auto P2PClientManager::InnerMountSegment(const Segment& segment,
                                         const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ErrorCode err = segment_manager_->MountSegment(segment, client_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment_id=" << segment.id
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto P2PClientManager::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_segments;
    ErrorCode err = segment_manager_->GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to get all segments" << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto P2PClientManager::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    size_t used = 0;
    size_t capacity = 0;
    ErrorCode err = segment_manager_->QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query segments" << ", segment=" << segment
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto P2PClientManager::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::shared_ptr<Segment>> segments;
    ErrorCode err = segment_manager_->GetClientSegments(client_id, segments);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
        }
        return tl::make_unexpected(err);
    }

    std::unordered_set<std::string> unique_ips;
    for (const auto& segment : segments) {
        if (!segment) continue;
        if (segment->is_p2p()) {
            const auto& extra = std::get<P2PSegmentExtraData>(segment->extra);
            if (!extra.ip_address.empty()) {
                unique_ips.insert(extra.ip_address);
            }
        }
    }

    return std::vector<std::string>(unique_ips.begin(), unique_ips.end());
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
    // Clean up segments
    std::vector<std::shared_ptr<Segment>> segments;
    if (segment_manager_->GetClientSegments(client_id, segments) ==
        ErrorCode::OK) {
        for (const auto& seg : segments) {
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
