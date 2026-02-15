#include "centralized_client_manager.h"

#include <glog/logging.h>

#include "master_metric_manager.h"

namespace mooncake {
CentralizedClientManager::CentralizedClientManager(
    const int64_t client_live_ttl_sec,
    const BufferAllocatorType memory_allocator_type,
    std::function<void()> segment_clean_func, const ViewVersionId view_version)
    : ClientManager(client_live_ttl_sec, client_live_ttl_sec, view_version),
      segment_clean_func_(segment_clean_func) {
    segment_manager_ =
        std::make_shared<CentralizedSegmentManager>(memory_allocator_type);
}

auto CentralizedClientManager::UnmountSegment(const UUID& segment_id,
                                              const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    // Check client is registered
    if (!IsClientRegistered(client_id)) {
        LOG(ERROR) << "UnmountSegment: client not registered"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    size_t metrics_dec_capacity = 0;
    std::string segment_name;
    ErrorCode err = segment_manager_->PrepareUnmountSegment(
        segment_id, metrics_dec_capacity, segment_name);
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        LOG(INFO) << "segment_id=" << segment_id << ", client_id=" << client_id
                  << ", error=segment_not_found";
        return {};
    } else if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to prepare unmount segment"
                   << "segment_id=" << segment_id << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }

    segment_clean_func_();

    err = segment_manager_->UnmountSegment(segment_id, client_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to unmount segment"
                   << "segment_id=" << segment_id << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }

    MasterMetricManager::instance().dec_total_mem_capacity(
        segment_name, metrics_dec_capacity);
    return {};
}

auto CentralizedClientManager::MountLocalDiskSegment(const UUID& client_id,
                                                     bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    auto err =
        segment_manager_->MountLocalDiskSegment(client_id, enable_offloading);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        return {};
    } else if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to mount local disk segment"
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto CentralizedClientManager::InnerMountSegment(const Segment& segment,
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

auto CentralizedClientManager::OffloadObjectHeartbeat(const UUID& client_id,
                                                      bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    return segment_manager_->OffloadObjectHeartbeat(client_id,
                                                    enable_offloading);
}

auto CentralizedClientManager::PushOffloadingQueue(
    const std::string& key, const int64_t size, const std::string& segment_name)
    -> tl::expected<void, ErrorCode> {
    ErrorCode err =
        segment_manager_->PushOffloadingQueue(key, size, segment_name);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

// ===== Virtual Factory =====

std::unique_ptr<ClientMeta> CentralizedClientManager::CreateClientMeta(
    const RegisterClientRequest& req) {
    auto meta = std::make_unique<ClientMeta>();
    meta->health_state.status = ClientStatus::HEALTH;
    meta->health_state.last_heartbeat = std::chrono::steady_clock::now();
    return meta;
}

// ===== Hook Functions =====

void CentralizedClientManager::OnClientDisconnected(const UUID& client_id) {
    // Centralized mode has no DISCONNECTION state (HEALTH -> CRASHED directly).
    // This should not be called, but log if it is.
    LOG(WARNING) << "client_id=" << client_id
                 << ", action=centralized_client_disconnected (unexpected)";
}

void CentralizedClientManager::OnClientCrashed(const UUID& client_id) {
    // Unmount all segments for this client
    LOG(WARNING) << "client_id=" << client_id
                 << ", action=centralized_client_crashed, cleaning_up";

    std::vector<UUID> expired_clients = {client_id};
    std::vector<UUID> unmount_segments;
    std::vector<size_t> dec_capacities;
    std::vector<UUID> client_ids;
    std::vector<std::string> segment_names;

    ErrorCode ret = segment_manager_->BatchPrepareUnmountClientSegments(
        expired_clients, unmount_segments, dec_capacities, client_ids,
        segment_names);
    if (ret != ErrorCode::OK) {
        LOG(ERROR) << "Failed to batch prepare unmount client segments: "
                   << toString(ret);
        return;
    }

    if (!unmount_segments.empty()) {
        segment_clean_func_();
        ret = segment_manager_->BatchUnmountSegments(unmount_segments,
                                                     client_ids, segment_names);
        if (ret != ErrorCode::OK) {
            LOG(ERROR) << "Failed to batch unmount segments: " << toString(ret);
        }
        for (size_t i = 0; i < unmount_segments.size(); ++i) {
            MasterMetricManager::instance().dec_total_mem_capacity(
                segment_names[i], dec_capacities[i]);
        }
    }
}

void CentralizedClientManager::OnClientRecovered(const UUID& client_id) {
    // In centralized mode, recovery only happens after re-registration.
    LOG(INFO) << "client_id=" << client_id
              << ", action=centralized_client_recovered";
    MasterMetricManager::instance().inc_active_clients();
}

HeartbeatTaskResult CentralizedClientManager::ProcessTask(
    const UUID& client_id, const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type();
    result.error = ErrorCode::NOT_IMPLEMENTED;

    return result;
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientManager::GetAllSegments() {
    std::vector<std::string> all_segments;
    ErrorCode err = segment_manager_->GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to get all segments" << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return all_segments;
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
CentralizedClientManager::QuerySegments(const std::string& segment) {
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

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientManager::QueryIp(const UUID& client_id) {
    std::vector<std::string> result;
    ErrorCode err = segment_manager_->QueryIp(client_id, result);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query ip" << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return result;
}

}  // namespace mooncake
