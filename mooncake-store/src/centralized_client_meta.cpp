#include "centralized_client_meta.h"

#include <glog/logging.h>

#include "master_metric_manager.h"

namespace mooncake {
CentralizedClientMeta::CentralizedClientMeta(const UUID& client_id,
                                             BufferAllocatorType allocator_type)
    : ClientMeta(client_id) {
    segment_manager_ =
        std::make_shared<CentralizedSegmentManager>(allocator_type);
}

std::shared_ptr<SegmentManager> CentralizedClientMeta::GetSegmentManager() {
    return segment_manager_;
}
std::shared_ptr<CentralizedSegmentManager>
CentralizedClientMeta::GetCentralizedSegmentManager() {
    return segment_manager_;
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientMeta::QueryIp(const UUID& client_id) {
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_->QueryIp();
}

tl::expected<void, ErrorCode> CentralizedClientMeta::MountLocalDiskSegment(
    bool enable_offloading) {
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }
    return segment_manager_->MountLocalDiskSegment(enable_offloading);
}

auto CentralizedClientMeta::OffloadObjectHeartbeat(bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_->OffloadObjectHeartbeat(enable_offloading);
}

tl::expected<void, ErrorCode> CentralizedClientMeta::PushOffloadingQueue(
    const std::string& key, const int64_t size,
    const std::string& segment_name) {
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }
    return segment_manager_->PushOffloadingQueue(key, size, segment_name);
}

void CentralizedClientMeta::OnDisconnected() {
    SharedMutexLocker lock(&health_mutex_, shared_lock);
    if (health_state_.status == ClientStatus::HEALTH) {
        // concurrent heartbeat might success, just do nothing
        return;
    } else if (health_state_.status != ClientStatus::DISCONNECTION) {
        LOG(ERROR) << "unexpected hook calling" << ", client_id=" << client_id_
                   << ", current status=" << (int)health_state_.status
                   << ", expected status=" << (int)ClientStatus::DISCONNECTION;
        return;
    } else {
        LOG(INFO) << "the client is diconnected, hide allocators"
                  << ", client_id=" << client_id_;
    }
    // Hide allocators from global view to prevent new allocations
    auto ret = segment_manager_->SetGlobalVisibility(false);
    if (!ret.has_value()) {
        LOG(ERROR) << "Failed to hide allocators for client " << client_id_
                   << " error=" << ret.error();
    }
    MasterMetricManager::instance().dec_active_clients();
}

void CentralizedClientMeta::OnRecovered() {
    SharedMutexLocker lock(&health_mutex_, shared_lock);
    if (health_state_.status != ClientStatus::HEALTH) {
        LOG(ERROR) << "unexpected hook calling" << ", client_id=" << client_id_
                   << ", current status=" << (int)health_state_.status
                   << ", expected status=" << (int)ClientStatus::DISCONNECTION;
        return;
    } else {
        LOG(INFO) << "the client is recovered, show allocators"
                  << ", client_id=" << client_id_;
    }

    // Restore allocators to global view
    auto ret = segment_manager_->SetGlobalVisibility(true);
    if (!ret.has_value()) {
        LOG(ERROR) << "Failed to show allocators for client " << client_id_
                   << " error=" << ret.error();
    }
    MasterMetricManager::instance().inc_active_clients();
}

void CentralizedClientMeta::OnCrashed() {
    LOG(INFO) << "the client is crashed, start to recycle meta"
              << ", client_id=" << client_id_;
    SharedMutexLocker lock(&health_mutex_, shared_lock);
    auto segments_res = segment_manager_->GetSegments();
    if (segments_res) {
        for (const auto& seg : *segments_res) {
            auto ret = segment_manager_->UnmountSegment(seg.id);
            if (!ret.has_value()) {
                LOG(ERROR) << "Failed to unmount segment"
                           << ", client_id=" << client_id_
                           << ", segment_id=" << seg.id
                           << " error=" << ret.error();
            }
        }
    }
    LOG(INFO) << "the client meta is recycled over"
              << ", client_id=" << client_id_;
}

}  // namespace mooncake
