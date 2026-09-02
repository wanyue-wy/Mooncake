#include "p2p/master/p2p_client_meta.h"

#include <algorithm>
#include <limits>

#include <glog/logging.h>

#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

P2PClientMeta::P2PClientMeta(const UUID& client_id, std::string ip_address,
                             uint16_t rpc_port,
                             int64_t disconnect_timeout_sec,
                             int64_t crash_timeout_sec)
    : client_id_(client_id),
      ip_address_(std::move(ip_address)),
      rpc_port_(rpc_port),
      disconnect_timeout_sec_(disconnect_timeout_sec),
      crash_timeout_sec_(crash_timeout_sec) {
    health_state_.status = P2PClientStatus::HEALTHY;
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
}

P2PClientMeta::~P2PClientMeta() {
    if (registered_.load(std::memory_order_acquire)) {
        P2PMasterMetricManager::instance().OnClientRemoved(client_id_);
    }
}

auto P2PClientMeta::MountSegment(const P2PSegment& segment)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto status = InnerStatusCheck();
    if (!status.has_value()) {
        return status;
    }
    auto result = segment_manager_.MountSegment(segment);
    if (!result.has_value() &&
        result.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        LOG(WARNING) << "attempt to mount segment but it already exists"
                     << ", client_id=" << client_id_
                     << ", segment_id=" << segment.id;
        return {};
    }
    if (!result.has_value()) {
        LOG(ERROR) << "fail to mount segment"
                   << ", client_id=" << client_id_
                   << ", segment_id=" << segment.id
                   << ", error=" << result.error();
    }
    return result;
}

auto P2PClientMeta::UnmountSegment(const UUID& segment_id)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto status = InnerStatusCheck();
    if (!status.has_value()) {
        return status;
    }
    auto result = segment_manager_.UnmountSegment(segment_id);
    if (!result.has_value() && result.error() == ErrorCode::SEGMENT_NOT_FOUND) {
        LOG(WARNING) << "attempt to unmount segment but it does not exist"
                     << ", client_id=" << client_id_
                     << ", segment_id=" << segment_id;
        return {};
    }
    if (!result.has_value()) {
        LOG(ERROR) << "fail to unmount segment"
                   << ", client_id=" << client_id_
                   << ", segment_id=" << segment_id
                   << ", error=" << result.error();
        return tl::make_unexpected(result.error());
    }
    return {};
}

std::vector<P2PSegment> P2PClientMeta::GetSegments() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return segment_manager_.GetSegments();
}

auto P2PClientMeta::QuerySegments(const std::string& segment_name) const
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto status = InnerStatusCheck();
    if (!status.has_value()) {
        return tl::make_unexpected(status.error());
    }
    return segment_manager_.QuerySegments(segment_name);
}

auto P2PClientMeta::QuerySegment(const UUID& segment_id) const
    -> tl::expected<P2PSegment, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto status = InnerStatusCheck();
    if (!status.has_value()) {
        return tl::make_unexpected(status.error());
    }
    return segment_manager_.QuerySegment(segment_id);
}

P2PClientHealthState P2PClientMeta::get_health_state() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return health_state_;
}

bool P2PClientMeta::is_health() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return health_state_.status == P2PClientStatus::HEALTHY;
}

std::pair<P2PClientStatus, P2PClientStatus> P2PClientMeta::Heartbeat() {
    SharedMutexLocker lock(&client_mutex_);
    InnerUpdateHeartbeat();
    return InnerUpdateHealthStatus();
}

std::pair<P2PClientStatus, P2PClientStatus> P2PClientMeta::CheckHealth() {
    SharedMutexLocker lock(&client_mutex_);
    return InnerUpdateHealthStatus();
}

void P2PClientMeta::InnerUpdateHeartbeat() {
    if (health_state_.status == P2PClientStatus::CRASHED) {
        LOG(WARNING) << "heartbeat received for crashed client"
                     << ", client_id=" << client_id_;
        return;
    }
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
}

std::pair<P2PClientStatus, P2PClientStatus>
P2PClientMeta::InnerUpdateHealthStatus() {
    const auto now = std::chrono::steady_clock::now();
    const auto old_status = health_state_.status;
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - health_state_.last_heartbeat)
            .count();
    const int64_t disconnect_timeout_ms = disconnect_timeout_sec_ * 1000;
    const int64_t crash_timeout_ms = crash_timeout_sec_ * 1000;

    switch (health_state_.status) {
        case P2PClientStatus::HEALTHY:
            if (elapsed_ms >= crash_timeout_ms) {
                health_state_.status = P2PClientStatus::CRASHED;
            } else if (elapsed_ms >= disconnect_timeout_ms) {
                health_state_.status = P2PClientStatus::DISCONNECTED;
            }
            break;
        case P2PClientStatus::DISCONNECTED:
            if (elapsed_ms < disconnect_timeout_ms) {
                health_state_.status = P2PClientStatus::HEALTHY;
            } else if (elapsed_ms >= crash_timeout_ms) {
                health_state_.status = P2PClientStatus::CRASHED;
            }
            break;
        case P2PClientStatus::CRASHED:
        case P2PClientStatus::UNREGISTERED:
            break;
    }
    if (health_state_.status != old_status) {
        LOG(INFO) << "Client status changed"
                  << ", client_id=" << client_id_
                  << ", old_status=" << HealthToString(old_status)
                  << ", new_status="
                  << HealthToString(health_state_.status);
    }
    return {old_status, health_state_.status};
}

auto P2PClientMeta::InnerStatusCheck() const
    -> tl::expected<void, ErrorCode> {
    if (health_state_.status != P2PClientStatus::HEALTHY) {
        return tl::make_unexpected(ErrorCode::CLIENT_UNHEALTHY);
    }
    return {};
}

std::vector<P2PRouteLocation> P2PClientMeta::RecycleSegments() {
    if (recycled_.exchange(true, std::memory_order_acq_rel)) {
        return {};
    }
    LOG(INFO) << "start to recycle client segments"
              << ", client_id=" << client_id_;

    std::vector<P2PRouteLocation> removed_locations;
    // The client lock protects the lifecycle decision. Segment removals are
    // collected here, but route cleanup is deliberately left to the caller
    // after this lock has been released.
    {
        SharedMutexLocker lock(&client_mutex_, shared_lock);
        auto segments = segment_manager_.GetSegments();
        removed_locations.reserve(segments.size());
        for (const auto& segment : segments) {
            auto removed = segment_manager_.UnmountSegment(segment.id);
            if (!removed.has_value()) {
                LOG(ERROR) << "Failed to recycle segment"
                           << ", client_id=" << client_id_
                           << ", segment_id=" << segment.id
                           << ", error=" << removed.error();
                continue;
            }
            removed_locations.push_back(P2PRouteLocation{
                .client_id = client_id_, .segment_id = segment.id});
        }
    }
    return removed_locations;
}

std::string P2PClientMeta::HealthToString(P2PClientStatus status) {
    switch (status) {
        case P2PClientStatus::HEALTHY:
            return "HEALTHY";
        case P2PClientStatus::DISCONNECTED:
            return "DISCONNECTED";
        case P2PClientStatus::CRASHED:
            return "CRASHED";
        case P2PClientStatus::UNREGISTERED:
            return "UNREGISTERED";
    }
    return "UNKNOWN";
}

auto P2PClientMeta::QueryIp() const
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto status = InnerStatusCheck();
    if (!status.has_value()) {
        return tl::make_unexpected(status.error());
    }
    return std::vector<std::string>{ip_address_};
}

SyncSegmentMetaResult P2PClientMeta::UpdateSegmentUsages(
    const std::vector<TierUsageInfo>& usages) {
    SyncSegmentMetaResult result;
    for (const auto& usage : usages) {
        SyncSegmentMetaResult::SubResult sub_result;
        sub_result.segment_id = usage.segment_id;
        auto old_usage =
            segment_manager_.UpdateSegmentUsage(usage.segment_id, usage.usage);
        sub_result.error = old_usage.has_value() ? ErrorCode::OK
                                                 : old_usage.error();
        if (!old_usage.has_value()) {
            LOG(WARNING) << "fail to update segment usage"
                         << ", client_id=" << client_id_
                         << ", segment_id=" << usage.segment_id
                         << ", usage=" << usage.usage
                         << ", error=" << old_usage.error();
        }
        result.sub_results.push_back(sub_result);
    }
    return result;
}

size_t P2PClientMeta::GetAvailableCapacity() const {
    const auto [capacity, usage] = segment_manager_.GetCapacityUsage();
    return capacity > usage ? capacity - usage : 0;
}

P2PClientMeta::CapacityStat P2PClientMeta::GetWriteScoreCapacity(
    const std::vector<std::string>& tag_filters, int priority_limit,
    bool top_tier_only) const {
    auto eligible = [&](const P2PSegment& segment) {
        if (segment.priority < priority_limit) {
            return false;
        }
        return std::none_of(tag_filters.begin(), tag_filters.end(),
                            [&](const std::string& tag) {
                                return std::find(segment.tags.begin(),
                                                 segment.tags.end(),
                                                 tag) != segment.tags.end();
                            });
    };

    CapacityStat all;
    CapacityStat top;
    int max_priority = std::numeric_limits<int>::min();
    for (const auto& segment : segment_manager_.GetSegments()) {
        if (!eligible(segment)) {
            continue;
        }
        const size_t free =
            segment.size > segment.usage ? segment.size - segment.usage : 0;
        all.total += segment.size;
        all.free += free;
        if (segment.priority > max_priority) {
            max_priority = segment.priority;
            top = {free, segment.size};
        } else if (segment.priority == max_priority) {
            top.total += segment.size;
            top.free += free;
        }
    }
    return top_tier_only ? top : all;
}

std::optional<WriteCandidate> P2PClientMeta::GetWriteRouteCandidate(
    const WriteRouteRequest& req) const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (!InnerStatusCheck().has_value()) {
        return std::nullopt;
    }
    const auto capacity = GetWriteScoreCapacity(
        req.config.tag_filters, req.config.priority_limit,
        req.config.top_tier_only);
    if (capacity.total == 0 || capacity.free < req.size) {
        return std::nullopt;
    }

    WriteCandidate candidate;
    candidate.client_id = client_id_;
    candidate.ip_address = ip_address_;
    candidate.rpc_port = rpc_port_;
    candidate.available_capacity = capacity.free;
    candidate.score = static_cast<double>(capacity.free) / capacity.total;
    return candidate;
}

}  // namespace mooncake
