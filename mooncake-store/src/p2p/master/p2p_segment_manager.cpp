#include "p2p/master/p2p_segment_manager.h"

#include <glog/logging.h>

#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {
namespace {

void AddSegmentMetrics(const P2PSegment& segment) {
    auto& metrics = P2PMasterMetricManager::instance();
    if (segment.memory_type == MemoryType::NVME) {
        metrics.inc_total_file_capacity(segment.size);
        metrics.inc_allocated_file_size(segment.usage);
        return;
    }
    if (segment.memory_type != MemoryType::DRAM) {
        LOG(WARNING) << "mounting segment with unsupported memory type, "
                        "counting toward mem capacity"
                     << ", segment_id=" << segment.id
                     << ", name=" << segment.name
                     << ", memory_type="
                     << MemoryTypeToString(segment.memory_type);
    }
    metrics.inc_total_mem_capacity(segment.name, segment.size);
    metrics.inc_allocated_mem_size(segment.name, segment.usage);
}

void RemoveSegmentMetrics(const P2PSegment& segment) {
    auto& metrics = P2PMasterMetricManager::instance();
    if (segment.memory_type == MemoryType::NVME) {
        metrics.dec_total_file_capacity(segment.size);
        metrics.dec_allocated_file_size(segment.usage);
        return;
    }
    if (segment.memory_type != MemoryType::DRAM) {
        LOG(WARNING) << "unmounting segment with unsupported memory type, "
                        "counting toward mem capacity"
                     << ", segment_id=" << segment.id
                     << ", name=" << segment.name
                     << ", memory_type="
                     << MemoryTypeToString(segment.memory_type);
    }
    metrics.dec_total_mem_capacity(segment.name, segment.size);
    metrics.dec_allocated_mem_size(segment.name, segment.usage);
}

}  // namespace

auto P2PSegmentManager::MountSegment(const P2PSegment& segment)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&segment_mutex_);
    if (mounted_segments_.contains(segment.id)) {
        LOG(WARNING) << "segment_name=" << segment.name
                     << ", warn=segment_already_exists";
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }
    mounted_segments_.emplace(segment.id, segment);
    total_capacity_ += segment.size;
    total_usage_ += segment.usage;
    AddSegmentMetrics(segment);
    return {};
}

auto P2PSegmentManager::UnmountSegment(const UUID& segment_id)
    -> tl::expected<P2PSegment, ErrorCode> {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "attempt to unmount segment but it does not exist"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    P2PSegment removed = std::move(it->second);
    if (removed.size > total_capacity_ || removed.usage > total_usage_) {
        LOG(ERROR) << "segment aggregate underflow during unmount"
                   << ", segment_id=" << segment_id
                   << ", total_capacity=" << total_capacity_
                   << ", segment_size=" << removed.size
                   << ", total_usage=" << total_usage_
                   << ", segment_usage=" << removed.usage;
        total_capacity_ = 0;
        total_usage_ = 0;
    } else {
        total_capacity_ -= removed.size;
        total_usage_ -= removed.usage;
    }
    RemoveSegmentMetrics(removed);
    mounted_segments_.erase(it);
    return removed;
}

auto P2PSegmentManager::QuerySegments(const std::string& segment_name) const
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    for (const auto& [id, segment] : mounted_segments_) {
        if (segment.name == segment_name) {
            return std::make_pair(segment.usage, segment.size);
        }
    }
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

auto P2PSegmentManager::QuerySegment(const UUID& segment_id) const
    -> tl::expected<P2PSegment, ErrorCode> {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "QuerySegment: segment not found"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return it->second;
}

std::vector<P2PSegment> P2PSegmentManager::GetSegments() const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    std::vector<P2PSegment> segments;
    segments.reserve(mounted_segments_.size());
    for (const auto& [id, segment] : mounted_segments_) {
        segments.push_back(segment);
    }
    return segments;
}

auto P2PSegmentManager::UpdateSegmentUsage(const UUID& segment_id,
                                            size_t usage)
    -> tl::expected<size_t, ErrorCode> {
    SharedMutexLocker lock(&segment_mutex_);
    auto it = mounted_segments_.find(segment_id);
    if (it == mounted_segments_.end()) {
        LOG(WARNING) << "fail to update segment usage, segment doesn't exist"
                     << ", segment_id=" << segment_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (usage > it->second.size) {
        LOG(ERROR) << "usage is larger than segment size"
                   << ", segment_id=" << segment_id << ", usage=" << usage
                   << ", segment_size=" << it->second.size;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const size_t old_usage = it->second.usage;
    it->second.usage = usage;
    total_usage_ = total_usage_ - old_usage + usage;

    const int64_t delta = usage >= old_usage
                              ? static_cast<int64_t>(usage - old_usage)
                              : -static_cast<int64_t>(old_usage - usage);
    auto& metrics = P2PMasterMetricManager::instance();
    if (it->second.memory_type == MemoryType::NVME) {
        if (delta >= 0) {
            metrics.inc_allocated_file_size(delta);
        } else {
            metrics.dec_allocated_file_size(-delta);
        }
    } else if (delta >= 0) {
        metrics.inc_allocated_mem_size(it->second.name, delta);
    } else {
        metrics.dec_allocated_mem_size(it->second.name, -delta);
    }
    return old_usage;
}

std::pair<size_t, size_t> P2PSegmentManager::GetCapacityUsage() const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    return {total_capacity_, total_usage_};
}

}  // namespace mooncake
