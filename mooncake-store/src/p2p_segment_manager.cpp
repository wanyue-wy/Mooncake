#include "p2p_segment_manager.h"
#include <glog/logging.h>
#include <unordered_set>

namespace mooncake {

ErrorCode P2PSegmentManager::MountSegment(const Segment& segment,
                                          const UUID& client_id) {
    if (!segment.is_p2p()) {
        LOG(ERROR) << "segment is not p2p";
        return ErrorCode::INVALID_PARAMS;
    }

    SharedMutexLocker lock(&segment_mutex_);
    if (mounted_segments_.count(segment.id)) {
        LOG(WARNING) << "segment " << segment.id << " already exists";
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }

    auto new_segment = std::make_shared<Segment>(segment);
    mounted_segments_[segment.id] = new_segment;
    client_segments_[client_id].push_back(segment.id);

    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::UnmountSegment(const UUID& segment_id,
                                            const UUID& client_id) {
    SharedMutexLocker lock(&segment_mutex_);
    if (!mounted_segments_.count(segment_id)) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    mounted_segments_.erase(segment_id);

    auto it = client_segments_.find(client_id);
    if (it != client_segments_.end()) {
        auto& segments = it->second;
        segments.erase(
            std::remove(segments.begin(), segments.end(), segment_id),
            segments.end());
        if (segments.empty()) {
            client_segments_.erase(it);
        }
    }
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::GetClientSegments(
    const UUID& client_id,
    std::vector<std::shared_ptr<Segment>>& segments) const {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    auto it = client_segments_.find(client_id);
    if (it == client_segments_.end()) {
        return ErrorCode::CLIENT_NOT_FOUND;
    }

    for (const auto& segment_id : it->second) {
        auto segment_it = mounted_segments_.find(segment_id);
        if (segment_it != mounted_segments_.end()) {
            segments.push_back(segment_it->second);
        }
    }
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::QuerySegments(const std::string& segment_name,
                                           size_t& used, size_t& capacity) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    bool found = false;
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment_name) {
            // For P2P segments, we currently only track capacity (size).
            // Used size tracking would require periodic updates from clients.
            capacity += entry.second->size;
            found = true;
        }
    }

    if (!found) {
        VLOG(1) << "Segment " << segment_name << " not found!";
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::GetAllSegments(
    std::vector<std::string>& all_segments) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    std::unordered_set<std::string> unique_names;
    for (const auto& entry : mounted_segments_) {
        unique_names.insert(entry.second->name);
    }
    all_segments.assign(unique_names.begin(), unique_names.end());
    return ErrorCode::OK;
}

}  // namespace mooncake
