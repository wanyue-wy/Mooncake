#include "p2p_segment_manager.h"
#include <glog/logging.h>
#include <unordered_set>

namespace mooncake {

ErrorCode P2PSegmentManager::InnerMountSegment(const Segment& segment,
                                               const UUID& client_id) {
    if (!segment.is_p2p()) {
        LOG(ERROR) << "segment is not p2p";
        return ErrorCode::INVALID_PARAMS;
    }

    if (mounted_segments_.count(segment.id)) {
        LOG(WARNING) << "segment " << segment.id << " already exists";
        return ErrorCode::SEGMENT_ALREADY_EXISTS;
    }

    auto new_segment = std::make_shared<Segment>(segment);
    mounted_segments_[segment.id] = new_segment;
    // client_segments_ removed logic

    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::InnerUnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id) {
    if (!mounted_segments_.count(segment_id)) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    }

    mounted_segments_.erase(segment_id);
    return ErrorCode::OK;
}

// GetClientSegments and GetAllSegments are inherited from SegmentManager

ErrorCode P2PSegmentManager::InnerQuerySegments(const std::string& segment_name,
                                                size_t& used,
                                                size_t& capacity) {
    bool found = false;
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment_name) {
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

ErrorCode P2PSegmentManager::InnerGetAllSegments(
    std::vector<std::string>& all_segments) {
    std::unordered_set<std::string> unique_names;
    for (const auto& entry : mounted_segments_) {
        unique_names.insert(entry.second->name);
    }
    all_segments.assign(unique_names.begin(), unique_names.end());
    return ErrorCode::OK;
}

ErrorCode P2PSegmentManager::InnerQueryIp(const UUID& client_id,
                                          std::vector<std::string>& result) {
    return ErrorCode::NOT_IMPLEMENTED;
}

}  // namespace mooncake
