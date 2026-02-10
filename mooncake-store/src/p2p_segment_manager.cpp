#include "p2p_segment_manager.h"
#include <glog/logging.h>

namespace mooncake {

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PSegmentManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock_(&segment_mutex_, shared_lock);
    bool found = false;
    size_t capacity = 0;
    size_t used = 0;
    for (const auto& entry : mounted_segments_) {
        if (entry.second->name == segment) {
            capacity += entry.second->size;
            found = true;
            break;
        }
    }

    if (!found) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

    return std::make_pair(used, capacity);
}

tl::expected<void, ErrorCode> P2PSegmentManager::InnerMountSegment(
    const Segment& segment) {
    if (!segment.IsP2PSegment()) {
        LOG(ERROR) << "segment is not p2p";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (mounted_segments_.count(segment.id)) {
        LOG(WARNING) << "segment " << segment.id << " already exists";
        return tl::make_unexpected(ErrorCode::SEGMENT_ALREADY_EXISTS);
    }

    auto new_segment = std::make_shared<Segment>(segment);
    mounted_segments_[new_segment->id] = new_segment;

    return {};
}

}  // namespace mooncake
