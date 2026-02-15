#include "segment_manager.h"

#include <glog/logging.h>

namespace mooncake {

ErrorCode SegmentManager::MountSegment(const Segment& segment,
                                       const UUID& client_id) {
    SharedMutexLocker lock(&segment_mutex_);
    return InnerMountSegment(segment, client_id);
}

ErrorCode SegmentManager::UnmountSegment(const UUID& segment_id,
                                         const UUID& client_id) {
    SharedMutexLocker lock(&segment_mutex_);
    return InnerUnmountSegment(segment_id, client_id);
}

ErrorCode SegmentManager::QuerySegments(const std::string& segment,
                                        size_t& used, size_t& capacity) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    return InnerQuerySegments(segment, used, capacity);
}

ErrorCode SegmentManager::GetAllSegments(
    std::vector<std::string>& all_segments) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    return InnerGetAllSegments(all_segments);
}

ErrorCode SegmentManager::QueryIp(const UUID& client_id,
                                  std::vector<std::string>& result) {
    SharedMutexLocker lock(&segment_mutex_, shared_lock);
    return InnerQueryIp(client_id, result);
}

}  // namespace mooncake
