#pragma once

#include <boost/functional/hash.hpp>
#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/common/p2p_types.h"

namespace mooncake {

/**
 * @brief Owns one client's mounted segment snapshots and capacity aggregate.
 */
class P2PSegmentManager final {
   public:
    auto MountSegment(const P2PSegment& segment)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<P2PSegment, ErrorCode>;
    auto QuerySegments(const std::string& segment_name) const
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& segment_id) const
        -> tl::expected<P2PSegment, ErrorCode>;
    std::vector<P2PSegment> GetSegments() const;

    /** Update segment usage and return the old usage. */
    auto UpdateSegmentUsage(const UUID& segment_id, size_t usage)
        -> tl::expected<size_t, ErrorCode>;

    std::pair<size_t, size_t> GetCapacityUsage() const;

   private:
    mutable SharedMutex segment_mutex_;
    std::unordered_map<UUID, P2PSegment, boost::hash<UUID>> mounted_segments_
        GUARDED_BY(segment_mutex_);
    size_t total_capacity_ GUARDED_BY(segment_mutex_){0};
    size_t total_usage_ GUARDED_BY(segment_mutex_){0};
};

}  // namespace mooncake
