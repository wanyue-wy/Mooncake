#pragma once

#include <boost/functional/hash.hpp>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/common/p2p_types.h"

namespace mooncake {

/**
 * @brief Manages the segments mounted by a single P2P client.
 */
class P2PSegmentManager {
   public:
    auto MountSegment(const P2PSegment& segment)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;
    // Segment names are not guaranteed to be unique within the cluster.
    // Returns the first matching segment.
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& segment_id)
        -> tl::expected<std::shared_ptr<P2PSegment>, ErrorCode>;
    auto GetSegments() -> tl::expected<std::vector<P2PSegment>, ErrorCode>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

    using OnSegmentAddedCallback =
        std::function<void(const P2PSegment& segment)>;
    using OnSegmentRemovedCallback =
        std::function<void(const P2PSegment& segment)>;

    void SetSegmentChangeCallbacks(OnSegmentAddedCallback on_add,
                                   OnSegmentRemovedCallback on_remove) {
        on_segment_added_ = std::move(on_add);
        on_segment_removed_ = std::move(on_remove);
    }
    /**
     * @brief update segment usage and return old usage
     */
    tl::expected<size_t, ErrorCode> UpdateSegmentUsage(const UUID& segment_id,
                                                       size_t usage);

    /**
     * @brief get segment usage
     */
    size_t GetSegmentUsage(const UUID& segment_id) const;

    /**
     * @brief Iterate over all mounted P2P segments under a single read lock.
     *        Visitor returns true to stop early.
     */
    using SegmentVisitor = std::function<bool(const P2PSegment& segment)>;
    void ForEachSegment(const SegmentVisitor& visitor) const;

   private:
    OnSegmentAddedCallback on_segment_added_;
    OnSegmentRemovedCallback on_segment_removed_;

    mutable SharedMutex segment_mutex_;
    SegmentRemovalCallback segment_removal_cb_;

    std::unordered_map<UUID, std::shared_ptr<P2PSegment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment
};

}  // namespace mooncake
