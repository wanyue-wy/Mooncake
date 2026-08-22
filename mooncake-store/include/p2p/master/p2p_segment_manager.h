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
#include "types.h"

namespace mooncake {

/**
 * @brief Manages the segments mounted by a single P2P client.
 *
 * Standalone class: the generic mount/unmount/query bookkeeping formerly
 * provided by the SegmentManager base class has been absorbed here; the
 * class no longer participates in any inheritance hierarchy.
 */
class P2PSegmentManager {
   public:
    auto MountSegment(const Segment& segment) -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;
    // TODO: wanyue-wy
    // There is currently no mechanism to guarantee `segment_name`'s
    // uniqueness within the cluster.
    // For backward compatibility during refactoring, we temporarily maintain
    // a weak assumption of `segment_name`'s uniqueness.
    // However, before merging to main, we need to discuss whether name
    // uniqueness is necessary and whether the query interface definition
    // should be modified.
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& segment_id)
        -> tl::expected<std::shared_ptr<Segment>, ErrorCode>;
    auto GetSegments() -> tl::expected<std::vector<Segment>, ErrorCode>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

    using OnSegmentAddedCallback = std::function<void(const Segment& segment)>;
    using OnSegmentRemovedCallback =
        std::function<void(const Segment& segment)>;

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
    using SegmentVisitor = std::function<bool(const Segment& segment)>;
    void ForEachSegment(const SegmentVisitor& visitor) const;

   private:
    // Implementation-specific mount/unmount steps (no locking required;
    // called while holding segment_mutex_).
    tl::expected<void, ErrorCode> InnerMountSegment(const Segment& segment);

    tl::expected<void, ErrorCode> OnUnmountSegment(
        const std::shared_ptr<Segment>& segment);

   private:
    OnSegmentAddedCallback on_segment_added_;
    OnSegmentRemovedCallback on_segment_removed_;

    mutable SharedMutex segment_mutex_;
    SegmentRemovalCallback segment_removal_cb_;

    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment
};

}  // namespace mooncake
