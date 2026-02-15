#pragma once

#include <boost/functional/hash.hpp>
#include "mutex.h"
#include "segment_manager.h"
#include "types.h"

namespace mooncake {
// TODO: this class is a tmp placeholder. it will be implemented later
class P2PSegmentManager : public SegmentManager {
   public:
    // MountSegment, UnmountSegment, QuerySegments are inherited
    // GetAllSegments is inherited
    // QueryIp is inherited

   protected:
    ErrorCode InnerMountSegment(const Segment& segment,
                                const UUID& client_id) override;

    ErrorCode InnerUnmountSegment(const UUID& segment_id,
                                  const UUID& client_id) override;

    ErrorCode InnerQuerySegments(const std::string& segment, size_t& used,
                                 size_t& capacity) override;
    ErrorCode InnerGetAllSegments(
        std::vector<std::string>& all_segments) override;
    ErrorCode InnerQueryIp(const UUID& client_id,
                           std::vector<std::string>& result) override;

   private:
    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment
};

}  // namespace mooncake