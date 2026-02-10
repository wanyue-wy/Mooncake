#pragma once

#include <boost/functional/hash.hpp>
#include "mutex.h"
#include "segment_manager.h"
#include "types.h"

namespace mooncake {
class P2PSegmentManager : public SegmentManager {
   public:
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> override;

   protected:
    tl::expected<void, ErrorCode> InnerMountSegment(
        const Segment& segment) override;

    tl::expected<void, ErrorCode> OnUnmountSegment(
        const std::shared_ptr<Segment>& segment) override {
        return {};
    };

   private:
    std::unordered_map<UUID, std::vector<UUID>, boost::hash<UUID>>
        client_segments_
            GUARDED_BY(segment_mutex_);  // client_id -> vector<segment_id>
};

}  // namespace mooncake