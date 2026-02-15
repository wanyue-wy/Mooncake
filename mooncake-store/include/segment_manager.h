#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "mutex.h"
#include "types.h"
#include "ylt/util/tl/expected.hpp"

namespace mooncake {

class SegmentManager {
   public:
    virtual ~SegmentManager() = default;

    // Public interfaces (Thread-safe, logic in base class)
    ErrorCode MountSegment(const Segment& segment, const UUID& client_id);
    ErrorCode UnmountSegment(const UUID& segment_id, const UUID& client_id);
    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);
    ErrorCode QueryIp(const UUID& client_id, std::vector<std::string>& result);

   protected:
    // Pure virtual inner methods (Implementation specific, No locking required)
    virtual ErrorCode InnerMountSegment(const Segment& segment,
                                        const UUID& client_id) = 0;
    virtual ErrorCode InnerUnmountSegment(const UUID& segment_id,
                                          const UUID& client_id) = 0;
    virtual ErrorCode InnerQuerySegments(const std::string& segment,
                                         size_t& used, size_t& capacity) = 0;
    virtual ErrorCode InnerGetAllSegments(
        std::vector<std::string>& all_segments) = 0;
    virtual ErrorCode InnerQueryIp(const UUID& client_id,
                                   std::vector<std::string>& result) = 0;

    mutable SharedMutex segment_mutex_;
};

}  // namespace mooncake
