#pragma once

#include "allocation_strategy.h"
#include "allocator.h"
#include "segment_manager.h"
#include <boost/functional/hash.hpp>

namespace mooncake {
/**
 * @brief Status of a mounted segment in master
 */
enum class SegmentStatus {
    UNDEFINED = 0,  // Uninitialized
    OK,             // Segment is mounted and available for allocation
    UNMOUNTING,     // Segment is under unmounting
};

/**
 * @brief Stream operator for SegmentStatus
 */
inline std::ostream& operator<<(std::ostream& os,
                                const SegmentStatus& status) noexcept {
    static const std::unordered_map<SegmentStatus, std::string_view>
        status_strings{{SegmentStatus::UNDEFINED, "UNDEFINED"},
                       {SegmentStatus::OK, "OK"},
                       {SegmentStatus::UNMOUNTING, "UNMOUNTING"}};

    os << (status_strings.count(status) ? status_strings.at(status)
                                        : "UNKNOWN");
    return os;
}

struct MountedCentralizedSegment : public Segment {
    SegmentStatus status;
    std::shared_ptr<BufferAllocatorBase> buf_allocator;
};

struct LocalDiskSegment {
    mutable Mutex offloading_mutex_;
    bool enable_offloading;
    std::unordered_map<std::string, int64_t> GUARDED_BY(offloading_mutex_)
        offloading_objects;
    explicit LocalDiskSegment(bool enable_offloading)
        : enable_offloading(enable_offloading) {}

    LocalDiskSegment(const LocalDiskSegment&) = delete;
    LocalDiskSegment& operator=(const LocalDiskSegment&) = delete;

    LocalDiskSegment(LocalDiskSegment&&) = delete;
    LocalDiskSegment& operator=(LocalDiskSegment&&) = delete;
};

class CentralizedSegmentManager : public SegmentManager {
   public:
    /**
     * @brief Constructor for CentralizedSegmentManager
     * @param memory_allocator Type of buffer allocator to use for new segments
     */
    explicit CentralizedSegmentManager(
        BufferAllocatorType memory_allocator = BufferAllocatorType::OFFSET)
        : allocation_strategy_(std::make_shared<RandomAllocationStrategy>()),
          memory_allocator_(memory_allocator) {}

    // MountSegment, UnmountSegment, QuerySegments are inherited from
    // SegmentManager

    ErrorCode BatchUnmountSegments(
        const std::vector<UUID>& unmount_segments,
        const std::vector<UUID>& client_ids,
        const std::vector<std::string>& segment_names);

    // ... PrepareUnmountSegment ...

   protected:
    ErrorCode InnerCheckMountSegment(const Segment& segment,
                                     const UUID& client_id);
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

    ErrorCode InnerPrepareUnmountSegment(
        MountedCentralizedSegment& mounted_segment);

   private:
    static constexpr size_t OFFLOADING_QUEUE_LIMIT = 50000;

    // segment_mutex_ inherited

    std::shared_ptr<AllocationStrategy> allocation_strategy_;
    const BufferAllocatorType
        memory_allocator_;  // Type of buffer allocator to use
    // allocator_manager_ only contains allocators whose segment status is OK.
    AllocatorManager allocator_manager_;
    std::unordered_map<std::string, UUID> client_by_name_
        GUARDED_BY(segment_mutex_);  // segment name -> client_id
    std::unordered_map<UUID, std::shared_ptr<LocalDiskSegment>,
                       boost::hash<UUID>>
        client_local_disk_segment_
            GUARDED_BY(segment_mutex_);  // client_id -> local_disk_segment

    std::unordered_map<UUID, std::shared_ptr<Segment>, boost::hash<UUID>>
        mounted_segments_
            GUARDED_BY(segment_mutex_);  // segment_id -> mounted segment

    friend class SegmentTest;  // for unit tests
};

}  // namespace mooncake