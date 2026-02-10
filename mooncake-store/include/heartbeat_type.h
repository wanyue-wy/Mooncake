#pragma once

#include <string>
#include <variant>
#include <vector>

#include "types.h"

namespace mooncake {

// =====================================================================
// Heartbeat Task Types
// =====================================================================

/**
 * @brief Types of tasks that can be carried in a heartbeat request.
 */
enum class HeartbeatTaskType {
    REMOUNT_SEGMENTS,   // Crash recovery: re-register segments
    SYNC_SEGMENT_META,  // Sync segment usage metadata
};

// =====================================================================
// Heartbeat Task Params (variant pattern, similar to Replica)
// =====================================================================

/**
 * @brief Usage info for a single tier/segment.
 */
struct TierUsageInfo {
    UUID segment_id;
    size_t usage = 0;
};

/**
 * @brief Route info for a key's replica, used during REMOUNT_SEGMENTS.
 */
struct KeyReplicaRoute {
    std::string key;
    UUID segment_id;
    size_t value_size = 0;
};

/**
 * @brief Param for REMOUNT_SEGMENTS task.
 * Centralized: only segments. P2P: segments + key replica routes.
 */
struct RemountSegmentsParam {
    std::vector<Segment> segments;
    std::optional<std::vector<KeyReplicaRoute>> key_replica_routes;  // P2P only
};

/**
 * @brief Param for SYNC_SEGMENT_META task.
 */
struct SyncSegmentMetaParam {
    std::vector<TierUsageInfo> tier_usages;
};

// =====================================================================
// HeartbeatTask
// =====================================================================

/**
 * @brief A single task carried in a heartbeat request.
 * Uses type + variant param pattern (similar to Replica).
 */
class HeartbeatTask {
   public:
    using ParamVariant =
        std::variant<RemountSegmentsParam, SyncSegmentMetaParam>;

    HeartbeatTask(HeartbeatTaskType type, ParamVariant param)
        : type_(type), param_(std::move(param)) {}

    [[nodiscard]] HeartbeatTaskType type() const { return type_; }

    template <typename T>
    [[nodiscard]] const T& get_param() const {
        return std::get<T>(param_);
    }

    template <typename T>
    [[nodiscard]] T& get_param() {
        return std::get<T>(param_);
    }

   private:
    HeartbeatTaskType type_;
    ParamVariant param_;
};

// =====================================================================
// Heartbeat Task Result
// =====================================================================

struct HeartbeatTaskResult {
    HeartbeatTaskType type;
    ErrorCode error = ErrorCode::OK;
};

}  // namespace mooncake
