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
 * Only lightweight info-sync tasks; heavy operations like RegisterClient
 * are handled via separate RPC.
 */
enum class HeartbeatTaskType {
    SYNC_SEGMENT_META,  // Sync segment usage metadata
};

// =====================================================================
// Heartbeat Task Params
// =====================================================================

/**
 * @brief Usage info for a single tier/segment.
 */
struct TierUsageInfo {
    UUID segment_id;
    size_t usage = 0;
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
    using ParamVariant = std::variant<SyncSegmentMetaParam>;

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
