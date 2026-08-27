#pragma once

#include <string>
#include <vector>

#include "types.h"

namespace mooncake {

/**
 * @brief Describes a storage segment managed by a P2P client.
 */
struct P2PSegment {
    UUID id{0, 0};
    std::string name;
    size_t size{0};
    int priority{0};
    std::vector<std::string> tags;
    MemoryType memory_type{MemoryType::DRAM};
    size_t usage{0};
};
YLT_REFL(P2PSegment, id, name, size, priority, tags, memory_type, usage);

}  // namespace mooncake
