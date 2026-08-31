#pragma once

#include <string>

#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_master_config.h"

namespace mooncake {

tl::expected<P2PMasterConfig, std::string> LoadP2PMasterConfig();

}  // namespace mooncake
