#pragma once

#include "p2p/common/p2p_master_config.h"

namespace mooncake {

/** Runs P2P leader election, standby promotion, and active-master restarts. */
class P2PMasterHARunner {
   public:
    explicit P2PMasterHARunner(const P2PMasterConfig& config);

    int Run();

   private:
    P2PMasterConfig config_;
};

}  // namespace mooncake
