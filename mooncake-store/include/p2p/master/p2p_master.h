#pragma once

#include "p2p/common/p2p_master_config.h"

namespace mooncake {

class P2PMaster {
   public:
    explicit P2PMaster(const P2PMasterConfig& config);

    int Run();

   private:
    int RunStandalone();
    int RunWithHA();

    P2PMasterConfig config_;
};

}  // namespace mooncake
