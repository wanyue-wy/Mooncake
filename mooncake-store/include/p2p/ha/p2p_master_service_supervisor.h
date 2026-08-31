#pragma once

#include "master_config.h"

namespace mooncake {

/** Supervisor for the P2P master service in HA mode. */
class P2PMasterServiceSupervisor {
   public:
    explicit P2PMasterServiceSupervisor(
        const MasterServiceSupervisorConfig& config);

    int Start();

   private:
    MasterServiceSupervisorConfig config_;
};

}  // namespace mooncake
