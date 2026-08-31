#pragma once

#include <memory>

#include "p2p/common/p2p_master_config.h"

namespace mooncake {

class P2PMaster {
   public:
    explicit P2PMaster(const P2PMasterConfig& config);
    ~P2PMaster();

    P2PMaster(const P2PMaster&) = delete;
    P2PMaster& operator=(const P2PMaster&) = delete;

    int Run();

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mooncake
