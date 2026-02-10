#pragma once

#include "p2p_master_service.h"
#include "rpc_service.h"

namespace mooncake {

class WrappedP2PMasterService final : public WrappedMasterService {
   public:
    WrappedP2PMasterService(const WrappedMasterServiceConfig& config);

    ~WrappedP2PMasterService() override = default;

    MasterService& GetMasterService() override { return master_service_; }

   private:
    P2PMasterService master_service_;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service);

}  // namespace mooncake
