#include "p2p_rpc_service.h"

namespace mooncake {

WrappedP2PMasterService::WrappedP2PMasterService(
    const WrappedMasterServiceConfig& config)
    : WrappedMasterService(config), master_service_(config) {}

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service) {
    RegisterRpcService(server, wrapped_master_service);
}

}  // namespace mooncake
