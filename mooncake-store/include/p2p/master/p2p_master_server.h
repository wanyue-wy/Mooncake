#pragma once

#include <cstdint>
#include <functional>
#include <optional>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "p2p/common/p2p_master_config.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"

namespace mooncake {

/** Owns one active P2P master RPC runtime in either HA or non-HA mode. */
class P2PMasterServer {
   public:
    explicit P2PMasterServer(const P2PMasterConfig& config,
                             ViewVersionId view_version = 0);

    void SetViewVersion(ViewVersionId view_version);

    void SetPromotedMetadata(
        P2PStandbyMetadataStore::ExportedMetadata metadata,
        uint64_t last_applied_sequence_id);

    int Run(std::function<void()> before_start = {});
    void Stop();

   private:
    P2PMasterConfig config_;
    ViewVersionId view_version_;
    coro_rpc::coro_rpc_server server_;
    std::optional<P2PStandbyMetadataStore::ExportedMetadata>
        promoted_metadata_;
    uint64_t promoted_sequence_id_ = 0;
};

}  // namespace mooncake
