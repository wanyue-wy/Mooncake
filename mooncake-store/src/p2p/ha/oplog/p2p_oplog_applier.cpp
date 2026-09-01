#include "p2p/ha/oplog/p2p_oplog_applier.h"

#include <glog/logging.h>

#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/ha_metric_manager.h"
#include "types.h"

namespace mooncake {
namespace {

class RejectingMetadataStore final : public MetadataStore {
   public:
    bool PutMetadata(const std::string&,
                     const StandbyObjectMetadata&) override {
        return false;
    }
    bool Put(const std::string&, const std::string&) override { return false; }
    std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string&) const override {
        return std::nullopt;
    }
    bool Remove(const std::string&) override { return false; }
    bool Exists(const std::string&) const override { return false; }
    size_t GetKeyCount() const override { return 0; }
};

MetadataStore* GetRejectingMetadataStore() {
    static RejectingMetadataStore store;
    return &store;
}

bool IsP2POpType(OpType type) {
    return type == OpType_PUBLISH_ROUTE || type == OpType_WITHDRAW_ROUTE ||
           type == OpType_MOUNT_SEGMENT || type == OpType_UNMOUNT_SEGMENT ||
           type == OpType_REMOVE_ALL || type == OpType_REGISTER_CLIENT ||
           type == OpType_UNREGISTER_CLIENT;
}

}  // namespace

P2POpLogApplier::P2POpLogApplier(P2PStandbyMetadataStore* p2p_store,
                                 const std::string& cluster_id,
                                 OpLogStore* oplog_store)
    : OpLogApplier(GetRejectingMetadataStore(), cluster_id, oplog_store),
      p2p_store_(p2p_store) {
    if (p2p_store_ == nullptr) {
        LOG(FATAL) << "P2POpLogApplier: p2p_store cannot be null";
    }
}

bool P2POpLogApplier::ApplyOpLogEntry(const OpLogEntry& entry) {
    if (!IsP2POpType(entry.op_type)) {
        LOG(ERROR) << "P2P OpLog rejected non-P2P operation"
                   << ", op_type=" << static_cast<int>(entry.op_type)
                   << ", sequence_id=" << entry.sequence_id;
        return false;
    }
    return OpLogApplier::ApplyOpLogEntry(entry);
}

bool P2POpLogApplier::ApplyCustomOpLogEntry(const OpLogEntry& entry) {
    if (entry.op_type == OpType_PUBLISH_ROUTE) {
        return ApplyPublishRoute(entry);
    } else if (entry.op_type == OpType_WITHDRAW_ROUTE) {
        return ApplyWithdrawRoute(entry);
    } else if (entry.op_type == OpType_MOUNT_SEGMENT) {
        return ApplyMountSegment(entry);
    } else if (entry.op_type == OpType_UNMOUNT_SEGMENT) {
        return ApplyUnmountSegment(entry);
    } else if (entry.op_type == OpType_REMOVE_ALL) {
        return ApplyRemoveAll(entry);
    } else if (entry.op_type == OpType_REGISTER_CLIENT) {
        return ApplyRegisterClient(entry);
    } else if (entry.op_type == OpType_UNREGISTER_CLIENT) {
        return ApplyUnregisterClient(entry);
    }
    return false;
}

bool P2POpLogApplier::IsBestEffortOpLogEntry(const OpLogEntry& entry) const {
    return IsBestEffortP2POpLog(entry.op_type);
}

bool P2POpLogApplier::IsLateSkippedDeleteLikeOpLogEntry(
    const OpLogEntry& entry) const {
    // TODO(P2P HA): Add per-replica/segment/client sequence guards so a stale
    // late delete cannot remove newer same-target state after a re-add.
    return OpLogApplier::IsLateSkippedDeleteLikeOpLogEntry(entry) ||
           entry.op_type == OpType_WITHDRAW_ROUTE ||
           entry.op_type == OpType_UNMOUNT_SEGMENT ||
           entry.op_type == OpType_UNREGISTER_CLIENT;
}

bool P2POpLogApplier::ApplyPublishRoute(const OpLogEntry& entry) {
    PublishRoutePayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR) << "P2POpLogApplier: failed to deserialize PublishRoutePayload"
                   << ", sequence_id=" << entry.sequence_id
                   << ", key=" << entry.object_key;
        return false;
    }

    return p2p_store_->PublishRoute(
        payload.object_key,
        P2PRouteLocation{.client_id = payload.client_id,
                         .segment_id = payload.segment_id},
        payload.size, entry.sequence_id);
}

bool P2POpLogApplier::ApplyWithdrawRoute(const OpLogEntry& entry) {
    WithdrawRoutePayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize WithdrawRoutePayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->WithdrawRoute(
        payload.object_key,
        P2PRouteLocation{.client_id = payload.client_id,
                         .segment_id = payload.segment_id});
    return true;
}

bool P2POpLogApplier::ApplyMountSegment(const OpLogEntry& entry) {
    MountSegmentPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize MountSegmentPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->MountSegment(payload.client_id, payload.segment);
    return true;
}

bool P2POpLogApplier::ApplyUnmountSegment(const OpLogEntry& entry) {
    UnmountSegmentPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize UnmountSegmentPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->UnmountSegment(
        P2PRouteLocation{.client_id = payload.client_id,
                         .segment_id = payload.segment_id});
    return true;
}

bool P2POpLogApplier::ApplyRemoveAll(const OpLogEntry& entry) {
    VLOG(1) << "P2POpLogApplier::ApplyRemoveAll, sequence_id="
            << entry.sequence_id;
    p2p_store_->RemoveAllMetadata();
    return true;
}

bool P2POpLogApplier::ApplyRegisterClient(const OpLogEntry& entry) {
    RegisterClientPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize RegisterClientPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->RegisterClient(payload.client_id, payload.ip_address,
                               payload.rpc_port, payload.segments);
    return true;
}

bool P2POpLogApplier::ApplyUnregisterClient(const OpLogEntry& entry) {
    UnregisterClientPayload payload;
    if (!DeserializeP2PPayload(entry.payload, payload)) {
        LOG(ERROR)
            << "P2POpLogApplier: failed to deserialize UnregisterClientPayload"
            << ", sequence_id=" << entry.sequence_id
            << ", key=" << entry.object_key;
        return false;
    }

    p2p_store_->UnregisterClient(payload.client_id);
    return true;
}

}  // namespace mooncake
