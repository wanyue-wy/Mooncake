#include "p2p/ha/oplog/p2p_oplog_types.h"

#include <string>

#include <glog/logging.h>
#include <ylt/struct_pack.hpp>

namespace mooncake {
namespace {

template <typename Payload>
std::string SerializePayload(const Payload& payload) {
    if (payload.schema_version != kP2PHAProtocolSchemaVersion) {
        LOG(ERROR) << "Refusing to serialize unsupported P2P HA payload"
                   << ", schema_version=" << payload.schema_version
                   << ", expected=" << kP2PHAProtocolSchemaVersion;
        return {};
    }
    return struct_pack::serialize<std::string>(payload);
}

template <typename Payload>
bool DeserializePayload(const std::string& data, Payload& payload) {
    const auto error =
        struct_pack::deserialize_to(payload, data.data(), data.size());
    if (error != struct_pack::errc::ok) {
        LOG(ERROR) << "Failed to deserialize P2P HA schema-v2 payload"
                   << ", struct_pack_error=" << static_cast<int>(error);
        return false;
    }
    if (payload.schema_version != kP2PHAProtocolSchemaVersion) {
        LOG(ERROR) << "Rejected incompatible P2P HA payload"
                   << ", schema_version=" << payload.schema_version
                   << ", expected=" << kP2PHAProtocolSchemaVersion;
        return false;
    }
    return true;
}

}  // namespace

std::string SerializeP2PPayload(const RegisterClientPayload& payload) {
    return SerializePayload(payload);
}
std::string SerializeP2PPayload(const UnregisterClientPayload& payload) {
    return SerializePayload(payload);
}
std::string SerializeP2PPayload(const PublishRoutePayload& payload) {
    return SerializePayload(payload);
}
std::string SerializeP2PPayload(const WithdrawRoutePayload& payload) {
    return SerializePayload(payload);
}
std::string SerializeP2PPayload(const MountSegmentPayload& payload) {
    return SerializePayload(payload);
}
std::string SerializeP2PPayload(const UnmountSegmentPayload& payload) {
    return SerializePayload(payload);
}

bool DeserializeP2PPayload(const std::string& data,
                           RegisterClientPayload& payload) {
    return DeserializePayload(data, payload);
}
bool DeserializeP2PPayload(const std::string& data,
                           UnregisterClientPayload& payload) {
    return DeserializePayload(data, payload);
}
bool DeserializeP2PPayload(const std::string& data,
                           PublishRoutePayload& payload) {
    return DeserializePayload(data, payload);
}
bool DeserializeP2PPayload(const std::string& data,
                           WithdrawRoutePayload& payload) {
    return DeserializePayload(data, payload);
}
bool DeserializeP2PPayload(const std::string& data,
                           MountSegmentPayload& payload) {
    return DeserializePayload(data, payload);
}
bool DeserializeP2PPayload(const std::string& data,
                           UnmountSegmentPayload& payload) {
    return DeserializePayload(data, payload);
}

}  // namespace mooncake
