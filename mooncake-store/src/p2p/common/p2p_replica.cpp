#include "replica.h"

#include "p2p/master/p2p_client_meta.h"

namespace mooncake {

P2PProxyReplicaData::P2PProxyReplicaData(
    std::shared_ptr<P2PClientMeta> client_param,
    std::shared_ptr<Segment> segment_param, uint64_t object_size)
    : client(std::move(client_param)), segment(std::move(segment_param)) {
    descriptor.object_size = object_size;
    if (!client) {
        LOG(ERROR) << "Cannot create P2P proxy replica without client metadata";
    } else {
        descriptor.client_id = client->get_client_id();
        descriptor.ip_address = client->get_ip_address();
        descriptor.rpc_port = client->get_rpc_port();
    }
    if (!segment) {
        LOG(ERROR) << "Cannot create P2P proxy replica without segment metadata";
    } else {
        descriptor.segment_id = segment->id;
    }
}

}  // namespace mooncake
