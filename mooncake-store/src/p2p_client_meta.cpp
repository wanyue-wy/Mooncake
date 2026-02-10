#include "p2p_client_meta.h"
#include <glog/logging.h>

namespace mooncake {
P2PClientMeta::P2PClientMeta(const UUID& client_id,
                             const std::string& ip_address, uint16_t rpc_port)
    : ClientMeta(client_id), ip_address_(ip_address), rpc_port_(rpc_port) {
    segment_manager_ = std::make_shared<P2PSegmentManager>();
}

std::shared_ptr<SegmentManager> P2PClientMeta::GetSegmentManager() {
    return segment_manager_;
}

tl::expected<std::vector<std::string>, ErrorCode> P2PClientMeta::QueryIp(
    const UUID& client_id) {
    return std::vector<std::string>{ip_address_};
}

void P2PClientMeta::OnDisconnected() {
    // TODO::wanyue-wy
}

void P2PClientMeta::OnRecovered() {
    // TODO: wanyue-wy
}

void P2PClientMeta::OnCrashed() {
    // TODO: wanyue-wy
}
}  // namespace mooncake
