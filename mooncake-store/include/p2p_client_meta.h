#pragma once

#include "client_meta.h"
#include "p2p_segment_manager.h"

namespace mooncake {
class P2PClientMeta final : public ClientMeta {
   public:
    P2PClientMeta(const UUID& client_id, const std::string& ip_address,
                  uint16_t rpc_port);

    std::shared_ptr<SegmentManager> GetSegmentManager() override;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

    const std::string& get_ip_address() const { return ip_address_; }
    uint16_t get_rpc_port() const { return rpc_port_; }

   public:
    void OnDisconnected() override;
    void OnRecovered() override;
    void OnCrashed() override;

   private:
    std::string ip_address_;
    uint16_t rpc_port_ = 0;
    std::shared_ptr<P2PSegmentManager> segment_manager_;
};

}  // namespace mooncake
