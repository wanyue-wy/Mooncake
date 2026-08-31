#pragma once

#include "ha_helper.h"

#ifdef STORE_USE_REDIS
#include "p2p/ha/redis_election_helper.h"
#endif

namespace mooncake {

#ifdef STORE_USE_REDIS
class RedisMasterViewReader final : public MasterViewReader {
   public:
    RedisMasterViewReader(const std::string& cluster_id,
                          const std::string& redis_endpoint,
                          const std::string& password, int db_index,
                          int ttl_sec, int heartbeat_interval_sec,
                          const std::string& username = "");

    ErrorCode Connect();
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override;

   private:
    RedisElectionHelper redis_election_helper_;
};
#endif

}  // namespace mooncake
