#ifndef MOONCAKE_REDIS_MASTER_VIEW_HELPER_H_
#define MOONCAKE_REDIS_MASTER_VIEW_HELPER_H_

#include "ha_helper.h"

#ifdef STORE_USE_REDIS
#include "p2p/ha/redis_election_helper.h"
#endif

namespace mooncake {

/*
 * @brief Redis-backed master-view adapter used by the current mixed client
 *        discovery boundary. Master lifecycle code uses RedisElectionHelper
 *        directly.
 *
 * NOTE: This class is introduced temporarily to minimize divergence
 * from the community mainline. It may be refactored or removed once
 * a unified election abstraction is upstreamed.
 */
#ifdef STORE_USE_REDIS
class RedisMasterViewHelper : public MasterViewHelper {
   public:
    RedisMasterViewHelper(const std::string& cluster_id,
                          const std::string& redis_endpoint,
                          const std::string& password, int db_index,
                          int ttl_sec, int heartbeat_interval_sec,
                          const std::string& username = "");

    ErrorCode Connect();

    void ElectLeader(const std::string& master_address, ViewVersionId& version,
                     EtcdLeaseId& lease_id);

    void KeepLeader(EtcdLeaseId lease_id);

    void CancelKeepAlive(EtcdLeaseId lease_id);

    int GetLeaderLeaseTTLSeconds() const;

    void CancelElection();

    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version) override;

   private:
    RedisElectionHelper redis_election_helper_;
    int ttl_sec_;
};
#endif  // STORE_USE_REDIS

}  // namespace mooncake

#endif  // MOONCAKE_REDIS_MASTER_VIEW_HELPER_H_
