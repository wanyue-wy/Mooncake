#ifdef STORE_USE_REDIS

#include "p2p/ha/redis_master_view_reader.h"

namespace mooncake {

RedisMasterViewReader::RedisMasterViewReader(
    const std::string& cluster_id, const std::string& redis_endpoint,
    const std::string& password, int db_index, int ttl_sec,
    int heartbeat_interval_sec, const std::string& username)
    : redis_election_helper_(cluster_id, redis_endpoint, password, db_index,
                             ttl_sec, heartbeat_interval_sec, username) {}

ErrorCode RedisMasterViewReader::Connect() {
    return redis_election_helper_.Connect();
}

ErrorCode RedisMasterViewReader::GetMasterView(
    std::string& master_address, ViewVersionId& version) {
    return redis_election_helper_.GetMasterView(master_address, version);
}

}  // namespace mooncake

#endif
