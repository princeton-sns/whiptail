//
// Created by jennifer on 2/28/25.
//

#ifndef TAPIR_WHIPTAILREPLICATIONGROUP_H
#define TAPIR_WHIPTAILREPLICATIONGROUP_H

#include <vector>
#include "store/strongstore/shardclient.h"
#include "store/strongstore/StrongSession.h"

namespace strongstore {

    class WhiptailReplicationGroup {

    public:
        WhiptailReplicationGroup(transport::Configuration &config, Transport *transport, int shard_idx,
                                 uint64_t client_id);

        void put_callback_whiptail(StrongSession &session, const put_callback &pcb, int status, const std::string &key,
                                   const std::string &value);

        void Put(StrongSession &session, uint64_t tid, const string &key, const string &value,
                 const put_callback &putCallback, const put_timeout_callback &putTimeoutCallback, uint32_t timeout);

        void RWCommitCoordinator(uint64_t transaction_id,
                                 const std::set<int> &participants,
                                 Timestamp &nonblock_timestamp,
                                 const rw_coord_commit_callback &ccb,
                                 const rw_coord_commit_timeout_callback &ctcb, uint32_t timeout);

        void Begin(uint64_t transaction_id, const Timestamp &start_time);

        void Abort(uint64_t transaction_id, abort_callback acb,
                   abort_timeout_callback atcb, uint32_t timeout);


    private:

        int shard_idx_;
        transport::Configuration &config_;
        Transport *transport_;

        std::vector<ShardClient *> shard_clients_;


    };

} // strongstore

#endif //TAPIR_WHIPTAILREPLICATIONGROUP_H
