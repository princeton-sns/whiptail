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
                                 uint64_t client_id, Stats& stats, uint8_t sent_redundancy);

        void PutCallbackWhiptail(StrongSession &session, const put_callback &pcb, int status, const std::string &key,
                                 const std::string &value);

        void Put(StrongSession &session, uint64_t tid, const string &key, const string &value,
                 const put_callback &putCallback, const put_timeout_callback &putTimeoutCallback, uint32_t timeout);

        void RWCommitCoordinator(StrongSession &session, uint64_t transaction_id,
                                 const Timestamp &commit_ts,
                                 const std::set<int> &participants,
                                 Timestamp &nonblock_timestamp,
                                 const rw_coord_commit_callback &ccb,
                                 const rw_coord_commit_timeout_callback &ctcb, uint32_t timeout);

        void RWCommitCallbackWhiptail(StrongSession &session, const rw_coord_commit_callback &ccb,
                                      int status, const std::vector<Value> &values,
                                      const Timestamp &commit_ts,
                                      const Timestamp &nonblock_ts);

        void Begin(uint64_t transaction_id, const Timestamp &start_time);

        void Abort(uint64_t transaction_id, abort_callback acb,
                   abort_timeout_callback atcb, uint32_t timeout);


        void Get(StrongSession &session, uint64_t transaction_id, const string &key, get_callback gcb,
                 get_timeout_callback gtcb, uint32_t timeout);

        void GetCallbackWhiptail(StrongSession &session, const get_callback &gcb, int status,
                                 const std::string &key, const std::string &value, const Timestamp &ts);

        void ROCommitCallbackWhiptail(StrongSession &session, const ro_commit_callback &rocc,
                                      int shard_idx,
                                      const std::vector<Value> &values,
                                      const std::vector<PreparedTransaction> &prepares
        );

        void ROCommit(StrongSession &session, uint64_t transaction_id, const std::vector<std::string> &keys,
                      const Timestamp &commit_timestamp,
                      const Timestamp &min_read_timestamp,
                      ro_commit_callback ccb, ro_commit_slow_callback cscb,
                      ro_commit_timeout_callback ctcb, uint32_t timeout);

    private:

        int shard_idx_;
        transport::Configuration &config_;
        Transport *transport_;

        std::vector<ShardClient *> shard_clients_;

        Stats& stats_;


    };

} // strongstore

#endif //TAPIR_WHIPTAILREPLICATIONGROUP_H
