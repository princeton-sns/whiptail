/***********************************************************************
 *
 * store/strongstore/client.h:
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef _STRONG_CLIENT_H_
#define _STRONG_CLIENT_H_

#include <bitset>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "replication/vr/client.h"
#include "store/common/frontend/client.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "store/strongstore/common.h"
#include "store/strongstore/networkconfig.h"
#include "store/strongstore/preparedtransaction.h"
#include "store/strongstore/shardclient.h"
#include "store/strongstore/strong-proto.pb.h"
#include "store/strongstore/WhiptailReplicationGroup.h"
#include "store/strongstore/StrongSession.h"

namespace strongstore {

    class CommittedTransaction {
    public:
        uint64_t transaction_id;
        Timestamp commit_ts;
        bool committed;
    };

    enum SnapshotState {
        WAIT,
        COMMIT
    };

    struct SnapshotResult {
        SnapshotState state;
        Timestamp max_read_ts;
        std::unordered_map<std::string, std::string> kv_;
    };

    class Client : public ::Client {
    public:
        Client(Consistency consistency, const NetworkConfiguration &net_config,
               const std::string &client_region, transport::Configuration &config,
               uint64_t id, int nshards, int closestReplic, Transport *transport,
               Partitioner *part, TrueTime &tt, bool debug_stats,
               double nb_time_alpha, uint8_t sent_redundancy);

        virtual ~Client();

        virtual Session &BeginSession() override;

        virtual Session &ContinueSession(rss::Session &session) override;

        virtual rss::Session EndSession(Session &session) override;

        // Overriding functions from ::Client
        // Begin a transaction
        virtual void
        Begin(Session &session, begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout) override;

        // Begin a retried transaction.
        virtual void Retry(Session &session, begin_callback bcb,
                           begin_timeout_callback btcb, uint32_t timeout) override;

        // Get the value corresponding to key.
        void Get(Session &session, const std::string &key,
                 get_callback gcb, get_timeout_callback gtcb,
                 uint32_t timeout = GET_TIMEOUT) override;

        // Get the value corresponding to key.
        // Provide hint that transaction will later write the key.
        void GetForUpdate(Session &session, const std::string &key,
                          get_callback gcb, get_timeout_callback gtcb,
                          uint32_t timeout = GET_TIMEOUT) override;

        // Set the value for the given key.
        virtual void Put(Session &session, const std::string &key, const std::string &value,
                         put_callback pcb, put_timeout_callback ptcb,
                         uint32_t timeout = PUT_TIMEOUT) override;

        // Commit all Get(s) and Put(s) since Begin().
        virtual void Commit(Session &session, commit_callback ccb, commit_timeout_callback ctcb,
                            uint32_t timeout) override;

        // Abort all Get(s) and Put(s) since Begin().
        virtual void Abort(Session &session, abort_callback acb, abort_timeout_callback atcb,
                           uint32_t timeout) override;

        // Force transaction to abort.
        void ForceAbort(const uint64_t transaction_id) override;

        // Commit all Get(s) and Put(s) since Begin().
        void ROCommit(Session &session, const std::unordered_set<std::string> &keys,
                      commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout) override;

    private:
        const static std::size_t MAX_SHARDS = 16;

        struct PendingRequest {
            PendingRequest(uint64_t id)
                    : id(id), outstandingPrepares(0) {}

            ~PendingRequest() {}

            commit_callback ccb;
            commit_timeout_callback ctcb;
            abort_callback acb;
            abort_timeout_callback atcb;
            uint64_t id;
            int outstandingPrepares;
        };

        void ContinueBegin(Session &session, begin_callback bcb);

        void ContinueRetry(Session &session, begin_callback bcb);

        // local Prepare function
        void
        CommitCallback(StrongSession &session, uint64_t req_id, int status, const std::vector<Value> &values,
                       Timestamp commit_ts, Timestamp nonblock_ts);

        void AbortCallback(StrongSession &session, uint64_t req_id);

        void ROCommitCallback(StrongSession &session, uint64_t req_id, int shard_idx,
                              const std::vector<Value> &values,
                              const std::vector<PreparedTransaction> &prepares);

        void ROCommitSlowCallback(StrongSession &session, uint64_t req_id, int shard_idx,
                                  uint64_t rw_transaction_id, const Timestamp &commit_ts, bool is_commit);
//        void HandleWound(const uint64_t transaction_id);

        void RealTimeBarrier(const rss::Session &session, rss::continuation_func_t continuation);

        // choose coordinator from participants
//        void CalculateCoordinatorChoices();
//        int ChooseCoordinator(StrongSession &session);

        // Choose nonblock time
        Timestamp ChooseNonBlockTimestamp(StrongSession &session);

        // For tracking RO reply progress
//        SnapshotResult ReceiveFastPath(StrongSession &session, uint64_t transaction_id,
//                                       int shard_idx,
//                                       const std::vector<Value> &values,
//                                       const std::vector<PreparedTransaction> &prepares);
//        SnapshotResult ReceiveSlowPath(StrongSession &session, uint64_t transaction_id,
//                                       uint64_t rw_transaction_id,
//                                       bool is_commit, const Timestamp &commit_ts);
//        SnapshotResult FindSnapshot(std::unordered_map<uint64_t, PreparedTransaction> &prepared,
//                                    std::vector<CommittedTransaction> &committed);
//        void AddValues(StrongSession &session, const std::vector<Value> &values);
//        void AddPrepares(StrongSession &session, const std::vector<PreparedTransaction> &prepares);
//        void ReceivedAllFastPaths(StrongSession &session);
        void FindCommittedKeys(StrongSession &session);

        void CalculateSnapshotTimestamp(StrongSession &session);
//        SnapshotResult CheckCommit(StrongSession &session);

        std::unordered_map<std::bitset<MAX_SHARDS>, int> coord_choices_;
        std::unordered_map<std::bitset<MAX_SHARDS>, uint16_t> min_lats_;

        std::unordered_map<uint64_t, StrongSession> sessions_;
        std::unordered_map<uint64_t, StrongSession &> sessions_by_transaction_id_;
        std::unordered_map<uint64_t, Timestamp> tmins_;

        const strongstore::NetworkConfiguration &net_config_;
        const std::string client_region_;

        const std::string service_name_;

        transport::Configuration &config_;

        // Unique ID for this client.
        uint64_t client_id_;

        // Number of shards in SpanStore.
        uint64_t nshards_;

        // Transport used by paxos client proxies.
        Transport *transport_;

        // Client for each shard.
        std::vector<WhiptailReplicationGroup *> sclients_;

        // Partitioner
        Partitioner *part_;

        // TrueTime server.
        TrueTime &tt_;

        uint64_t next_transaction_id_;

        uint64_t last_req_id_;
        std::unordered_map<uint64_t, PendingRequest *> pending_reqs_;

        Latency_t op_lat_;
        Latency_t commit_lat_;

        Consistency consistency_;

        double nb_time_alpha_;

        bool debug_stats_;
    };

} // namespace strongstore

#endif /* _STRONG_CLIENT_H_ */
