//
// Created by jennifer on 2/28/25.
//

#include "store/strongstore/WhiptailReplicationGroup.h"

namespace strongstore {

    WhiptailReplicationGroup::WhiptailReplicationGroup(transport::Configuration &config, Transport *transport,
                                                       int shard_idx, uint64_t client_id)
            : shard_idx_(shard_idx), config_(config), transport_(transport) {

        for (int repl_idx = 0; repl_idx < config_.n; repl_idx++) {
            shard_clients_.push_back(new ShardClient(config_, transport_, client_id, shard_idx_,
                                                     [](uint64_t transaction_id) {}, repl_idx));
        }
    }

    void WhiptailReplicationGroup::PutCallbackWhiptail(StrongSession &session, const put_callback &pcb, int status,
                                                       const std::string &key, const std::string &value) {
        session.mark_success_or_fail_reply(shard_idx_, status);

        if (session.success_count(shard_idx_) >= config_.QuorumSize()) {
            session.mark_successfully_replicated(shard_idx_);
            Debug("[%lu] replication count %d", session.transaction_id(), session.success_count(shard_idx_));
            session.clear_success_count(shard_idx_);
            pcb(REPLY_OK, key, value);
        } else if (session.failure_count(shard_idx_) >= config_.QuorumSize()) {
            Panic("Failed txn! Not enough replicas replied");
        }
    }

    void WhiptailReplicationGroup::Put(StrongSession &session, uint64_t tid, const string &key, const string &value,
                                       const put_callback &putCallback, const put_timeout_callback &putTimeoutCallback,
                                       uint32_t timeout) {

        auto pcbw = [this, putCallback, session = std::ref(session)](int s, const std::string &k,
                                                                     const std::string &v) {
            this->PutCallbackWhiptail(session, putCallback, s, k, v);
        };

        for (ShardClient *shard_client: shard_clients_) {
            shard_client->Put(tid, key, value, pcbw, putTimeoutCallback, timeout);
        }
    }

    void WhiptailReplicationGroup::RWCommitCoordinator(uint64_t transaction_id,
                                                       const std::set<int> &participants,
                                                       Timestamp &nonblock_timestamp,
                                                       const rw_coord_commit_callback &ccb,
                                                       const rw_coord_commit_timeout_callback &ctcb, uint32_t timeout) {
        for (ShardClient *shard_client: shard_clients_) {
            shard_client->RWCommitCoordinator(transaction_id, participants, nonblock_timestamp, ccb, ctcb, timeout);
        }
    }

    void WhiptailReplicationGroup::Begin(uint64_t transaction_id, const Timestamp &start_time) {
        for (ShardClient *shard_client: shard_clients_) {
            shard_client->Begin(transaction_id, start_time);
        }
    }

    void WhiptailReplicationGroup::Abort(uint64_t transaction_id, abort_callback acb,
                                         abort_timeout_callback atcb, uint32_t timeout) {
        for (ShardClient *shard_client: shard_clients_) {
            shard_client->Abort(transaction_id, acb, atcb, timeout);
        }
    }
//
//    void WhiptailReplicationGroup::Get(uint64_t transaction_id, const string &key, get_callback gcb,
//                                       get_timeout_callback gtcb, uint32_t timeout) {
//
//        for (ShardClient *shard_client: shard_clients_) {
//            shard_client->Get(transaction_id, key, gcb, gtcb, timeout);
//        }
//    }

    void WhiptailReplicationGroup::ROCommitCallbackWhiptail(StrongSession &session, const ro_commit_callback &roccb,
                                  int shard_idx,
                                  const std::vector<Value> &values,
                                  const std::vector<PreparedTransaction> &prepares) {
        ASSERT(prepares.size() == 0);

        session.mark_success_or_fail_reply(shard_idx_, REPLY_OK);
        session.add_reply_values(shard_idx_, values);

        if (session.success_count(shard_idx_) >= config_.QuorumSize() && session.has_quorum(shard_idx_, config_.QuorumSize())) {

            const std::vector<Value> majority_values = session.quorum_resp(shard_idx_, config_.QuorumSize());
            session.clear_success_count(shard_idx_);
            session.clear_reply_values(shard_idx_);
            roccb(shard_idx, majority_values, prepares);
        }

        if (session.success_count(shard_idx_) == config_.n && !session.has_quorum(shard_idx_, config_.QuorumSize())) {
            Debug("jenndebug OOPS no majority, do something wrong for now");
                session.clear_reply_values(shard_idx_);
            roccb(shard_idx, values, prepares); // TODO jenndebug WRONG
        }
    }

    void WhiptailReplicationGroup::ROCommit(StrongSession& session, uint64_t transaction_id, const std::vector<std::string> &keys,
                                            const Timestamp &commit_timestamp,
                                            const Timestamp &min_read_timestamp,
                                            ro_commit_callback ccb, ro_commit_slow_callback cscb,
                                            ro_commit_timeout_callback ctcb, uint32_t timeout) {

        auto roccbw = std::bind(&WhiptailReplicationGroup::ROCommitCallbackWhiptail, this, std::ref(session), ccb,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

        session.clear_reply_values(shard_idx_);
        for (ShardClient *shard_client: shard_clients_) {
            shard_client->ROCommit(transaction_id, keys, commit_timestamp, min_read_timestamp, roccbw, cscb, ctcb,
                                   timeout);
        }
    }
} // strongstore