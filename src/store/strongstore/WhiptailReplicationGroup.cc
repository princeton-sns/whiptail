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

    void WhiptailReplicationGroup::put_callback_whiptail(StrongSession &session, const put_callback &pcb, int status,
                                                         const std::string &key, const std::string &value) {
        session.mark_success_or_fail_reply(shard_idx_, status);

        if (session.success_count(shard_idx_) >= config_.QuorumSize()) {
            session.mark_successfully_replicated(shard_idx_);
            Debug("[%lu] replication count %d", session.transaction_id(), session.success_count(shard_idx_));
        } else if (session.failure_count(shard_idx_) >= config_.QuorumSize()) {
            Panic("Failed txn! Not enough replicas replied");
        }

        if (session.all_keys_replicated()) {
            pcb(REPLY_OK, key, value);
        }
    }

    void WhiptailReplicationGroup::Put(StrongSession &session, uint64_t tid, const string &key, const string &value,
                                       const put_callback &putCallback, const put_timeout_callback &putTimeoutCallback,
                                       uint32_t timeout) {

        auto pcbw = [this, putCallback, session = std::ref(session)](int s, const std::string &k,
                                                                     const std::string &v) {
            this->put_callback_whiptail(session, putCallback, s, k, v);
        };

        for (ShardClient *shard_client: shard_clients_) {
            shard_client->Put(tid, key, value, pcbw, putTimeoutCallback, timeout);
        }
    }

    void WhiptailReplicationGroup::RWCommitCoordinator(uint64_t transaction_id,
                                                       const std::set<int>& participants,
                                                       Timestamp &nonblock_timestamp,
                                                       const rw_coord_commit_callback& ccb,
                                                       const rw_coord_commit_timeout_callback& ctcb, uint32_t timeout) {
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
} // strongstore