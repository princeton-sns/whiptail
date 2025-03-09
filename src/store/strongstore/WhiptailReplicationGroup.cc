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

//        Debug("jenndebug [%lu] PUT %s, %s", session.transaction_id(), key.c_str(), value.c_str());

        session.mark_success_or_fail_reply(shard_idx_, status);

        if (session.success_count(shard_idx_) >= config_.n) {
            session.mark_successfully_replicated(shard_idx_);
//            Debug("[%lu] replication count %d", session.transaction_id(), session.success_count(shard_idx_));
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

        session.clear_success_count(shard_idx_);
        for (ShardClient *shard_client: shard_clients_) {
            shard_client->Put(tid, key, value, pcbw, putTimeoutCallback, timeout);
        }
    }

    std::string stringify(const std::vector<Value> &values) {
        std::string result = "[";

        for (const auto &v: values)
            result += v.to_string() + ", ";

        result += "]";
        return result;
    }

    void WhiptailReplicationGroup::RWCommitCallbackWhiptail(StrongSession &session, const rw_coord_commit_callback &ccb,
                                                            int status, const std::vector<Value> &values,
                                                            const Timestamp &commit_ts,
                                                            const Timestamp &nonblock_ts) {
        session.mark_success_or_fail_reply(shard_idx_, status);
        session.add_reply_values(shard_idx_, values);

//        Debug("jenndebug [%lu] values %s", session.transaction_id(), stringify(values).c_str());

        if (session.success_count(shard_idx_) >= config_.QuorumSize()) {

//            Debug("[%lu] successful replication count %d", session.transaction_id(), session.success_count(shard_idx_));
            if (!values.empty()) {
                // has reads
                if (session.has_quorum(shard_idx_, config_.QuorumSize())) {
                    const std::vector<Value> majority_values = session.quorum_resp(shard_idx_, config_.QuorumSize());
                    session.clear_success_count(shard_idx_);
                    session.clear_reply_values(shard_idx_);
                    ccb(REPLY_OK, majority_values, commit_ts, nonblock_ts);
                } else if (session.success_count(shard_idx_) == config_.n &&
                           !session.has_quorum(shard_idx_, config_.QuorumSize())) {
                    Debug("jenndebug [%lu] OOPS no majority, do something wrong for now", session.transaction_id());
                    session.clear_reply_values(shard_idx_);
                    ccb(REPLY_OK, {}, commit_ts, nonblock_ts); // TODO jenndebug WRONG
                }
            } else {
                // just writes
                session.clear_success_count(shard_idx_);
                ccb(REPLY_OK, {}, commit_ts, nonblock_ts);
            }
            session.mark_successfully_replicated(shard_idx_);
        } else if (session.failure_count(shard_idx_) >= config_.QuorumSize()) {
            Panic("Failed txn! Not enough replicas replied");
        }
    }

    void WhiptailReplicationGroup::RWCommitCoordinator(StrongSession &session, uint64_t transaction_id,
                                                       const Timestamp &commit_ts,
                                                       const std::set<int> &participants,
                                                       Timestamp &nonblock_ts,
                                                       const rw_coord_commit_callback &ccb,
                                                       const rw_coord_commit_timeout_callback &ctcb, uint32_t timeout) {
        auto ccbw = [this, ccb, session = std::ref(session)](int status, const std::vector<Value> &values,
                                                             const Timestamp &commit_timestamp,
                                                             const Timestamp &nonblock_timestamp) {
            this->RWCommitCallbackWhiptail(session, ccb, status, values, commit_timestamp, nonblock_timestamp);
        };

        session.clear_success_count(shard_idx_);
        for (ShardClient *shard_client: shard_clients_) {
            shard_client->RWCommitCoordinator(transaction_id, commit_ts, participants, nonblock_ts, ccbw, ctcb,
                                              timeout);
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

    void WhiptailReplicationGroup::GetCallbackWhiptail(StrongSession &session, const get_callback &gcb, int status,
                                                       const std::string &key, const std::string &value,
                                                       const Timestamp &ts) {
        session.mark_success_or_fail_reply(shard_idx_, status);
//        Debug("jenndebug [%lu] GET_BUFFERED %s, %s", session.transaction_id(), key.c_str(), value.c_str());

        if (session.success_count(shard_idx_) >= config_.n) {
            session.mark_successfully_replicated(shard_idx_);
//            Debug("[%lu] replication count %d", session.transaction_id(), session.success_count(shard_idx_));
            session.clear_success_count(shard_idx_);
            gcb(REPLY_OK, key, value, ts);
        } else if (session.failure_count(shard_idx_) >= config_.QuorumSize()) {
            Panic("Failed txn! Not enough replicas replied");
        }
    }

    void
    WhiptailReplicationGroup::Get(StrongSession &session, uint64_t transaction_id, const string &key, get_callback gcb,
                                  get_timeout_callback gtcb, uint32_t timeout) {
        auto gcbw = [this, gcb, session = std::ref(session)](int s, const std::string &k,
                                                             const std::string &v, const Timestamp &ts) {
            this->GetCallbackWhiptail(session, gcb, s, k, v, ts);
        };

        session.clear_success_count(shard_idx_);
        for (ShardClient *shard_client: shard_clients_) {
            shard_client->GetBuffered(transaction_id, key, gcbw, gtcb, timeout);
        }
    }

    void WhiptailReplicationGroup::ROCommitCallbackWhiptail(StrongSession &session, const ro_commit_callback &roccb,
                                                            int shard_idx,
                                                            const std::vector<Value> &values,
                                                            const std::vector<PreparedTransaction> &prepares) {
        ASSERT(prepares.size() == 0);

        session.mark_success_or_fail_reply(shard_idx_, REPLY_OK);
        session.add_reply_values(shard_idx_, values);

        if (session.success_count(shard_idx_) >= config_.QuorumSize() &&
            session.has_quorum(shard_idx_, config_.QuorumSize())) {

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

    void WhiptailReplicationGroup::ROCommit(StrongSession &session, uint64_t transaction_id,
                                            const std::vector<std::string> &keys,
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