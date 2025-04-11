#include "store/strongstore/server.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <unordered_set>

namespace strongstore {

    using namespace std;
    using namespace proto;
    using namespace replication;

    Server::Server(Consistency consistency,
                   const std::vector<transport::Configuration> &shard_configs,
                   const transport::Configuration &replica_config,
                   uint64_t server_id, int shard_idx, int replica_idx,
                   std::vector<Transport *> transports, const TrueTime &tt, bool debug_stats,
                   uint64_t network_latency_window, uint8_t sent_redundancy, uint64_t loop_queue_interval_us)
            : PingServer(transports),
              tt_{tt},
              transactions_{shard_idx, consistency, tt_},
              store_(network_latency_window),
              shard_configs_{shard_configs},
              shard_config_{shard_configs[0]},
              replica_config_{replica_config},
              transports_{transports},
              transport_{transports[0]},
              server_id_{server_id},
              min_prepare_timestamp_{},
              shard_idx_{shard_idx},
              replica_idx_{replica_idx},
              consistency_{consistency},
              debug_stats_{debug_stats},
              sent_redundancy_{sent_redundancy},
              loop_queue_interval_us_(loop_queue_interval_us),
              cancel_timer_fd_(-1) {

        for (int redundancy_idx = 0; redundancy_idx < sent_redundancy_; redundancy_idx++) {
            transport_->Register(this, shard_configs_[redundancy_idx], shard_idx_, replica_idx_, redundancy_idx);
//            transport_->Register(this, shard_configs_[i], shard_idx_, replica_idx_);
        }

        for (int i = 0; i < shard_config_.g; i++) {
            shard_clients_.push_back(new ShardClient(shard_configs_, transports, server_id_, i));
        }
        Debug("jenndebug loop queue interval %lu us", loop_queue_interval_us_);

//        replica_client_ =
//                new ReplicaClient(replica_config_, transport_, server_id_, shard_idx_);

        if (debug_stats_) {
            _Latency_Init(&ro_wait_lat_, "ro_wait_lat");
        }
    }

    Server::~Server() {
        for (auto s: shard_clients_) {
            delete s;
        }

//        delete replica_client_;

        if (debug_stats_) {
            Latency_Dump(&ro_wait_lat_);
        }
    }

// Assume GetStats called once before exiting protgram
    Stats &Server::GetStats() {
        Stats &s = transactions_.GetStats();
        stats_.Merge(s);
        return stats_;
    }

    void Server::ReceiveMessage(const TransportAddress &remote,
                                const std::string &type, const std::string &data,
                                void *meta_data) {
        if (type == get_.GetTypeName()) {
            get_.ParseFromString(data);
            HandleGet(remote, get_);
        } else if (type == rw_commit_c_.GetTypeName()) {
            rw_commit_c_.ParseFromString(data);
            Debug("jenndebug calling EnqueueOps %lu", tt_.Now().mid());
            EnqueueOps(remote, rw_commit_c_);
        } else if (type == rw_commit_p_.GetTypeName()) {
            rw_commit_p_.ParseFromString(data);
            HandleRWCommitParticipant(remote, rw_commit_p_);
        } else if (type == prepare_ok_.GetTypeName()) {
            prepare_ok_.ParseFromString(data);
            HandlePrepareOK(remote, prepare_ok_);
        } else if (type == prepare_abort_.GetTypeName()) {
            prepare_abort_.ParseFromString(data);
            HandlePrepareAbort(remote, prepare_abort_);
        } else if (type == ro_commit_.GetTypeName()) {
            ro_commit_.ParseFromString(data);
            HandleROCommit(remote, ro_commit_);
        } else if (type == abort_.GetTypeName()) {
            abort_.ParseFromString(data);
            HandleAbort(remote, abort_);
        } else if (type == wound_.GetTypeName()) {
            wound_.ParseFromString(data);
            HandleWound(remote, wound_);
        } else if (type == ping_.GetTypeName()) {
            ping_.ParseFromString(data);
            HandlePingMessage(this, remote, ping_);
        } else {
            Panic("Received unexpected message type: %s", type.c_str());
        }
    }

    void Server::HandleGet(const TransportAddress &remote, proto::Get &msg) {
        uint64_t client_id = msg.rid().client_id();
        uint64_t client_req_id = msg.rid().client_req_id();
        uint64_t transaction_id = msg.transaction_id();

        const std::string &key = msg.key();
        const Timestamp timestamp{msg.timestamp()};

        bool for_update = msg.has_for_update() && msg.for_update();

        Debug("[%lu] Received GET request: %s %d", transaction_id, key.c_str(), for_update);

        transactions_.StartGet(transaction_id, remote, key, for_update);

        LockAcquireResult r;
        if (for_update) {
            r = locks_.AcquireReadWriteLock(transaction_id, timestamp, key);
        } else {
            r = locks_.AcquireReadLock(transaction_id, timestamp, key);
        }

        if (r.status == LockStatus::ACQUIRED) {
            ASSERT(r.wound_rws.size() == 0);

            std::pair<TimestampID, std::string> value;
            ASSERT(store_.get(key, value));

            get_reply_.Clear();
            get_reply_.mutable_rid()->CopyFrom(msg.rid());
            get_reply_.set_status(REPLY_OK);
            get_reply_.set_key(msg.key());

            get_reply_.set_val(value.second);
            value.first.timestamp.serialize(get_reply_.mutable_timestamp());

            transport_->SendMessage(this, remote, get_reply_);

            transactions_.FinishGet(transaction_id, key);
        } else if (r.status == LockStatus::FAIL) {
            ASSERT(r.wound_rws.size() == 0);

            get_reply_.Clear();
            get_reply_.mutable_rid()->CopyFrom(msg.rid());
            get_reply_.set_status(REPLY_FAIL);
            get_reply_.set_key(msg.key());

            transport_->SendMessage(this, remote, get_reply_);

            const Transaction &transaction = transactions_.GetTransaction(transaction_id);

            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
            transactions_.AbortGet(transaction_id, key);

            NotifyPendingRWs(transaction_id, rr.notify_rws);
        } else if (r.status == LockStatus::WAITING) {
            auto reply = new PendingGetReply(client_id, client_req_id, remote.clone());
            reply->key = key;

            pending_get_replies_[msg.transaction_id()] = reply;

            transactions_.PauseGet(transaction_id, key);

            WoundPendingRWs(transaction_id, r.wound_rws);
        } else {
            NOT_REACHABLE();
        }
    }

    void Server::ContinueGet(uint64_t transaction_id) {
        auto search = pending_get_replies_.find(transaction_id);
        if (search == pending_get_replies_.end()) {
            return;
        }

        PendingGetReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        const std::string &key = reply->key;

        Debug("[%lu] Continuing GET request %s", transaction_id, key.c_str());

        get_reply_.Clear();
        get_reply_.mutable_rid()->set_client_id(client_id);
        get_reply_.mutable_rid()->set_client_req_id(client_req_id);
        get_reply_.set_key(key);

        TransactionState s = transactions_.ContinueGet(transaction_id, key);
        if (s == READING) {
            ASSERT(locks_.HasReadLock(transaction_id, key));

            std::pair<TimestampID, std::string> value;
            ASSERT(store_.get(key, value));

            get_reply_.set_status(REPLY_OK);
            get_reply_.set_val(value.second);

            value.first.timestamp.serialize(get_reply_.mutable_timestamp());

            transport_->SendMessage(this, *remote, get_reply_);

            transactions_.FinishGet(transaction_id, key);
        } else if (s == ABORTED) {  // Already aborted
            get_reply_.set_status(REPLY_FAIL);
            transport_->SendMessage(this, *remote, get_reply_);
        } else {
            NOT_REACHABLE();
        }

        delete remote;
        delete reply;
        pending_get_replies_.erase(search);
    }

    const Timestamp Server::GetPrepareTimestamp(uint64_t client_id) {
        uint64_t ts = std::max(tt_.Now().earliest(), min_prepare_timestamp_.getTimestamp() + 1);
        const Timestamp prepare_timestamp{ts, client_id};
        min_prepare_timestamp_ = prepare_timestamp;

        return prepare_timestamp;
    }

    void Server::WoundPendingRWs(uint64_t transaction_id, const std::unordered_set<uint64_t> &rws) {
        for (uint64_t rw: rws) {
            ASSERT(transaction_id != rw);
            //Debug("[%lu] Wounding %lu", transaction_id, rw);
            TransactionState s = transactions_.GetRWTransactionState(rw);
            ASSERT(s != NOT_FOUND);

            if (s == READING || s == READ_WAIT) {
                // Send wound to client
                std::shared_ptr<TransportAddress> remote = transactions_.GetClientAddr(rw);
                wound_.set_transaction_id(rw);
                transport_->SendMessage(this, *remote, wound_);
            } else if (s == PREPARING || s == WAIT_PARTICIPANTS || s == PREPARE_WAIT || s == PREPARED) {
                // Send wound to coordinator
                int coordinator = transactions_.GetCoordinator(rw);
                ASSERT(coordinator >= 0);
                shard_clients_[coordinator]->Wound(rw);

            } else if (s == COMMITTING || s == COMMITTED || s == ABORTED) {
                //Debug("[%lu] Not wounding. Will complete soon", rw);
            } else {
                NOT_REACHABLE();
            }
        }
    }

    void Server::NotifyPendingRWs(uint64_t transaction_id, const std::unordered_set<uint64_t> &rws) {
        for (uint64_t waiting_rw: rws) {
            if (transaction_id != waiting_rw) {
                Debug("[%lu] continuing %lu", transaction_id, waiting_rw);
                ContinueGet(waiting_rw);
                ContinueCoordinatorPrepare(waiting_rw);
                ContinueParticipantPrepare(waiting_rw);
            }
        }
    }

    void Server::NotifyPendingROs(const std::unordered_set<uint64_t> &ros) {
        for (uint64_t waiting_ro: ros) {
            ContinueROCommit(waiting_ro);
        }
    }

    void Server::NotifySlowPathROs(const std::unordered_set<uint64_t> &ros, uint64_t rw_transaction_id,
                                   bool is_commit, const Timestamp &commit_ts) {
        for (uint64_t ro: ros) {
            SendROSlowPath(ro, rw_transaction_id, is_commit, commit_ts);
        }
    }

    void Server::SendROSlowPath(uint64_t ro_transaction_id, uint64_t rw_transaction_id,
                                bool is_commit, const Timestamp &commit_ts) {
        ASSERT(consistency_ == RSS);
        auto search = pending_ro_commit_replies_.find(ro_transaction_id);
        ASSERT(search != pending_ro_commit_replies_.end());

        //Debug("[%lu] Sending slow path reply for %lu", ro_transaction_id, rw_transaction_id);

        PendingROCommitReply *reply = search->second;
        ASSERT(reply->n_slow_path_replies > 0);

        if (transactions_.GetROTransactionState(ro_transaction_id) != SLOW_PATH) {
            //Debug("[%lu] Fast path reply not yet sent", ro_transaction_id);
            //Debug("s: %d", static_cast<int>(transactions_.GetROTransactionState(ro_transaction_id)));
            reply->n_slow_path_replies -= 1;
            return;
        }

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        ro_commit_slow_reply_.mutable_rid()->set_client_id(client_id);
        ro_commit_slow_reply_.mutable_rid()->set_client_req_id(client_req_id);
        ro_commit_slow_reply_.set_transaction_id(rw_transaction_id);
        ro_commit_slow_reply_.set_is_commit(is_commit);
        commit_ts.serialize(ro_commit_slow_reply_.mutable_commit_timestamp());

        transport_->SendMessage(this, *remote, ro_commit_slow_reply_);

        uint64_t n_slow_path_replies = reply->n_slow_path_replies - 1;
        if (n_slow_path_replies == 0) {
            delete remote;
            delete reply;
            pending_ro_commit_replies_.erase(search);

            transactions_.FinishROSlowPath(ro_transaction_id);
        } else {
            reply->n_slow_path_replies = n_slow_path_replies;
        }
    }

    void Server::HandleROCommit(const TransportAddress &remote, proto::ROCommit &msg) {
        uint64_t client_id = msg.rid().client_id();
        uint64_t client_req_id = msg.rid().client_req_id();
        uint64_t transaction_id = msg.transaction_id();

        Debug("[%lu] Received ROCommit request", transaction_id);

        std::unordered_set<std::string> keys{msg.keys().begin(), msg.keys().end()};

        const Timestamp commit_ts{msg.commit_timestamp()};
        const Timestamp min_ts{msg.min_timestamp()};

        min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);  // TODO: is this correct?
        // ^ Bro how would i know

        TransactionState s = transactions_.StartRO(transaction_id, keys, min_ts, commit_ts);
        if (s == PREPARE_WAIT) {
            Debug("[%lu] Waiting for prepared transactions", transaction_id);
//        auto reply = new PendingROCommitReply(client_id, client_req_id, remote.clone());
//        reply->n_slow_path_replies = transactions_.GetRONumberSkipped(transaction_id);
//        pending_ro_commit_replies_[transaction_id] = reply;
//
//        if (debug_stats_) {
//            _Latency_StartRec(&reply->wait_lat);
//        }
//
//        return;
        }

        ro_commit_reply_.Clear();
        ro_commit_reply_.mutable_rid()->set_client_id(client_id);
        ro_commit_reply_.mutable_rid()->set_client_req_id(client_req_id);
        ro_commit_reply_.set_transaction_id(transaction_id);

        std::pair<TimestampID, std::string> value;
        for (auto &k: keys) {
            ASSERT(store_.get(k, {commit_ts, transaction_id}, value));
            proto::ReadReply *rreply = ro_commit_reply_.add_values();
            rreply->set_transaction_id(value.first.transaction_id);
            value.first.timestamp.serialize(rreply->mutable_timestamp());
            rreply->set_key(k.c_str());
            rreply->set_val(value.second.c_str());
        }

//    if (consistency_ == RSS && transactions_.GetRONumberSkipped(transaction_id) > 0) {
//        const std::vector<PreparedTransaction> skipped_prepares = transactions_.GetROSkippedRWTransactions(transaction_id);
//
//        // Add for slow replies
//        auto reply = new PendingROCommitReply(client_id, client_req_id, remote.clone());
//        reply->n_slow_path_replies = skipped_prepares.size();
//        pending_ro_commit_replies_[transaction_id] = reply;
//
//        for (auto &pt : skipped_prepares) {
//            //Debug("[%lu] replying with skipped prepare: %lu", transaction_id, pt.transaction_id());
//            proto::PreparedTransactionMessage *ptm = ro_commit_reply_.add_prepares();
//            pt.serialize(ptm);
//        }
//
//        transport_->SendMessage(this, remote, ro_commit_reply_);
//
//        transactions_.StartROSlowPath(transaction_id);
//    } else {
        transport_->SendMessage(this, remote, ro_commit_reply_);

        transactions_.CommitRO(transaction_id);
//    }
    }

    void Server::ContinueROCommit(uint64_t transaction_id) {
        auto search = pending_ro_commit_replies_.find(transaction_id);
        ASSERT(search != pending_ro_commit_replies_.end());

        PendingROCommitReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        //Debug("[%lu] Continuing RO commit", transaction_id);

        transactions_.ContinueRO(transaction_id);

        const Timestamp &commit_ts = transactions_.GetROCommitTimestamp(transaction_id);
        const std::unordered_set<std::string> &keys = transactions_.GetROKeys(transaction_id);

        ro_commit_reply_.Clear();
        ro_commit_reply_.mutable_rid()->set_client_id(client_id);
        ro_commit_reply_.mutable_rid()->set_client_req_id(client_req_id);
        ro_commit_reply_.set_transaction_id(transaction_id);

        std::pair<TimestampID, std::string> value;
        for (auto &k: keys) {
            ASSERT(store_.get(k, {commit_ts, transaction_id}, value));
            proto::ReadReply *rreply = ro_commit_reply_.add_values();
            rreply->set_transaction_id(value.first.transaction_id);
            value.first.timestamp.serialize(rreply->mutable_timestamp());
            rreply->set_key(k.c_str());
            rreply->set_val(value.second.c_str());
        }

        if (consistency_ == RSS && transactions_.GetRONumberSkipped(transaction_id) > 0) {
            const std::vector<PreparedTransaction> skipped_prepares = transactions_.GetROSkippedRWTransactions(
                    transaction_id);
            // Add for slow replies
            reply->n_slow_path_replies = skipped_prepares.size();

            for (auto &pt: skipped_prepares) {
                //Debug("[%lu] replying with skipped prepare: %lu", transaction_id, pt.transaction_id());
                proto::PreparedTransactionMessage *ptm = ro_commit_reply_.add_prepares();
                pt.serialize(ptm);
            }

            transport_->SendMessage(this, *remote, ro_commit_reply_);

            transactions_.StartROSlowPath(transaction_id);
        } else {
            transport_->SendMessage(this, *remote, ro_commit_reply_);

            delete remote;
            delete reply;
            pending_ro_commit_replies_.erase(search);

            transactions_.CommitRO(transaction_id);
        }
    }

    void Server::HandleRWCommitCoordinator(/*uint64_t transaction_id, const std::string& key, std::string& value,
                                        const Timestamp& commit_ts, const Timestamp& nonblock_ts*/) {

        // bool should_check_queue = true;
        // while (should_check_queue && this->q_mutex_.try_lock() && !this->queue_.empty()) {
        //
        //     PendingOp pendingOp = this->queue_.top();
        //     this->queue_.pop();
        //
        //     if (this->queue_.top().execute_time() <= tt_.Now().mid()) {
        //         should_check_queue = true;
        //     } else {
        //         should_check_queue = false;
        //     }
        //     this->q_mutex_.unlock();
        // }

        if (cancel_timer_fd_ != -1) {
            transport_->CancelTimer(cancel_timer_fd_);
            cancel_timer_fd_ = -1;
        }

        std::vector<PendingOp> safe_to_execute;

        // if function can't acquire lock, it means another callback has gotten here first
        if (this->q_mutex_.try_lock()) {
            bool should_check_queue = true;

            while (!this->queue_.empty() && should_check_queue) {
                PendingOp pendingOp = this->queue_.top();
                if (pendingOp.execute_time() <= tt_.Now().mid()) {
                    safe_to_execute.push_back(pendingOp);
                    this->queue_.pop();
                } else {
                    should_check_queue = false;
                }
            }
            this->q_mutex_.unlock();
        }

        for (const auto &pendingOp: safe_to_execute) {

            const PendingOpType &pendingOpType = pendingOp.pendingOpType();
            const std::string &key = pendingOp.key();
            const std::string &val = pendingOp.value();
            const Timestamp &commit_ts = pendingOp.commit_ts();
            uint64_t transaction_id = pendingOp.transaction_id();
            const Timestamp &nonblock_ts = pendingOp.nonblock_ts();


            if (pendingOpType == PUT) {
                Debug("jenndebug [%lu] executing PUT %s, %s, %s", transaction_id, key.c_str(), val.c_str(),
                      commit_ts.to_string().c_str());
                store_.put(key, val, {commit_ts, transaction_id});
            } else if (pendingOpType == GET) {

                std::tuple<TimestampID, std::string, uint64_t> value;
                ASSERT(store_.getWithHash(key, {commit_ts, transaction_id}, value));
                transactions_.read_results(transaction_id)[key] = std::pair<std::string, uint64_t>(std::get<1>(value),
                                                                                                   std::get<2>(value));

                Debug("jenndebug [%lu] executing GET %s, commit_ts %s", transaction_id, key.c_str(),
                      commit_ts.to_string().c_str());
            }
//            this->transaction_still_pending_ops_[transaction_id]--;
            transactions_.still_pending_ops(transaction_id)--;

            // Reply to client
//            if (this->transaction_still_pending_ops_[transaction_id] == 0) {
            if (0 == transactions_.still_pending_ops(transaction_id)) {
                if (!transactions_.is_inconsistent(transaction_id)) {
                    Debug("jenndebug [%lu][replica %d] sending success", transaction_id, replica_idx_);
                    SendRWCommmitCoordinatorReplyOK(transaction_id, commit_ts, nonblock_ts,
                                                    transactions_.read_results(transaction_id));
                }
            }
        }

        // TODO jenndebug wait for 200us, change from hardcode
        cancel_timer_fd_ = transport_->TimerMicro(loop_queue_interval_us_,
                                                  std::bind(&Server::HandleRWCommitCoordinator, this));

        // for (LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
        // ar.status != LockStatus::ACQUIRED; ar = locks_.AcquireLocks(transaction_id, transaction)) {}

        // store_.put(key, value, {commit_ts, transaction_id});

        // if (!transaction.getWriteSet().empty()) {
        // min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);
        // }

        // LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
        // TransactionFinishResult fr = transactions_.Commit(transaction_id);
        // Reply to client
        // SendRWCommmitCoordinatorReplyOK(transaction_id, commit_ts, nonblock_ts);
    }

    void Server::EnqueueOps(const TransportAddress &remote, proto::RWCommitCoordinator &msg) {

        uint64_t client_id = msg.rid().client_id();
        uint64_t client_req_id = msg.rid().client_req_id();

        uint64_t transaction_id = msg.transaction_id();

//        Debug("jenndebug [%lu] arrived", transaction_id);

        const RequestID requestId(client_id, client_req_id, &remote);
        {
//            std::lock_guard<std::mutex> lock(multi_sent_reqs_recvd_yet_mutex_);
            if (multi_sent_reqs_recvd_yet_.find(requestId) != multi_sent_reqs_recvd_yet_.end()) {

                // already seen this request, don't need to process it again
                multi_sent_reqs_recvd_yet_[requestId]++;
                Debug("jenndebug [%lu] redundancy %d", transaction_id, multi_sent_reqs_recvd_yet_[requestId]);

                if (sent_redundancy_ <= multi_sent_reqs_recvd_yet_[requestId]) {
                    multi_sent_reqs_recvd_yet_.erase(requestId);
                }

                return;
            }
            multi_sent_reqs_recvd_yet_[requestId] = 1;
        }

        std::unordered_set<int> participants{msg.participants().begin(),
                                             msg.participants().end()};

        const Transaction transaction{msg.transaction()};
        const Timestamp nonblock_ts{msg.nonblock_timestamp()};

//        Debug("jenndebug [%lu] Coordinator for transaction", transaction_id);

        const TrueTimeInterval now = tt_.Now();
        const Timestamp start_ts{now.mid(), client_id};
        TransactionState s = transactions_.StartCoordinatorPrepare(transaction_id, start_ts, shard_idx_,
                                                                   participants, transaction, nonblock_ts);
        if (s == PREPARING) {
//            Debug("jenndebug [%lu] Coordinator preparing", transaction_id);

            // for (LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
            // ar.status != LockStatus::ACQUIRED; ar = locks_.AcquireLocks(transaction_id, transaction)) {}
//         if (ar.status == LockStatus::ACQUIRED) {
            // ASSERT(ar.wound_rws.size() == 0);
            const Timestamp prepare_ts = GetPrepareTimestamp(client_id);
            transactions_.FinishCoordinatorPrepare(transaction_id, prepare_ts);
            // const Timestamp &commit_ts = transactions_.GetRWCommitTimestamp(transaction_id);
            const Timestamp &commit_ts = msg.commit_timestamp();

            auto *reply = new PendingRWCommitCoordinatorReply(client_id, client_req_id, remote.clone());
            pending_rw_commit_c_replies_[transaction_id] = reply;

            /*****************************************/

            // const Timestamp nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

            // Commit writes
            // const Transaction &transaction = transactions_.GetTransaction(transaction_id);

            uint64_t latest_execution_time = 0;
            TrueTimeInterval now_tt = tt_.Now();
            transactions_.mark_inconsistent(transaction_id, false);
            Debug("jenndebug [%lu] transactions.is_inconsistent() %s", transaction_id,
                  transactions_.is_inconsistent(transaction_id) ? "true" : "false");
//            Debug("jenndebug [%lu] write_set size %lu", transaction_id, transaction.getWriteSet().size());
            for (auto &write: transaction.getWriteSet()) {
                const std::chrono::microseconds network_latency_window = store_.get_network_latency_window(write.first);

                PendingOp pendingOp(PUT, write.first, write.second, commit_ts, transaction_id, nonblock_ts,
                                    network_latency_window);

                if (pendingOp.execute_time() < now_tt.mid()) {
                    stats_.Increment("missed_latency_window_" + std::to_string(client_id));
                    stats_.Add("missed_by_" + std::to_string(client_id) + "_us",
                               now_tt.mid() - pendingOp.execute_time());

                    TimestampID lastRead(0, 0);
                    if (store_.lastRead(pendingOp.key(), lastRead) &&
                        lastRead > TimestampID(pendingOp.commit_ts(), pendingOp.transaction_id())) {
                        transactions_.mark_inconsistent(transaction_id, true);
                        stats_.Increment("inconsistent_");
                    }
                    Debug("jennbdebug [%lu] missed latency_window by a bit, transactions missed window",
                          transaction_id);

                    // let the write through, even though we're not on time. The odds that we're not on time to
                    // a majority are pretty low
//                    return;
                } else {
                    stats_.Increment("on_time");
//                Debug("jenndebug [%lu] on time", transaction_id);
                }

                this->queue_.push(pendingOp);

                Debug("jenndebug [%lu] enqueued PUT %s, %s", transaction_id, write.first.c_str(), write.second.c_str());
                //
                //     store_.put(write.first, write.second, {commit_ts, transaction_id});
                if (pendingOp.execute_time() > latest_execution_time) {
                    latest_execution_time = pendingOp.execute_time();
                }
            }
//            Debug("jenndebug [%lu] ooo...k?", transaction_id);
            //
            if (!transaction.getWriteSet().empty()) {
                min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);
            }
            transactions_.still_pending_ops(transaction_id) += transaction.getWriteSet().size();
//            this->transaction_still_pending_ops_[transaction_id] = transaction.getWriteSet().size();
            Debug("jenndebug [%lu] transactions.is_inconsistent() %s", transaction_id,
                  transactions_.is_inconsistent(transaction_id) ? "true" : "false");

            for (auto &key: transaction.getPendingReadSet()) {
                const std::chrono::microseconds network_latency_window = store_.get_network_latency_window(key);
                PendingOp pendingOp(GET, key, "", commit_ts, transaction_id, nonblock_ts, network_latency_window);

                this->queue_.push(pendingOp);

                Debug("jenndebug [%lu] enqueued GET %s", transaction_id, key.c_str());

                if (pendingOp.execute_time() > latest_execution_time) {
                    latest_execution_time = pendingOp.execute_time();
                }
            }

            transactions_.still_pending_ops(transaction_id) += transaction.getPendingReadSet().size();
//            this->transaction_still_pending_ops_[transaction_id] += transaction.getPendingReadSet().size();

            uint64_t wait_until_us = latest_execution_time > now_tt.mid() ? latest_execution_time - tt_.Now().mid() : 0;
//        Debug("jenndebug latest_execution_time [%lu], tt_.Now().mid() [%lu]", latest_execution_time, tt_.Now().mid());

            transport_->TimerMicro(wait_until_us + 300, std::bind(&Server::HandleRWCommitCoordinator, this));
            if (transactions_.is_inconsistent(transaction_id)) {
                Debug("jenndebug [%lu][replica %d] send fail, req_id %lu", transaction_id, replica_idx_, client_req_id);
                SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);
            }
//            Debug("[%lu] Coordinator for wait_until_us %lu", transaction_id, wait_until_us);
            //
            // LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

            // TODO jenndebug I think I don't need the following line about commits
            // TransactionFinishResult fr = transactions_.Commit(transaction_id);
            // // Reply to client
            // SendRWCommmitCoordinatorReplyOK(transaction_id, commit_ts, nonblock_ts);

            /**********************************************************/

            // // TODO: Handle timeout
            // replica_client_->CoordinatorCommit(
            //     transaction_id, start_ts, shard_idx_,
            //     participants, transaction, nonblock_ts, commit_ts,
            //     std::bind(&Server::CommitCoordinatorCallback, this,
            //               transaction_id, std::placeholders::_1),
            //     []() {}, COMMIT_TIMEOUT);

        }
            // else if (ar.status == LockStatus::FAIL) {
            //     ASSERT(ar.wound_rws.size() == 0);
            //     //Debug("[%lu] Coordinator prepare failed", transaction_id);
            //     LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
            //
            //     SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);
            //
            //     NotifyPendingRWs(transaction_id, rr.notify_rws);
            //
            //     transactions_.AbortPrepare(transaction_id);
            // } else if (ar.status == LockStatus::WAITING) {
            //     Debug("[%lu] Waiting", transaction_id);
            //
            //     auto reply = new PendingRWCommitCoordinatorReply(client_id, client_req_id, remote.clone());
            //     pending_rw_commit_c_replies_[transaction_id] = reply;
            //
            //     transactions_.PausePrepare(transaction_id);
            //
            //     WoundPendingRWs(transaction_id, ar.wound_rws);
            // } else {
            //     NOT_REACHABLE();
            // }

        else if (s == ABORTED) {
            Debug("jenndebug [%lu] Already aborted", transaction_id);

            SendRWCommmitCoordinatorReplyFail(remote, client_id, client_req_id);

//         SendAbortParticipants(transaction_id, participants);

        } else if (s == WAIT_PARTICIPANTS) {
            Debug("jenndebug [%lu] Waiting for other participants", transaction_id);

            auto reply = new PendingRWCommitCoordinatorReply(client_id, client_req_id, remote.clone());
            pending_rw_commit_c_replies_[transaction_id] = reply;
        } else {
            NOT_REACHABLE();
        }
    }

    void Server::ContinueCoordinatorPrepare(uint64_t transaction_id) {
        auto search = pending_rw_commit_c_replies_.find(transaction_id);
        if (search == pending_rw_commit_c_replies_.end()) {
            return;
        }
        Debug("[%lu] Cont coord prep", transaction_id);

        PendingRWCommitCoordinatorReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        TransactionState s = transactions_.ContinuePrepare(transaction_id);
        if (s == PREPARING) {
            const Transaction &transaction = transactions_.GetTransaction(transaction_id);
            LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
            if (ar.status == LockStatus::ACQUIRED) {
                ASSERT(ar.wound_rws.size() == 0);
                const Timestamp prepare_ts = GetPrepareTimestamp(client_id);
                transactions_.FinishCoordinatorPrepare(transaction_id, prepare_ts);
                const Timestamp &commit_ts = transactions_.GetRWCommitTimestamp(transaction_id);

                const Timestamp &start_ts = transactions_.GetStartTimestamp(transaction_id);
                const std::unordered_set<int> &participants = transactions_.GetParticipants(transaction_id);
                const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

                // TODO: Handle timeout
//                replica_client_->CoordinatorCommit(
//                        transaction_id, start_ts, shard_idx_,
//                        participants, transaction, nonblock_ts, commit_ts,
//                        std::bind(&Server::CommitCoordinatorCallback, this,
//                                  transaction_id, std::placeholders::_1),
//                        []() {}, COMMIT_TIMEOUT);

            } else if (ar.status == LockStatus::FAIL) {
                ASSERT(ar.wound_rws.size() == 0);
                //Debug("[%lu] Coordinator prepare failed", transaction_id);
                LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

                SendRWCommmitCoordinatorReplyFail(*remote, client_id, client_req_id);
                delete remote;
                delete reply;
                pending_rw_commit_c_replies_.erase(search);

                NotifyPendingRWs(transaction_id, rr.notify_rws);

                transactions_.AbortPrepare(transaction_id);
            } else if (ar.status == LockStatus::WAITING) {
                Debug("[%lu] Waiting", transaction_id);

                transactions_.PausePrepare(transaction_id);

                WoundPendingRWs(transaction_id, ar.wound_rws);
            } else {
                NOT_REACHABLE();
            }
        } else if (s == PREPARED || s == COMMITTING || s == COMMITTED) {
            //Debug("[%lu] Already prepared", transaction_id);
        } else if (s == ABORTED) {  // Already aborted
            //Debug("[%lu] Already aborted", transaction_id);
        } else {
            NOT_REACHABLE();
        }
    }

    void Server::SendRWCommmitCoordinatorReplyOK(uint64_t transaction_id,
                                                 const Timestamp &commit_ts,
                                                 const Timestamp &nonblock_ts,
                                                 std::unordered_map<std::string, std::pair<std::string, uint64_t> > reads) {
        Debug("[%lu] Sending RW Commit Coordinator reply", transaction_id);
        auto search = pending_rw_commit_c_replies_.find(transaction_id);
        if (search == pending_rw_commit_c_replies_.end()) {
            Debug("[%lu] No pending commit coordinator reply found", transaction_id);
            return;
        }

        PendingRWCommitCoordinatorReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        rw_commit_c_reply_.mutable_rid()->set_client_id(client_id);
        rw_commit_c_reply_.mutable_rid()->set_client_req_id(client_req_id);
        rw_commit_c_reply_.set_status(REPLY_OK);
        commit_ts.serialize(rw_commit_c_reply_.mutable_commit_timestamp());
        nonblock_ts.serialize(rw_commit_c_reply_.mutable_nonblock_timestamp());

        for (const auto &kv: reads) {
            const std::string &k = kv.first;
            const std::string &value = kv.second.first;
            uint64_t rolling_hash = kv.second.second;
            proto::ReadReply *rreply = rw_commit_c_reply_.add_values();
            rreply->set_transaction_id(transaction_id);
            commit_ts.serialize(rreply->mutable_timestamp());
            rreply->set_key(k);
            rreply->set_val(value);
            rreply->set_rolling_hash(rolling_hash);
//            Debug("jenndebug [%lu] rreply %s, %s",transaction_id,  rreply->key().c_str(), rreply->val().c_str());
        }

        transport_->SendMessage(this, *remote, rw_commit_c_reply_);
        rw_commit_c_reply_.Clear();

        delete remote;
        delete reply;
        pending_rw_commit_c_replies_.erase(search);
    }

    void Server::SendRWCommmitCoordinatorReplyFail(const TransportAddress &remote,
                                                   uint64_t client_id,
                                                   uint64_t client_req_id) {
        rw_commit_c_reply_.mutable_rid()->set_client_id(client_id);
        rw_commit_c_reply_.mutable_rid()->set_client_req_id(client_req_id);
        rw_commit_c_reply_.set_status(REPLY_FAIL);
        rw_commit_c_reply_.clear_commit_timestamp();
        rw_commit_c_reply_.clear_nonblock_timestamp();

        transport_->SendMessage(this, remote, rw_commit_c_reply_);
    }

    void Server::SendPrepareOKRepliesOK(uint64_t transaction_id, const Timestamp &commit_ts) {
        auto search = pending_prepare_ok_replies_.find(transaction_id);
        if (search == pending_prepare_ok_replies_.end()) {
            //Debug("[%lu] No pending prepare ok reply found", transaction_id);
            return;
        }
        PendingPrepareOKReply *reply = search->second;

        prepare_ok_reply_.set_status(REPLY_OK);
        commit_ts.serialize(prepare_ok_reply_.mutable_commit_timestamp());

        for (auto &rid: reply->rids) {
            uint64_t client_id = rid.client_id();
            uint64_t client_req_id = rid.client_req_id();
            const TransportAddress *remote = rid.addr();

            prepare_ok_reply_.mutable_rid()->set_client_id(client_id);
            prepare_ok_reply_.mutable_rid()->set_client_req_id(client_req_id);

            transport_->SendMessage(this, *remote, prepare_ok_reply_);
            delete remote;
        }

        delete reply;
        pending_prepare_ok_replies_.erase(search);
    }

    void Server::SendPrepareOKRepliesFail(PendingPrepareOKReply *reply) {
        prepare_ok_reply_.set_status(REPLY_FAIL);
        prepare_ok_reply_.clear_commit_timestamp();

        for (auto &rid: reply->rids) {
            uint64_t client_id = rid.client_id();
            uint64_t client_req_id = rid.client_req_id();
            const TransportAddress *remote = rid.addr();

            prepare_ok_reply_.mutable_rid()->set_client_id(client_id);
            prepare_ok_reply_.mutable_rid()->set_client_req_id(client_req_id);

            transport_->SendMessage(this, *remote, prepare_ok_reply_);
            delete remote;
        }
    }

    void Server::CommitCoordinatorCallback(uint64_t transaction_id, transaction_status_t status) {
        ASSERT(status == REPLY_OK);

        Debug("[%lu] COMMIT callback: %d", transaction_id, status);
    }

    void Server::SendRWCommmitParticipantReplyOK(uint64_t transaction_id) {
        auto search = pending_rw_commit_p_replies_.find(transaction_id);
        ASSERT(search != pending_rw_commit_p_replies_.end());

        PendingRWCommitParticipantReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        rw_commit_p_reply_.mutable_rid()->set_client_id(client_id);
        rw_commit_p_reply_.mutable_rid()->set_client_req_id(client_req_id);
        rw_commit_p_reply_.set_status(REPLY_OK);

        transport_->SendMessage(this, *remote, rw_commit_p_reply_);

        delete remote;
        delete reply;
        pending_rw_commit_p_replies_.erase(search);
    }

    void Server::SendRWCommmitParticipantReplyFail(uint64_t transaction_id) {
        auto search = pending_rw_commit_p_replies_.find(transaction_id);
        ASSERT(search != pending_rw_commit_p_replies_.end());

        PendingRWCommitParticipantReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        rw_commit_p_reply_.mutable_rid()->set_client_id(client_id);
        rw_commit_p_reply_.mutable_rid()->set_client_req_id(client_req_id);
        rw_commit_p_reply_.set_status(REPLY_FAIL);

        transport_->SendMessage(this, *remote, rw_commit_p_reply_);

        delete remote;
        delete reply;
        pending_rw_commit_p_replies_.erase(search);
    }

    void Server::SendRWCommmitParticipantReplyFail(const TransportAddress &remote,
                                                   uint64_t client_id,
                                                   uint64_t client_req_id) {
        rw_commit_p_reply_.mutable_rid()->set_client_id(client_id);
        rw_commit_p_reply_.mutable_rid()->set_client_req_id(client_req_id);
        rw_commit_p_reply_.set_status(REPLY_FAIL);

        transport_->SendMessage(this, remote, rw_commit_p_reply_);
    }

    void Server::HandleRWCommitParticipant(const TransportAddress &remote, proto::RWCommitParticipant &msg) {
        uint64_t client_id = msg.rid().client_id();
        uint64_t client_req_id = msg.rid().client_req_id();

        uint64_t transaction_id = msg.transaction_id();
        int coordinator = msg.coordinator_shard();

        const Transaction transaction{msg.transaction()};
        const Timestamp nonblock_ts{msg.nonblock_timestamp()};

        Debug("[%lu] Participant for transaction", transaction_id);

        TransactionState s = transactions_.StartParticipantPrepare(transaction_id, coordinator, transaction,
                                                                   nonblock_ts);
        if (s == PREPARING) {
            LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
            if (ar.status == LockStatus::ACQUIRED) {
                ASSERT(ar.wound_rws.size() == 0);
                const Timestamp prepare_ts = GetPrepareTimestamp(client_id);

                transactions_.SetParticipantPrepareTimestamp(transaction_id, prepare_ts);

                auto reply = new PendingRWCommitParticipantReply(client_id, client_req_id, remote.clone());
                pending_rw_commit_p_replies_[transaction_id] = reply;

                // TODO: Handle timeout
//                replica_client_->Prepare(
//                        transaction_id, transaction, prepare_ts,
//                        coordinator, nonblock_ts,
//                        std::bind(&Server::PrepareCallback, this, transaction_id,
//                                  std::placeholders::_1, std::placeholders::_2),
//                        [](int, Timestamp) {}, PREPARE_TIMEOUT);
            } else if (ar.status == LockStatus::FAIL) {
                ASSERT(ar.wound_rws.size() == 0);
                //Debug("[%lu] Participant prepare failed", transaction_id);
                LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

                // TODO: Handle timeout
                shard_clients_[coordinator]->PrepareAbort(
                        transaction_id, shard_idx_,
                        std::bind(&Server::PrepareAbortCallback, this, transaction_id,
                                  placeholders::_1, placeholders::_2),
                        [](int, Timestamp) {}, PREPARE_TIMEOUT);

                // Reply to client
                SendRWCommmitParticipantReplyFail(remote, client_id, client_req_id);

                NotifyPendingRWs(transaction_id, rr.notify_rws);

                transactions_.AbortPrepare(transaction_id);
            } else if (ar.status == LockStatus::WAITING) {
                Debug("[%lu] Waiting", transaction_id);

                auto reply = new PendingRWCommitParticipantReply(client_id, client_req_id, remote.clone());
                pending_rw_commit_p_replies_[transaction_id] = reply;

                transactions_.PausePrepare(transaction_id);

                WoundPendingRWs(transaction_id, ar.wound_rws);
            } else {
                NOT_REACHABLE();
            }
        } else if (s == ABORTED) {
            //Debug("[%lu] Already aborted", transaction_id);

            // Reply to client
            SendRWCommmitParticipantReplyFail(remote, client_id, client_req_id);

        } else {
            NOT_REACHABLE();
        }
    }

    void Server::ContinueParticipantPrepare(uint64_t transaction_id) {
        auto search = pending_rw_commit_p_replies_.find(transaction_id);
        if (search == pending_rw_commit_p_replies_.end()) {
            return;
        }

        Debug("[%lu] Cont part prep", transaction_id);
        PendingRWCommitParticipantReply *reply = search->second;

        uint64_t client_id = reply->rid.client_id();
        uint64_t client_req_id = reply->rid.client_req_id();
        const TransportAddress *remote = reply->rid.addr();

        TransactionState s = transactions_.ContinuePrepare(transaction_id);
        if (s == PREPARING) {
            const int coordinator = transactions_.GetCoordinator(transaction_id);
            const Transaction &transaction = transactions_.GetTransaction(transaction_id);

            LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
            if (ar.status == LockStatus::ACQUIRED) {
                ASSERT(ar.wound_rws.size() == 0);
                const Timestamp prepare_ts = GetPrepareTimestamp(client_id);

                transactions_.SetParticipantPrepareTimestamp(transaction_id, prepare_ts);

                const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

                // TODO: Handle timeout
//                replica_client_->Prepare(
//                        transaction_id, transaction, prepare_ts,
//                        coordinator, nonblock_ts,
//                        std::bind(&Server::PrepareCallback, this, transaction_id,
//                                  std::placeholders::_1, std::placeholders::_2),
//                        [](int, Timestamp) {}, PREPARE_TIMEOUT);

            } else if (ar.status == LockStatus::FAIL) {
                ASSERT(ar.wound_rws.size() == 0);
                //Debug("[%lu] Participant prepare failed", transaction_id);
                LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

                // TODO: Handle timeout
                shard_clients_[coordinator]->PrepareAbort(
                        transaction_id, shard_idx_,
                        std::bind(&Server::PrepareAbortCallback, this, transaction_id,
                                  placeholders::_1, placeholders::_2),
                        [](int, Timestamp) {}, PREPARE_TIMEOUT);

                // Reply to client
                SendRWCommmitParticipantReplyFail(*remote, client_id, client_req_id);
                delete remote;
                delete reply;
                pending_rw_commit_p_replies_.erase(search);

                NotifyPendingRWs(transaction_id, rr.notify_rws);

                transactions_.AbortPrepare(transaction_id);
            } else if (ar.status == LockStatus::WAITING) {
                Debug("[%lu] Waiting", transaction_id);

                transactions_.PausePrepare(transaction_id);

                WoundPendingRWs(transaction_id, ar.wound_rws);
            } else {
                NOT_REACHABLE();
            }
        } else if (s == PREPARED || s == COMMITTING || s == COMMITTED) {
            Debug("[%lu] Already prepared", transaction_id);
        } else if (s == ABORTED) {  // Already aborted
            Debug("[%lu] Already aborted", transaction_id);
        } else {
            NOT_REACHABLE();
        }
    }

    void Server::PrepareCallback(uint64_t transaction_id, int status, Timestamp timestamp) {
        TransactionState s = transactions_.FinishParticipantPrepare(transaction_id);
        if (s == PREPARED) {
            int coordinator = transactions_.GetCoordinator(transaction_id);
            const Timestamp &prepare_ts = transactions_.GetPrepareTimestamp(transaction_id);
            const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);
            // TODO: Handle timeout
            shard_clients_[coordinator]->PrepareOK(
                    transaction_id, shard_idx_, prepare_ts, nonblock_ts,
                    std::bind(&Server::PrepareOKCallback, this, transaction_id,
                              placeholders::_1, placeholders::_2),
                    [](int, Timestamp) {}, PREPARE_TIMEOUT);

            // Reply to client
            SendRWCommmitParticipantReplyOK(transaction_id);

        } else if (s == ABORTED) {  // Already aborted

            SendRWCommmitParticipantReplyFail(transaction_id);

        } else {
            NOT_REACHABLE();
        }
    }

    void Server::PrepareOKCallback(uint64_t transaction_id, int status, Timestamp commit_ts) {
        Debug("[%lu] Received PREPARE_OK callback: %d %d", transaction_id, shard_idx_, status);

        if (status == REPLY_OK) {
            TransactionState s = transactions_.ParticipantReceivePrepareOK(transaction_id);
            ASSERT(s == COMMITTING);

            // TODO: Handle timeout
//            replica_client_->Commit(
//                    transaction_id, commit_ts,
//                    std::bind(&Server::CommitParticipantCallback, this, transaction_id, std::placeholders::_1),
//                    []() {}, COMMIT_TIMEOUT);

        } else if (status == REPLY_FAIL) {
            TransactionState s = transactions_.GetRWTransactionState(transaction_id);
            if (s == ABORTED) {
                Debug("[%lu] Already aborted", transaction_id);
                return;
            }

            ASSERT(s == PREPARED);

            const Transaction &transaction = transactions_.GetTransaction(transaction_id);

            LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
            TransactionFinishResult fr = transactions_.Abort(transaction_id);

            // TODO: Handle timeout
//            replica_client_->Abort(
//                    transaction_id,
//                    std::bind(&Server::AbortParticipantCallback, this, transaction_id),
//                    []() {}, ABORT_TIMEOUT);

            NotifyPendingRWs(transaction_id, rr.notify_rws);
            NotifyPendingROs(fr.notify_ros);
            NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);

        } else {
            NOT_REACHABLE();
        }
    }

    void Server::PrepareAbortCallback(uint64_t transaction_id, int status,
                                      Timestamp timestamp) {
        ASSERT(status == REPLY_OK);

        Debug("[%lu] Received PREPARE_ABORT callback: %d %d", transaction_id, shard_idx_, status);
    }

    void Server::CommitParticipantCallback(uint64_t transaction_id, transaction_status_t status) {
        ASSERT(status == REPLY_OK);

        Debug("[%lu] Received COMMIT participant callback: %d %d", transaction_id, status, shard_idx_);
    }

    void Server::AbortParticipantCallback(uint64_t transaction_id) {
        Debug("[%lu] Received ABORT participant callback: %d", transaction_id, shard_idx_);
    }

    void Server::HandlePrepareOK(const TransportAddress &remote, proto::PrepareOK &msg) {
        uint64_t client_id = msg.rid().client_id();
        uint64_t client_req_id = msg.rid().client_req_id();

        uint64_t transaction_id = msg.transaction_id();

        int participant_shard = msg.participant_shard();
        const Timestamp prepare_ts{msg.prepare_timestamp()};
        const Timestamp nonblock_ts{msg.nonblock_timestamp()};

        Debug("[%lu] Received Prepare OK", transaction_id);

        PendingPrepareOKReply *reply = nullptr;
        auto search = pending_prepare_ok_replies_.find(transaction_id);
        if (search == pending_prepare_ok_replies_.end()) {
            reply = new PendingPrepareOKReply(client_id, client_req_id, remote.clone());
            pending_prepare_ok_replies_[transaction_id] = reply;
        } else {
            reply = pending_prepare_ok_replies_[transaction_id];
        }

        // Check for duplicates
        if (reply->rids.count({client_id, client_req_id, nullptr}) == 0) {
            reply->rids.insert({client_id, client_req_id, remote.clone()});
        }

        TransactionState s = transactions_.CoordinatorReceivePrepareOK(transaction_id, participant_shard, prepare_ts,
                                                                       nonblock_ts);
        if (s == PREPARING) {
            Debug("[%lu] Coordinator preparing", transaction_id);

            const std::unordered_set<int> &participants = transactions_.GetParticipants(transaction_id);
            const Transaction &transaction = transactions_.GetTransaction(transaction_id);

            LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
            if (ar.status == LockStatus::ACQUIRED) {
                ASSERT(ar.wound_rws.size() == 0);
                const Timestamp prepare_ts = GetPrepareTimestamp(client_id);
                transactions_.FinishCoordinatorPrepare(transaction_id, prepare_ts);

                const Timestamp &commit_ts = transactions_.GetRWCommitTimestamp(transaction_id);
                const Timestamp &start_ts = transactions_.GetStartTimestamp(transaction_id);
                const Timestamp &nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

                // TODO: Handle timeout
//                replica_client_->CoordinatorCommit(
//                        transaction_id, start_ts, shard_idx_,
//                        participants, transaction, nonblock_ts, commit_ts,
//                        std::bind(&Server::CommitCoordinatorCallback, this,
//                                  transaction_id, std::placeholders::_1),
//                        []() {}, COMMIT_TIMEOUT);
            } else if (ar.status == FAIL) {
                ASSERT(ar.wound_rws.size() == 0);
                //Debug("[%lu] Coordinator prepare failed", transaction_id);
                LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

                // Reply to participants
                SendPrepareOKRepliesFail(reply);
                delete reply;
                pending_prepare_ok_replies_.erase(transaction_id);

                // Notify other participants
                SendAbortParticipants(transaction_id, participants);

                // Reply to client
                PendingRWCommitCoordinatorReply *cr = pending_rw_commit_c_replies_[transaction_id];
                uint64_t client_id = cr->rid.client_id();
                uint64_t client_req_id = cr->rid.client_req_id();
                const TransportAddress *addr = cr->rid.addr();
                SendRWCommmitCoordinatorReplyFail(*addr, client_id, client_req_id);
                delete addr;
                delete cr;
                pending_rw_commit_c_replies_.erase(transaction_id);

                // Notify waiting RW transactions
                NotifyPendingRWs(transaction_id, rr.notify_rws);

                transactions_.AbortPrepare(transaction_id);
            } else if (ar.status == WAITING) {
                Debug("[%lu] Waiting", transaction_id);

                transactions_.PausePrepare(transaction_id);

                WoundPendingRWs(transaction_id, ar.wound_rws);
            } else {
                NOT_REACHABLE();
            }

        } else if (s == ABORTED) {  // Already aborted
            Debug("[%lu] Already aborted", transaction_id);

            // Reply to participants
            SendPrepareOKRepliesFail(reply);
            delete reply;
            pending_prepare_ok_replies_.erase(transaction_id);

        } else if (s == WAIT_PARTICIPANTS) {
            Debug("[%lu] Waiting for other participants", transaction_id);
        } else {
            NOT_REACHABLE();
        }
    }

    void Server::HandlePrepareAbort(const TransportAddress &remote, proto::PrepareAbort &msg) {
        uint64_t transaction_id = msg.transaction_id();

        //Debug("[%lu] Received Prepare ABORT", transaction_id);

        prepare_abort_reply_.mutable_rid()->CopyFrom(msg.rid());

        TransactionState state = transactions_.GetRWTransactionState(transaction_id);
        if (state == NOT_FOUND) {
            //Debug("[%lu] Transaction not in progress", transaction_id);

            prepare_abort_reply_.set_status(REPLY_OK);
            transport_->SendMessage(this, remote, prepare_abort_reply_);

            TransactionFinishResult fr = transactions_.Abort(transaction_id);
            ASSERT(fr.notify_ros.size() == 0);
            ASSERT(fr.notify_slow_path_ros.size() == 0);
            return;
        }

        if (state == ABORTED) {  // Already aborted
            //Debug("[%lu] Transaction already aborted", transaction_id);

            prepare_abort_reply_.set_status(REPLY_OK);
            transport_->SendMessage(this, remote, prepare_abort_reply_);
            return;
        }

        ASSERT(state == READING || state == READ_WAIT || state == WAIT_PARTICIPANTS);

        // Release locks acquired during GETs
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);
        LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);

        // Reply to client
        auto search = pending_rw_commit_c_replies_.find(transaction_id);
        if (search != pending_rw_commit_c_replies_.end()) {
            PendingRWCommitCoordinatorReply *reply = search->second;

            uint64_t client_id = reply->rid.client_id();
            uint64_t client_req_id = reply->rid.client_req_id();
            const TransportAddress *addr = reply->rid.addr();

            SendRWCommmitCoordinatorReplyFail(*addr, client_id, client_req_id);

            std::unordered_set<int> participants = transactions_.GetParticipants(transaction_id);

            // Notify participants
            SendAbortParticipants(transaction_id, participants);

            delete addr;
            delete reply;
            pending_rw_commit_c_replies_.erase(search);
        }

        // Reply to OK participants
        auto search2 = pending_prepare_ok_replies_.find(transaction_id);
        if (search2 != pending_prepare_ok_replies_.end()) {
            PendingPrepareOKReply *reply = search2->second;
            SendPrepareOKRepliesFail(reply);
            delete reply;
            pending_prepare_ok_replies_.erase(search2);
        }

        prepare_abort_reply_.set_status(REPLY_OK);
        transport_->SendMessage(this, remote, prepare_abort_reply_);

        NotifyPendingRWs(transaction_id, rr.notify_rws);

        transactions_.Abort(transaction_id);
    }

    void Server::HandleWound(const TransportAddress &remote, proto::Wound &msg) {
        uint64_t transaction_id = msg.transaction_id();

        //Debug("[%lu] Received Wound request", transaction_id);

        TransactionState state = transactions_.GetRWTransactionState(transaction_id);

        if (state == ABORTED) {
            //Debug("[%lu] Transaction already aborted", transaction_id);
            return;
        }

        if (state == COMMITTING || state == COMMITTED) {
            //Debug("[%lu] Transaction already committing", transaction_id);
            return;
        }

        ASSERT(state != PREPARED);

        if (state == PREPARING || state == WAIT_PARTICIPANTS || state == PREPARE_WAIT) {
            // Only coordinator should handle wounds
            ASSERT(transactions_.GetCoordinator(transaction_id) == shard_idx_);

            // Reply to client
            auto search = pending_rw_commit_c_replies_.find(transaction_id);
            if (search != pending_rw_commit_c_replies_.end()) {
                PendingRWCommitCoordinatorReply *reply = search->second;

                uint64_t client_id = reply->rid.client_id();
                uint64_t client_req_id = reply->rid.client_req_id();
                const TransportAddress *addr = reply->rid.addr();

                SendRWCommmitCoordinatorReplyFail(*addr, client_id, client_req_id);

                delete addr;
                delete reply;
                pending_rw_commit_c_replies_.erase(search);
            }

            // Reply to OK participants
            auto search2 = pending_prepare_ok_replies_.find(transaction_id);
            if (search2 != pending_prepare_ok_replies_.end()) {
                PendingPrepareOKReply *reply = search2->second;
                SendPrepareOKRepliesFail(reply);
                delete reply;
                pending_prepare_ok_replies_.erase(search2);
            }

            const std::unordered_set<int> &participants = transactions_.GetParticipants(transaction_id);
            SendAbortParticipants(transaction_id, participants);
        }

        LockReleaseResult rr;
        TransactionFinishResult fr;

        // Coordinator may not yet know about this transaction
        // If so, no locks to release.
        if (state != NOT_FOUND) {
            const Transaction &transaction = transactions_.GetTransaction(transaction_id);
            rr = locks_.ReleaseLocks(transaction_id, transaction);
        }

        fr = transactions_.Abort(transaction_id);

        NotifyPendingRWs(transaction_id, rr.notify_rws);
        NotifyPendingROs(fr.notify_ros);
        NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);
    }

    void Server::HandleAbort(const TransportAddress &remote, proto::Abort &msg) {
        uint64_t transaction_id = msg.transaction_id();

        Debug("[%lu] Received Abort request", transaction_id);

        abort_reply_.mutable_rid()->CopyFrom(msg.rid());

        TransactionState state = transactions_.GetRWTransactionState(transaction_id);

        if (state == ABORTED) {
            //Debug("[%lu] Transaction already aborted", transaction_id);
            abort_reply_.set_status(REPLY_OK);
            transport_->SendMessage(this, remote, abort_reply_);
            return;
        }

        if (state == COMMITTING || state == COMMITTED) {
            //Debug("[%lu] Transaction already committing", transaction_id);
            abort_reply_.set_status(REPLY_FAIL);
            transport_->SendMessage(this, remote, abort_reply_);
            return;
        }

        LockReleaseResult rr;
        TransactionFinishResult fr;

        // Participant may not yet know about this transaction
        // If so, no locks to release.
        if (state != NOT_FOUND) {
            const Transaction &transaction = transactions_.GetTransaction(transaction_id);
            rr = locks_.ReleaseLocks(transaction_id, transaction);
        }

        fr = transactions_.Abort(transaction_id);

//        if (state == PREPARING || state == PREPARED) {
//            // TODO: Handle timeout
//            replica_client_->Abort(
//                    transaction_id,
//                    std::bind(&Server::AbortParticipantCallback, this, transaction_id),
//                    []() {}, ABORT_TIMEOUT);
//        }

        abort_reply_.set_status(REPLY_OK);
        transport_->SendMessage(this, remote, abort_reply_);

        // Reply to client for any ongoing GETs
        ContinueGet(transaction_id);

        NotifyPendingRWs(transaction_id, rr.notify_rws);
        NotifyPendingROs(fr.notify_ros);
        NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);
    }

    void Server::SendAbortParticipants(uint64_t transaction_id, const std::unordered_set<int> &participants) {
        for (int p: participants) {
            if (p != shard_idx_) {  // Don't send abort to self (coordinator)
                // TODO: Handle timeout
                shard_clients_[p]->Abort(
                        transaction_id,
                        [transaction_id]() { /*Debug("[%lu] Received ABORT participant callback", transaction_id);*/ },
                        []() {}, ABORT_TIMEOUT);
            }
        }
    }

    void Server::CoordinatorCommitTransaction(uint64_t transaction_id, const Timestamp commit_ts) {
        Debug("[%lu] Commiting", transaction_id);

        const Timestamp nonblock_ts = transactions_.GetNonBlockTimestamp(transaction_id);

        // Commit writes
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);
        for (auto &write: transaction.getWriteSet()) {
            store_.put(write.first, write.second, {commit_ts, transaction_id});
        }

        if (transaction.getWriteSet().size() > 0) {
            min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);
        }

        LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
        TransactionFinishResult fr = transactions_.Commit(transaction_id);

        // Reply to client
        SendRWCommmitCoordinatorReplyOK(transaction_id, commit_ts, nonblock_ts, {});

        // // Reply to participants
        // SendPrepareOKRepliesOK(transaction_id, commit_ts);
        //
        // // Continue waiting RW transactions
        // NotifyPendingRWs(transaction_id, rr.notify_rws);
        //
        // // Continue waiting RO transactions
        // NotifyPendingROs(fr.notify_ros);
        // NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, true, commit_ts);
    }

    void Server::ParticipantCommitTransaction(uint64_t transaction_id, const Timestamp commit_ts) {
        Debug("[%lu] Commiting", transaction_id);

        // Commit writes
        const Transaction &transaction = transactions_.GetTransaction(transaction_id);
        for (auto &write: transaction.getWriteSet()) {
            store_.put(write.first, write.second, {commit_ts, transaction_id});
        }

        if (transaction.getWriteSet().size() > 0) {
            min_prepare_timestamp_ = std::max(min_prepare_timestamp_, commit_ts);
        }

        LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
        TransactionFinishResult fr = transactions_.Commit(transaction_id);

        // Continue waiting RW transactions
        NotifyPendingRWs(transaction_id, rr.notify_rws);

        // Continue waiting RO transactions
        NotifyPendingROs(fr.notify_ros);
        NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, true, commit_ts);
    }

    void Server::LeaderUpcall(opnum_t opnum, const string &op, bool &replicate,
                              string &response) {
        Debug("Received LeaderUpcall: %lu %s", opnum, op.c_str());

        Request request;

        request.ParseFromString(op);

        switch (request.op()) {
            case strongstore::proto::Request::PREPARE:
            case strongstore::proto::Request::COMMIT:
            case strongstore::proto::Request::ABORT:
                replicate = true;
                response = op;
                break;
            default:
                Panic("Unrecognized operation.");
        }
    }

/* Gets called when a command is issued using client.Invoke(...) to this
 * replica group.
 * opnum is the operation number.
 * op is the request string passed by the client.
 * response is the reply which will be sent back to the client.
 */
    void Server::ReplicaUpcall(opnum_t opnum, const string &op, string &response) {
        Debug("Received Upcall: %lu %s", opnum, op.c_str());
        Request request;
        Reply reply;

        request.ParseFromString(op);

        int status = REPLY_OK;
        uint64_t transaction_id = request.txnid();

        if (request.op() == strongstore::proto::Request::PREPARE) {
            Debug("[%lu] Received PREPARE", transaction_id);

            // TransactionState s = transactions_.GetRWTransactionState(transaction_id);
            // if (s == ABORTED) {
            //     Debug("[%lu] Already aborted", transaction_id);
            //     status = REPLY_FAIL;
            // } else if (s == NOT_FOUND) {  // Replica prepare
            //     Debug("[%lu] Preparing", transaction_id);
            //     const Timestamp prepare_ts{request.prepare().timestamp()};
            //     int coordinator = request.prepare().coordinator();
            //     const Transaction transaction{request.prepare().txn()};
            //     const Timestamp nonblock_ts{request.prepare().nonblock_ts()};
            //
            //     s = transactions_.StartParticipantPrepare(transaction_id, coordinator, transaction, nonblock_ts);
            //     ASSERT(s == PREPARING);
            //
            //     LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
            //     ASSERT(ar.status == LockStatus::ACQUIRED);
            //
            //     transactions_.SetParticipantPrepareTimestamp(transaction_id, prepare_ts);
            //
            //     transactions_.FinishParticipantPrepare(transaction_id);
            // } else if (s == PREPARING || s == PREPARED) {
            //     Debug("[%lu] Already prepared", transaction_id);
            // } else {
            //     NOT_REACHABLE();
            // }
        } else if (request.op() == strongstore::proto::Request::COMMIT) {

            Debug("[%lu] Received COMMIT", transaction_id);

            const Timestamp commit_ts{request.commit().commit_timestamp()};

            if (request.has_prepare()) {  // Coordinator commit
                Debug("[%lu] Coordinator commit", transaction_id);

                if (transactions_.GetRWTransactionState(transaction_id) != COMMITTING) {
                    const Timestamp start_ts{request.prepare().timestamp()};

                    // const TrueTimeInterval now = tt_.Now();
                    // const Timestamp start_ts{now.mid(), client_id};
                    int coordinator = request.prepare().coordinator();
                    const std::unordered_set<int> participants{request.prepare().participants().begin(),
                                                               request.prepare().participants().end()};
                    const Transaction transaction{request.prepare().txn()};
                    const Timestamp nonblock_ts{request.prepare().nonblock_ts()};

                    ASSERT(coordinator == shard_idx_);

                    TransactionState s = transactions_.StartCoordinatorPrepare(transaction_id, start_ts, coordinator,
                                                                               participants, transaction, nonblock_ts);
                    // for (int p : participants) {
                    //     if (p != coordinator) {
                    //         s = transactions_.CoordinatorReceivePrepareOK(transaction_id, p, commit_ts, nonblock_ts);
                    //     }
                    // }
                    // ASSERT(s == PREPARING);

                    LockAcquireResult ar = locks_.AcquireLocks(transaction_id, transaction);
                    ASSERT(ar.status == LockStatus::ACQUIRED);

                    transactions_.FinishCoordinatorPrepare(transaction_id, commit_ts);
                } else {
                    Debug("[%lu] Already prepared", transaction_id);
                }

                uint64_t commit_wait_us = tt_.TimeToWaitUntilMicros(commit_ts.getTimestamp());
                Debug("[%lu] delaying commit by %lu us", transaction_id, commit_wait_us);
                transport_->TimerMicro(commit_wait_us,
                                       std::bind(&Server::CoordinatorCommitTransaction, this, transaction_id,
                                                 commit_ts));
            } else {  // Participant commit
                Debug("[%lu] Participant commit", transaction_id);
                // if (transactions_.GetRWTransactionState(transaction_id) != COMMITTING) {
                //     transactions_.ParticipantReceivePrepareOK(transaction_id);
                // }
                //
                // ParticipantCommitTransaction(transaction_id, commit_ts);
            }

        } else if (request.op() == strongstore::proto::Request::ABORT) {
            Debug("[%lu] Received ABORT", transaction_id);

            if (transactions_.GetRWTransactionState(transaction_id) != ABORTED) {  // replica abort
                const Transaction &transaction = transactions_.GetTransaction(transaction_id);

                LockReleaseResult rr = locks_.ReleaseLocks(transaction_id, transaction);
                TransactionFinishResult fr = transactions_.Abort(transaction_id);

                NotifyPendingRWs(transaction_id, rr.notify_rws);
                NotifyPendingROs(fr.notify_ros);
                NotifySlowPathROs(fr.notify_slow_path_ros, transaction_id, false);
            }
        } else {
            NOT_REACHABLE();
        }

        reply.set_status(status);
        reply.SerializeToString(&response);
    }

    void Server::UnloggedUpcall(const string &op, string &response) {
        NOT_IMPLEMENTED();
    }

    void Server::Load(const string &key, const string &value,
                      const Timestamp timestamp) {
        store_.put(key, value, {timestamp, 0});
    }

}  // namespace strongstore
