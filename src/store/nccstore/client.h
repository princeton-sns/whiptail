/***********************************************************************
 *
 * store/nccstore/client.h:
 *   NCC client with client-side transaction coordination
 *
 * Copyright 2024
 *
 **********************************************************************/

#ifndef _NCC_CLIENT_H_
#define _NCC_CLIENT_H_

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "lib/configuration.h"
#include "lib/latency.h"
#include "lib/transport.h"
#include "store/common/frontend/client.h"
#include "store/common/partitioner.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/nccstore/common.h"
#include "store/nccstore/shardclient.h"
#include "store/nccstore/ncc-proto.pb.h"

namespace nccstore {

class NCCSession : public ::Session {
public:
    NCCSession(rss::Session &&rss_session)
        : ::Session(std::move(rss_session)), transaction_id_(static_cast<uint64_t>(-1)),
          tx_ts_(0, 0), committed_(false), pending_commit_(false), 
          commit_outstanding_(0) {}

    uint64_t transaction_id() const { return transaction_id_; }
    const Timestamp &tx_ts() const { return tx_ts_; }
    bool is_committed() const { return committed_; }

    const std::set<int> &participants() const { return participants_; }
    const std::map<std::string, std::string> &reads() const { return reads_; }
    const std::map<std::string, std::string> &writes() const { return writes_; }

protected:
    friend class NCCClient;

    void start_transaction(uint64_t tid, const Timestamp &ts) {
        transaction_id_ = tid;
        tx_ts_ = ts;
        committed_ = false;
        participants_.clear();
        reads_.clear();
        writes_.clear();
        read_timestamps_.clear();
        write_timestamps_.clear();
        pending_commit_ = false;
        commit_outstanding_ = 0;
    }

    void add_participant(int shard) { participants_.insert(shard); }
    void add_read(const std::string &key, const std::string &value) {
        reads_[key] = value;
    }
    void add_write(const std::string &key, const std::string &value) {
        writes_[key] = value;
    }
    void add_read_timestamp(const std::string &key, const Timestamp &tw, const Timestamp &tr) {
        read_timestamps_[key] = std::make_pair(tw, tr);
    }
    void add_write_timestamp(const std::string &key, const Timestamp &tw, const Timestamp &tr) {
        write_timestamps_[key] = std::make_pair(tw, tr);
    }

    void set_committed(bool c) { committed_ = c; }

    uint64_t transaction_id_;
    Timestamp tx_ts_;
    bool committed_;
    std::set<int> participants_;
    std::map<std::string, std::string> reads_;
    std::map<std::string, std::string> writes_;
    std::map<std::string, std::pair<Timestamp, Timestamp>> read_timestamps_;  // (tw, tr)
    std::map<std::string, std::pair<Timestamp, Timestamp>> write_timestamps_; // (tw, tr)
    
    // Commit state (stored in session, not PendingRequest)
    bool pending_commit_;
    int commit_outstanding_;
    commit_callback commit_cb_;
    
    NCCSession() : ::Session(), transaction_id_(static_cast<uint64_t>(-1)),
                   tx_ts_(0, 0), committed_(false), pending_commit_(false), 
                   commit_outstanding_(0) {}
};

class NCCClient : public Client {
public:
    NCCClient(Consistency consistency,
              const transport::Configuration &config,
              uint64_t client_id,
              int num_shards,
              Transport *transport,
              Partitioner *part,
              TrueTime &tt,
              bool debug_stats);
    ~NCCClient();

    // Session management (required by Client)
    Session &BeginSession() override;
    Session &ContinueSession(rss::Session &session) override;
    rss::Session EndSession(Session &session) override;

    // Transaction APIs (override from Client)
    void Begin(Session &session, begin_callback bcb,
               begin_timeout_callback btcb, uint32_t timeout) override;
    
    void Retry(Session &session, begin_callback bcb,
               begin_timeout_callback btcb, uint32_t timeout) override;

    void Get(Session &s, const std::string &key, ::get_callback gcb,
             ::get_timeout_callback gtcb, uint32_t timeout) override;

    void GetForUpdate(Session &s, const std::string &key, ::get_callback gcb,
                      ::get_timeout_callback gtcb, uint32_t timeout) override;

    void Put(Session &s, const std::string &key, const std::string &value,
             put_callback pcb, put_timeout_callback ptcb,
             uint32_t timeout) override;

    void Commit(Session &s, commit_callback ccb,
                commit_timeout_callback ctcb, uint32_t timeout) override;

    void Abort(Session &s, abort_callback acb, abort_timeout_callback atcb,
               uint32_t timeout) override;

    void ForceAbort(uint64_t transaction_id) override;

    // Read-only transaction optimization
    void ROCommit(Session &s, const std::unordered_set<std::string> &keys,
                  commit_callback ccb, commit_timeout_callback ctcb,
                  uint32_t timeout) override;

    Stats &GetStats();

private:
    struct PendingRequest {
        uint64_t req_id;
        commit_callback ccb;
        commit_timeout_callback ctcb;
        int outstanding_responses;
        bool aborted;
        int smart_retry_attempts;
        bool is_read_only;

        PendingRequest(uint64_t rid) : req_id(rid), outstanding_responses(0),
                                       aborted(false), smart_retry_attempts(0),
                                       is_read_only(false) {}
    };

    // Safeguard check for natural consistency
    bool SafeguardCheck(NCCSession &session);

    // Smart retry when safeguard check fails (Algorithm 5.4)
    void TrySmartRetry(NCCSession &session, uint64_t req_id);

    // Send commit/abort decision to all participants
    void SendCommitDecision(NCCSession &session, bool commit, uint64_t req_id);

    // Callback handlers
    void HandleGetReply(NCCSession &session, uint64_t req_id,
                        int shard, int status, const proto::NCCGetReply &reply);
    void HandleExecuteReply(NCCSession &session, uint64_t req_id, 
                            int shard, int status, const proto::NCCExecuteReply &reply);
    void HandleCommitReply(uint64_t req_id, int shard, int status);

    // Read-only handlers
    void HandleReadOnlyReply(NCCSession &session, uint64_t req_id,
                             int shard, int status, const proto::NCCReadOnlyReply &reply);

    // Smart retry handlers
    void HandleSmartRetryReply(NCCSession &session, uint64_t req_id,
                               int shard, bool can_retry, const proto::NCCSmartRetryReply &reply);

    const transport::Configuration &config_;
    uint64_t client_id_;
    int num_shards_;
    Transport *transport_;
    Partitioner *part_;
    TrueTime &tt_;
    Consistency consistency_;
    bool debug_stats_;

    uint64_t next_transaction_id_;
    uint64_t last_req_id_;

    std::vector<ShardClient *> shard_clients_;

    std::unordered_map<uint64_t, PendingRequest *> pending_requests_;
    std::unordered_map<uint64_t, std::pair<std::string, ::get_callback>> pending_gets_;  // req_id -> (key, callback from Client interface)
    std::unordered_map<uint64_t, NCCSession> sessions_;  // Store sessions by id

    // per-shard tro: tw of the most recent committed write on each shard
    std::unordered_map<int, Timestamp> tro_per_shard_;

    Stats stats_;
    Latency_t op_lat_;
    Latency_t commit_lat_;
};

} // namespace nccstore

#endif /* _NCC_CLIENT_H_ */

