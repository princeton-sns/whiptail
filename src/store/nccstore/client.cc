/***********************************************************************
 *
 * store/nccstore/client.cc:
 *   NCC client implementation with client-side coordination
 *
 * Copyright 2024
 *
 **********************************************************************/

#include "store/nccstore/client.h"
#include "lib/message.h"
#include "store/common/common.h"

namespace nccstore {

using namespace std;

NCCClient::NCCClient(Consistency consistency,
                     const transport::Configuration &config,
                     uint64_t client_id,
                     int num_shards,
                     Transport *transport,
                     Partitioner *part,
                     TrueTime &tt,
                     bool debug_stats)
    : config_(config),
      client_id_(client_id),
      num_shards_(num_shards),
      transport_(transport),
      part_(part),
      tt_(tt),
      consistency_(consistency),
      debug_stats_(debug_stats),
      next_transaction_id_(client_id << 26),
      last_req_id_(0) {
    
    Debug("Initializing NCC client with id [%lu]", client_id_);

    // Create shard clients
    for (int i = 0; i < num_shards_; i++) {
        ShardClient *sc = new ShardClient(config_, transport_, client_id_, i);
        shard_clients_.push_back(sc);
    }

    if (debug_stats_) {
        _Latency_Init(&op_lat_, "op_lat");
        _Latency_Init(&commit_lat_, "commit_lat");
    }

    Debug("NCC client [%lu] created!", client_id_);
}

NCCClient::~NCCClient() {
    for (auto sc : shard_clients_) {
        delete sc;
    }

    for (auto &kv : pending_requests_) {
        delete kv.second;
    }

    if (debug_stats_) {
        Latency_Dump(&op_lat_);
        Latency_Dump(&commit_lat_);
    }
}

Session &NCCClient::BeginSession() {
    // Create a new session (will auto-assign id via rss::Session constructor)
    NCCSession session{};
    auto sid = session.id();
    
    Debug("BeginSession: created session %lu", sid);
    
    // Store session in map
    sessions_.emplace(sid, std::move(session));
    
    // Return reference to stored session
    return sessions_.find(sid)->second;
}

Session &NCCClient::ContinueSession(rss::Session &rss_session) {
    // Create NCCSession from rss::Session
    NCCSession session{std::move(rss_session)};
    auto sid = session.id();
    
    Debug("ContinueSession: session %lu", sid);
    
    // Store session in map
    sessions_.emplace(sid, std::move(session));
    
    // Return reference to stored session
    return sessions_.find(sid)->second;
}

rss::Session NCCClient::EndSession(Session &session) {
    auto sid = session.id();
    
    Debug("EndSession: session %lu", sid);
    
    // Remove from map and return rss::Session for continuation
    auto it = sessions_.find(sid);
    if (it != sessions_.end()) {
        rss::Session rss_session = std::move(it->second);
        sessions_.erase(it);
        return rss_session;
    }
    
    return rss::Session();
}

void NCCClient::GetForUpdate(Session &s, const string &key, get_callback gcb,
                              get_timeout_callback gtcb, uint32_t timeout) {
    // For NCC, GetForUpdate is similar to Get
    Get(s, key, gcb, gtcb, timeout);
}

void NCCClient::ForceAbort(uint64_t transaction_id) {
    // Force abort a transactiocln
    Debug("[%lu] ForceAbort", transaction_id);
}

void NCCClient::Begin(Session &session, begin_callback bcb,
                      begin_timeout_callback btcb, uint32_t timeout) {
    auto &ncc_session = static_cast<NCCSession &>(session);

    uint64_t tx_id = next_transaction_id_++;
    
    // Pre-assign timestamp at transaction start
    Timestamp tx_ts{tt_.Now().latest(), client_id_};

    Debug("[%lu] Begin, ts=%lu.%lu", tx_id, tx_ts.getTimestamp(), tx_ts.getID());

    ncc_session.start_transaction(tx_id, tx_ts);

    bcb();
}

void NCCClient::Retry(Session &session, begin_callback bcb,
                      begin_timeout_callback btcb, uint32_t timeout) {
    // For NCC, retry is similar to begin with a new timestamp
    Begin(session, bcb, btcb, timeout);
}

void NCCClient::Get(Session &s, const string &key, get_callback gcb,
                    get_timeout_callback gtcb, uint32_t timeout) {
    auto &session = static_cast<NCCSession &>(s);

    Debug("[%lu] GET %s", session.transaction_id(), key.c_str());

    // Determine shard for this key
    vector<int> participants_vec;
    int shard = (*part_)(key, num_shards_, -1, participants_vec);
    
    session.add_participant(shard);

    // In NCC, reads are buffered and executed at commit time
    // For now, we just track the read in the session
    // The actual read will happen during commit

    // Return immediately with empty value (will be filled at commit)
    Timestamp zero_ts(0, 0);
    gcb(REPLY_OK, key, "", zero_ts);
}

void NCCClient::Put(Session &s, const string &key, const string &value,
                    put_callback pcb, put_timeout_callback ptcb,
                    uint32_t timeout) {
    auto &session = static_cast<NCCSession &>(s);

    Debug("[%lu] PUT %s=%s", session.transaction_id(), key.c_str(), value.c_str());

    // Determine shard for this key
    vector<int> participants_vec;
    int shard = (*part_)(key, num_shards_, -1, participants_vec);
    
    session.add_participant(shard);
    session.add_write(key, value);

    // Return immediately
    pcb(REPLY_OK, key, value);
}

void NCCClient::Commit(Session &s, commit_callback ccb,
                       commit_timeout_callback ctcb, uint32_t timeout) {
    auto &session = static_cast<NCCSession &>(s);

    uint64_t tx_id = session.transaction_id();

    Debug("[%lu] COMMIT", tx_id);

    uint64_t req_id = last_req_id_++;
    PendingRequest *req = new PendingRequest(req_id);
    pending_requests_[req_id] = req;
    req->ccb = ccb;
    req->ctcb = ctcb;

    const auto &participants = session.participants();
    req->outstanding_responses = participants.size();

    Debug("[%lu] Participants: %lu shards", tx_id, participants.size());

    // Organize operations by shard
    map<int, vector<string>> shard_reads;
    map<int, map<string, string>> shard_writes;

    for (const auto &kv : session.reads()) {
        vector<int> participants_vec;
        int shard = (*part_)(kv.first, num_shards_, -1, participants_vec);
        shard_reads[shard].push_back(kv.first);
    }

    for (const auto &kv : session.writes()) {
        vector<int> participants_vec;
        int shard = (*part_)(kv.first, num_shards_, -1, participants_vec);
        shard_writes[shard][kv.first] = kv.second;
    }

    // Send execute to all participant shards
    for (int shard : participants) {
        vector<string> read_keys = shard_reads[shard];
        map<string, string> writes = shard_writes[shard];

        auto ecb = bind(&NCCClient::HandleExecuteReply, this, ref(session),
                       req_id, shard, placeholders::_1, placeholders::_2);
        auto etcb = [](int) {};

        shard_clients_[shard]->Execute(tx_id, session.tx_ts(), read_keys,
                                       writes, ecb, etcb, timeout);
    }
}

void NCCClient::HandleExecuteReply(NCCSession &session, uint64_t req_id,
                                   int shard, int status,
                                   const proto::NCCExecuteReply &reply) {
    Debug("[%lu] HandleExecuteReply from shard %d, status=%d",
          session.transaction_id(), shard, status);

    auto req_it = pending_requests_.find(req_id);
    if (req_it == pending_requests_.end()) {
        Warning("Received execute reply for unknown request %lu", req_id);
        return;
    }

    PendingRequest *req = req_it->second;

    if (status == STATUS_ABORT) {
        // Server aborted early
        Debug("[%lu] Server early abort from shard %d", session.transaction_id(), shard);
        req->aborted = true;
    }

    if (!req->aborted) {
        // Collect (tw, tr) pairs from this shard
        for (int i = 0; i < reply.reads_size(); i++) {
            const auto &read_result = reply.reads(i);
            Timestamp tw(read_result.tw());
            Timestamp tr(read_result.tr());
            session.add_read(read_result.key(), read_result.value());
            session.add_read_timestamp(read_result.key(), tw, tr);
        }

        for (int i = 0; i < reply.writes_size(); i++) {
            const auto &write_result = reply.writes(i);
            Timestamp tw(write_result.tw());
            Timestamp tr(write_result.tr());
            session.add_write_timestamp(write_result.key(), tw, tr);
        }
    }

    req->outstanding_responses--;

    if (req->outstanding_responses == 0) {
        // All Execute responses received, perform safeguard check
        bool commit = !req->aborted && SafeguardCheck(session);

        Debug("[%lu] Safeguard check result: %s", session.transaction_id(),
              commit ? "COMMIT" : "ABORT");

        if (!commit && req->smart_retry_attempts < 3) {
            // Safeguard check failed, try SmartRetry (Algorithm 5.1, Line 10)
            Debug("[%lu] Attempting SmartRetry (attempt %d)", 
                  session.transaction_id(), req->smart_retry_attempts + 1);
            TrySmartRetry(session, req_id);
            return; 
        }

        // Store callback in session
        session.commit_cb_ = req->ccb;

        // Send commit decision to all participants
        SendCommitDecision(session, commit, req_id);
        
        // Clean up PendingRequest
        delete req;
        pending_requests_.erase(req_it);
    }
}

bool NCCClient::SafeguardCheck(NCCSession &session) {
    // Safeguard check from Algorithm 5.1, Lines 18-27
    // Collect all tw and tr from t_pairs
    // Check if tw_max ≤ tr_min

    const auto &read_timestamps = session.read_timestamps_;
    const auto &write_timestamps = session.write_timestamps_;

    Timestamp tw_max(0, 0);
    Timestamp tr_min(UINT64_MAX, UINT64_MAX);

    // Collect all tw and tr from reads
    for (const auto &kv : read_timestamps) {
        Timestamp tw = kv.second.first;
        Timestamp tr = kv.second.second;
        tw_max = std::max(tw_max, tw);
        tr_min = std::min(tr_min, tr);
    }

    // Collect all tw and tr from writes
    for (const auto &kv : write_timestamps) {
        Timestamp tw = kv.second.first;
        Timestamp tr = kv.second.second;
        tw_max = std::max(tw_max, tw);
        tr_min = std::min(tr_min, tr);
    }

    // Safeguard check: tw_max ≤ tr_min
    bool ok = (tw_max <= tr_min);

    if (!ok) {
        Debug("[%lu] Safeguard check failed: tw_max=%lu.%lu > tr_min=%lu.%lu",
              session.transaction_id(),
              tw_max.getTimestamp(), tw_max.getID(),
              tr_min.getTimestamp(), tr_min.getID());
    } else {
        Debug("[%lu] Safeguard check passed: tw_max=%lu.%lu ≤ tr_min=%lu.%lu",
              session.transaction_id(),
              tw_max.getTimestamp(), tw_max.getID(),
              tr_min.getTimestamp(), tr_min.getID());
    }

    return ok;
}

void NCCClient::TrySmartRetry(NCCSession &session, uint64_t req_id) {
    uint64_t tx_id = session.transaction_id();

    // Generate new timestamp (Algorithm 5.1, Line 3: t.clk ← AsynchronyAwareTS)
    Timestamp new_ts{tt_.Now().latest(), client_id_};

    Debug("[%lu] TrySmartRetry with new_ts=%lu.%lu", tx_id,
          new_ts.getTimestamp(), new_ts.getID());

    auto req_it = pending_requests_.find(req_id);
    if (req_it == pending_requests_.end()) {
        return;
    }

    PendingRequest *req = req_it->second;
    req->smart_retry_attempts++;
    req->outstanding_responses = session.participants().size();

    // Collect read and write results
    std::vector<proto::NCCReadResult> reads;
    std::vector<proto::NCCWriteResult> writes;

    // Build read results from session
    for (const auto &kv : session.read_timestamps_) {
        proto::NCCReadResult read;
        read.set_key(kv.first);
        read.set_value(session.reads_.at(kv.first));
        kv.second.first.serialize(read.mutable_tw());
        kv.second.second.serialize(read.mutable_tr());
        reads.push_back(read);
    }

    // Build write results from session
    for (const auto &kv : session.write_timestamps_) {
        proto::NCCWriteResult write;
        write.set_key(kv.first);
        kv.second.first.serialize(write.mutable_tw());
        kv.second.second.serialize(write.mutable_tr());
        writes.push_back(write);
    }

    // Send SmartRetry to all participant shards
    for (int shard : session.participants()) {
        auto srcb = bind(&NCCClient::HandleSmartRetryReply, this, ref(session),
                        req_id, shard, placeholders::_1, placeholders::_2);
        auto srtcb = []() {};

        shard_clients_[shard]->SmartRetry(tx_id, new_ts, reads, writes,
                                          srcb, srtcb, 5000);
    }
}

void NCCClient::HandleSmartRetryReply(NCCSession &session, uint64_t req_id,
                                      int shard, bool can_retry,
                                      const proto::NCCSmartRetryReply &reply) {
    Debug("[%lu] HandleSmartRetryReply from shard %d, can_retry=%d",
          session.transaction_id(), shard, can_retry);

    auto req_it = pending_requests_.find(req_id);
    if (req_it == pending_requests_.end()) {
        Warning("Received smart retry reply for unknown request %lu", req_id);
        return;
    }

    PendingRequest *req = req_it->second;

    if (!can_retry) {
        // SmartRetry failed on at least one shard
        req->aborted = true;
    }

    req->outstanding_responses--;

    if (req->outstanding_responses == 0) {
        // All SmartRetry responses received
        if (!req->aborted) {
            // SmartRetry succeeded on all shards
            Debug("[%lu] SmartRetry succeeded on all shards", 
                  session.transaction_id());

            // Re-run safeguard check with updated timestamps
            bool commit = SafeguardCheck(session);

            if (!commit && req->smart_retry_attempts < 3) {
                // Still failed, try again
                TrySmartRetry(session, req_id);
                return;
            }

            // Store callback in session
            session.commit_cb_ = req->ccb;

            // Send final decision
            SendCommitDecision(session, commit, req_id);
        } else {
            // SmartRetry failed
            Debug("[%lu] SmartRetry failed, aborting", session.transaction_id());
            
            // Store callback in session
            session.commit_cb_ = req->ccb;
            
            SendCommitDecision(session, false, req_id);
        }

        // Clean up PendingRequest
        delete req;
        pending_requests_.erase(req_it);
    }
}

void NCCClient::SendCommitDecision(NCCSession &session, bool commit, uint64_t req_id) {
    uint64_t tx_id = session.transaction_id();
    uint64_t sid = session.id();
    
    int num_shards = session.participants().size();
    
    // Only set commit state if not already set (for cases where it was set earlier)
    if (!session.commit_cb_) {
        auto req_it = pending_requests_.find(req_id);
        if (req_it != pending_requests_.end()) {
            session.commit_cb_ = req_it->second->ccb;
        }
    }
    
    session.commit_outstanding_ = num_shards;
    session.pending_commit_ = commit;

    Debug("[%lu] Sending commit decision: %s to %lu shards",
          tx_id, commit ? "COMMIT" : "ABORT", num_shards);

      auto session_it = sessions_.find(sid);
            if (session_it == sessions_.end()) {
                Warning("Commit reply for unknown session %lu", sid);
                return;
            }
            
            NCCSession &sess = session_it->second;
            sess.commit_cb_ (::COMMITTED);

    
    for (int shard : session.participants()) {
        // auto ccb = [this, sid](int status) {
        //       Debug("[%lu] Commit reply from shard, status=%d", sid, status);
            
        //     auto session_it = sessions_.find(sid);
        //     if (session_it == sessions_.end()) {
        //         Warning("Commit reply for unknown session %lu", sid);
        //         return;
        //     }
            
        //     NCCSession &sess = session_it->second;
            
        //     // Decrement counter
        //     sess.commit_outstanding_--;
            
        //     if (sess.commit_outstanding_ == 0) {
        //         // All commit replies received, invoke callback
        //         if (sess.pending_commit_) {
        //             sess.commit_cb_(::COMMITTED);
        //         } else {
        //             sess.commit_cb_(::ABORTED_SYSTEM);
        //         }
                
        //         // Clean up commit state
        //         sess.commit_cb_ = commit_callback();
        //         return ;
        //     }
        //     return ;
        // };
        auto ccb = [](int){};
        auto ctcb = [](int) {};

        shard_clients_[shard]->Commit(tx_id, commit, ccb, ctcb, 5000);
    }
}

void NCCClient::Abort(Session &s, abort_callback acb,
                      abort_timeout_callback atcb, uint32_t timeout) {
    auto &session = static_cast<NCCSession &>(s);

    uint64_t tx_id = session.transaction_id();

    Debug("[%lu] ABORT", tx_id);

    // For Abort, we don't wait for replies, just send and call callback immediately
    // Send abort decision to all participants (fire and forget)
    for (int shard : session.participants()) {
        auto ccb = [](int) {};
        auto ctcb = [](int) {};
        shard_clients_[shard]->Commit(tx_id, false, ccb, ctcb, 5000);
    }

    session.set_committed(false);
    acb();
}

void NCCClient::ROCommit(Session &s, const unordered_set<string> &keys,
                         commit_callback ccb, commit_timeout_callback ctcb,
                         uint32_t timeout) {
    auto &session = static_cast<NCCSession &>(s);

    uint64_t tx_id = session.transaction_id();

    Debug("[%lu] ROCommit with %lu keys", tx_id, keys.size());

    // Assign snapshot timestamp
    Timestamp snapshot_ts{tt_.Now().latest(), client_id_};

    uint64_t req_id = last_req_id_++;
    PendingRequest *req = new PendingRequest(req_id);
    pending_requests_[req_id] = req;
    req->ccb = ccb;
    req->ctcb = ctcb;

    // Organize keys by shard
    map<int, vector<string>> shard_keys;
    for (const string &key : keys) {
        vector<int> participants_vec;
        int shard = (*part_)(key, num_shards_, -1, participants_vec);
        shard_keys[shard].push_back(key);
        session.add_participant(shard);
    }

    req->outstanding_responses = shard_keys.size();

    // Send read-only request to each shard
    for (const auto &kv : shard_keys) {
        int shard = kv.first;
        const vector<string> &shard_key_list = kv.second;

        auto rocb = bind(&NCCClient::HandleReadOnlyReply, this, ref(session),
                        req_id, shard, placeholders::_1, placeholders::_2);
        auto rotcb = [](int) {};

        shard_clients_[shard]->ReadOnly(tx_id, snapshot_ts, shard_key_list,
                                        rocb, rotcb, timeout);
    }
}

void NCCClient::HandleReadOnlyReply(NCCSession &session, uint64_t req_id,
                                    int shard, int status,
                                    const proto::NCCReadOnlyReply &reply) {
    Debug("[%lu] HandleReadOnlyReply from shard %d", session.transaction_id(), shard);

    auto req_it = pending_requests_.find(req_id);
    if (req_it == pending_requests_.end()) {
        Warning("Received read-only reply for unknown request %lu", req_id);
        return;
    }

    PendingRequest *req = req_it->second;

    // Collect read results
    for (int i = 0; i < reply.reads_size(); i++) {
        const auto &read_result = reply.reads(i);
        session.add_read(read_result.key(), read_result.value());
    }

    req->outstanding_responses--;

    if (req->outstanding_responses == 0) {
        // All responses received
        session.set_committed(true);
        req->ccb(::COMMITTED);

        delete req;
        pending_requests_.erase(req_it);
    }
}

Stats &NCCClient::GetStats() {
    return stats_;
}
} // namespace nccstore

