/***********************************************************************
 *
 * store/nccstore/server.cc:
 *   NCC server implementation
 *
 * Copyright 2024
 *
 **********************************************************************/

#include "store/nccstore/server.h"
#include "lib/message.h"
#include "store/common/common.h"

namespace nccstore {

using namespace std;
using namespace proto;
using ::PingMessage;

NCCServer::NCCServer(Consistency consistency,
                     const transport::Configuration &shard_config,
                     const transport::Configuration &replica_config,
                     uint64_t server_id, int shard_idx, int replica_idx,
                     Transport *transport, const TrueTime &tt,
                     bool debug_stats, bool enable_replica)
    : PingServer(transport),
      replica_client_(nullptr),
      store_(),
      tt_(tt),
      shard_config_(shard_config),
      replica_config_(replica_config),
      transport_(transport),
      server_id_(server_id),
      shard_idx_(shard_idx),
      replica_idx_(replica_idx),
      consistency_(consistency),
      debug_stats_(debug_stats),
      enable_replica_(enable_replica) {
    
    transport_->Register(this, shard_config_, shard_idx_, replica_idx_);

    if (enable_replica_) {
        replica_client_ = new ReplicaClient(replica_config_, transport_, server_id_, shard_idx_);
    }

    if (debug_stats_) {
        _Latency_Init(&exec_lat_, "exec_lat");
        _Latency_Init(&commit_lat_, "commit_lat");
    }
}

NCCServer::~NCCServer() {
    if (enable_replica_) {
        delete replica_client_;
    }

    if (debug_stats_) {
        Latency_Dump(&exec_lat_);
        Latency_Dump(&commit_lat_);
    }
}

void NCCServer::ReceiveMessage(const TransportAddress &remote,
                                const string &type,
                                const string &data,
                                void *meta_data) {

    Debug("Received message type: %s", type.c_str());
    if (type == execute_.GetTypeName()) {
        execute_.ParseFromString(data);
        HandleExecute(remote, execute_);
    } else if (type == commit_.GetTypeName()) {
        commit_.ParseFromString(data);
        HandleCommit(remote, commit_);
    } else if (type == readonly_.GetTypeName()) {
        readonly_.ParseFromString(data);
        HandleReadOnly(remote, readonly_);
    } else if (type == PingMessage().GetTypeName()) {
        PingMessage ping_msg;
        ping_msg.ParseFromString(data);
        HandlePingMessage(this, remote, ping_msg);
    } else {
        Panic("Received unexpected message type: %s", type.c_str());
    }
}

void NCCServer::HandleExecute(const TransportAddress &remote, const NCCExecute &msg) {
    uint64_t tx_id = msg.tx_id();
    Timestamp tx_ts(msg.tx_ts());

    Debug("[%lu] HandleExecute, ts=%lu.%lu", tx_id, tx_ts.getTimestamp(), tx_ts.getID());

    TxnRecord &txn = transactions_[tx_id];
    txn.tx_id = tx_id;
    txn.tx_ts = tx_ts;
    txn.committed = false;
    txn.responded = false;
    txn.client_addr = remote.clone();
    txn.execute_msg.CopyFrom(msg);  // Store execute message for replication

    // Replicate Execute operation through VR
    if (enable_replica_) {
        replica_client_->Execute(
            tx_id, msg,
            bind(&NCCServer::ExecuteCallback, this, tx_id, placeholders::_1),
            []() {}, 5000);
    } else {
        // Execute directly if replication disabled
        ExecuteTransaction(msg, txn);
    }
}

void NCCServer::ExecuteTransaction(const NCCExecute &msg, TxnRecord &txn) {

    // For Leader, we don't need to execute the transaction again, finished
    if (txn.executed) {
        Debug("[%lu] Transaction already executed", txn.tx_id);
        return;
    }

    NCCExecuteReply reply;
    reply.mutable_rid()->set_client_id(msg.rid().client_id());
    reply.mutable_rid()->set_client_req_id(msg.rid().client_req_id());
    reply.set_status(STATUS_OK);

    bool should_abort = false;

    // Execute reads
    for (int i = 0; i < msg.read_keys_size(); i++) {
        const string &key = msg.read_keys(i);
        txn.read_set.insert(key);

        // Check for early abort
        if (CheckEarlyAbort(txn.tx_id, txn.tx_ts, key)) {
            Debug("[%lu] Early abort on read of key %s", txn.tx_id, key.c_str());
            should_abort = true;
            break;
        }

        // Read the version (Algorithm 5.2: curr_ver <- DS[req.key].most_recent)
        auto result = store_.Read(key, txn.tx_ts);
        
        NCCReadResult *read_result = reply.add_reads();
        read_result->set_key(key);
        
        if (result.first) {
            // Found a version
            Timestamp tw = result.second.second;  // tw of the version read
            
            // Algorithm 5.2: curr_ver.tr <- max{t, curr_ver.tr}
            store_.UpdateReadTimestamp(key, tw, txn.tx_ts);
            
        
            read_result->set_value(result.second.first);
            tw.serialize(read_result->mutable_tw());
            Timestamp updated_tr = std::max(txn.tx_ts, result.second.second);
            updated_tr.serialize(read_result->mutable_tr());
        } else {
            // Key doesn't exist or no version available
            read_result->set_value("");
            Timestamp zero_ts(0, 0);
            zero_ts.serialize(read_result->mutable_tw());
            txn.tx_ts.serialize(read_result->mutable_tr());
        }
    }

    if (should_abort) {
        reply.set_status(STATUS_ABORT);
        txn.committed = false;
        SendExecuteReply(*txn.client_addr, reply);
        return;
    }

    // Execute writes (non-blocking)
    for (int i = 0; i < msg.writes_size(); i++) {
        const WriteMessage &write = msg.writes(i);
        const string &key = write.key();
        const string &value = write.value();

        txn.write_set[key] = value;

        // Algorithm 5.2: 
        // curr_ver <- DS[req.key].most_recent
        // tw.clk <- max{t.clk, curr_ver.tr.clk+1}; tw.cid <- t.cid
        auto curr_ver = store_.GetMostRecentVersion(key);
        Timestamp tw;
        
        if (curr_ver.first) {
            // tw = max{t, curr_ver.tr + 1}
            Timestamp tr_plus_one(curr_ver.second.tr.getTimestamp() + 1, 
                                  txn.tx_ts.getID());
            tw = std::max(txn.tx_ts, tr_plus_one);
        } else {
            // No existing version, use t
            tw = txn.tx_ts;
        }

        // Algorithm 5.2:
        // tr <- tw
        // new_ver <- [req.value, (tw, tr), "undecided"]
        // DS[req.key] <- DS[req.key] + new_ver

        Debug("[%lu] Getting versions for key %s ********************************** before write", txn.tx_id, key.c_str());
        auto versions = store_.GetVersions(key);
        Debug("[%lu] Versions: %d", txn.tx_id, versions.size());
        for (const auto& version : versions) {
            Debug("[%lu] Version: %s, tw=%lu.%lu, tr=%lu.%lu, status=%d", txn.tx_id, version.value.c_str(), version.tw.getTimestamp(), version.tw.getID(), version.tr.getTimestamp(), version.tr.getID(), version.status);
        }
        Debug("[%lu] End of versions for key %s ********************************** before write", txn.tx_id, key.c_str());


        Debug("[%lu] Writing key %s, value %s, tw=%lu.%lu", txn.tx_id, key.c_str(), value.c_str(), tw.getTimestamp(), tw.getID());
        store_.Write(key, value, tw);

        Debug("[%lu] Getting versions for key %s ********************************** after write", txn.tx_id, key.c_str());
        versions = store_.GetVersions(key);
        Debug("[%lu] Versions: %d", txn.tx_id, versions.size());
        for (const auto& version : versions) {
            Debug("[%lu] Version: %s, tw=%lu.%lu, tr=%lu.%lu, status=%d", txn.tx_id, version.value.c_str(), version.tw.getTimestamp(), version.tw.getID(), version.tr.getTimestamp(), version.tr.getID(), version.status);
        }
        Debug("[%lu] End of versions for key %s ********************************** after write", txn.tx_id, key.c_str());

        // Algorithm 5.2: resp <- ["done", (tw, tr)]
        NCCWriteResult *write_result = reply.add_writes();
        write_result->set_key(key);
        tw.serialize(write_result->mutable_tw());
        tw.serialize(write_result->mutable_tr());  // tr = tw initially
    }

    if (should_abort) {
        reply.set_status(STATUS_ABORT);
        txn.committed = false;
        // Remove all versions written by this transaction
        for (const auto& kv : txn.write_set) {
            store_.RemoveVersion(kv.first, txn.tx_ts);
        }
        SendExecuteReply(*txn.client_addr, reply);
        return;
    }

    // Save reply for Response Timing Control
    txn.reply = reply;
    txn.executed = true;
    // RTC Debug Switch: ENABLE_RTC can be toggled in server.h
    if (ENABLE_RTC) {
        // RTC enabled: Add to response queues and check dependencies
        for (const string &key : txn.read_set) {
            PendingResponse pr;
            pr.tx_id = txn.tx_id;
            pr.key = key;
            pr.tw = txn.tx_ts;
            response_queues_[key].push(pr);
        }

        for (const auto &kv : txn.write_set) {
            PendingResponse pr;
            pr.tx_id = txn.tx_id;
            pr.key = kv.first;
            pr.tw = txn.tx_ts;
            response_queues_[kv.first].push(pr);
        }

        // Try to send response immediately if no dependencies
        for (const string &key : txn.read_set) {
            CheckAndSendResponse(key);
        }
        for (const auto &kv : txn.write_set) {
            CheckAndSendResponse(kv.first);
        }
    } else {
        // RTC disabled: Send response immediately for performance testing
        Debug("[%lu] RTC disabled, sending response immediately", txn.tx_id);
        SendExecuteReply(*txn.client_addr, reply);
        txn.responded = true;
    }
}

bool NCCServer::CheckEarlyAbort(uint64_t tx_id, const Timestamp &tx_ts, const string &key) {
    // Get undecided versions (from Algorithm 5.2)
    auto undecided = store_.GetUndecidedVersions(key, Timestamp(0, 0));
    
    for (const auto &v : undecided) {
        if (v.tw < tx_ts) {
            // There's an earlier undecided write, must abort this transaction
            return true;
        }
    }

    return false;
}

void NCCServer::CheckAndSendResponse(const string &key) {
    auto &queue = response_queues_[key];
    
    while (!queue.empty()) {
        PendingResponse &pr = queue.front();
        
        Debug("[%lu] Checking if all preceding writes are committed for key %s, tw=%lu.%lu", pr.tx_id, key.c_str(), pr.tw.getTimestamp(), pr.tw.getID());
        // Check if all preceding writes are committed
        if (AllPrecedingCommitted(key, pr.tx_id, pr.tw)) {
            auto txn_it = transactions_.find(pr.tx_id);
            if (txn_it != transactions_.end() && !txn_it->second.responded) {
                SendExecuteReply(*txn_it->second.client_addr, txn_it->second.reply);
                txn_it->second.responded = true;
            }
            queue.pop();
        } else {
            // Can't send yet, wait for dependencies
            break;
        }
    }
}

bool NCCServer::AllPrecedingCommitted(const string &key, uint64_t tx_id, const Timestamp &tx_ts) {
    // Check if all writes to this key with timestamp < tx_ts are committed
    auto undecided = store_.GetUndecidedVersions(key, Timestamp(0, 0));
    
    for (const auto &v : undecided) {
        if (v.tw < tx_ts) {
            // There's an earlier undecided write
            Debug("[%lu] Waiting for preceding undecided write with tw=%lu.%lu on key %s",
                  tx_id, v.tw.getTimestamp(), v.tw.getID(), key.c_str());
            return false;
        }
    }

    return true;
}

void NCCServer::HandleCommit(const TransportAddress &remote, const NCCCommit &msg) {
    uint64_t tx_id = msg.tx_id();
    bool commit = msg.commit();

    Debug("[%lu] HandleCommit, decision=%s", tx_id, commit ? "COMMIT" : "ABORT");

    auto txn_it = transactions_.find(tx_id);
    if (txn_it == transactions_.end()) {
        Warning("[%lu] Commit for unknown transaction", tx_id);
        SendCommitReply(remote, tx_id, STATUS_OK);
        return;
    }

    // Replicate Commit/Abort decision through VR
    if (enable_replica_) {
        replica_client_->CommitOrAbort(
            tx_id, commit,
            bind(&NCCServer::CommitCallback, this, tx_id, placeholders::_1),
            []() {}, 5000);
    } else {
        // Execute directly if replication disabled
        if (commit) {
            CommitTransaction(tx_id);
        } else {
            AbortTransaction(tx_id);
        }

        SendCommitReply(remote, tx_id, STATUS_OK);

        // Notify waiting transactions
        for (const string &key : txn_it->second.read_set) {
            NotifyWaitingTransactions(key);
        }
        for (const auto &kv : txn_it->second.write_set) {
            NotifyWaitingTransactions(kv.first);
        }
    }
}

void NCCServer::CommitTransaction(uint64_t tx_id) {
    auto txn_it = transactions_.find(tx_id);
    if (txn_it == transactions_.end()) {
        return;
    }

    if (txn_it->second.committed) {
        Debug("[%lu] Transaction already committed", tx_id);
        return;
    }

    // Mark all versions of this transaction as committed
    for (const auto& kv : txn_it->second.write_set) {
        const std::string& key = kv.first;
        // Set status to committed for version with tw = txn.tx_ts
        Debug("[%lu] Setting key %s version %lu.%lu as committed", tx_id, key.c_str(), txn_it->second.tx_ts.getTimestamp(), txn_it->second.tx_ts.getID());
        store_.SetCommitted(key, txn_it->second.tx_ts);

        Debug("[%lu] Getting versions for key %s **********************************", tx_id, key.c_str());
        auto versions = store_.GetVersions(key);
        Debug("[%lu] Versions: %d", tx_id, versions.size());
        for (const auto& version : versions) {
            Debug("[%lu] Version: %s, tw=%lu.%lu, tr=%lu.%lu, status=%d", tx_id, version.value.c_str(), version.tw.getTimestamp(), version.tw.getID(), version.tr.getTimestamp(), version.tr.getID(), version.status);
        }
        Debug("[%lu] End of versions for key %s **********************************", tx_id, key.c_str());
    }

    txn_it->second.committed = true;

    Debug("[%lu] Transaction committed", tx_id);
}

void NCCServer::AbortTransaction(uint64_t tx_id) {
    auto txn_it = transactions_.find(tx_id);
    if (txn_it == transactions_.end()) {
        return;
    }

    // Remove all versions of this transaction
    for (const auto& kv : txn_it->second.write_set) {
        const std::string& key = kv.first;
        // Remove version with tw = txn.tx_ts
        store_.RemoveVersion(key, txn_it->second.tx_ts);
    }

    txn_it->second.committed = false;

    Debug("[%lu] Transaction aborted", tx_id);
}

void NCCServer::NotifyWaitingTransactions(const string &key) {
    CheckAndSendResponse(key);
}

void NCCServer::HandleReadOnly(const TransportAddress &remote, const NCCReadOnly &msg) {
    uint64_t tx_id = msg.tx_id();
    Timestamp snapshot_ts(msg.snapshot_ts());

    Debug("[%lu] HandleReadOnly, snapshot_ts=%lu.%lu", tx_id,
          snapshot_ts.getTimestamp(), snapshot_ts.getID());

    NCCReadOnlyReply reply;
    reply.mutable_rid()->set_client_id(msg.rid().client_id());
    reply.mutable_rid()->set_client_req_id(msg.rid().client_req_id());
    reply.set_status(STATUS_OK);

    // Read all requested keys at snapshot timestamp
    for (int i = 0; i < msg.keys_size(); i++) {
        const string &key = msg.keys(i);
        
        auto result = store_.Read(key, snapshot_ts);
        
        NCCReadResult *read_result = reply.add_reads();
        read_result->set_key(key);
        
        if (result.first) {
            // Found a version
            read_result->set_value(result.second.first);
            result.second.second.serialize(read_result->mutable_tw());
        } else {
            // Key doesn't exist
            read_result->set_value("");
            Timestamp zero_ts(0, 0);
            zero_ts.serialize(read_result->mutable_tw());
        }
        
        snapshot_ts.serialize(read_result->mutable_tr());
    }

    SendReadOnlyReply(remote, reply);
}

void NCCServer::SendExecuteReply(const TransportAddress &remote, const NCCExecuteReply &reply) {
    transport_->SendMessage(this, remote, reply);
}

void NCCServer::SendCommitReply(const TransportAddress &remote, uint64_t tx_id, int status) {
    NCCCommitReply reply;
    reply.set_tx_id(tx_id);
    reply.set_status(status);
    transport_->SendMessage(this, remote, reply);
}

void NCCServer::SendReadOnlyReply(const TransportAddress &remote, const NCCReadOnlyReply &reply) {
    transport_->SendMessage(this, remote, reply);
}

void NCCServer::SendSmartRetryReply(const TransportAddress &remote, const NCCSmartRetryReply &reply) {
    transport_->SendMessage(this, remote, reply);
}

void NCCServer::HandleSmartRetry(const TransportAddress &remote, const proto::NCCSmartRetry &msg) {
    uint64_t tx_id = msg.tx_id();
    Timestamp new_ts(msg.new_ts());

    Debug("[%lu] HandleSmartRetry, new_ts=%lu.%lu", tx_id,
          new_ts.getTimestamp(), new_ts.getID());

    // Convert proto messages to vectors
    std::vector<proto::NCCReadResult> reads;
    std::vector<proto::NCCWriteResult> writes;
    
    for (int i = 0; i < msg.accessed_reads_size(); i++) {
        reads.push_back(msg.accessed_reads(i));
    }
    for (int i = 0; i < msg.accessed_writes_size(); i++) {
        writes.push_back(msg.accessed_writes(i));
    }

    // Execute SmartRetry algorithm
    bool can_retry = SmartRetry(tx_id, new_ts, reads, writes);

    // Send reply
    NCCSmartRetryReply reply;
    reply.mutable_rid()->set_client_id(msg.rid().client_id());
    reply.mutable_rid()->set_client_req_id(msg.rid().client_req_id());
    reply.set_can_retry(can_retry);

    SendSmartRetryReply(remote, reply);
}

bool NCCServer::SmartRetry(uint64_t tx_id, const Timestamp &new_ts,
                            const std::vector<proto::NCCReadResult> &reads,
                            const std::vector<proto::NCCWriteResult> &writes) {
    // Algorithm 5.4: SmartRetry

    auto txn_it = transactions_.find(tx_id);
    if (txn_it == transactions_.end()) {
        return false;
    }

    TxnRecord &txn = txn_it->second;

    // Foreach ver accessed by tx do
    // Check reads
    for (const auto &read_result : reads) {
        const string &key = read_result.key();
        Timestamp tw(read_result.tw());
        Timestamp tr(read_result.tr());

        // Algorithm 5.4: next_ver <- ver.next()
        auto next_ver = store_.GetNextVersion(key, tw);
        
        // Algorithm 5.4: if next_ver.tw <= t' then return false
        if (next_ver.first && next_ver.second.tw <= new_ts) {
            Debug("[%lu] SmartRetry failed: next version exists with tw=%lu.%lu ≤ t'=%lu.%lu",
                  tx_id, next_ver.second.tw.getTimestamp(), next_ver.second.tw.getID(),
                  new_ts.getTimestamp(), new_ts.getID());
            return false;
        }

        // Check if ver created by tx (it's in write_set)
        bool created_by_tx = (txn.write_set.find(key) != txn.write_set.end());

        // Algorithm 5.4: if ver created by tx and ver.tw != ver.tr then return false
        if (created_by_tx && tw != tr) {
            Debug("[%lu] SmartRetry failed: version %s created by tx but tw!=tr",
                  tx_id, key.c_str());
            return false;
        }

        // Algorithm 5.4: Update timestamps
        if (created_by_tx) {
            // Algorithm 5.4: ver.tw <- t'; ver.tr <- t'
            store_.UpdateVersionTimestamps(key, tw, new_ts, new_ts);
            Debug("[%lu] SmartRetry: updated version %s, tw=tr=%lu.%lu",
                  tx_id, key.c_str(), new_ts.getTimestamp(), new_ts.getID());
        } else {
            // Algorithm 5.4: ver.tr <- max{ver.tr, t'}
            Timestamp new_tr = std::max(tr, new_ts);
            store_.UpdateReadTimestamp(key, tw, new_tr);
            Debug("[%lu] SmartRetry: updated tr for %s to %lu.%lu",
                  tx_id, key.c_str(), new_tr.getTimestamp(), new_tr.getID());
        }
    }

    // Check writes (versions created by tx)
    for (const auto &write_result : writes) {
        const string &key = write_result.key();
        Timestamp tw(write_result.tw());
        Timestamp tr(write_result.tr());

        // Get next version
        auto next_ver = store_.GetNextVersion(key, tw);
        
        if (next_ver.first && next_ver.second.tw <= new_ts) {
            Debug("[%lu] SmartRetry failed: next version exists after write to %s",
                  tx_id, key.c_str());
            return false;
        }

        // Writes are always created by tx
        // Check if tw != tr
        if (tw != tr) {
            Debug("[%lu] SmartRetry failed: write version %s has tw!=tr",
                  tx_id, key.c_str());
            return false;
        }

        // Update version: ver.tw <- t'; ver.tr <- t'
        store_.UpdateVersionTimestamps(key, tw, new_ts, new_ts);
        Debug("[%lu] SmartRetry: updated write version %s, tw=tr=%lu.%lu",
              tx_id, key.c_str(), new_ts.getTimestamp(), new_ts.getID());
    }

    // Algorithm 5.4: return true
    Debug("[%lu] SmartRetry succeeded, can retry with new_ts=%lu.%lu",
          tx_id, new_ts.getTimestamp(), new_ts.getID());
    return true;
}

void NCCServer::Load(const string &key, const string &value, const Timestamp timestamp) {
    // Load initial data as committed version
    store_.Write(key, value, timestamp);
    store_.SetCommitted(key, timestamp);
}

Stats &NCCServer::GetStats() {
    return stats_;
}

void NCCServer::ExecuteCallback(uint64_t tx_id, int status) {
    Debug("[%lu] Execute replicated, status=%d", tx_id, status);

    auto txn_it = transactions_.find(tx_id);
    if (txn_it == transactions_.end()) {
        Warning("[%lu] Execute callback for unknown transaction", tx_id);
        return;
    }

    // Now execute the transaction (after replication)
    ExecuteTransaction(txn_it->second.execute_msg, txn_it->second);
}

void NCCServer::CommitCallback(uint64_t tx_id, int status) {
    Debug("[%lu] Commit replicated, status=%d", tx_id, status);

    auto txn_it = transactions_.find(tx_id);
    if (txn_it == transactions_.end()) {
        Warning("[%lu] Commit callback for unknown transaction", tx_id);
        return;
    }

    // After replication, send reply to client and notify waiters
    if (txn_it->second.client_addr) {
        SendCommitReply(*txn_it->second.client_addr, tx_id, STATUS_OK);
    }

    // Notify waiting transactions
    for (const string &key : txn_it->second.read_set) {
        NotifyWaitingTransactions(key);
    }
    for (const auto &kv : txn_it->second.write_set) {
        NotifyWaitingTransactions(kv.first);
    }
}

void NCCServer::LeaderUpcall(opnum_t opnum, const string &op, bool &replicate, string &response) {
    // For VR integration
    proto::Request request;
    request.ParseFromString(op);

    switch (request.op()) {
    case proto::Request::EXECUTE:
    case proto::Request::COMMIT:
        replicate = true;
        response = op;
        break;
    default:
        Panic("Unrecognized operation.");
    }
}

void NCCServer::ReplicaUpcall(opnum_t opnum, const string &op, string &response) {
    // For VR integration - execute replicated operations
    proto::Request request;
    proto::Reply reply;

    request.ParseFromString(op);

    int status = STATUS_OK;
    uint64_t tx_id = request.txnid();

    if (request.op() == proto::Request::EXECUTE) {
        Debug("[%lu] Replica received EXECUTE", tx_id);
        
        // Execute the operation
        if (request.has_execute()) {
            const proto::NCCExecute &execute_msg = request.execute();
            
            auto txn_it = transactions_.find(tx_id);
            if (txn_it != transactions_.end()) {
                // Execute transaction
                ExecuteTransaction(execute_msg, txn_it->second);
            }
        }
    } else if (request.op() == proto::Request::COMMIT) {
        Debug("[%lu] Replica received COMMIT", tx_id);
        
        if (request.has_commit()) {
            const proto::NCCCommit &commit_msg = request.commit();
            
            if (commit_msg.commit()) {
                CommitTransaction(tx_id);
            } else {
                AbortTransaction(tx_id);
            }
        }
    }

    reply.set_status(status);
    reply.SerializeToString(&response);
}

void NCCServer::UnloggedUpcall(const string &op, string &response) {
    // Handle unlogged operations
    response = "";
}

} // namespace nccstore

