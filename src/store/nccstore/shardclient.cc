/***********************************************************************
 *
 * store/nccstore/shardclient.cc:
 *   NCC shard client implementation
 *
 * Copyright 2024
 *
 **********************************************************************/

#include "store/nccstore/shardclient.h"
#include "lib/message.h"
#include "store/nccstore/common.h"

namespace nccstore {

using namespace std;

ShardClient::ShardClient(const transport::Configuration &config,
                         Transport *transport,
                         uint64_t client_id,
                         int shard_idx)
    : config_(config),
      transport_(transport),
      client_id_(client_id),
      shard_idx_(shard_idx),
      replica_(0),
      last_req_id_(0) {
    
    // Register with transport for receiving replies
    transport_->Register(this, config_, shard_idx_, -1);
}

ShardClient::~ShardClient() {
    for (auto &kv : pending_executes_) {
        delete kv.second;
    }
    for (auto &kv : pending_commits_) {
        delete kv.second;
    }
    for (auto &kv : pending_readonly_) {
        delete kv.second;
    }
    for (auto &kv : pending_smart_retry_) {
        delete kv.second;
    }
}

void ShardClient::Execute(uint64_t tx_id,
                          const Timestamp &tx_ts,
                          const vector<string> &read_keys,
                          const map<string, string> &writes,
                          execute_callback ecb,
                          execute_timeout_callback etcb,
                          uint32_t timeout) {
    uint64_t req_id = last_req_id_++;

    Debug("[shard %d] Execute tx=%lu, req=%lu", shard_idx_, tx_id, req_id);

    PendingExecute *pe = new PendingExecute(tx_id, req_id);
    pe->ecb = ecb;
    pe->etcb = etcb;
    pending_executes_[req_id] = pe;

    execute_.Clear();
    execute_.mutable_rid()->set_client_id(client_id_);
    execute_.mutable_rid()->set_client_req_id(req_id);
    execute_.set_tx_id(tx_id);
    tx_ts.serialize(execute_.mutable_tx_ts());

    for (const string &key : read_keys) {
        execute_.add_read_keys(key);
    }

    for (const auto &kv : writes) {
        WriteMessage *write = execute_.add_writes();
        write->set_key(kv.first);
        write->set_value(kv.second);
    }

    // Send to closest replica in this shard
    transport_->SendMessageToReplica(this, shard_idx_, replica_, execute_);

    // TODO: Setup timeout
}

void ShardClient::Commit(uint64_t tx_id,
                         bool commit,
                         ncc_commit_callback ccb,
                         ncc_commit_timeout_callback ctcb,
                         uint32_t timeout) {
    uint64_t req_id = last_req_id_++;

    Debug("[shard %d] Commit tx=%lu, decision=%s", shard_idx_, tx_id,
          commit ? "COMMIT" : "ABORT");

    PendingCommit *pc = new PendingCommit(tx_id, req_id);
    pc->ccb = ccb;
    pc->ctcb = ctcb;
    pending_commits_[req_id] = pc;

    commit_.Clear();
    commit_.set_tx_id(tx_id);
    commit_.set_commit(commit);

    // Send to closest replica in this shard
    transport_->SendMessageToReplica(this, shard_idx_, replica_, commit_);

    // TODO: Setup timeout
}

void ShardClient::ReadOnly(uint64_t tx_id,
                           const Timestamp &snapshot_ts,
                           const vector<string> &keys,
                           readonly_callback rocb,
                           readonly_timeout_callback rotcb,
                           uint32_t timeout) {
    uint64_t req_id = last_req_id_++;

    Debug("[shard %d] ReadOnly tx=%lu", shard_idx_, tx_id);

    PendingReadOnly *pro = new PendingReadOnly(tx_id, req_id);
    pro->rocb = rocb;
    pro->rotcb = rotcb;
    pending_readonly_[req_id] = pro;

    readonly_.Clear();
    readonly_.mutable_rid()->set_client_id(client_id_);
    readonly_.mutable_rid()->set_client_req_id(req_id);
    readonly_.set_tx_id(tx_id);
    snapshot_ts.serialize(readonly_.mutable_snapshot_ts());

    for (const string &key : keys) {
        readonly_.add_keys(key);
    }

    // Send to closest replica in this shard
    transport_->SendMessageToReplica(this, shard_idx_, replica_, readonly_);

    // TODO: Setup timeout
}

void ShardClient::ReceiveMessage(const TransportAddress &remote,
                                 const string &type,
                                 const string &data,
                                 void *meta_data) {
    if (type == execute_reply_.GetTypeName()) {
        execute_reply_.ParseFromString(data);
        HandleExecuteReply(execute_reply_);
    } else if (type == commit_reply_.GetTypeName()) {
        commit_reply_.ParseFromString(data);
        HandleCommitReply(commit_reply_);
    } else if (type == readonly_reply_.GetTypeName()) {
        readonly_reply_.ParseFromString(data);
        HandleReadOnlyReply(readonly_reply_);
    } else if (type == smart_retry_reply_.GetTypeName()) {
        smart_retry_reply_.ParseFromString(data);
        HandleSmartRetryReply(smart_retry_reply_);
    } else {
        Panic("Received unexpected message type: %s", type.c_str());
    }
}

void ShardClient::HandleExecuteReply(const proto::NCCExecuteReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    Debug("[shard %d] HandleExecuteReply req=%lu, status=%d",
          shard_idx_, req_id, reply.status());

    auto it = pending_executes_.find(req_id);
    if (it == pending_executes_.end()) {
        Warning("Received execute reply for unknown request %lu", req_id);
        return;
    }

    PendingExecute *pe = it->second;
    pe->ecb(reply.status(), reply);

    delete pe;
    pending_executes_.erase(it);
}

void ShardClient::HandleCommitReply(const proto::NCCCommitReply &reply) {
    uint64_t tx_id = reply.tx_id();

    Debug("[shard %d] HandleCommitReply tx=%lu, status=%d",
          shard_idx_, tx_id, reply.status());

    // Find pending commit by tx_id
    // Note: we might need to track req_id better here
    for (auto it = pending_commits_.begin(); it != pending_commits_.end(); ++it) {
        if (it->second->tx_id == tx_id) {
            PendingCommit *pc = it->second;
            pc->ccb(reply.status());
            delete pc;
            pending_commits_.erase(it);
            return;
        }
    }

    Warning("Received commit reply for unknown transaction %lu", tx_id);
}

void ShardClient::HandleReadOnlyReply(const proto::NCCReadOnlyReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    Debug("[shard %d] HandleReadOnlyReply req=%lu, status=%d",
          shard_idx_, req_id, reply.status());

    auto it = pending_readonly_.find(req_id);
    if (it == pending_readonly_.end()) {
        Warning("Received read-only reply for unknown request %lu", req_id);
        return;
    }

    PendingReadOnly *pro = it->second;
    pro->rocb(reply.status(), reply);

    delete pro;
    pending_readonly_.erase(it);
}

void ShardClient::SmartRetry(uint64_t tx_id,
                             const Timestamp &new_ts,
                             const vector<proto::NCCReadResult> &reads,
                             const vector<proto::NCCWriteResult> &writes,
                             smart_retry_callback srcb,
                             smart_retry_timeout_callback srtcb,
                             uint32_t timeout) {
    uint64_t req_id = last_req_id_++;

    Debug("[shard %d] SmartRetry tx=%lu, new_ts=%lu.%lu", shard_idx_, tx_id,
          new_ts.getTimestamp(), new_ts.getID());

    PendingSmartRetry *psr = new PendingSmartRetry(tx_id, req_id);
    psr->srcb = srcb;
    psr->srtcb = srtcb;
    pending_smart_retry_[req_id] = psr;

    smart_retry_.Clear();
    smart_retry_.mutable_rid()->set_client_id(client_id_);
    smart_retry_.mutable_rid()->set_client_req_id(req_id);
    smart_retry_.set_tx_id(tx_id);
    new_ts.serialize(smart_retry_.mutable_new_ts());

    for (const auto &r : reads) {
        proto::NCCReadResult *read = smart_retry_.add_accessed_reads();
        read->CopyFrom(r);
    }

    for (const auto &w : writes) {
        proto::NCCWriteResult *write = smart_retry_.add_accessed_writes();
        write->CopyFrom(w);
    }

    // Send to closest replica in this shard
    transport_->SendMessageToReplica(this, shard_idx_, replica_, smart_retry_);

    // TODO: Setup timeout
}

void ShardClient::HandleSmartRetryReply(const proto::NCCSmartRetryReply &reply) {
    uint64_t req_id = reply.rid().client_req_id();

    Debug("[shard %d] HandleSmartRetryReply req=%lu, can_retry=%d",
          shard_idx_, req_id, reply.can_retry());

    auto it = pending_smart_retry_.find(req_id);
    if (it == pending_smart_retry_.end()) {
        Warning("Received smart retry reply for unknown request %lu", req_id);
        return;
    }

    PendingSmartRetry *psr = it->second;
    psr->srcb(reply.can_retry(), reply);

    delete psr;
    pending_smart_retry_.erase(it);
}

} // namespace nccstore

