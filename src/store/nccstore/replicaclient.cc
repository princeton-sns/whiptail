/***********************************************************************
 *
 * store/nccstore/replicaclient.cc:
 *   NCC replica client implementation
 *
 * Copyright 2024
 *
 **********************************************************************/

#include "store/nccstore/replicaclient.h"
#include "lib/configuration.h"
#include "lib/message.h"

namespace nccstore {

using namespace std;

ReplicaClient::ReplicaClient(const transport::Configuration &config,
                             Transport *transport,
                             uint64_t client_id,
                             int shard)
    : config_(config),
      transport_(transport),
      client_id_(client_id),
      shard_idx_(shard),
      last_req_id_(0) {
    
    vr_client_ = new replication::vr::VRClient(config_, transport_, shard_idx_, client_id_);
}

ReplicaClient::~ReplicaClient() {
    delete vr_client_;
    
    for (auto &kv : pending_requests_) {
        delete kv.second;
    }
}

void ReplicaClient::Execute(uint64_t tx_id,
                            const proto::NCCExecute &execute_msg,
                            replica_callback rcb,
                            replica_timeout_callback rtcb,
                            uint32_t timeout) {
    Debug("[shard %d] Replicating EXECUTE: %lu", shard_idx_, tx_id);

    // Create request for VR
    string request_str;
    proto::Request request;
    request.set_op(proto::Request::EXECUTE);
    request.set_txnid(tx_id);
    request.mutable_execute()->CopyFrom(execute_msg);
    request.SerializeToString(&request_str);

    uint64_t req_id = last_req_id_++;
    PendingRequest *pending = new PendingRequest(req_id);
    pending_requests_[req_id] = pending;
    pending->rcb = rcb;
    pending->rtcb = rtcb;

    vr_client_->Invoke(request_str,
                       bind(&ReplicaClient::ReplicaCallback, this, req_id,
                            placeholders::_1, placeholders::_2));
}

void ReplicaClient::CommitOrAbort(uint64_t tx_id,
                                  bool commit,
                                  replica_callback rcb,
                                  replica_timeout_callback rtcb,
                                  uint32_t timeout) {
    Debug("[shard %d] Replicating %s: %lu", shard_idx_, 
          commit ? "COMMIT" : "ABORT", tx_id);

    // Create request for VR
    string request_str;
    proto::Request request;
    request.set_op(proto::Request::COMMIT);
    request.set_txnid(tx_id);
    request.mutable_commit()->set_tx_id(tx_id);
    request.mutable_commit()->set_commit(commit);
    request.SerializeToString(&request_str);

    uint64_t req_id = last_req_id_++;
    PendingRequest *pending = new PendingRequest(req_id);
    pending_requests_[req_id] = pending;
    pending->rcb = rcb;
    pending->rtcb = rtcb;

    vr_client_->Invoke(request_str,
                       bind(&ReplicaClient::ReplicaCallback, this, req_id,
                            placeholders::_1, placeholders::_2));
}

bool ReplicaClient::ReplicaCallback(uint64_t req_id,
                                    const string &request_str,
                                    const string &reply_str) {
    proto::Reply reply;
    reply.ParseFromString(reply_str);

    Debug("[shard %d] Received replica callback, status=%d", shard_idx_, reply.status());

    auto itr = pending_requests_.find(req_id);
    if (itr == pending_requests_.end()) {
        Warning("Received replica callback for unknown request %lu", req_id);
        return true;
    }

    PendingRequest *pending = itr->second;
    replica_callback rcb = pending->rcb;
    
    pending_requests_.erase(itr);
    delete pending;
    
    rcb(reply.status());
    
    return true;
}

} // namespace nccstore

