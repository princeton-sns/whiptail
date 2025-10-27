/***********************************************************************
 *
 * store/nccstore/replicaclient.h:
 *   NCC replica client for VR integration
 *
 * Copyright 2024
 *
 **********************************************************************/

#ifndef _NCC_REPLICA_CLIENT_H_
#define _NCC_REPLICA_CLIENT_H_

#include <functional>
#include <unordered_map>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "store/common/transaction.h"
#include "store/nccstore/ncc-proto.pb.h"

namespace nccstore {

typedef std::function<void(int)> replica_callback;
typedef std::function<void()> replica_timeout_callback;

class ReplicaClient {
public:
    ReplicaClient(const transport::Configuration &config,
                  Transport *transport,
                  uint64_t client_id,
                  int shard);
    ~ReplicaClient();

    // Replicate Execute operation
    void Execute(uint64_t tx_id,
                 const proto::NCCExecute &execute_msg,
                 replica_callback rcb,
                 replica_timeout_callback rtcb,
                 uint32_t timeout);

    // Replicate Commit/Abort decision
    void CommitOrAbort(uint64_t tx_id,
                       bool commit,
                       replica_callback rcb,
                       replica_timeout_callback rtcb,
                       uint32_t timeout);

private:
    struct PendingRequest {
        uint64_t req_id;
        replica_callback rcb;
        replica_timeout_callback rtcb;

        PendingRequest(uint64_t rid) : req_id(rid) {}
    };

    bool ReplicaCallback(uint64_t req_id, const std::string &request_str,
                         const std::string &reply_str);

    const transport::Configuration &config_;
    Transport *transport_;
    uint64_t client_id_;
    int shard_idx_;

    replication::vr::VRClient *vr_client_;

    std::unordered_map<uint64_t, PendingRequest *> pending_requests_;
    uint64_t last_req_id_;
};

} // namespace nccstore

#endif /* _NCC_REPLICA_CLIENT_H_ */

