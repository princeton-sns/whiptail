/***********************************************************************
 *
 * store/nccstore/server.h:
 *   NCC server implementation
 *
 * Copyright 2024
 *
 **********************************************************************/

#ifndef _NCC_SERVER_H_
#define _NCC_SERVER_H_

#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "lib/latency.h"
#include "lib/transport.h"
#include "replication/vr/client.h"
#include "replication/vr/replica.h"
#include "store/common/backend/pingserver.h"
#include "store/common/truetime.h"
#include "store/server.h"
#include "store/common/common-proto.pb.h"
#include "store/nccstore/common.h"
#include "store/nccstore/versionstore.h"
#include "store/nccstore/replicaclient.h"
#include "store/nccstore/ncc-proto.pb.h"

namespace nccstore {

class NCCServer : public TransportReceiver,
                   public ::Server,
                   public replication::AppReplica,
                   public PingServer {
public:
    NCCServer(Consistency consistency,
              const transport::Configuration &shard_config,
              const transport::Configuration &replica_config,
              uint64_t server_id, int shard_idx, int replica_idx,
              Transport *transport, const TrueTime &tt,
              bool debug_stats, bool enable_replica);
    ~NCCServer();

    // Override TransportReceiver
    void ReceiveMessage(const TransportAddress &remote, const std::string &type,
                        const std::string &data, void *meta_data) override;

    // Override AppReplica
    void LeaderUpcall(opnum_t opnum, const std::string &op, bool &replicate,
                      std::string &response) override;
    void ReplicaUpcall(opnum_t opnum, const std::string &op,
                       std::string &response) override;
    void UnloggedUpcall(const std::string &op, std::string &response) override;

    // Override Server
    void Load(const std::string &key, const std::string &value,
              const Timestamp timestamp) override;
    Stats &GetStats() override;

private:
    // Transaction record
    struct TxnRecord {
        uint64_t tx_id;
        Timestamp tx_ts;
        std::set<std::string> read_set;
        std::map<std::string, std::string> write_set;
        bool executed;
        bool committed;
        bool is_committing;
        bool responded;
        TransportAddress *client_addr;
        proto::NCCExecute execute_msg;  // Store original execute message for replication
        proto::NCCExecuteReply reply;

        TxnRecord() : tx_id(0), tx_ts(0, 0), executed(false), committed(false), is_committing(false),
                      responded(false), client_addr(nullptr) {}
    };

    // Pending response for Response Timing Control
    struct PendingResponse {
        uint64_t tx_id;
        std::string key;
        Timestamp tw;  // timestamp of this operation
    };

    // Message handlers
    void HandleExecute(const TransportAddress &remote, const proto::NCCExecute &msg);
    void HandleCommit(const TransportAddress &remote, const proto::NCCCommit &msg);
    void HandleReadOnly(const TransportAddress &remote, const proto::NCCReadOnly &msg);
    void HandleSmartRetry(const TransportAddress &remote, const proto::NCCSmartRetry &msg);

    // Core NCC algorithms
    void ExecuteTransaction(const proto::NCCExecute &msg, TxnRecord &txn);
    bool CheckEarlyAbort(uint64_t tx_id, const Timestamp &tx_ts, const std::string &key);
    void CheckAndSendResponse(const std::string &key, bool is_replica);
    void NotifyWaitingTransactions(const std::string &key);
    bool AllPrecedingCommitted(const std::string &key, uint64_t tx_id, const Timestamp &tx_ts);

    // Transaction commit/abort
    void CommitTransaction(uint64_t tx_id);
    void AbortTransaction(uint64_t tx_id);

    // Send responses
    void SendExecuteReply(const TransportAddress &remote, const proto::NCCExecuteReply &reply);
    void SendCommitReply(const TransportAddress &remote, uint64_t tx_id, int status);
    void SendReadOnlyReply(const TransportAddress &remote, const proto::NCCReadOnlyReply &reply);
    void SendSmartRetryReply(const TransportAddress &remote, const proto::NCCSmartRetryReply &reply);

    // SmartRetry (Algorithm 5.4)
    bool SmartRetry(uint64_t tx_id, const Timestamp &new_ts,
                    const std::vector<proto::NCCReadResult> &reads,
                    const std::vector<proto::NCCWriteResult> &writes);

    // Replica callbacks
    void ExecuteCallback(uint64_t tx_id, int status);
    void CommitCallback(uint64_t tx_id, int status);

    // VR replication integration
    ReplicaClient *replica_client_;
    
    // Storage
    VersionedKVStore store_;
    
    // Transaction state
    std::unordered_map<uint64_t, TxnRecord> transactions_;
    
    // Response queues per key (for Response Timing Control)
    std::unordered_map<std::string, std::queue<PendingResponse>> response_queues_;

    // Configuration
    const TrueTime &tt_;
    const transport::Configuration &shard_config_;
    const transport::Configuration &replica_config_;
    Transport *transport_;
    uint64_t server_id_;
    int shard_idx_;
    int replica_idx_;
    Consistency consistency_;
    bool debug_stats_;
    bool enable_replica_;
    
    // Debug switches (change these for performance testing)
    static constexpr bool ENABLE_RTC = true;  // Set to true to enable RTC

    // Stats
    Stats stats_;
    Latency_t exec_lat_;
    Latency_t commit_lat_;

    // Protocol message buffers
    proto::NCCExecute execute_;
    proto::NCCExecuteReply execute_reply_;
    proto::NCCCommit commit_;
    proto::NCCCommitReply commit_reply_;
    proto::NCCReadOnly readonly_;
    proto::NCCReadOnlyReply readonly_reply_;
    proto::NCCSmartRetry smart_retry_;
    proto::NCCSmartRetryReply smart_retry_reply_;
};

} // namespace nccstore

#endif /* _NCC_SERVER_H_ */

