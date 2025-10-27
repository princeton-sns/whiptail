/***********************************************************************
 *
 * store/nccstore/shardclient.h:
 *   NCC shard client for per-shard communication
 *
 * Copyright 2024
 *
 **********************************************************************/

#ifndef _NCC_SHARD_CLIENT_H_
#define _NCC_SHARD_CLIENT_H_

#include <functional>
#include <unordered_map>

#include "lib/configuration.h"
#include "lib/transport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/nccstore/ncc-proto.pb.h"

namespace nccstore {

typedef std::function<void(int, const proto::NCCExecuteReply &)> execute_callback;
typedef std::function<void(int)> execute_timeout_callback;

typedef std::function<void(int)> ncc_commit_callback;
typedef std::function<void(int)> ncc_commit_timeout_callback;

typedef std::function<void(int, const proto::NCCReadOnlyReply &)> readonly_callback;
typedef std::function<void(int)> readonly_timeout_callback;

typedef std::function<void(bool, const proto::NCCSmartRetryReply &)> smart_retry_callback;
typedef std::function<void()> smart_retry_timeout_callback;

class ShardClient : public TransportReceiver {
public:
    ShardClient(const transport::Configuration &config,
                Transport *transport,
                uint64_t client_id,
                int shard_idx);
    ~ShardClient();

    // Execute transaction at this shard
    void Execute(uint64_t tx_id,
                 const Timestamp &tx_ts,
                 const std::vector<std::string> &read_keys,
                 const std::map<std::string, std::string> &writes,
                 execute_callback ecb,
                 execute_timeout_callback etcb,
                 uint32_t timeout);

    // Send commit decision to shard
    void Commit(uint64_t tx_id,
                bool commit,
                ncc_commit_callback ccb,
                ncc_commit_timeout_callback ctcb,
                uint32_t timeout);

    // Read-only transaction
    void ReadOnly(uint64_t tx_id,
                  const Timestamp &snapshot_ts,
                  const std::vector<std::string> &keys,
                  readonly_callback rocb,
                  readonly_timeout_callback rotcb,
                  uint32_t timeout);

    // Smart retry (Algorithm 5.4)
    void SmartRetry(uint64_t tx_id,
                    const Timestamp &new_ts,
                    const std::vector<proto::NCCReadResult> &reads,
                    const std::vector<proto::NCCWriteResult> &writes,
                    smart_retry_callback srcb,
                    smart_retry_timeout_callback srtcb,
                    uint32_t timeout);

    // Override TransportReceiver
    void ReceiveMessage(const TransportAddress &remote,
                        const std::string &type,
                        const std::string &data,
                        void *meta_data) override;

private:
    struct PendingExecute {
        uint64_t tx_id;
        uint64_t req_id;
        execute_callback ecb;
        execute_timeout_callback etcb;

        PendingExecute(uint64_t tid, uint64_t rid)
            : tx_id(tid), req_id(rid) {}
    };

    struct PendingCommit {
        uint64_t tx_id;
        uint64_t req_id;
        ncc_commit_callback ccb;
        ncc_commit_timeout_callback ctcb;

        PendingCommit(uint64_t tid, uint64_t rid)
            : tx_id(tid), req_id(rid) {}
    };

    struct PendingReadOnly {
        uint64_t tx_id;
        uint64_t req_id;
        readonly_callback rocb;
        readonly_timeout_callback rotcb;

        PendingReadOnly(uint64_t tid, uint64_t rid)
            : tx_id(tid), req_id(rid) {}
    };

    struct PendingSmartRetry {
        uint64_t tx_id;
        uint64_t req_id;
        smart_retry_callback srcb;
        smart_retry_timeout_callback srtcb;

        PendingSmartRetry(uint64_t tid, uint64_t rid)
            : tx_id(tid), req_id(rid) {}
    };

    void HandleExecuteReply(const proto::NCCExecuteReply &reply);
    void HandleCommitReply(const proto::NCCCommitReply &reply);
    void HandleReadOnlyReply(const proto::NCCReadOnlyReply &reply);
    void HandleSmartRetryReply(const proto::NCCSmartRetryReply &reply);

    const transport::Configuration &config_;
    Transport *transport_;
    uint64_t client_id_;
    int shard_idx_;
    int replica_;  // Closest replica index
    uint64_t last_req_id_;

    std::unordered_map<uint64_t, PendingExecute *> pending_executes_;
    std::unordered_map<uint64_t, PendingCommit *> pending_commits_;
    std::unordered_map<uint64_t, PendingReadOnly *> pending_readonly_;
    std::unordered_map<uint64_t, PendingSmartRetry *> pending_smart_retry_;

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

#endif /* _NCC_SHARD_CLIENT_H_ */

