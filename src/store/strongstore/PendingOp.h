//
// Created by jennifer on 3/4/25.
//

#ifndef PENDINGOP_H
#define PENDINGOP_H

#include "store/strongstore/preparedtransaction.h"
#include <chrono>
#include <utility>

enum PendingOpType {
    PUT = 0,
    GET,
};

namespace strongstore {

    class PendingOp {

    public:
        PendingOp(PendingOpType pendingOpType, std::string key, std::string value, const Timestamp &commit_ts,
                  uint64_t transaction_id, const Timestamp &nonblock_ts,
                  const std::chrono::microseconds &network_latency_window)
                : pendingOpType_(pendingOpType), key_(std::move(key)), value_(std::move(value)), commit_ts_(commit_ts),
                  transaction_id_(transaction_id), nonblock_ts_(nonblock_ts),
                  network_latency_window_(network_latency_window),
                  execute_time_(commit_ts.getTimestamp() + network_latency_window.count()),
                  did_missed_window_(false) {

//    Debug("jenndebug commit.getTimestamp() [%lu], network_latency_window.count() [%lu]", commit_ts.getTimestamp(), network_latency_window.count());

        }

        uint64_t execute_time() const {
            return execute_time_;
        }

        bool operator>(const PendingOp &other) const {
            return execute_time_ > other.execute_time();
        }

        const PendingOpType& pendingOpType() const {
            return pendingOpType_;
        }

        const std::string &key() const {
            return key_;
        }

        const std::string &value() const {
            return value_;
        }

        uint64_t transaction_id() const {
            return transaction_id_;
        }

        const Timestamp &commit_ts() const {
            return commit_ts_;
        }

        const std::chrono::microseconds &network_latency_window() const {
            return network_latency_window_;
        }

        const Timestamp &nonblock_ts() const {
            return nonblock_ts_;
        }

        bool did_miss_window() const {
            return did_missed_window_;
        }

        bool& did_miss_window() {
            return did_missed_window_;
        }

    private:
        PendingOpType pendingOpType_;
        std::string key_;
        std::string value_;
        Timestamp commit_ts_;
        Timestamp nonblock_ts_;
        uint64_t transaction_id_;
        std::chrono::microseconds network_latency_window_;
        uint64_t execute_time_;
        bool did_missed_window_;

    };

} // strongstore

#endif //PENDINGOP_H
