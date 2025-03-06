//
// Created by jennifer on 3/4/25.
//

#ifndef PENDINGOP_H
#define PENDINGOP_H

#include "store/strongstore/preparedtransaction.h"
#include <chrono>
#include <utility>

namespace strongstore {

class PendingOp {

public:
PendingOp(std::string  key, std::string  value, const Timestamp& commit_ts, uint64_t transaction_id,
const Timestamp& nonblock_ts, const std::chrono::microseconds& network_latency_window)
    : key_(std::move(key))
    , value_(std::move(value))
    , commit_ts_(commit_ts)
    , transaction_id_(transaction_id)
    , nonblock_ts_(nonblock_ts)
    , network_latency_window_(network_latency_window)
    , execute_time_(commit_ts.getTimestamp() + network_latency_window.count()) {

//    Debug("jenndebug commit.getTimestamp() [%lu], network_latency_window.count() [%lu]", commit_ts.getTimestamp(), network_latency_window.count());

    }

uint64_t execute_time() const {
    return execute_time_;
}

bool operator>(const PendingOp& other) const {
    return execute_time_ > other.execute_time();
}

const std::string& key() const {
    return key_;
}

const std::string& value() const {
    return value_;
}

uint64_t transaction_id() const {
    return transaction_id_;
}

const Timestamp& commit_ts() const {
    return commit_ts_;
}

const std::chrono::microseconds& network_latency_window() const {
    return network_latency_window_;
}

const Timestamp& nonblock_ts() const {
    return nonblock_ts_;
}

private:
    std::string key_;
    std::string value_;
    Timestamp commit_ts_;
    Timestamp nonblock_ts_;
    uint64_t transaction_id_;
    std::chrono::microseconds network_latency_window_;
    uint64_t execute_time_;

};

} // strongstore

#endif //PENDINGOP_H
