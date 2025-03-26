//
// Created by jennifer on 2/28/25.
//

#ifndef STRONGSESSION_H
#define STRONGSESSION_H

#include "store/common/frontend/client.h"
#include "rss/lib.h"
#include "store/common/timestamp.h"
#include "store/strongstore/preparedtransaction.h"
#include "store/common/transaction.h"

// Custom hash function for std::vector<T>
template<typename T>
struct VectorHash {
    std::size_t operator()(const std::vector<T> &v) const {
        std::size_t seed = 0;
        for (const auto &elem: v) {
            seed ^= std::hash<T>{}(elem) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

namespace strongstore {

    class Client;

    class StrongSession : public ::Session {

    public:
        StrongSession()
                : ::Session(), transaction_id_{static_cast<uint64_t>(-1)}, start_ts_{0, 0}, min_read_ts_{0, 0},
                  participants_{}, prepares_{}, values_{}, snapshot_ts_{}, current_participant_{-1},
                  state_{EXECUTING} {}

        StrongSession(rss::Session &&rss_session)
                : ::Session(std::move(rss_session)), transaction_id_{static_cast<uint64_t>(-1)}, start_ts_{0, 0},
                  min_read_ts_{0, 0}, participants_{}, prepares_{}, values_{}, snapshot_ts_{}, current_participant_{-1},
                  state_{EXECUTING} {}

        uint64_t transaction_id() const { return transaction_id_; }

        const Timestamp &start_ts() const { return start_ts_; }

        const Timestamp &min_read_ts() const { return min_read_ts_; }

        void advance_min_read_ts(const Timestamp &ts) { min_read_ts_ = std::max(min_read_ts_, ts); }

        const std::set<int> &participants() const { return participants_; }

        const std::unordered_map<uint64_t, PreparedTransaction> prepares() const { return prepares_; }

        const Timestamp &snapshot_ts() const { return snapshot_ts_; }

        void mark_success_or_fail_reply(int participant_shard, int status) {
            if (status == REPLY_OK) {
                this->replica_replied_ok_counter_[participant_shard]++;
            } else {
                this->replica_replied_fail_counter_[participant_shard]++;
            }
        }

        int success_count(int participant_shard) {
            return this->replica_replied_ok_counter_[participant_shard];
        }

        void clear_success_count(int participant_shard) {
            this->replica_replied_ok_counter_[participant_shard] = 0;
            this->replica_replied_fail_counter_[participant_shard] = 0;
        }

        int failure_count(int participant_shard) {
            return this->replica_replied_fail_counter_[participant_shard];
        }

        void mark_successfully_replicated(int participant_shard) {
            this->successfully_replicated_.insert(participant_shard);
        }

        void clear_reply_values(int participant_shard) {
            this->all_replies_count_[participant_shard].clear();
        }

        void add_reply_values(int participant_shard, const std::vector<Value> &values) {
            all_replies_count_[participant_shard][values]++;
        }

        bool has_quorum(int participant_shard, int quorum) {
            for (const auto &replies_count_pair : this->all_replies_count_[participant_shard]) {
                int received_count = replies_count_pair.second;
                if (received_count >= quorum) {
                    return true;
                }
            }

            return false;
        }

        std::vector<Value> quorum_resp(int participant_shard, int quorum) {
            for (const auto &replies_count_pair : this->all_replies_count_[participant_shard]) {
                int received_count = replies_count_pair.second;
                if (received_count >= quorum) {
                    std::vector<Value> quorum_resp = replies_count_pair.first;
                    return quorum_resp;
                }
            }

            Panic("jenndebug Do not call this method without calling has_quorum() to check first");
        }

        uint64_t current_req_id() const {
            return current_req_id_;
        }

        uint64_t& current_req_id() {
            return current_req_id_;
        }


    protected:
        friend class Client;

        void start_transaction(uint64_t transaction_id, const Timestamp &start_ts) {
            transaction_id_ = transaction_id;
            start_ts_ = start_ts;
            participants_.clear();
            prepares_.clear();
            values_.clear();
            snapshot_ts_ = Timestamp();
            current_participant_ = -1;
            state_ = EXECUTING;
        }

        void retry_transaction(uint64_t transaction_id) {
            transaction_id_ = transaction_id;
            participants_.clear();
            prepares_.clear();
            values_.clear();
            snapshot_ts_ = Timestamp();
            current_participant_ = -1;
            state_ = EXECUTING;
        }

        enum State {
            EXECUTING = 0,
            GETTING,
            PUTTING,
            COMMITTING,
            NEEDS_ABORT,
            ABORTING
        };

        State state() const { return state_; }

        bool executing() const { return (state_ == EXECUTING); }

        bool needs_aborts() const { return (state_ == NEEDS_ABORT); }

        int current_participant() const { return current_participant_; }

        void set_executing() {
            current_participant_ = -1;
            state_ = EXECUTING;
        }

        void set_getting(int p) {
            current_participant_ = p;
            state_ = GETTING;
        }

        void set_putting(int p) {
            current_participant_ = p;
            state_ = PUTTING;
        }

        void set_committing() { state_ = COMMITTING; }

        void set_needs_abort() { state_ = NEEDS_ABORT; }

        void set_aborting() { state_ = ABORTING; }

        std::set<int> &mutable_participants() { return participants_; }

        void add_participant(int p) { participants_.insert(p); }

        void clear_participants() { participants_.clear(); }

        std::unordered_map<uint64_t, PreparedTransaction> &mutable_prepares() { return prepares_; }

        std::unordered_map<std::string, std::list<Value>> &mutable_values() { return values_; }

        void set_snapshot_ts(const Timestamp &ts) { snapshot_ts_ = ts; }

    private:
        uint64_t transaction_id_;
        Timestamp start_ts_;
        Timestamp min_read_ts_;
        std::set<int> participants_;
        std::unordered_map<uint64_t, PreparedTransaction> prepares_;
        std::unordered_map<std::string, std::list<Value>> values_;
        Timestamp snapshot_ts_;
        int current_participant_;
        State state_;

        std::unordered_map<int, uint8_t> replica_replied_ok_counter_; // shard -> count
        std::unordered_map<int, uint8_t> replica_replied_fail_counter_;
        std::set<int> successfully_replicated_; // shard

        // shard -> hash(vector of replies) -> how many of them there are
        std::unordered_map<int, std::unordered_map<std::vector<Value>, int, VectorHash < Value>>>
        all_replies_count_;
        uint64_t current_req_id_;


    };

} // strongstore

#endif //STRONGSESSION_H
