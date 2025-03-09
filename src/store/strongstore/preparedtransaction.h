/***********************************************************************
 *
 * store/strongstore/preparedtransaction.h:
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef _STRONG_PREPARED_TRANSACTION_H_
#define _STRONG_PREPARED_TRANSACTION_H_

#include <algorithm>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

#include "store/common/timestamp.h"
#include "store/strongstore/strong-proto.pb.h"

namespace strongstore {

    class Value {
    public:
        Value(uint64_t transaction_id, const Timestamp &ts,
              const std::string &key, const std::string &val);

        Value(const proto::ReadReply &msg);

        ~Value();

        uint64_t
        transaction_id() const { return transaction_id_; }

        const Timestamp &ts() const { return ts_; }

        const std::string &key() const { return key_; }

        const std::string &val() const { return val_; }

        bool operator==(const Value &other) const {
            return transaction_id_ == other.transaction_id() && ts_ == other.ts() && key_ == other.key() &&
                   val_ == other.val();
        }

        bool operator!=(const Value &other) const {
            return !(*this == other);
        }

        std::string to_string() const {
            return "(" + key_ + ", " + val_ + ", " + ts_.to_string() + ")";
        }

    private:
        uint64_t transaction_id_;
        Timestamp ts_;
        std::string key_;
        std::string val_;
    };

    // Custom hash function for Value class
    struct ValueHash {
        std::size_t operator()(const Value &v) const {
            std::size_t seed = 0;
            seed ^= std::hash<uint64_t>{}(v.transaction_id()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= std::hash<Timestamp>{}(v.ts()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= std::hash<std::string>{}(v.key()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= std::hash<std::string>{}(v.val()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            return seed;
        }
    };

    class PreparedTransaction {
    public:
        PreparedTransaction(uint64_t transaction_id, const Timestamp &prepare_ts);

        PreparedTransaction(const proto::PreparedTransactionMessage &msg);

        ~PreparedTransaction();

        void serialize(proto::PreparedTransactionMessage *msg) const;

        const Timestamp &prepare_ts() const { return prepare_ts_; }

        void update_prepare_ts(const Timestamp &prepare_ts) { prepare_ts_ = std::max(prepare_ts_, prepare_ts); }

        uint64_t transaction_id() const { return transaction_id_; }

        const std::unordered_map<std::string, std::string> &write_set() const { return write_set_; }

        void add_write_set(const std::unordered_map<std::string, std::string> &write_set) {
            write_set_.insert(write_set.begin(), write_set.end());
        }

        void add_write_set(const std::pair<std::string, std::string> &w) {
            write_set_.insert(w);
        }

    private:
        uint64_t transaction_id_;
        Timestamp prepare_ts_;
        std::unordered_map<std::string, std::string> write_set_;
    };

}; // namespace strongstore

// Specialize std::hash for Value class
namespace std {
    template <>  // Explicit specialization (only for non-template types)
    struct hash<strongstore::Value> {
        size_t operator()(const strongstore::Value &v) const {
            return strongstore::ValueHash{}(v);
        }
    };
}

#endif /* _STRONG_PREPARED_TRANSACTION_H_ */