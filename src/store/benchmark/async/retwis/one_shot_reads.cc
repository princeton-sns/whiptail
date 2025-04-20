//
// Created by jennifer on 2/19/25.
//

#include "store/benchmark/async/retwis/one_shot_reads.h"

namespace retwis {
    OneShotReads::OneShotReads(KeySelector *keySelector, std::mt19937 &rand, uint64_t readOpsTxn,
                               Partitioner *partitioner, int nShards)
            : RetwisTransaction(keySelector, readOpsTxn, rand, "one_shot_reads", partitioner, nShards),
              readOpsTxn(readOpsTxn) {}

    OneShotReads::~OneShotReads() {
    }

    Operation OneShotReads::GetNextOperation(std::size_t op_index) {
        Debug("ONE_SHOT_READS %lu", op_index);
        if (op_index == 0) {
            return BeginRW();
        } else if (op_index < this->readOpsTxn + 1) {
            return Get(GetKey(op_index - 1));
        } else if (op_index == this->readOpsTxn + 1) {
            return Commit();
        } else {
            return Wait();
        }
    }
}
