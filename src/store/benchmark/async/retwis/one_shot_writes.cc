//
// Created by jennifer on 2/19/25.
//

#include "store/benchmark/async/retwis/one_shot_writes.h"

namespace retwis {
    OneShotWrites::OneShotWrites(KeySelector *keySelector, std::mt19937 &rand, uint64_t rwNumOpsTxn)
            : RetwisTransaction(keySelector, rwNumOpsTxn, rand, "one_shot_writes"), rwNumOpsTxn(rwNumOpsTxn) {}

    OneShotWrites::~OneShotWrites() = default;

    Operation OneShotWrites::GetNextOperation(std::size_t op_index) {
        Debug("ONE_SHOT_WRITES %lu", op_index);
        if (op_index == 0) {
            return BeginRW();
        } else if (op_index < this->rwNumOpsTxn + 1) {
            return Put(GetKey(op_index - 1), GetKey(op_index - 1));
        } else if (op_index == this->rwNumOpsTxn + 1) {
            return Commit();
        } else {
            return Wait();
        }
    }
}
