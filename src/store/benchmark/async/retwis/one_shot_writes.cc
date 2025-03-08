//
// Created by jennifer on 2/19/25.
//

#include "store/benchmark/async/retwis/one_shot_writes.h"

namespace retwis {
    OneShotWrites::OneShotWrites(KeySelector *keySelector, std::mt19937 &rand)
            : RetwisTransaction(keySelector, 1, rand, "one_shot_writes") {}

    OneShotWrites::~OneShotWrites() = default;

    Operation OneShotWrites::GetNextOperation(std::size_t op_index) {
        Debug("ONE_SHOT_WRITES %lu", op_index);
        if (op_index == 0) {
            return BeginRW();
        } else if (op_index == 1) {
            return Put(GetKey(0), GetKey(0));
        } else if (op_index == 2) {
            return Commit();
        } else {
            return Wait();
        }
    }
}
