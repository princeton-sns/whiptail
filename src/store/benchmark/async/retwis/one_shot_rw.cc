//
// Created by jennifer on 3/6/25.
//

#include "store/benchmark/async/retwis/one_shot_rw.h"

namespace retwis {

    OneShotRW::OneShotRW(KeySelector *keySelector, std::mt19937 &rand)
    : RetwisTransaction(keySelector, 2, rand, "one_shot_rw"){}

    OneShotRW::~OneShotRW() = default;

    Operation OneShotRW::GetNextOperation(std::size_t op_index) {
        Debug("ONE_SHOT_RW %lu", op_index);
        if (op_index == 0) {
            return BeginRW();
        } else if (op_index == 1) {
            return Put(GetKey(0), GetKey(0));
        } else if (op_index == 2) {
            return Get(GetKey(1));
        } else if (op_index == 3) {
            return Commit();
        } else {
            return Wait();
        }
    }
} // retwis