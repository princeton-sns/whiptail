//
// Created by jennifer on 2/19/25.
//

#include "store/benchmark/async/retwis/one_shot_writes.h"

namespace retwis {
    OneShotWrites::OneShotWrites(KeySelector *keySelector, std::mt19937 &rand)
            : RetwisTransaction(keySelector, 1, rand, "one_shot_writes") {}

    OneShotWrites::~OneShotWrites() {
    }

    Operation OneShotWrites::GetNextOperation(std::size_t op_index) {
        Debug("ONE_SHOT_WRITES %lu", op_index);
        if (op_index == 0) {
            return BeginRW();
        } else if (op_index == 1) {
            // std::vector<std::string> keys, values;
            // for (std::size_t i = 0; i < 1; i++) {
            //     keys.push_back(GetKey(i));
            //     values.push_back(GetKey(i));
            // }
            // return PutMulti(keys, values);
            return Put(GetKey(0), GetKey(0));
        } else if (op_index == 2) {
            return Commit();
        } else {
            return Wait();
        }
    }
}
