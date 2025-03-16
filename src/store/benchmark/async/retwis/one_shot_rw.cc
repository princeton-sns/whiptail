//
// Created by jennifer on 3/6/25.
//

#include "store/benchmark/async/retwis/one_shot_rw.h"

namespace retwis {

    OneShotRW::OneShotRW(KeySelector *keySelector, std::mt19937 &rand,
                         uint64_t mixedWriteOpsTxn, uint64_t mixedReadOpsTxn)
    : RetwisTransaction(keySelector, mixedWriteOpsTxn + mixedReadOpsTxn, rand, "one_shot_rw")
    , mixedWriteOpsTxn(mixedWriteOpsTxn)
    , mixedReadOpsTxn(mixedReadOpsTxn){}

    OneShotRW::~OneShotRW() = default;

    Operation OneShotRW::GetNextOperation(std::size_t op_index) {
        Debug("ONE_SHOT_RW %lu", op_index);
        if (op_index == 0) {
            return BeginRW();
        } else if (op_index < mixedWriteOpsTxn + 1) {
            return Put(GetKey(op_index - 1), GetKey(op_index - 1));
        } else if (op_index < mixedWriteOpsTxn + mixedReadOpsTxn + 1) {
            return Get(GetKey(op_index - 1));
        } else if (op_index == mixedWriteOpsTxn + mixedReadOpsTxn + 1 ) {
            return Commit();
        } else {
            return Wait();
        }
    }
} // retwis