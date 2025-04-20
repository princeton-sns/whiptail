//
// Created by jennifer on 3/6/25.
//

#ifndef TAPIR_ONE_SHOT_RW_H
#define TAPIR_ONE_SHOT_RW_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

    class OneShotRW : public retwis::RetwisTransaction {

    public:
        OneShotRW(KeySelector *keySelector, std::mt19937 &rand, uint64_t mixedWriteOpsTxn, uint64_t mixedReadOpsTxn,
                  Partitioner *partitioner, int nShards);

        virtual ~OneShotRW();

    protected:
        Operation GetNextOperation(std::size_t op_index) override;

    private:
        uint64_t mixedWriteOpsTxn;
        uint64_t mixedReadOpsTxn;

    };

} // retwis

#endif //TAPIR_ONE_SHOT_RW_H
