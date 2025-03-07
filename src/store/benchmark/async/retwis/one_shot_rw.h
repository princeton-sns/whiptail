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
        OneShotRW(KeySelector *keySelector, std::mt19937 &rand);

        virtual ~OneShotRW();

    protected:
        Operation GetNextOperation(std::size_t op_index) override;

    };

} // retwis

#endif //TAPIR_ONE_SHOT_RW_H
