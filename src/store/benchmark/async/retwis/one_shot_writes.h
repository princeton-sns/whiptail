//
// Created by jennifer on 2/19/25.
//

#ifndef TAPIR_ONE_SHOT_WRITES_H
#define TAPIR_ONE_SHOT_WRITES_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

    class OneShotWrites : public retwis::RetwisTransaction {
    public:
        OneShotWrites(KeySelector *keySelector, std::mt19937 &rand);

        virtual ~OneShotWrites();

    protected:
        Operation GetNextOperation(std::size_t op_index) override;
    };

}  // namespace retwis

#endif //TAPIR_ONE_SHOT_WRITES_H
