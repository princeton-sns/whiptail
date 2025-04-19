//
// Created by jennifer on 2/19/25.
//

#ifndef TAPIR_ONE_SHOT_READS_H
#define TAPIR_ONE_SHOT_READS_H

#include <functional>

#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

    class OneShotReads : public retwis::RetwisTransaction {
    public:
        OneShotReads(KeySelector *keySelector, std::mt19937 &rand, uint64_t readOpsTxn, Partitioner *partitioner,
                     int nShards);

        virtual ~OneShotReads();

    protected:
        Operation GetNextOperation(std::size_t op_index) override;

    private:
        uint64_t readOpsTxn;
    };

}  // namespace retwis

#endif //TAPIR_ONE_SHOT_READS_H
