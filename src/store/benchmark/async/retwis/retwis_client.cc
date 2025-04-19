#include "store/benchmark/async/retwis/retwis_client.h"

#include <iostream>

#include "store/benchmark/async/retwis/one_shot_rw.h"
#include "store/benchmark/async/retwis/one_shot_writes.h"
#include "store/benchmark/async/retwis/one_shot_reads.h"

namespace retwis {

    RetwisClient::RetwisClient(KeySelector *keySelector, const std::vector<Client *> &clients, uint32_t timeout,
                               Transport &transport, uint64_t id,
                               BenchmarkClientMode mode,
                               double switch_probability,
                               double arrival_rate, double think_time, double stay_probability,
                               int mpl,
                               int expDuration, int warmupSec, int cooldownSec, int tputInterval, uint32_t abortBackoff,
                               bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, uint32_t writeOpsTxn,
                               uint32_t readOpsTxn, uint32_t mixedWriteOpsTxn, uint32_t mixedReadOpsTxn,
                               uint32_t readPercent, uint32_t writePercent, uint32_t mixedRWPercent,
                               Partitioner* partitioner, int nShards, const std::string &latencyFilename)
            : BenchmarkClient(clients, timeout, transport, id,
                              mode,
                              switch_probability,
                              arrival_rate, think_time, stay_probability,
                              mpl,
                              expDuration, warmupSec, cooldownSec, abortBackoff,
                              retryAborted, maxBackoff, maxAttempts, latencyFilename),
              keySelector(keySelector),
              writeOpsTxn(writeOpsTxn),
              readOpsTxn(readOpsTxn),
              mixedWriteOpsTxn(mixedWriteOpsTxn),
              mixedReadOpsTxn(mixedReadOpsTxn),
              readPercent(readPercent),
              writePercent(writePercent),
              mixedRWPercent(mixedRWPercent),
              part_(partitioner),
              nShards_(nShards){

        ASSERT(readPercent + writePercent + mixedRWPercent == 100);
        Debug("jenndebug read: %u, write: %u, mixedRW: %u", readPercent, writePercent, mixedRWPercent);
    }

    RetwisClient::~RetwisClient() {
    }

    AsyncTransaction *RetwisClient::GetNextTransaction() {
//        lastOp = "one_shot_rw";
//        return new OneShotRW(keySelector, GetRand());
        int ttype = GetRand()() % 100;
        if (ttype < writePercent) {
            lastOp = "one_shot_writes";
            return new OneShotWrites(keySelector, GetRand(), writeOpsTxn, part_, nShards_);
        } else if (ttype < writePercent + mixedRWPercent) {
            lastOp = "one_shot_rw";
            return new OneShotRW(keySelector, GetRand(), mixedWriteOpsTxn, mixedReadOpsTxn, part_, nShards_);
        } else {
            lastOp = "one_shot_reads";
            return new OneShotReads(keySelector, GetRand(), readOpsTxn, part_, nShards_);
        }
    }

}  //namespace retwis
