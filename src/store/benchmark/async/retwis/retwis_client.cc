#include "store/benchmark/async/retwis/retwis_client.h"

#include <iostream>

#include "store/benchmark/async/retwis/one_shot_rw.h"
#include "store/benchmark/async/retwis/one_shot_writes.h"
#include "store/benchmark/async/retwis/one_shot_reads.h"
#include "store/benchmark/async/retwis/add_user.h"
#include "store/benchmark/async/retwis/follow.h"
#include "store/benchmark/async/retwis/get_timeline.h"
#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis {

RetwisClient::RetwisClient(KeySelector *keySelector, const std::vector<Client *> &clients, uint32_t timeout,
                           Transport &transport, uint64_t id,
                           BenchmarkClientMode mode,
                           double switch_probability,
                           double arrival_rate, double think_time, double stay_probability,
                           int mpl,
                           int expDuration, int warmupSec, int cooldownSec, int tputInterval, uint32_t abortBackoff,
                           bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, const std::string &latencyFilename)
    : BenchmarkClient(clients, timeout, transport, id,
                      mode,
                      switch_probability,
                      arrival_rate, think_time, stay_probability,
                      mpl,
                      expDuration, warmupSec, cooldownSec, abortBackoff,
                      retryAborted, maxBackoff, maxAttempts, latencyFilename),
      keySelector(keySelector) {
}

RetwisClient::~RetwisClient() {
}

AsyncTransaction *RetwisClient::GetNextTransaction() {
    // int ttype = GetRand()() % 100;
    lastOp = "one_shot_rw";
    return new OneShotRW(keySelector, GetRand());
    // if (ttype < 50) {
//        lastOp = "one_shot_writes";
//        return new OneShotWrites(keySelector, GetRand());
    // } else {
        // lastOp = "one_shot_reads";
        // return new OneShotReads(keySelector, GetRand());
    // }
//    if (ttype < 5) {
//        lastOp = "add_user";
//        return new AddUser(keySelector, GetRand());
//    } else if (ttype < 20) {
//        lastOp = "follow";
//        return new Follow(keySelector, GetRand());
//    } else if (ttype < 50) {
//        lastOp = "post_tweet";
//        return new PostTweet(keySelector, GetRand());
//    } else if (ttype < 100) {
//        lastOp = "get_timeline";
//        return new GetTimeline(keySelector, GetRand());
//    } else if (ttype > 150) {
//        lastOp = "one_shot_writes";
//        return new OneShotWrites(keySelector, GetRand());
//    } else {
//        lastOp = "one_shot_reads";
//        return new OneShotReads(keySelector, GetRand());
//    }
}

}  //namespace retwis
