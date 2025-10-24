/***********************************************************************
 *
 * store/benchmark/async/retwis/retwis_client.cc:
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#include "store/benchmark/async/retwis/retwis_client.h"

#include <iostream>

#include "store/benchmark/async/retwis/add_user.h"
#include "store/benchmark/async/retwis/follow.h"
#include "store/benchmark/async/retwis/get_timeline.h"
#include "store/benchmark/async/retwis/post_tweet.h"

namespace retwis
{

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
                      Partitioner* partitioner, int nShards,
                     const std::string &latencyFilename)
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
            nShards_(nShards)
    {
    }

    RetwisClient::~RetwisClient()
    {
    }

    AsyncTransaction *RetwisClient::GetNextTransaction()
    {
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
        // int ttype = GetRand()() % 100;
        // if (ttype < 5)
        // {
        //     lastOp = "add_user";
        //     return new AddUser(keySelector, GetRand());
        // }
        // else if (ttype < 20)
        // {
        //     lastOp = "follow";
        //     return new Follow(keySelector, GetRand());
        // }
        // else if (ttype < 50)
        // {
        //     lastOp = "post_tweet";
        //     return new PostTweet(keySelector, GetRand());
        // }
        // else
        // {
        //     lastOp = "get_timeline";
        //     return new GetTimeline(keySelector, GetRand());
        // }
    }

} // namespace retwis
