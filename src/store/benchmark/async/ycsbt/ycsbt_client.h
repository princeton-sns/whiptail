/***********************************************************************
 *
 * store/benchmark/async/retwis/retwis_client.h:
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
#ifndef YCSBT_CLIENT_H
#define YCSBT_CLIENT_H

#include <cstdint>
#include <string>
#include <vector>

#include "store/benchmark/async/bench_client.h"
#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/retwis/retwis_transaction.h"
#include "store/benchmark/async/ycsbt/ycsbt/CoreWorkloadT.h"
#include "store/common/frontend/client.h"

namespace ycsbt
{

    class YcsbtClient : public BenchmarkClient
    {
    public:
        YcsbtClient(KeySelector *keySelector, const std::vector<Client *> &clients, uint32_t timeout,
                     Transport &transport, uint64_t id,
                     BenchmarkClientMode mode,
                     double switch_probability,
                     double arrival_rate, double think_time, double stay_probability,
                     int mpl,
                     int expDuration, int warmupSec, int cooldownSec, int tputInterval, uint32_t abortBackoff,
                     bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
                     double  zipf_coefficient, int num_keys, int num_ops_txn, double ycsbt_read_percentage,double ycsbt_write_percentage,
                     const std::string &latencyFilename = "latency");

        virtual ~YcsbtClient();

    protected:
        virtual AsyncTransaction *GetNextTransaction() override;

    private:
        KeySelector *keySelector;
        std::string lastOp;
        CoreWorkloadT *coreWorkload;
        int num_ops_txn;
        bool readOnly;
    };

} // namespace retwis

#endif /* YCSBT_CLIENT_H */
