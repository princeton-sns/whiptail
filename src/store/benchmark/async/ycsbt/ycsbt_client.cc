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
#include "store/benchmark/async/ycsbt/ycsbt_client.h"
#include "lib/message.h"
#include "store/benchmark/async/ycsbt/ycsbt_transaction.h"
#include "store/benchmark/async/ycsbt/ycsbt_ro_transaction.h"
#include <iostream>



namespace ycsbt
{

    YcsbtClient::YcsbtClient(KeySelector *keySelector, const std::vector<Client *> &clients, uint32_t timeout,
                               Transport &transport, uint64_t id,
                               BenchmarkClientMode mode,
                               double switch_probability,
                               double arrival_rate, double think_time, double stay_probability,
                               int mpl,
                               int expDuration, int warmupSec, int cooldownSec, int tputInterval, uint32_t abortBackoff,
                               bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, double  zipf_coefficient, int num_keys, int num_ops_txn, double ycsbt_read_percentage,double ycsbt_write_percentage, const std::string &latencyFilename)
        : BenchmarkClient(clients, timeout, transport, id,
                          mode,
                          switch_probability,
                          arrival_rate, think_time, stay_probability,
                          mpl,
                          expDuration, warmupSec, cooldownSec, abortBackoff,
                          retryAborted, maxBackoff, maxAttempts, latencyFilename),
          keySelector(keySelector), num_ops_txn(num_ops_txn)
    {
        coreWorkload = new CoreWorkloadT(zipf_coefficient, num_keys, num_ops_txn, ycsbt_read_percentage, ycsbt_write_percentage);
        if(ycsbt_write_percentage == 0) {
            readOnly = true;
        }
    }

    YcsbtClient::~YcsbtClient()
    {
        delete coreWorkload;
    }

    AsyncTransaction *YcsbtClient::GetNextTransaction()
    {
        Debug("Num ops txn: %d", num_ops_txn);
        if(readOnly){
            return new YcsbtROTransaction(coreWorkload, num_ops_txn + 1);
        }
        return new YcsbtTransaction(coreWorkload, num_ops_txn + 1);
    }

} // namespace retwis
