/***********************************************************************
 *
 * store/benchmark/async/retwis/retwis_transaction.cc:
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
 #include "store/benchmark/async/ycsbt/ycsbt_ro_transaction.h"
 #include "store/benchmark/async/ycsbt/ycsbt/CoreWorkloadT.h"
 namespace ycsbt
 {
 
    YcsbtROTransaction::YcsbtROTransaction(CoreWorkloadT *coreWorkload, uint64_t batch_size)
         : coreWorkload(coreWorkload), batch_size(batch_size)
     {
     }
    
 
     YcsbtROTransaction::~YcsbtROTransaction()
     {
     }
 
 
     Operation YcsbtROTransaction::GetNextOperation(std::size_t op_index)
     {
        if (op_index == 0)
        {
            return BeginRO();
        }
        else if (op_index == 1)
        {
            std::unordered_set<std::string> keys;
            for (std::size_t i = 0; i < batch_size; i++)
            {
                keys.insert(coreWorkload->NextOp().key);
            }
            return ROCommit(std::move(keys));
        }
        else
        {
            return Wait();
        }

     }
 
 } // namespace ycsbt
 