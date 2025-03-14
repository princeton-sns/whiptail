/***********************************************************************
 *
 * store/benchmark/async/retwis/retwis_transaction.h:
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
 #ifndef YCSBT_ROTRANSACTION_H
 #define YCSBT_ROTRANSACTION_H
 
 #include <cstdint>
 #include <random>
 #include <string>
 #include <sys/types.h>
 #include <vector>
 #include <unordered_set>

 #include "store/benchmark/async/common/key_selector.h"
 #include "store/benchmark/async/ycsbt/ycsbt/CoreWorkloadT.h"
 #include "store/common/frontend/async_transaction.h"
 #include "store/common/frontend/client.h"
 
 namespace ycsbt
 {
 
     class YcsbtROTransaction : public AsyncTransaction
     {
     public:
     YcsbtROTransaction(CoreWorkloadT* coreWorkload, uint64_t batch_size);
         ~YcsbtROTransaction();
         Operation GetNextOperation(std::size_t op_index) override;
 
     protected:
         const std::string &GetTransactionType() override { return  ttype_; };
 
     private:
         const std::string ttype_ = "YCSBT_RO";
         CoreWorkloadT *coreWorkload;
         uint64_t batch_size;
         std::unordered_set<std::string> keys;
     };
 
 } // namespace ycsbt
 
 #endif /* YCSBT_TRANSACTION_H */
 