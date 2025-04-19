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
#include "store/benchmark/async/retwis/retwis_transaction.h"

namespace retwis {

    RetwisTransaction::RetwisTransaction(
            KeySelector *keySelector,
            int numKeys, std::mt19937 &rand, const std::string ttype,
            Partitioner *partitioner, int nShards)
            : keySelector(keySelector), ttype_{ttype} {
        if (partitioner != nullptr && numKeys > nShards) {
            Panic("jenndebug numKeys per txn > nShards, can't check duplicates by shard");
        }
        std::set<int> seen_before;
        std::set<int> seen_shards_before;

        const std::vector<int> placeholder;
        for (int i = 0; i < numKeys; ++i) {
            int keyIdx = keySelector->GetKey(rand);
            std::string new_key = keySelector->GetKey(keyIdx);
            int shard = -1;
            if (partitioner != nullptr) {
                shard = (*partitioner)(new_key, nShards, -1, placeholder);
            }
            while (seen_before.find(keyIdx) != seen_before.end() ||
                   (partitioner != nullptr && seen_shards_before.find(shard) != seen_shards_before.end())) {
                keyIdx = keySelector->GetKey(rand);
                new_key = keySelector->GetKey(keyIdx);
                if (partitioner != nullptr) {
                    shard = (*partitioner)(new_key, nShards, -1, placeholder);
                }
                Debug("jenndebug duplicate keys, or on the same shard");
            }
            seen_before.insert(keyIdx);
            if (partitioner != nullptr) {
                seen_shards_before.insert(shard);
            }

            Debug("jenndebug key %s shard %d", new_key.c_str(), shard);

            keyIdxs.push_back(keyIdx);
        }
    }

    RetwisTransaction::~RetwisTransaction() {
    }

} // namespace retwis
