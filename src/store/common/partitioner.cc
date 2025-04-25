/***********************************************************************
 *
 * store/common/partitioner.cc:
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
#include "store/common/partitioner.h"

#include <functional>
#include <string>

#include "lib/message.h"

uint64_t Partitioner::operator()(const std::string &key, uint64_t num_shards,
                                 int group, const std::set<int> &txn_groups)
{
    std::vector<int> txn_groups_vec(txn_groups.begin(), txn_groups.end());
    return this->operator()(key, num_shards, group, txn_groups_vec);
}

uint64_t DefaultPartitioner::operator()(const std::string &key, uint64_t nshards,
                                        int group, const std::vector<int> &txnGroups)
{
    std::size_t hash = std::hash<std::string>{}(key);
    return (hash % nshards);
};

/*partitioner warehouse_partitioner = [](const std::string &key, uint64_t nshards,
    int group, const std::vector<int> &txnGroups) {
  // keys format is defined in tpcc_utils.cc
  // bytes 1 through 4 (0-indexed) contain the warehouse id for each table except
  // for the item table (which has no warehouse ids associated with rows).
  uint32_t w_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 1);
  return w_id % nshards;
};*/

uint64_t WarehouseDistItemsPartitioner::operator()(const std::string &key,
                                                   uint64_t nshards, int group, const std::vector<int> &txnGroups)
{
    uint32_t w_id = *reinterpret_cast<const uint32_t *>(key.c_str() + 1);
    uint32_t d_id = 0;
    if (key.length() >= 9)
    {
        d_id = *reinterpret_cast<const uint32_t *>(key.c_str() + 5);
    }
    return (((w_id - 1) * 10) + d_id) % nshards;
}

uint64_t WarehousePartitioner::operator()(const std::string &key,
                                          uint64_t nshards, int group, const std::vector<int> &txnGroups)
{
    switch (key[0])
    {
    case 0: // WAREHOUSE
    case 8: // STOCK
    {
        uint32_t w_id = *reinterpret_cast<const uint32_t *>(key.c_str() + 1);
        return w_id % nshards;
    }
    case 7: // ITEM
    {
        if (group == -1)
        {
            if (txnGroups.size() > 0)
            {
                size_t idx = std::uniform_int_distribution<size_t>(0, txnGroups.size() - 1)(rd);
                return static_cast<uint64_t>(txnGroups[idx]);
            }
            else
            {
                return std::uniform_int_distribution<uint64_t>(0, nshards)(rd);
            }
        }
        else
        {
            return static_cast<uint64_t>(group);
        }
    }
    case 1:  // DISTRICT
    case 2:  // CUSTOMER
    case 3:  // HISTORY
    case 4:  // NEW_ORDER
    case 5:  // ORDER
    case 6:  // ORDER_LINE
    case 9:  // CUSTOMER_BY_NAME
    case 10: // ORDER_BY_CUSTOMER
    case 11: // EARLIEST_NEW_ORDER
    {
        uint32_t w_id = *reinterpret_cast<const uint32_t *>(key.c_str() + 1);
        uint32_t d_id = *reinterpret_cast<const uint32_t *>(key.c_str() + 5);
        return (((w_id - 1) * 10) + (d_id - 1)) % nshards;
    }
    default:
        return 0UL;
    }
}
