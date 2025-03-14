/**
 * Defines all the constants used in the basic workload,
 * e.g., which operations are supported by the workload.
 */

#pragma once

#include <utility>
#include <cstdint>

namespace ycsbt {
    // Supported operation types
    // First two are reads, and rest are writes
    using OP = int; // operation index is of type int
    inline constexpr int SELECT = 0;
    // inline constexpr int DELETE = 1;
    inline constexpr int UPDATE = 1;
    inline constexpr int INSERT = 2;
    // inline constexpr int SCAN = 4;
    // inline constexpr int UPSERT = 5;
    // Distribution: uniform and zipfian for now
    inline constexpr int UNIFORM = 10;
    inline constexpr int ZIPFIAN = 11;
    // Key partition
    using PARTITION = std::pair<int, int>; // a pair has the start and end
                                           // delimiters specifying a partition

    // number of supporting operations, now we support the first 4 operations:
    // SELECT, DELETE, UPDATE, INSERT: now we only have 3 ops select update insert
    inline constexpr int NUM_OPS = 3;
    // number of partitions, can be changed into configurable
    inline constexpr uint64_t NUM_PARTITIONS = 3;

    // FNV hash function
    inline constexpr uint64_t FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325;
    inline constexpr uint64_t FNV_PRIME_64 = 0x00000100000001B3;
} // namespace eaas::workload::ycsb
