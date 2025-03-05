/**
 * Defines all the constants used in the basic workload,
 * e.g., which operations are supported by the workload.
 */

#pragma once

#include <utility>

namespace ycsbt {
    // Supported Transaction types
    using TXN_TYPE = int; // operation index is of type int
    inline constexpr int ONESHOT = 0;
    inline constexpr int MULTISHOT = 1;
    
} // namespace eaas::workload::ycsbt
