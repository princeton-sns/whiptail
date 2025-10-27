/***********************************************************************
 *
 * store/nccstore/common.h:
 *   NCC common constants, enums, and data structures
 *
 * Copyright 2024
 *
 **********************************************************************/

#ifndef _NCC_COMMON_H_
#define _NCC_COMMON_H_

#include <cstdint>

namespace nccstore {

// Execute reply status codes
const int STATUS_OK = 0;
const int STATUS_ABORT = -1;
const int STATUS_RETRY = -2;

// Consistency levels (reuse from strongstore)
enum Consistency {
    SS = 0,   // Strict Serializability
    RSS = 1   // Read-committed Strict Serializability
};

} // namespace nccstore

#endif /* _NCC_COMMON_H_ */

