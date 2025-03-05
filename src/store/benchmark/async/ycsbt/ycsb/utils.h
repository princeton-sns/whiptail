#pragma once

#include <cmath>
#include <cstdint>

namespace ycsbt {
inline constexpr uint64_t FNV_OFFSET_BASIS_64 = 0xcbf29ce484222325;
inline constexpr uint64_t FNV_PRIME_64 = 0x00000100000001B3;
inline long long fnvhash64(long long val) {
  long long hashval = FNV_OFFSET_BASIS_64;

  for (int i = 0; i < 8; i++) {
    long long octet = val & 0x00ff;
    val = val >> 8;

    hashval = hashval ^ octet;
    hashval = hashval * FNV_PRIME_64;
  }
  return std::abs(hashval);
}

} // namespace ycsb
