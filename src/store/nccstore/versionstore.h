/***********************************************************************
 *
 * store/nccstore/versionstore.h:
 *   Multi-version key-value store for NCC (based on Algorithm 5.2)
 *
 * Copyright 2024
 *
 **********************************************************************/

#ifndef _NCC_VERSIONSTORE_H_
#define _NCC_VERSIONSTORE_H_

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <utility>

#include "store/common/timestamp.h"
#include "lib/assert.h"

namespace nccstore {

// Version status (from Algorithm 5.2)
enum VersionStatus {
    UNDECIDED = 0,  // Transaction not yet decided
    COMMITTED = 1   // Transaction committed (aborted versions are deleted)
};

// A version of a key (from Algorithm 5.2)
// Each version has: value, (tw, tr), status
struct Version {
    std::string value;      // The data value
    Timestamp tw;           // Write timestamp: timestamp of txn that created this version
    Timestamp tr;           // Read timestamp: highest timestamp of txns that read this version
    VersionStatus status;   // undecided or committed (aborted versions are removed)

    Version(const std::string& val, const Timestamp& write_ts)
        : value(val), tw(write_ts), tr(write_ts), status(UNDECIDED) {}

    // For ordering in set by tw
    bool operator<(const Version& other) const {
        return tw < other.tw;
    }
};

// Multi-version key-value store
// Stores versions in the order they are created by the server
class VersionedKVStore {
public:
    VersionedKVStore();
    ~VersionedKVStore();

    // Read the version valid at timestamp ts
    // Returns pair<found, pair<value, tw>>
    // Reads the version with largest tw such that tw <= ts AND status == committed
    std::pair<bool, std::pair<std::string, Timestamp>> Read(const std::string& key, 
                                                             const Timestamp& ts);

    // Write a new version (status = undecided)
    // Creates a version with tw = write_ts, tr = write_ts, status = undecided
    void Write(const std::string& key, const std::string& value, const Timestamp& write_ts);

    // Update tr for a version
    // Updates tr to max(current_tr, new_tr) for the version with given tw
    void UpdateReadTimestamp(const std::string& key, const Timestamp& tw, const Timestamp& new_tr);

    // Set version status to committed
    // Marks the version with given tw as committed
    bool SetCommitted(const std::string& key, const Timestamp& tw);

    // Remove a version (for aborted transactions)
    // Deletes the version with given tw
    bool RemoveVersion(const std::string& key, const Timestamp& tw);

    // Get all versions for a key (for debugging/conflict checking)
    std::vector<Version> GetVersions(const std::string& key);

    // Get undecided versions with tw >= ts (for early abort check)
    std::vector<Version> GetUndecidedVersions(const std::string& key, const Timestamp& ts);

    // Get version with specific tw
    std::pair<bool, Version> GetVersion(const std::string& key, const Timestamp& tw);

    // Get most recent version (even if undecided) - for Algorithm 5.2 Line 35
    std::pair<bool, Version> GetMostRecentVersion(const std::string& key);

    // Get next version after given tw (for SmartRetry Algorithm 5.4)
    std::pair<bool, Version> GetNextVersion(const std::string& key, const Timestamp& tw);

    // Update version timestamps (for SmartRetry Algorithm 5.4)
    // Used to update both tw and tr of a version
    bool UpdateVersionTimestamps(const std::string& key, const Timestamp& old_tw,
                                  const Timestamp& new_tw, const Timestamp& new_tr);

private:
    // Per-key version list (sorted by tw)
    std::unordered_map<std::string, std::set<Version>> versions_;

    // Helper: find version with specific tw
    std::set<Version>::iterator FindVersionByTw(const std::string& key, const Timestamp& tw);
};

} // namespace nccstore

#endif /* _NCC_VERSIONSTORE_H_ */
