/***********************************************************************
 *
 * store/nccstore/versionstore.cc:
 *   Multi-version key-value store implementation for NCC
 *
 * Copyright 2024
 *
 **********************************************************************/

#include "store/nccstore/versionstore.h"
#include "lib/message.h"

namespace nccstore {

VersionedKVStore::VersionedKVStore() {}

VersionedKVStore::~VersionedKVStore() {}

std::pair<bool, std::pair<std::string, Timestamp>>
VersionedKVStore::Read(const std::string& key, const Timestamp& ts) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end() || key_it->second.empty()) {
        // Key doesn't exist
        return std::make_pair(false, std::make_pair("", Timestamp(0, 0)));
    }

    // Find the version with largest tw such that tw <= ts AND status == committed
    // Search from largest tw down
    Version search_version("", ts);
    auto it = key_it->second.upper_bound(search_version);

    // upper_bound returns first element with tw > ts, so we go backwards
    while (it != key_it->second.begin()) {
        --it;
        // Found a version with tw <= ts
        if (it->status == COMMITTED) {
            // Return this committed version
            return std::make_pair(true, std::make_pair(it->value, it->tw));
        }
        // If undecided, keep searching for earlier committed version
    }

    // No committed version found with tw <= ts
    return std::make_pair(false, std::make_pair("", Timestamp(0, 0)));
}

void VersionedKVStore::Write(const std::string& key, const std::string& value,
                              const Timestamp& write_ts) {
    // Create new version with tw = write_ts, tr = write_ts, status = undecided
    Version new_version(value, write_ts);
    versions_[key].insert(new_version);
}

void VersionedKVStore::UpdateReadTimestamp(const std::string& key, 
                                            const Timestamp& tw, 
                                            const Timestamp& new_tr) {
    auto it = FindVersionByTw(key, tw);
    if (it != versions_[key].end()) {
        // Update tr to max(current_tr, new_tr)
        // Need const_cast since set elements are const
        // This is safe because we're not modifying the sorting key (tw)
        if (new_tr > it->tr) {
            const_cast<Version&>(*it).tr = new_tr;
        }
    }
}

bool VersionedKVStore::SetCommitted(const std::string& key, const Timestamp& tw) {
    auto it = FindVersionByTw(key, tw);
    if (it != versions_[key].end()) {
        const_cast<Version&>(*it).status = COMMITTED;
        return true;
    }
    return false;
}

bool VersionedKVStore::RemoveVersion(const std::string& key, const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return false;
    }

    auto it = FindVersionByTw(key, tw);
    if (it != key_it->second.end()) {
        key_it->second.erase(it);
        return true;
    }
    return false;
}

std::vector<Version> VersionedKVStore::GetVersions(const std::string& key) {
    std::vector<Version> result;
    
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return result;
    }

    for (const auto& version : key_it->second) {
        result.push_back(version);
    }

    return result;
}

std::vector<Version> VersionedKVStore::GetUndecidedVersions(const std::string& key, 
                                                             const Timestamp& ts) {
    std::vector<Version> result;
    
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return result;
    }

    // Find all undecided versions with tw >= ts
    for (const auto& version : key_it->second) {
        if (version.tw >= ts && version.status == UNDECIDED) {
            result.push_back(version);
        }
    }

    return result;
}

std::pair<bool, Version> VersionedKVStore::GetVersion(const std::string& key, 
                                                       const Timestamp& tw) {
    auto it = FindVersionByTw(key, tw);
    if (it != versions_[key].end()) {
        return std::make_pair(true, *it);
    }
    return std::make_pair(false, Version("", Timestamp(0, 0)));
}

std::pair<bool, Version> VersionedKVStore::GetMostRecentVersion(const std::string& key) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end() || key_it->second.empty()) {
        return std::make_pair(false, Version("", Timestamp(0, 0)));
    }

    // Return the version with largest tw (last element in set)
    auto it = key_it->second.rbegin();
    return std::make_pair(true, *it);
}

std::pair<bool, Version> VersionedKVStore::GetNextVersion(const std::string& key, 
                                                           const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end() || key_it->second.empty()) {
        return std::make_pair(false, Version("", Timestamp(0, 0)));
    }

    // Find version with given tw
    Version search_version("", tw);
    auto it = key_it->second.find(search_version);
    
    if (it == key_it->second.end()) {
        return std::make_pair(false, Version("", Timestamp(0, 0)));
    }

    // Get next version
    ++it;
    if (it != key_it->second.end()) {
        return std::make_pair(true, *it);
    }

    return std::make_pair(false, Version("", Timestamp(0, 0)));
}

bool VersionedKVStore::UpdateVersionTimestamps(const std::string& key,
                                                const Timestamp& old_tw,
                                                const Timestamp& new_tw,
                                                const Timestamp& new_tr) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return false;
    }

    // Find version with old_tw
    Version search_version("", old_tw);
    auto it = key_it->second.find(search_version);
    
    if (it == key_it->second.end()) {
        return false;
    }

    // Need to remove and re-insert because tw is the sorting key
    Version updated_version = *it;
    key_it->second.erase(it);
    
    // Update timestamps
    updated_version.tw = new_tw;
    updated_version.tr = new_tr;
    
    // Re-insert with new tw
    key_it->second.insert(updated_version);
    
    return true;
}

std::set<Version>::iterator VersionedKVStore::FindVersionByTw(const std::string& key, 
                                                               const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        // Return a dummy end iterator
        static std::set<Version> empty_set;
        return empty_set.end();
    }

    Version search_version("", tw);
    auto it = key_it->second.find(search_version);
    return it;
}

} // namespace nccstore
