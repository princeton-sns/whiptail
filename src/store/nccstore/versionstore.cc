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

    // OPTIMIZATION: Use index to directly access latest committed version
    auto index_it = latest_committed_index_.find(key);
    if (index_it != latest_committed_index_.end() && index_it->second >= 0) {
        const auto& versions = key_it->second;
        int latest_idx = index_it->second;
        
        if (latest_idx < static_cast<int>(versions.size())) {
            const Version& latest = versions[latest_idx];
            if (latest.tw <= ts && latest.status == COMMITTED) {
                // Found latest committed version that satisfies tw <= ts
                return std::make_pair(true, std::make_pair(latest.value, latest.tw));
            }
        }
    }

    // Fallback: search backwards from the end for committed version with tw <= ts
    const auto& versions = key_it->second;
    for (int i = versions.size() - 1; i >= 0; --i) {
        if (versions[i].tw <= ts && versions[i].status == COMMITTED) {
            return std::make_pair(true, std::make_pair(versions[i].value, versions[i].tw));
        }
    }

    // No committed version found with tw <= ts
    return std::make_pair(false, std::make_pair("", Timestamp(0, 0)));
}

void VersionedKVStore::Write(const std::string& key, const std::string& value,
                              const Timestamp& write_ts) {
    // Create new version with tw = write_ts, tr = write_ts, status = undecided
    Version new_version(value, write_ts);
    versions_[key].push_back(new_version);
}

void VersionedKVStore::UpdateReadTimestamp(const std::string& key, 
                                            const Timestamp& tw, 
                                            const Timestamp& new_tr) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return;
    }
    
    auto& versions = key_it->second;
    for (auto& v : versions) {
        if (v.tw == tw) {
            // Update tr to max(current_tr, new_tr)
            if (new_tr > v.tr) {
                v.tr = new_tr;
            }
            break;
        }
    }
}

void VersionedKVStore::UpdateReadTimestamp(std::set<Version>::iterator it, const Timestamp& new_tr) {
    // Deprecated - this overload is no longer needed
    // Kept for compatibility
}

bool VersionedKVStore::SetCommitted(const std::string& key, const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return false;
    }
    
    auto& versions = key_it->second;
    for (size_t i = 0; i < versions.size(); ++i) {
        auto& version = versions[i];
        if (version.tw == tw) {
            version.status = COMMITTED;
            
            auto index_it = latest_committed_index_.find(key);
            if (index_it == latest_committed_index_.end() || 
                index_it->second < 0 || 
                version.tw < tw) {
                latest_committed_index_[key] = i;
            }
        }
    }
    return true;
}

bool VersionedKVStore::RemoveVersion(const std::string& key, const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return false;
    }

    auto& versions = key_it->second;
    for (auto it = versions.begin(); it != versions.end(); ++it) {
        if (it->tw == tw) {
            // Check if we're removing the latest committed version
            auto index_it = latest_committed_index_.find(key);
            int removed_idx = it - versions.begin();
            if (index_it != latest_committed_index_.end() && index_it->second == removed_idx) {
                // Invalidate cache - will search for new latest on next access
                latest_committed_index_.erase(key);
            } else if (index_it != latest_committed_index_.end() && index_it->second > removed_idx) {
                // Need to adjust index since we're removing an earlier element
                --(index_it->second);
            }
            
            versions.erase(it);
            return true;
        }
    }
    return false;
}

std::vector<Version> VersionedKVStore::GetVersions(const std::string& key) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return std::vector<Version>();
    }
    return key_it->second;
}

std::vector<Version> VersionedKVStore::GetUndecidedVersions(const std::string& key, 
                                                             const Timestamp& ts) {
    std::vector<Version> result;
    
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return result;
    }

    int start_idx = 0;
    auto idx_it = latest_committed_index_.find(key);
    if (idx_it != latest_committed_index_.end() && idx_it->second >= 0) {
        start_idx = idx_it->second + 1;
    }

    const auto &versions = key_it->second;
    for (size_t i = static_cast<size_t>(start_idx); i < versions.size(); ++i) {
        const auto &version = versions[i];
        if (version.tw >= ts && version.status == UNDECIDED) {
            result.push_back(version);
        }
    }

    return result;
}

std::pair<bool, Version> VersionedKVStore::GetVersion(const std::string& key, 
                                                       const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return std::make_pair(false, Version("", Timestamp(0, 0)));
    }

    int idx = FindVersionByTw(key, tw);
    if (idx >= 0) {
        return std::make_pair(true, key_it->second[static_cast<size_t>(idx)]);
    }
    return std::make_pair(false, Version("", Timestamp(0, 0)));
}

std::pair<bool, Version> VersionedKVStore::GetMostRecentVersion(const std::string& key) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end() || key_it->second.empty()) {
        return std::make_pair(false, Version("", Timestamp(0, 0)));
    }

    auto index_it = latest_committed_index_.find(key);
    if (index_it != latest_committed_index_.end() && index_it->second >= 0) {
        int latest_idx = index_it->second;
        if (latest_idx < static_cast<int>(key_it->second.size())) {
            return std::make_pair(true, key_it->second[latest_idx]);
        }
    }

    return std::make_pair(true, key_it->second.back());
}

std::pair<bool, Version> VersionedKVStore::GetNextVersion(const std::string& key, 
                                                           const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end() || key_it->second.empty()) {
        return std::make_pair(false, Version("", Timestamp(0, 0)));
    }

    // Find version with given tw
    for (size_t i = 0; i < key_it->second.size(); ++i) {
        if (key_it->second[i].tw == tw) {
            // Check if there's a next version
            if (i + 1 < key_it->second.size()) {
                return std::make_pair(true, key_it->second[i + 1]);
            }
        }
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
    
    auto& versions = key_it->second;
    for (auto& v : versions) {
        if (v.tw == old_tw) {
            // Update timestamps
            v.tw = new_tw;
            v.tr = new_tr;
            
            // Update latest committed index if needed
            auto index_it = latest_committed_index_.find(key);
            if (index_it != latest_committed_index_.end() && 
                index_it->second >= 0 && 
                versions[index_it->second].status == COMMITTED &&
                versions[index_it->second].tw < new_tw) {
                // Find the index of the updated version
                for (size_t i = 0; i < versions.size(); ++i) {
                    if (versions[i].tw == new_tw) {
                        latest_committed_index_[key] = i;
                        break;
                    }
                }
            }
            return true;
        }
    }
    return false;
}

int VersionedKVStore::FindVersionByTw(const std::string& key, const Timestamp& tw) {
    auto key_it = versions_.find(key);
    if (key_it == versions_.end()) {
        return -1;
    }
    
    const auto& versions = key_it->second;
    for (size_t i = 0; i < versions.size(); ++i) {
        if (versions[i].tw == tw) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

} // namespace nccstore
