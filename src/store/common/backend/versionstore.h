// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/versionstore.cc:
 *   Timestamped version store
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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

#ifndef _VERSIONED_KV_STORE_H_
#define _VERSIONED_KV_STORE_H_

#include <map>
#include <set>
#include <unordered_map>
#include <chrono>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include <functional>

template<typename V>
std::size_t hashFunction(const V &value, std::size_t previousHash) {
    std::hash<V> hasher;
    std::size_t valueHash = hasher(value);
    return valueHash ^ (previousHash << 1); // Combine the hashes
}

template<class T, class V>
class VersionedKVStore {
public:
    VersionedKVStore(uint64_t network_latency_window = 0);

    ~VersionedKVStore();

    bool get(const std::string &key, std::pair<T, V> &value);

    bool get(const std::string &key, const T &t, std::pair<T, V> &value);

    bool getWithHash(const std::string &key, const T &t, std::tuple<T, V, size_t> &valueWithHash, bool taint = true);

    bool getRange(const std::string &key, const T &t, std::pair<T, T> &range);

    bool getLastRead(const std::string &key, T &readTime);

    bool getLastRead(const std::string &key, const T &t, T &readTime);

    bool getCommittedAfter(const std::string &key, const T &t,
                           std::vector<std::pair<T, V>> &values);

    bool remove(const std::string &key, const T &t);

    void put(const std::string &key, const V &v, const T &t);

    bool lastRead(const std::string &key, T &result) {
        if (last_read.find(key) != last_read.end()) {
            result = last_read[key];
            return true;
        }
        return false;
    }

    void put(const std::string &key, const V &v, const T &t, std::chrono::microseconds network_latency_window);

    void commitGet(const std::string &key, const T &readTime, const T &commit);

    bool getUpperBound(const std::string &key, const T &t, T &result);

    std::chrono::microseconds get_network_latency_window(const std::string &key) const;

private:
    struct VersionedValue {
        T write;
        V value;
        size_t rolling_hash;

        VersionedValue(const T &commit) : write(commit) {};

        VersionedValue(const T &commit, const V &val)
                : write(commit), value(val) {};

        VersionedValue(const T &commit, const V &val, const size_t &rolling_hash)
                : write(commit), value(val), rolling_hash(rolling_hash) {};

        friend bool operator>(const VersionedValue &v1,
                              const VersionedValue &v2) {
            return v1.write > v2.write;
        };

        friend bool operator<(const VersionedValue &v1,
                              const VersionedValue &v2) {
            return v1.write < v2.write;
        };
    };

    /* Global store which keeps key -> (timestamp, value) list. */
    std::unordered_map<std::string, std::set<VersionedValue>> store;
//    std::unordered_map<std::string, std::map<T, T>> lastReads;
    std::unordered_map<std::string, std::chrono::microseconds> network_latency_windows;
    std::unordered_map<std::string, T> last_read;

    bool inStore(const std::string &key);

    void getValue(
            const std::string &key, const T &t,
            typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator
            &it);

    uint64_t network_latency_window_;
};

template<class T, class V>
VersionedKVStore<T, V>::VersionedKVStore(uint64_t network_latency_window) : network_latency_window_(
        network_latency_window) {}

template<class T, class V>
VersionedKVStore<T, V>::~VersionedKVStore() {}

template<class T, class V>
bool VersionedKVStore<T, V>::inStore(const std::string &key) {
    return store.find(key) != store.end() && store[key].size() > 0;
}

template<class T, class V>
void VersionedKVStore<T, V>::getValue(
        const std::string &key, const T &t,
        typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator &it) {
    VersionedKVStore<T, V>::VersionedValue v(t);
    it = store[key].upper_bound(v);

    // if there is no valid version at this timestamp
    if (it == store[key].begin()) {
        it = store[key].end();
    } else {
        it--;
    }
}

/* Returns the most recent value and timestamp for given key.
 * Error if key does not exist. */
template<class T, class V>
bool VersionedKVStore<T, V>::get(const std::string &key,
                                 std::pair<T, V> &value) {
    // check for existence of key in store
    if (inStore(key)) {
        VersionedKVStore<T, V>::VersionedValue v = *(store[key].rbegin());
        value = std::make_pair(v.write, v.value);
        return true;
    }
    return false;
}

/* Returns the value valid at given timestamp.
 * Error if key did not exist at the timestamp. */
template<class T, class V>
bool VersionedKVStore<T, V>::get(const std::string &key, const T &t,
                                 std::pair<T, V> &value) {
    if (inStore(key)) {
        typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator it;
        getValue(key, t, it);
        if (it != store[key].end()) {
            value = std::make_pair((*it).write, (*it).value);
            return true;
        }
    }
    return false;
}

template<class T, class V>
bool VersionedKVStore<T, V>::getWithHash(const std::string &key, const T &t,
                                         std::tuple<T, V, size_t> &value, bool taint) {
    if (inStore(key)) {
        typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator it;
        getValue(key, t, it);
        if (it != store[key].end()) {
            value = std::make_tuple((*it).write, (*it).value, (*it).rolling_hash);
            return true;
        }
        if (taint && last_read[key] < t) {
            last_read[key] = t;
        }
    }
    return false;
}

template<class T, class V>
std::chrono::microseconds VersionedKVStore<T, V>::get_network_latency_window(const std::string &key) const {

    std::chrono::microseconds duration(this->network_latency_window_);
    return duration;
}

template<class T, class V>
bool VersionedKVStore<T, V>::remove(const std::string &key, const T &t) {
    auto storeKeyItr = store.find(key);
    if (storeKeyItr == store.end()) {
        return false;
    }

    typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator it;
    getValue(key, t, it);

    if (it == storeKeyItr->second.end() || it->write != t) {
        return false;
    }

    storeKeyItr->second.erase(it);
    return true;
}

template<class T, class V>
bool VersionedKVStore<T, V>::getRange(const std::string &key, const T &t,
                                      std::pair<T, T> &range) {
    if (inStore(key)) {
        typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator it;
        getValue(key, t, it);

        if (it != store[key].end()) {
            range.first = (*it).write;
            it++;
            if (it != store[key].end()) {
                range.second = (*it).write;
            }
            return true;
        }
    }
    return false;
}

template<class T, class V>
bool VersionedKVStore<T, V>::getUpperBound(const std::string &key, const T &t,
                                           T &result) {
    VersionedKVStore<T, V>::VersionedValue v(t);
    auto it = store[key].upper_bound(v);

    // if there is no valid version at this timestamp
    if (it == store[key].end()) {
        return false;
    } else {
        result = (*it).write;
        return true;
    }
}

template<class T, class V>
void VersionedKVStore<T, V>::put(const std::string &key, const V &value,
                                 const T &t) {
    size_t hash_value = 0;
    std::tuple<T, V, size_t> v_tuple;
    if (this->getWithHash(key, t, v_tuple, false)) {
        size_t prev_hash = std::get<2>(v_tuple);
        hash_value = hashFunction(value, prev_hash);
    }
    // Key does not exist. Create a list and an entry.
    VersionedKVStore<T, V>::VersionedValue v(t, value, hash_value);
    store[key].insert(v);

    // Recompute the hashes of the values ahead of it
    VersionedKVStore<T, V>::VersionedValue prev = v;
    for (typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator it = store[key].upper_bound(prev);
         it != store[key].end(); it = store[key].upper_bound(prev)) {

        VersionedKVStore<T, V>::VersionedValue modified_value = *it;
        store[key].erase(it);
        modified_value.rolling_hash = hashFunction(modified_value.value, prev.rolling_hash);
        store[key].insert(modified_value);
        prev = modified_value;
    }

    if (last_read.find(key) == last_read.end()) {
        last_read[key] = t;
    }
}

template<class T, class V>
void VersionedKVStore<T, V>::put(const std::string &key, const V &value,
                                 const T &t, std::chrono::microseconds network_latency_window) {

    size_t hash_value = 0;
    std::pair<T, V> v_pair;
    if (this->get(key, t, v_pair)) {
        hash_value = hashFunction(value, v_pair.second.rolling_hash);
    }
    // Key does not exist. Create a list and an entry.
    store[key].insert(VersionedKVStore<T, V>::VersionedValue(t, value, hash_value));
    this->network_latency_windows[key] = network_latency_window;
}

/*
 * Commit a read by updating the timestamp of the latest read txn for
 * the version of the key that the txn read.
 */
template<class T, class V>
void VersionedKVStore<T, V>::commitGet(const std::string &key,
                                       const T &readTime, const T &commit) {
    Panic("Unimplemented commitGet");
//    // Hmm ... could read a key we don't have if we are behind ... do we commit
//    // this or wait for the log update?
//    if (inStore(key)) {
//        typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator it;
//        getValue(key, readTime, it);
//
//        if (it != store[key].end()) {
//            // figure out if anyone has read this version before
//            if (lastReads.find(key) != lastReads.end() &&
//                lastReads[key].find((*it).write) != lastReads[key].end() &&
//                lastReads[key][(*it).write] < commit) {
//                lastReads[key][(*it).write] = commit;
//            }
//        }
//    } // otherwise, ignore the read
}

template<class T, class V>
bool VersionedKVStore<T, V>::getLastRead(const std::string &key, T &lastRead) {
    Panic("Unimplemented getLastRead");
//    if (inStore(key)) {
//        VersionedValue v = *(store[key].rbegin());
//        if (lastReads.find(key) != lastReads.end() &&
//            lastReads[key].find(v.write) != lastReads[key].end()) {
//            last_read = lastReads[key][v.write];
//            return true;
//        }
//    }
//    return false;
}

/*
 * Get the latest read for the write valid at timestamp t
 */
template<class T, class V>
bool VersionedKVStore<T, V>::getLastRead(const std::string &key, const T &t,
                                         T &lastRead) {
    Panic("unimplemented getLastRead");
//    if (inStore(key)) {
//        typename std::set<VersionedKVStore<T, V>::VersionedValue>::iterator it;
//        getValue(key, t, it);
//        // TODO: this ASSERT seems incorrect. Why should we expect to find a
//        // value
//        //    at given time t? There is no constraint on t, so we have no
//        //    guarantee that a valid version exists.
//        // UW_ASSERT(it != store[key].end());
//
//        // figure out if anyone has read this version before
//        if (lastReads.find(key) != lastReads.end() &&
//            lastReads[key].find((*it).write) != lastReads[key].end()) {
//            last_read = lastReads[key][(*it).write];
//            return true;
//        }
//    }
//    return false;
}

template<class T, class V>
bool VersionedKVStore<T, V>::getCommittedAfter(
        const std::string &key, const T &t, std::vector<std::pair<T, V>> &values) {
    VersionedKVStore<T, V>::VersionedValue v(t);
    const auto itr = store.find(key);
    if (itr != store.end()) {
        auto setItr = itr->second.upper_bound(v);
        while (setItr != itr->second.end()) {
            values.push_back(std::make_pair(setItr->write, setItr->value));
            setItr++;
        }
        return true;
    }
    return false;
}

#endif /* _VERSIONED_KV_STORE_H_ */
