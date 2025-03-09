// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/timestamp.h
 *   A transaction timestamp
 *
 **********************************************************************/

#ifndef _TIMESTAMP_H_
#define _TIMESTAMP_H_

#include <string>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/common-proto.pb.h"
#include "build/store/strongstore/common-proto.pb.h"

class Timestamp {
   public:
    const static Timestamp MAX;

    Timestamp() : timestamp(0), id(0){};
    Timestamp(uint64_t t) : timestamp(t), id(0){};
    Timestamp(uint64_t t, uint64_t i) : timestamp(t), id(i){};
    Timestamp(const Timestamp &t)
        : timestamp(t.getTimestamp()), id(t.getID()){};
    Timestamp(const TimestampMessage &msg)
        : timestamp(msg.timestamp()), id(msg.id()){};
    ~Timestamp(){};
    void operator=(const Timestamp &t);
    bool operator==(const Timestamp &t) const;
    bool operator!=(const Timestamp &t) const;
    bool operator>(const Timestamp &t) const;
    bool operator<(const Timestamp &t) const;
    bool operator>=(const Timestamp &t) const;
    bool operator<=(const Timestamp &t) const;
    bool isValid() const;
    uint64_t getID() const { return id; };
    uint64_t getTimestamp() const { return timestamp; };
    void setID(uint64_t i) { id = i; };
    void setTimestamp(uint64_t t) { timestamp = t; };
    void serialize(TimestampMessage *msg) const;

    std::string to_string() const {
        return std::to_string(timestamp) + "," + std::to_string(id);
    }

   private:
    uint64_t timestamp;
    uint64_t id;
};

// Custom hash function for Timestamp class
namespace std {
    template <>
    struct hash<Timestamp> {
        std::size_t operator()(const Timestamp &ts) const {
            std::size_t seed = 0;
            seed ^= std::hash<uint64_t>{}(ts.getTimestamp()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= std::hash<uint64_t>{}(ts.getID()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            return seed;
        }
    };
}

#endif /* _TIMESTAMP_H_ */
