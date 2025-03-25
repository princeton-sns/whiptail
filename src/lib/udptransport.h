// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
 *
 * Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _LIB_UDPTRANSPORT_H_
#define _LIB_UDPTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/transport.h"
#include "lib/transportcommon.h"
// #include "lib/threadpool.h"

#include <event2/event.h>

#include <list>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <random>
#include <unordered_map>
#include <vector>

class UDPTransportAddress : public TransportAddress
{
public:
    UDPTransportAddress *clone() const;

private:
    UDPTransportAddress(const sockaddr_in &addr);
    sockaddr_in addr;
    friend class UDPTransport;
    friend bool operator==(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator!=(const UDPTransportAddress &a,
                           const UDPTransportAddress &b);
    friend bool operator<(const UDPTransportAddress &a,
                          const UDPTransportAddress &b);
};

class UDPTransport : public TransportCommon<UDPTransportAddress>
{
public:
    UDPTransport(double dropRate = 0.0, double reorderRate = 0.0,
                 int dscp = 0, bool handleSignals = true);
    virtual ~UDPTransport();
    void Register(TransportReceiver *receiver,
                          const transport::Configuration &config,
                          int groupIdx,
                          int replicaIdx) override;

    void Register(TransportReceiver *receiver,
                  const transport::Configuration &config,
                  int groupIdx,
                  int replicaIdx, int send_n_more_times) override {
        this->Register(receiver, config, groupIdx, replicaIdx);
    }

    virtual void Run() override;
    virtual void Stop() override;
    virtual void Close(TransportReceiver *receiver) override;
    virtual int Timer(uint64_t ms, timer_callback_t cb) override;
    virtual int TimerMicro(uint64_t us, timer_callback_t cb) override;
    virtual bool CancelTimer(int id) override;
    virtual void CancelAllTimers() override;

    virtual void DispatchTP(std::function<void *()> f, std::function<void(void *)> cb) override;

private:
    int TimerInternal(struct timeval &tv, timer_callback_t cb);
    struct UDPTransportTimerInfo
    {
        UDPTransport *transport;
        timer_callback_t cb;
        event *ev;
        int id;
    };

    double dropRate;
    double reorderRate;
    std::uniform_real_distribution<double> uniformDist;
    std::default_random_engine randomEngine;
    struct
    {
        bool valid;
        UDPTransportAddress *addr;
        string msgType;
        string message;
        int fd;
    } reorderBuffer;
    int dscp;

    event_base *libeventBase;
    std::vector<event *> listenerEvents;
    std::vector<event *> signalEvents;
    std::map<int, TransportReceiver *> receivers; // fd -> receiver
    std::map<TransportReceiver *, int> fds;       // receiver -> fd
    std::map<const transport::Configuration *, int> multicastFds;
    std::map<int, const transport::Configuration *> multicastConfigs;
    std::set<int> rawFds;
    int lastTimerId;
    std::map<int, UDPTransportTimerInfo *> timers;
    std::mutex timersLock;
    uint64_t lastFragMsgId;
    struct UDPTransportFragInfo
    {
        uint64_t msgId;
        string data;
    };
    std::map<UDPTransportAddress, UDPTransportFragInfo> fragInfo;
    // ThreadPool tp;

    bool _SendMessageInternal(TransportReceiver *src,
                              const UDPTransportAddress &dst,
                              const Message &m,
                              size_t meta_len,
                              void *meta_data);
    bool SendMessageInternal(TransportReceiver *src,
                             const UDPTransportAddress &dst,
                             const Message &m) override;

    UDPTransportAddress
    LookupAddress(const transport::ReplicaAddress &addr);
    UDPTransportAddress
    LookupAddress(const transport::Configuration &cfg,
                  int groupIdx,
                  int replicaIdx) override;
    UDPTransportAddress
    LookupAddress(const transport::Configuration &cfg,
                  int groupIdx,
                  int replicaIdx,
                  int send_n_more_times) override {
         return this->LookupAddress(cfg, groupIdx, replicaIdx);
    }
    const UDPTransportAddress *
    LookupMulticastAddress(const transport::Configuration *cfg) override;
    void ListenOnMulticastPort(const transport::Configuration
                                   *canonicalConfig,
                               int groupIdx,
                               int replicaIdx);
    void OnReadable(int fd);
    void ProcessPacket(int fd, sockaddr_in sender, socklen_t senderSize,
                       char *buf, ssize_t sz);
    void OnTimer(UDPTransportTimerInfo *info);
    static void SocketCallback(evutil_socket_t fd,
                               short what, void *arg);
    static void TimerCallback(evutil_socket_t fd,
                              short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd,
                               short what, void *arg);
};

#endif // _LIB_UDPTRANSPORT_H_
