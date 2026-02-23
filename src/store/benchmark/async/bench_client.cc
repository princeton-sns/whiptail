/***********************************************************************
*
* store/benchmark/async/bench_client.cc:
*
* Copyright 2022 Jeffrey Helt, Matthew Burke, Amit Levy, Wyatt Lloyd
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
#include "store/benchmark/async/bench_client.h"

#include <sys/time.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <utility>

#include "lib/latency.h"
#include "lib/message.h"
#include "lib/timeval.h"
#include "lib/transport.h"
#include "store/strongstore/client.h"
#include <thread>
#include <chrono>

DEFINE_LATENCY(op);

BenchmarkClient::BenchmarkClient(const std::vector<Client *> &clients, uint32_t timeout,
                                Transport &transport, uint64_t id,
                                BenchmarkClientMode mode,
                                double switch_probability,
                                double arrival_rate, double think_time, double stay_probability,
                                int mpl,
                                int expDuration, int warmupSec, int cooldownSec,
                                uint32_t abortBackoff, bool retryAborted,
                                uint32_t maxBackoff, uint32_t maxAttempts,
                                const std::string &latencyFilename)
    : transport_(transport),
    session_states_{},
    clients_{clients},
    client_id_{id},
    timeout_{timeout},
    rand_{id},
    next_arrival_dist_{arrival_rate * 1e-6},
    think_time_dist_{1 / think_time * 1e-6},
    stay_dist_{stay_probability},
    switch_dist_{switch_probability},
    mpl_{mpl},
    exp_duration_{expDuration},
    warmupSec{warmupSec},
    cooldownSec{cooldownSec},
    latencyFilename{latencyFilename},
    maxBackoff{maxBackoff},
    abortBackoff{abortBackoff},
    retryAborted{retryAborted},
    maxAttempts{maxAttempts},
    started{false},
    done{false},
    cooldownStarted{false},
    mode_{mode}
{
    if (arrival_rate <= 0)
    {
        Panic("Arrival rate must be (strictly) positive!");
    }

    _Latency_Init(&latency, "txn");
}

BenchmarkClient::~BenchmarkClient()
{
    Debug("session_states_.size(): %lu", session_states_.size());
}

void BenchmarkClient::Start(bench_done_callback bdcb)
{
    n_sessions_started_ = 0;
    n = 0;
    curr_bdcb_ = bdcb;
    transport_.Timer(warmupSec * 1000, std::bind(&BenchmarkClient::WarmupDone, this));
    gettimeofday(&startTime, NULL);

    transport_.TimerMicro(0, std::bind(&BenchmarkClient::SendNext, this));
}

void BenchmarkClient::IssueTransaction(const uint64_t session_id, bool abortTxn) {
    /* Issue each part of a transaction */
    Debug("[%lu] IssueTransaction", session_id);
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());
    auto &ss = search->second;
    auto transaction = ss.transaction();
    auto &session = ss.session();

    auto client_index = ss.current_client_index();
    auto &client = *clients_[client_index];
    
    // Are we at the very beginning of a transaction?
    // Begin takes callbacks but we're just going to continue executing here so the callbacks can be empty
    auto bcb = []() {};
    auto btcb = []() {};
    
    Operation first_op = transaction->GetNextOperation(ss.op_index());
    if (ss.op_index() == 0) {
        switch (first_op.type)
            {
            case BEGIN_RO:
                client.Begin(session, bcb, btcb, timeout_);
                break;
            case BEGIN_RW:
                client.Begin(session, bcb, btcb, timeout_);
                break;
            default:
                NOT_REACHABLE(); 
            }
        ss.incr_op_index();
    }

    // Take a peek at the first operation of the transaction to see if it's a read
    // If so, we can enter a reading phase
    Operation potential_read_op = transaction->GetNextOperation(ss.op_index());
    bool reading = (potential_read_op.type == GET || potential_read_op.type == GET_FOR_UPDATE);
    
read_phase_label:
    // Reading Phase
    // These callbacks can be empty
    auto gcb = std::bind(&BenchmarkClient::GetCallback, this, session_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    auto gtcb = std::bind(&BenchmarkClient::GetTimeout, this, session_id, std::placeholders::_1, std::placeholders::_2);
    // Loop through reads (if applicable)
    while (reading) {
        Debug("Entering read phase");
        Operation read_op = transaction->GetNextOperation(ss.op_index());
        if (read_op.type == GET) {
            ss.increment_outstanding_gets();
            client.Get(session, read_op.key, gcb, gtcb, timeout_);
        } else if (read_op.type == GET_FOR_UPDATE) {
            ss.increment_outstanding_gets();
            client.GetForUpdate(session, read_op.key, gcb, gtcb, timeout_);
        } else {
            // If we've reached this point, the read phase is over. 
            reading = false; // This seems unnecessary but also not incorrect
            return; // We return because we want the execution to suspend 
        }
        ss.incr_op_index();
    }
    
    // Invariant: All reads, if any, should have been completed by this point
    // Terminate Execution and wait for callback
    // CallBack should make it back here
    
    
    // Set up put callbacks
    auto pcb = std::bind(&BenchmarkClient::PutCallback, this, session_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    auto ptcb = std::bind(&BenchmarkClient::PutTimeout, this, session_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

    // Write Phase
    // Take a peek at the next operation of the transaction to see if it's a write
    // If so, we can enter a writing phase
    Operation potential_write_op = transaction->GetNextOperation(ss.op_index());
    bool writing = (potential_write_op.type == PUT);
    
    while (writing) {
        Operation write_op = transaction->GetNextOperation(ss.op_index());
        if (write_op.type == PUT)
            client.Put(session, write_op.key, write_op.value, pcb, ptcb, timeout_);
        else
            break;
        ss.incr_op_index();
    }
    Debug("Exited writing phase");

    // There could be another read phase. Check for that here and jump back to the read phase if so.
    Operation potential_read_op_2 = transaction->GetNextOperation(ss.op_index());
    bool second_read_phase = (potential_read_op_2.type == GET || potential_read_op_2.type == GET_FOR_UPDATE);
    if (second_read_phase) goto read_phase_label;
    
    auto ccb = std::bind(&BenchmarkClient::CommitCallback, this, session_id, std::placeholders::_1);
    auto ctcb = std::bind(&BenchmarkClient::CommitTimeout, this);
    auto acb = std::bind(&BenchmarkClient::AbortCallback, this, session_id, ABORTED_SYSTEM);
    auto atcb = std::bind(&BenchmarkClient::AbortTimeout, this);
    // Time to commit or abort

    if (abortTxn) {
        client.Abort(session, acb, atcb, timeout_);
    }
    else {
        Operation end_op = transaction->GetNextOperation(ss.op_index());
        if (end_op.type == COMMIT)
            client.Commit(session, ccb, ctcb, timeout_);
        else if (end_op.type == ROCOMMIT)
            client.ROCommit(session, end_op.keys, ccb, ctcb, timeout_);
        else if (end_op.type == ABORT)
            client.Abort(session, acb, atcb, timeout_);
        else if (end_op.type == WAIT)
            ;
        else
            NOT_REACHABLE();
    }
   
}

void BenchmarkClient::SendNext()
{
    Debug("[%lu] SendNext", n_sessions_started_);
    n_sessions_started_++;

    std::size_t client_index = n_sessions_started_ % clients_.size();
    auto &client = *clients_[client_index];

    auto &session = client.BeginSession();
    auto sid = session.id();

    Debug("session id: %lu", sid);

    auto ecb = std::bind(&BenchmarkClient::ExecuteCallback, this, sid, std::placeholders::_1);
    auto transaction = GetNextTransaction();
    stats.Increment(transaction->GetTransactionType() + "_attempts", 1);

    session_states_.emplace(sid, SessionState{session, transaction, ecb, client_index});

    auto &ss = session_states_.find(sid)->second;
    _Latency_StartRec(ss.lat());

    IssueTransaction(sid);

    if (!cooldownStarted)
    {
        bool send_next = false;
        uint64_t next_arrival_us = 0;
        switch (mode_)
        {
        case BenchmarkClientMode::OPEN:
            send_next = true;
            next_arrival_us = static_cast<uint64_t>(next_arrival_dist_(rand_));
            break;

        case BenchmarkClientMode::CLOSED:
            send_next = (n_sessions_started_ < mpl_);
            std::cerr << "n_sessions_started: " << n_sessions_started_ << std::endl;
            next_arrival_us = 0;
            break;
        default:
            Panic("Unexpected client mode!");
        }

        if (send_next)
        {
            Notice("next arrival in %lu us", next_arrival_us);
            transport_.TimerMicro(next_arrival_us, std::bind(&BenchmarkClient::SendNext, this));
        }
    }
}

void BenchmarkClient::SendNextInSession(const uint64_t session_id)
{
    Debug("[%lu] SendNextInSession", session_id);

    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());
    auto &ss = search->second;

    auto ecb = std::bind(&BenchmarkClient::ExecuteCallback, this, session_id, std::placeholders::_1);
    auto transaction = GetNextTransaction();
    stats.Increment(transaction->GetTransactionType() + "_attempts", 1);

    if (switch_dist_(rand_))
    {
        Notice("Switching to next session");
        auto cur_client_index = ss.current_client_index();
        std::size_t next_client_index = (cur_client_index + 1) % clients_.size();

        auto &cur_client = *clients_[cur_client_index];
        rss::Session rss_session = cur_client.EndSession(ss.session());

        auto &next_client = *clients_[next_client_index];

        auto &session = next_client.ContinueSession(rss_session);
        ASSERT(session_id == session.id());

        ss.start_transaction(session, transaction, ecb, next_client_index);
    }
    else
    {
        ss.start_transaction(ss.session(), transaction, ecb, ss.current_client_index());
    }

    auto &session = ss.session();
    auto &client = *clients_[ss.current_client_index()];

    _Latency_StartRec(ss.lat());

    IssueTransaction(session_id);
}

/*void BenchmarkClient::ExecuteNextOperation(const uint64_t session_id)
{
    Debug("[%lu] ExecuteNextOperation", session_id);
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;
    auto transaction = ss.transaction();
    auto op_index = ss.op_index();
    auto &session = ss.session();

    Operation op = transaction->GetNextOperation(op_index);
    ss.incr_op_index();

    auto gcb = std::bind(&BenchmarkClient::GetCallback, this, session_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    auto igcb = std::bind(&BenchmarkClient::ImmediateGetCallback, this, session_id, std::placeholders::_1);
    auto gtcb = std::bind(&BenchmarkClient::GetTimeout, this, session_id, std::placeholders::_1, std::placeholders::_2);
    auto pcb = std::bind(&BenchmarkClient::PutCallback, this, session_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    auto ptcb = std::bind(&BenchmarkClient::PutTimeout, this, session_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
    auto ccb = std::bind(&BenchmarkClient::CommitCallback, this, session_id, std::placeholders::_1);
    auto ctcb = std::bind(&BenchmarkClient::CommitTimeout, this);
    auto acb = std::bind(&BenchmarkClient::AbortCallback, this, session_id, ABORTED_USER);
    auto atcb = std::bind(&BenchmarkClient::AbortTimeout, this);

    auto client_index = ss.current_client_index();
    auto &client = *clients_[client_index];

    switch (op.type)
    {
    case GET:
        //std::cerr << "Outstanding gets before: " <<  outstanding_gets_ << std::endl;
        Debug("Outstanding gets before: %lu",  outstanding_gets_);
        client.Get(session, op.key, gcb, igcb, gtcb, timeout_);
        break;

    case GET_FOR_UPDATE:
        Debug("Outstanding gets before: %lu",  outstanding_gets_);
        outstanding_gets_++;
        client.GetForUpdate(session, op.key, gcb, igcb, gtcb, timeout_);
        break;

    case PUT:
        client.Put(session, op.key, op.value, pcb, ptcb, timeout_);
        break;

    case COMMIT:
        if (outstanding_gets_ > 0) {
            Debug("Commit needs to wait for [%lu] outstanding gets",  outstanding_gets_);
        }
        else {
            client.Commit(session, ccb, ctcb, timeout_);
        }
        break;

    case ABORT:
        client.Abort(session, acb, atcb, timeout_);
        break;

    case ROCOMMIT:
        client.ROCommit(session, op.keys, ccb, ctcb, timeout_);
        break;

    case WAIT:
        break;

    default:
        NOT_REACHABLE();
    }
}*/

void BenchmarkClient::ExecuteAbort(const uint64_t session_id, transaction_status_t status)
{
    Debug("[%lu] ExecuteAbort", session_id);
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;
    auto transaction = ss.transaction();
    auto op_index = ss.op_index();
    auto &session = ss.session();

    auto client_index = ss.current_client_index();
    auto &client = *clients_[client_index];

    auto acb = std::bind(&BenchmarkClient::AbortCallback, this, session_id, status);
    auto atcb = std::bind(&BenchmarkClient::AbortTimeout, this);

    client.Abort(session, acb, atcb, timeout_);
}

void BenchmarkClient::GetCallback(const uint64_t session_id, int status,
                                const std::string &key, const std::string &val, Timestamp ts)
{   

    // Chris: I'm not handling aborts right now. Handle that. 
    Debug("[%lu] Get(%s) callback", session_id, key.c_str());
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());
    auto &ss = search->second;

    if (status == REPLY_FAIL) {
        IssueTransaction(session_id, true);
        return;
    }

    ss.decrement_outstanding_gets();
    if (ss.get_outstanding_gets() == 0)
    {
        Debug("All gets completed, issuing transaction");
        IssueTransaction(session_id, false);
    }
    else {
        return;     
    }
}

void BenchmarkClient::GetTimeout(const uint64_t session_id,
                                int status, const std::string &key)
{
    Warning("[%lu] Get(%s) timed out :(", session_id, key.c_str());
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;
    auto &session = ss.session();

    auto client_index = ss.current_client_index();
    auto &client = *clients_[client_index];

    auto gcb = std::bind(&BenchmarkClient::GetCallback, this, session_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4);
    auto gtcb = std::bind(&BenchmarkClient::GetTimeout, this, session_id, std::placeholders::_1, std::placeholders::_2);

    client.Get(session, key, gcb, gtcb, timeout_);
}

void BenchmarkClient::PutCallback(const uint64_t session_id, int status,
                                const std::string &key, const std::string &val)
{
    Debug("[%lu] Put(%s,%s) callback.", session_id, key.c_str(), val.c_str());
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;

    if (status == REPLY_OK)
    {
        return;
    }
    else if (status == REPLY_FAIL)
    {
        // Chris: I'm worried that this doesn't properly terminate the rest of this transaction
        ExecuteAbort(session_id, ABORTED_SYSTEM);
    }
    else
    {
        Panic("Unknown status for Put %d.", status);
    }
}

void BenchmarkClient::PutTimeout(const uint64_t session_id, int status,
                                const std::string &key, const std::string &val)
{
    Warning("[%lu] Put(%s,%s) timed out :(", session_id, key.c_str(), val.c_str());
}

void BenchmarkClient::CommitCallback(const uint64_t session_id, transaction_status_t status)
{
    Notice("[%lu] Commit callback.", session_id);
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;
    auto ecb = ss.ecb();

    ecb(status);
}

void BenchmarkClient::CommitTimeout()
{
    Warning("Commit timed out :(");
}

void BenchmarkClient::AbortCallback(const uint64_t session_id, transaction_status_t status)
{
    Notice("[%lu] Abort callback.", session_id);
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;
    auto ecb = ss.ecb();

    ecb(status);
}

void BenchmarkClient::AbortTimeout()
{
    Warning("Abort timed out :(");
}

void BenchmarkClient::ExecuteCallback(uint64_t session_id,
                                    transaction_status_t result)
{
    Notice("[%lu] ExecuteCallback with result %d.", session_id, result);
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;
    auto transaction = ss.transaction();
    auto &ttype = transaction->GetTransactionType();
    auto n_attempts = ss.n_attempts();

    // Commit or Abort callback or max attempts reached or retry aborted is disabled
    if (result == COMMITTED || result == ABORTED_USER ||
        (maxAttempts != -1 && n_attempts >= static_cast<uint64_t>(maxAttempts)) ||
        !retryAborted)
    {
        Debug("Enter Commit/Abort callback for transaction %lu with result %d, n_attempts: %lu, max attempts: %lu, retry aborted: %d", session_id, result, n_attempts, maxAttempts, retryAborted);
        bool erase_session = true;
        if (result == COMMITTED)
        {
            Debug("Enter Commit callback for transaction %lu", session_id);
            stats.Increment(ttype + "_committed", 1);
        }

        if (retryAborted)
        {
            stats.Add(ttype + "_attempts_list", n_attempts);
        }


        if (!cooldownStarted)
        {
            bool send_next_in_session = false;
            uint64_t next_arrival_us = 0;
            switch (mode_)
            {
            case BenchmarkClientMode::OPEN:
                send_next_in_session = stay_dist_(rand_);
                next_arrival_us = static_cast<uint64_t>(think_time_dist_(rand_));
                break;

            case BenchmarkClientMode::CLOSED:
                send_next_in_session = true;
                next_arrival_us = 0;
                break;
            default:
                Panic("Unexpected client mode!");
            }

            if (send_next_in_session)
            {
                erase_session = false;
                Debug("next arrival in session %lu us", next_arrival_us);

                transport_.TimerMicro(next_arrival_us, std::bind(&BenchmarkClient::SendNextInSession, this, session_id));
            }
        }

        OnReply(session_id, result, erase_session);
    }
    else
    {
        Debug("Enter Abort callback for transaction %lu", session_id);
        stats.Increment(ttype + "_" + std::to_string(result), 1);
        BenchmarkClient::BenchState state = GetBenchState();
        Debug("Current bench state: %d.", state);
        if (state == DONE)
        {
            OnReply(session_id, ABORTED_SYSTEM, true);
        }
        else
        {
            Debug("Enter Retry callback for transaction %lu", session_id);
            uint64_t backoff = 0;
            if (abortBackoff > 0)
            {
                uint64_t exp = n_attempts - 1;
                backoff = static_cast<uint64_t>(1000 * 50 * (std::pow(1.3, exp)));
                backoff = std::min(backoff, 1000 * maxBackoff);
                // uint64_t exp = std::min(n_attempts - 1UL, 56UL);
                // Debug("Exp is %lu (min of %lu and 56.", exp, n_attempts - 1UL);
                // uint64_t upper = std::min((1UL << exp) * abortBackoff, maxBackoff);
                // Debug("Upper is %lu (min of %lu and %lu.", upper, (1UL << exp) * abortBackoff,
                //       maxBackoff);
                // backoff = std::uniform_int_distribution<uint64_t>(0UL, upper)(GetRand());
                // stats.Increment(ttype + "_backoff", backoff);
                Debug("Backing off for %lu us: %lu", backoff, n_attempts);
            }
            OnReply(session_id, ABORTED_SYSTEM, false);
            Notice("Set up retry for transaction %lu, backoff: %lu us", session_id, backoff);
            transport_.TimerMicro(backoff, [this, session_id]
                                {
                Notice("Begin Retrying transaction %lu", session_id);
                auto search = session_states_.find(session_id);
                ASSERT(search != session_states_.end());
                Notice("Found session %lu", session_id);
                auto &ss = search->second;
                ss.retry_transaction();
                Notice("Retried transaction %lu", session_id);
                stats.Increment(ss.transaction()->GetTransactionType() + "_attempts", 1);
                Notice("Incremented attempts for transaction %lu", session_id);
                // auto bcb = std::bind(&BenchmarkClient::ExecuteNextOperation, this, session_id);
                // auto btcb = []() {};

                // auto &client = *clients_[ss.current_client_index()];
                // client.Retry(ss.session(), bcb, btcb, timeout_); });
                Notice("End Retrying transaction %lu", session_id);
                IssueTransaction(session_id, false);
            });
        }
    }
}

void BenchmarkClient::WarmupDone()
{
    started = true;
    Notice("Completed warmup period of %d seconds with %d requests", warmupSec, n);
    n = 0;
}

void BenchmarkClient::CleanupContinue()
{
    auto n = session_states_.size();
    Notice("Waiting for %lu outstanding transactions.", n);

    if (n > 0 && cooldown_counter_ < 10)
    {
        cooldown_counter_++;
        transport_.TimerMicro(1e6, std::bind(&BenchmarkClient::CleanupContinue, this));
    }
    else
    {
        CooldownDone();
    }
}

void BenchmarkClient::Cleanup()
{
    auto n = session_states_.size();
    Notice("Aborting %lu outstanding transactions.", n);

    if (n > 0)
    {
        for (auto &kv : session_states_)
        {
            auto transaction_id = kv.first;
            auto &ss = kv.second;

            auto op_index = ss.op_index();

            auto client_index = ss.current_client_index();
            auto &client = *clients_[client_index];

            client.ForceAbort(transaction_id);
        }

        transport_.TimerMicro(1e6, std::bind(&BenchmarkClient::CleanupContinue, this));
    }
    else
    {
        CooldownDone();
    }
}

void BenchmarkClient::CooldownDone()
{
    done = true;

    char buf[1024];
    Notice("Finished cooldown period.");
    std::sort(latencies.begin(), latencies.end());

    if (latencies.size() > 0)
    {
        uint64_t ns = latencies[latencies.size() / 2];
        LatencyFmtNS(ns, buf);
        Notice("Median latency is %ld ns (%s)", ns, buf);

        ns = 0;
        for (auto latency : latencies)
        {
            ns += latency;
        }
        ns = ns / latencies.size();
        LatencyFmtNS(ns, buf);
        Notice("Average latency is %ld ns (%s)", ns, buf);

        ns = latencies[latencies.size() * 90 / 100];
        LatencyFmtNS(ns, buf);
        Notice("90th percentile latency is %ld ns (%s)", ns, buf);

        ns = latencies[latencies.size() * 95 / 100];
        LatencyFmtNS(ns, buf);
        Notice("95th percentile latency is %ld ns (%s)", ns, buf);

        ns = latencies[latencies.size() * 99 / 100];
        LatencyFmtNS(ns, buf);
        Notice("99th percentile latency is %ld ns (%s)", ns, buf);
    }
    curr_bdcb_();
}

void BenchmarkClient::OnReply(uint64_t session_id, int result, bool erase_session)
{
    Notice("OnReply with result %d for session %lu. Erase session: %d", result, session_id, erase_session);
    auto search = session_states_.find(session_id);
    ASSERT(search != session_states_.end());

    auto &ss = search->second;
    auto transaction = ss.transaction();
    auto lat = ss.lat();

    if (started)
    {
        // record latency
        if (!cooldownStarted)
        {
            _Latency_EndRec(&latency, lat);
            uint64_t ns = lat->accum;
            // TODO: use standard definitions across all clients for
            // success/commit and failure/abort
            if (result == 0)
            { // only record result if success
                struct timespec curr;
                clock_gettime(CLOCK_MONOTONIC, &curr);
                if (latencies.size() == 0UL)
                {
                    gettimeofday(&startMeasureTime, NULL);
                    startMeasureTime.tv_sec -= ns / 1000000000ULL;
                    startMeasureTime.tv_usec -= (ns % 1000000000ULL) / 1000ULL;
                    // std::cout << "#start," << startMeasureTime.tv_sec << ","
                    // << startMeasureTime.tv_usec << std::endl;
                }
                uint64_t currNanos = curr.tv_sec * 1000000000ULL + curr.tv_nsec;
                std::cout << transaction->GetTransactionType() << ',' << ns << ',' << currNanos << ','
                        << client_id_ << std::endl;
                latencies.push_back(ns);
            }
        }

        struct timeval diff;
        BenchState state = GetBenchState(diff);
        if ((state == COOL_DOWN || state == DONE) && !cooldownStarted)
        {
            Debug("Starting cooldown after %ld seconds.", diff.tv_sec);
            Finish();
        }
        else
        {
            Debug("Not done after %ld seconds.", diff.tv_sec);
        }
    }

    if (result == COMMITTED) {
        delete transaction;
    } 
    

    if (erase_session)
    {
        Notice("Erasing session %lu", session_id);
        auto &client = *clients_[ss.current_client_index()];
        client.EndSession(ss.session());
        session_states_.erase(search);
        n_sessions_started_--;
    }

    n++;
}

BenchmarkClient::BenchState BenchmarkClient::GetBenchState(struct timeval &diff) const
{
    struct timeval currTime;
    gettimeofday(&currTime, NULL);

    diff = timeval_sub(currTime, startTime);
    if (diff.tv_sec > exp_duration_)
    {
        return DONE;
    }
    else if (diff.tv_sec > exp_duration_ - cooldownSec)
    {
        return COOL_DOWN;
    }
    else if (started)
    {
        return MEASURE;
    }
    else
    {
        return WARM_UP;
    }
}

BenchmarkClient::BenchState BenchmarkClient::GetBenchState() const
{
    struct timeval diff;
    return GetBenchState(diff);
}

void BenchmarkClient::Finish()
{
    gettimeofday(&endTime, NULL);
    struct timeval diff = timeval_sub(endTime, startMeasureTime);

    std::cout << "#end," << diff.tv_sec << "," << diff.tv_usec << "," << client_id_
            << std::endl;

    Notice("Completed %d requests in " FMT_TIMEVAL_DIFF " seconds", n,
        VA_TIMEVAL_DIFF(diff));
    Notice("%lu outstanding transactions.", session_states_.size());

    if (latencyFilename.size() > 0)
    {
        Latency_FlushTo(latencyFilename.c_str());
    }

    cooldownStarted = true;

    uint64_t cooldown_us = cooldownSec * 1e6;
    transport_.TimerMicro(cooldown_us, std::bind(&BenchmarkClient::Cleanup, this));
}