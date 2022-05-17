/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "../c_unittests/helpers.hpp"
#include "SocketException.hpp"
#include "TCPSocket.hpp"
#include "echo_server.hpp"

#include <benchmark/benchmark.h>
#include <linux/prctl.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <iostream>

extern "C" {
#include "entity_cache.h"
#include "log_private.h"
#include "parse_tree.h"

#include "qpid/dispatch.h"

// declarations that don't have .h file
void qd_error_initialize();
}  // extern "C"

static const bool VERBOSE = false;

static unsigned short findFreePort()
{
    TCPServerSocket serverSocket(0);
    unsigned short port = serverSocket.getLocalPort();
    return port;
}

static std::stringstream oneRouterTcpConfig(const unsigned short tcpConnectorPort, unsigned short tcpListenerPort)
{
    std::stringstream router_config;
    router_config << R"END(
router {
    mode: standalone
    id : QDR
}

listener {
    port : 0
}

tcpListener {
    host : 0.0.0.0
    port : )END" << tcpListenerPort
                  << R"END(
    address : ES
    siteId : siteId
}

tcpConnector {
    host : 127.0.0.1
    port : )END" << tcpConnectorPort
                  << R"END(
    address : ES
    siteId : siteId
}

log {
    module: DEFAULT
    enable: warning+
})END";

    return router_config;
}

static std::stringstream multiRouterTcpConfig(std::string routerId, const std::vector<unsigned short> listenerPorts,
                                              const std::vector<unsigned short> connectorPorts,
                                              unsigned short tcpConnectorPort, unsigned short tcpListenerPort)
{
    std::stringstream router_config;
    router_config << R"END(
router {
    mode: interior
    id : )END" << routerId
                  << R"END(
})END";

    for (auto connectorPort : connectorPorts) {
        router_config << R"END(
connector {
    host: 127.0.0.1
    port : )END" << connectorPort
                      << R"END(
    role: inter-router
})END";
    }

    for (auto listenerPort : listenerPorts) {
        router_config << R"END(
listener {
    host: 0.0.0.0
        port : )END" << listenerPort
                      << R"END(
    role: inter-router
        })END";
    }

    if (tcpListenerPort != 0) {
        router_config << R"END(
tcpListener {
    host : 0.0.0.0
    port : )END" << tcpListenerPort
                      << R"END(
    address : ES
    siteId : siteId
})END";
    }
    if (tcpConnectorPort != 0) {
        router_config << R"END(
tcpConnector {
    host : 127.0.0.1
    port : )END" << tcpConnectorPort
                      << R"END(
    address : ES
    siteId : siteId
})END";
    }

    router_config << R"END(
log {
    module: DEFAULT
    enable: warn+
})END";

    return router_config;
}

static void writeRouterConfig(const std::string &configName, const std::stringstream &router_config)
{
    std::fstream f(configName, std::ios::out);
    f << router_config.str();
    f.close();
}

static TCPSocket try_to_connect(const std::string &servAddress, int echoServPort)
{
    auto then = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - then < std::chrono::seconds(3)) {
        try {
            TCPSocket sock(servAddress, echoServPort);
            return sock;
        } catch (SocketException &e) {
        }
    }
    throw std::runtime_error("Failed to connect in time");
}

class LatencyMeasure
{
    static const int RCVBUFSIZE = 32;
    char echoBuffer[RCVBUFSIZE + 1];  // '\0'

    std::string servAddress = "127.0.0.1";
    std::string echoString  = "echoString";
    int echoStringLen       = echoString.length();

   public:
    inline TCPSocket createSocket(benchmark::State &state, unsigned short echoServerPort) {
        for (int i = 0; i < 30; i++) {
            try {
                TCPSocket sock = try_to_connect(servAddress, echoServerPort);
                sock.setRecvTimeout(std::chrono::seconds(10));
                // run few times outside benchmark to clean the pipes first
                latencyMeasureSendReceive(state, sock);
                latencyMeasureSendReceive(state, sock);
                latencyMeasureSendReceive(state, sock);
                return sock;
            } catch (SocketException&) {
                if(VERBOSE) printf("latencyMeasureLoop: recv err\n");
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        throw std::runtime_error("Could not get working socket");
    }
    inline void latencyMeasureLoop(benchmark::State &state, unsigned short echoServerPort)
    {
        TCPSocket sock = createSocket(state, echoServerPort);

        if(VERBOSE) printf("latencyMeasureLoop: BEGIN state loop\n");

        for (auto _ : state) {
            if (!latencyMeasureSendReceive(state, sock)) {
                state.SkipWithError("unable to read from socket");
                break;
            }
        }

        if(VERBOSE) printf("latencyMeasureLoop: END state loop\n");
    }

    inline bool latencyMeasureSendReceive(benchmark::State &state, TCPSocket &sock)
    {
        // todo: ensure we always receive what we sent, cannot build a bunch of in-flight values
        sock.send(echoString.c_str(), echoStringLen);

        int totalBytesReceived = 0;
        while (totalBytesReceived < echoStringLen) {
            int bytesReceived = sock.recv(echoBuffer, RCVBUFSIZE);
            if (bytesReceived <= 0) {
                return false;  // we failed, bail out
            }
            totalBytesReceived += bytesReceived;
            echoBuffer[bytesReceived] = '\0';
        }
        return true;
    }
};

/// Measures latency between a TCP send and a receive.
/// There is only one request in flight at all times, so this is the
///  lowest conceivable latency at the most ideal condition
/// In addition, all sends are of the same (tiny) size
static void BM_TCPEchoServerLatencyWithoutQDR(benchmark::State &state)
{
    EchoServerThread est;

    LatencyMeasure lm;
    lm.latencyMeasureLoop(state, est.port());
}

BENCHMARK(BM_TCPEchoServerLatencyWithoutQDR)->Unit(benchmark::kMillisecond);

class DispatchRouterThreadTCPLatencyTest
{
    QDR mQdr{};
    std::thread mT;

   public:
    DispatchRouterThreadTCPLatencyTest(const std::string configName)
    {
        Latch mx;

        mT = std::thread([&mx, this, &configName]() {
            mQdr.initialize(configName);
            mQdr.wait();

            mx.notify();
            mQdr.run();

            mQdr.deinitialize(false);
        });

        mx.wait();
    }

    ~DispatchRouterThreadTCPLatencyTest()
    {
        mQdr.stop();
        mT.join();
    }
};

class DispatchRouterSubprocessTcpLatencyTest
{
    int pid;

   public:
    explicit DispatchRouterSubprocessTcpLatencyTest(std::string configName)
    {
        pid = fork();
        if (pid == 0) {
            // https://stackoverflow.com/questions/10761197/prctlpr-set-pdeathsig-signal-is-called-on-parent-thread-exit-not-parent-proc
            prctl(PR_SET_PDEATHSIG, SIGHUP);
            QDR qdr{};
            qdr.initialize(configName);
            qdr.wait();

            qdr.run();  // this never returns until signal is sent, and then process dies
        }
    }

    ~DispatchRouterSubprocessTcpLatencyTest()
    {
        int ret = kill(pid, SIGTERM);
        if (ret != 0) {
            perror("Killing router");
        }
        int status;
        ret = waitpid(pid, &status, 0);
        if (ret != pid) {
            perror("Waiting for child");
        }
    }
};

static void BM_TCPEchoServerLatency1QDRThread(benchmark::State &state)
{
    auto est                        = make_unique<EchoServerThread>();
    unsigned short tcpConnectorPort = est->port();
    unsigned short tcpListenerPort  = findFreePort();

    std::string configName          = "BM_TCPEchoServerLatency1QDRThread";
    std::stringstream router_config = oneRouterTcpConfig(tcpConnectorPort, tcpListenerPort);
    writeRouterConfig(configName, router_config);

    DispatchRouterThreadTCPLatencyTest drt{configName};

    {
        LatencyMeasure lm;
        lm.latencyMeasureLoop(state, tcpListenerPort);
    }
    // kill echo server first
    // when dispatch is stopped first, echo server then sometimes hangs on socket recv, and dispatch leaks more
    // (suppressed leaks):
    /*
        76: Assertion `leak_reports.length() == 0` failed in ../tests/c_benchmarks/../c_unittests/helpers.hpp line 136:
            alloc.c: Items of type 'qd_buffer_t' remain allocated at shutdown: 3 (SUPPRESSED)
        76: alloc.c: Items of type 'qd_message_t' remain allocated at shutdown: 2 (SUPPRESSED)
        76: alloc.c: Items of type 'qd_message_content_t' remain allocated at shutdown: 2 (SUPPRESSED)
        76: alloc.c: Items of type 'qdr_delivery_t' remain allocated at shutdown: 2 (SUPPRESSED)
     */
    est.reset();
}

BENCHMARK(BM_TCPEchoServerLatency1QDRThread)->Unit(benchmark::kMillisecond);

static void BM_TCPEchoServerLatency1QDRSubprocess(benchmark::State &state)
{
    EchoServerThread est;
    unsigned short tcpConnectorPort = est.port();
    unsigned short tcpListenerPort  = findFreePort();

    std::string configName          = "BM_TCPEchoServerLatency1QDRSubprocess.conf";
    std::stringstream router_config = oneRouterTcpConfig(tcpConnectorPort, tcpListenerPort);
    writeRouterConfig(configName, router_config);

    DispatchRouterSubprocessTcpLatencyTest drt(configName);

    {
        LatencyMeasure lm;
        lm.latencyMeasureLoop(state, tcpListenerPort);
    }
}

BENCHMARK(BM_TCPEchoServerLatency1QDRSubprocess)->Unit(benchmark::kMillisecond);

static void BM_TCPEchoServerLatency2QDRSubprocess(benchmark::State &state)
{
    EchoServerThread est;

    unsigned short listener_2    = findFreePort();
    unsigned short tcpListener_2 = findFreePort();

    std::string configName_1          = "BM_TCPEchoServerLatency2QDRSubprocess_1.conf";
    std::stringstream router_config_1 = multiRouterTcpConfig("QDRL1", {}, {listener_2}, est.port(), 0);
    writeRouterConfig(configName_1, router_config_1);

    std::string configName_2          = "BM_TCPEchoServerLatency2QDRSubprocess_2.conf";
    std::stringstream router_config_2 = multiRouterTcpConfig("QDRL2", {listener_2}, {}, 0, tcpListener_2);
    writeRouterConfig(configName_2, router_config_2);

    DispatchRouterSubprocessTcpLatencyTest qdr1{configName_1};
    DispatchRouterSubprocessTcpLatencyTest qdr2{configName_2};

    {
        LatencyMeasure lm;
        lm.latencyMeasureLoop(state, tcpListener_2);
    }
}

BENCHMARK(BM_TCPEchoServerLatency2QDRSubprocess)->Unit(benchmark::kMillisecond);

static void BM_TCPEchoServerLatencyNQDRSubprocess(benchmark::State &state)
{
    EchoServerThread est;

    int N                       = state.range(0);
    unsigned short tcpConnector = est.port();
    unsigned short tcpListener  = findFreePort();

    std::vector<unsigned short> listeners{};  // first one is unused
    listeners.reserve(N);
    for (int i = 0; i < N; ++i) {
        listeners.push_back(findFreePort());
    }

    std::string configName_1          = "BM_TCPEchoServerLatencyNQDRSubprocess_first.conf";
    std::stringstream router_config_1 = multiRouterTcpConfig("QDRL1", {}, {listeners[1]}, tcpConnector, 0);
    writeRouterConfig(configName_1, router_config_1);

    std::string configName_2 = "BM_TCPEchoServerLatencyNQDRSubprocess_last.conf";
    std::stringstream router_config_2 =
        multiRouterTcpConfig("QDRL2", {listeners[listeners.size() - 1]}, {}, 0, tcpListener);
    writeRouterConfig(configName_2, router_config_2);

    DispatchRouterSubprocessTcpLatencyTest qdr1{configName_1};
    DispatchRouterSubprocessTcpLatencyTest qdr2{configName_2};

    // interior routers
    std::vector<DispatchRouterSubprocessTcpLatencyTest> interior;
    interior.reserve(N - 2);
    for (int i = 1; i < N - 1; i++) {
        std::stringstream ss;
        ss << "BM_TCPEchoServerLatencyNQDRSubprocess_" << i << ".conf";
        std::stringstream id;
        id << "ROUTER" << i;
        std::string configName          = ss.str();
        std::stringstream router_config = multiRouterTcpConfig(id.str(), {listeners[i]}, {listeners[i + 1]}, 0, 0);
        writeRouterConfig(configName, router_config);

        interior.emplace_back(configName);  // this ends up starting router subprocesses; behold magic of C++
    }

    {
        LatencyMeasure lm;
        lm.latencyMeasureLoop(state, tcpListener);
    }
}

BENCHMARK(BM_TCPEchoServerLatencyNQDRSubprocess)
    ->Unit(benchmark::kMillisecond)
    ->Arg(2)
    ->Arg(3)
    ->Arg(4)
    ->Arg(5)
    ->Arg(6)
    ->Arg(7)
    ->Arg(8)
    ->Arg(9)
    ;