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

#ifndef QPID_DISPATCH_ECHO_SERVER_HPP
#define QPID_DISPATCH_ECHO_SERVER_HPP

#include "../c_unittests/helpers.hpp"
#include "SocketException.hpp"
#include "TCPServerSocket.hpp"

#include <iostream>
#include <thread>
int run_echo_server();

void stop_echo_server();

const unsigned int recv_buffer_size = 32;

class EchoServer
{
    unsigned short mPort;
    TCPServerSocket servSock;

   public:
    // if mPort is 0, random free port will be allocated and assigned
    EchoServer(unsigned short port = 0) : mPort(port), servSock(mPort)
    {
        if (mPort == 0) {
            mPort = servSock.getLocalPort();
        }
    }

    // will handle TCP clients one at a time and it will return on failure (when server socket is closed, most likely)
    void run()
    {
        while(true) {
            try {
                HandleTCPClient(servSock.accept());
            } catch (SocketException &e) {
                std::cerr << e.what() << std::endl;  // TODO: error is expected now, make it silent
                break;
            }
        }
    }

    void stop()
    {
        servSock.shutdown();
    }

    unsigned short port()
    {
        return mPort;
    }

   private:
    void HandleTCPClient(TCPSocket *sock)
    {
        char echoBuffer[recv_buffer_size];
        int recvMsgSize;
        while ((recvMsgSize = sock->recv(echoBuffer, recv_buffer_size)) > 0) {
            sock->send(echoBuffer, recvMsgSize);
        }
        delete sock;
    }
};

class EchoServerThread
{
    Latch portLatch;
    Latch echoServerLatch;
    unsigned short echoServerPort;
    std::thread u;

    EchoServer es{0};

   public:
    EchoServerThread()
    {
        u = std::thread([this]() {
            echoServerPort = es.port();
            portLatch.notify();
            es.run();
        });

        portLatch.wait();
    }

    ~EchoServerThread()
    {
        es.stop();  // if recv failed, echo server may be stuck waiting to accept(); call stop() here
        u.join();
    }

    unsigned short port() const
    {
        return echoServerPort;
    }
};

#endif  // QPID_DISPATCH_ECHO_SERVER_HPP
