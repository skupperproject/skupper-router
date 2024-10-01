#!/bin/bash -ex

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License
#

# Creates TLS certificate files for use by the system tests.

export SERVER=localhost
export CLIENT=127.0.0.1

# Create a self-signed CA
openssl genrsa -aes256 -passout pass:ca-password -out ca-private-key.pem 4096
openssl req -key ca-private-key.pem -new -x509 -days 99999 -out ca-certificate.pem -passin pass:ca-password -subj "/C=US/ST=New York/L=Brooklyn/O=Trust Me Inc./CN=Trusted.CA.com"

# Create a server certificate signed by the CA
openssl genrsa -aes256 -passout pass:server-password -out server-private-key.pem 4096
openssl req -new -key server-private-key.pem -passin pass:server-password -out server.csr -subj "/C=US/ST=CA/L=San Francisco/O=Server/CN=$SERVER"
openssl x509 -req -in server.csr -CA ca-certificate.pem -CAkey ca-private-key.pem -CAcreateserial -days 99999 -out server-certificate.pem -passin pass:ca-password
# strip the password to make a passwordless key for testing
openssl rsa -in server-private-key.pem -passin pass:server-password -out server-private-key-no-pass.pem

# Create a client certificate signed by the CA
openssl genrsa -aes256 -passout pass:client-password -out client-private-key.pem 4096
openssl req -new -key client-private-key.pem -passin pass:client-password -out client.csr -subj "/C=US/ST=CA/L=San Francisco/OU=Dev/O=Client/CN=$CLIENT"
openssl x509 -req -in client.csr -CA ca-certificate.pem -CAkey ca-private-key.pem -CAcreateserial -days 99999 -out client-certificate.pem -passin pass:ca-password
# strip the password to make a passwordless key for testing
openssl rsa -in client-private-key.pem -passin pass:client-password -out client-private-key-no-pass.pem

# Verify the certs:
openssl verify -verbose -CAfile ca-certificate.pem server-certificate.pem
openssl verify -verbose -CAfile ca-certificate.pem client-certificate.pem

#
# Create a "bad" CA for negative testing
#

openssl genrsa -aes256 -passout pass:bad-ca-password -out bad-ca-private-key.pem 4096
openssl req -key bad-ca-private-key.pem -new -x509 -days 99999 -out bad-ca-certificate.pem -passin pass:bad-ca-password -subj "/C=US/ST=New York/L=Brooklyn/O=Do Not Trust Me Inc./CN=Bad.CA.com"

cat server-certificate.pem ca-certificate.pem > chained.pem

#
# Generate an alternative set of certificats for testing certificate update
#

openssl genrsa -aes256 -passout pass:ca2-password -out ca2-private-key.pem 4096
openssl req -key ca2-private-key.pem -new -x509 -days 99999 -out ca2-certificate.pem -passin pass:ca2-password -subj "/C=US/ST=New York/L=Brooklyn/O=Trust Me Too Inc./CN=Trusted.CA2.com"

openssl genrsa -aes256 -passout pass:server2-password -out server2-private-key.pem 4096
openssl req -new -key server2-private-key.pem -passin pass:server2-password -out server2.csr -subj "/C=US/ST=CA/L=San Francisco/O=Server2/CN=$SERVER"
openssl x509 -req -in server2.csr -CA ca2-certificate.pem -CAkey ca2-private-key.pem -CAcreateserial -days 99999 -out server2-certificate.pem -passin pass:ca2-password
openssl rsa -in server2-private-key.pem -passin pass:server2-password -out server2-private-key-no-pass.pem

openssl genrsa -aes256 -passout pass:client2-password -out client2-private-key.pem 4096
openssl req -new -key client2-private-key.pem -passin pass:client2-password -out client2.csr -subj "/C=US/ST=CA/L=San Francisco/OU=Dev/O=Client2/CN=$CLIENT"
openssl x509 -req -in client2.csr -CA ca2-certificate.pem -CAkey ca2-private-key.pem -CAcreateserial -days 99999 -out client2-certificate.pem -passin pass:ca2-password
openssl rsa -in client2-private-key.pem -passin pass:client2-password -out client2-private-key-no-pass.pem

# Verify the certs:
openssl verify -verbose -CAfile ca2-certificate.pem server2-certificate.pem
openssl verify -verbose -CAfile ca2-certificate.pem client2-certificate.pem



