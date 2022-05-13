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
# under the License.
#

# Adds connectors in order to ensure a full mesh for a deployment or
# similar

import json
import http.client
import os
import ssl
import sys
import time
import traceback


TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token"
NAMESPACE_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

IP = 0  # Field to reference pod IP address
TS = 1  # Field to reference pod timestamp


def read_lines(name):
    with open(name, 'r') as f:
        return f.readlines()


def read_file(name):
    with open(name, 'r') as f:
        return f.read()


def retrieve_token():
    return read_file(TOKEN_FILE).replace('\n', '')


def retrieve_namespace():
    return os.environ.get("POD_NAMESPACE") or read_file(NAMESPACE_FILE)


def api_request(host, port, path, token):
    ''' Makes REST request to API server and returns response'''
    context = ssl._create_unverified_context()
    conn = http.client.HTTPSConnection(host, port, timeout=5, context=context)
    conn._context.check_hostname = False
    conn._context.verify_mode = ssl.CERT_NONE
    # conn.set_debuglevel(1)

    headers = {}
    headers['Authorization'] = "Bearer %s" % token

    conn.request("GET", path, headers=headers)
    res = conn.getresponse()

    return res


def extract_ips(response, name):
    '''
    Given an API response, return list of pod ips ordered by pod start time.
    Deleted pods will not be included in this list.
    '''
    pod_list = []

    js = json.loads(response.read())

    # Check if the HTTP response code is >= 400 and print the failure message
    if response.code >= 400:
        print("HTTP ERROR CODE: " + str(response.code))
        print("ERROR MESSAGE  : " + response.reason)
        return pod_list

    if js.get("items"):
        for pod in js['items']:
            # Filter non router pods
            if('application' in pod['metadata']['labels'] and
               pod['metadata']['labels']['application'] == name):
                if 'podIP' not in pod['status']:
                    print("Waiting for IP address...")
                    return None

                # If the pod's deletionTimestamp is set, it has been
                # deleted and we don't want to include it in our pod_list
                pod_metadata = pod.get("metadata")
                if pod_metadata.get('deletionTimestamp'):
                    continue

                ip = pod['status']['podIP']
                st = pod['status']['startTime']

                # Format timestamp so it can be sorted
                timestamp = time.strptime(st, '%Y-%m-%dT%H:%M:%SZ')
                pod_list.append([ip, timestamp])

    # There is a possibility that all pods have been deleted and
    # pod_list is empty
    if pod_list:
        # sort pods in ascending order by timestamp
        pod_list.sort(key=lambda x: x[TS])
        return [pod[IP] for pod in pod_list]

    return pod_list


def write_connectors(hosts, port="55672", properties=None):
    if properties is None:
        properties = {}
    for host in hosts:
        print("connector {")
        print("  role: inter-router")
        print("  host: %s", host)
        print("  port: %s", port)
        for prop in properties:
            print("  %s: %s", prop)
        print("}")


def get_connectors(hosts, port="55672", properties=None):
    if properties is None:
        properties = {}
    connectors = [{"role": "inter-router", "host": host, "port": port} for host in hosts]
    for c in connectors:
        c.update(properties)
    return connectors


# add SASL properties, if they are set on the environment
def add_sasl_properties(properties):
    sasl_mechanisms = os.getenv('QDROUTERD_AUTO_MESH_SASL_MECHANISMS')
    sasl_username = os.getenv('QDROUTERD_AUTO_MESH_SASL_USERNAME')
    sasl_password = os.getenv('QDROUTERD_AUTO_MESH_SASL_PASSWORD')
    if sasl_username and sasl_password:
        if sasl_mechanisms:
            properties["saslMechanisms"] = sasl_mechanisms
        properties["saslUsername"] = sasl_username
        properties["saslPassword"] = sasl_password
    return properties


class SimpleParser:

    def __init__(self, conf_file):
        self.lines = read_lines(conf_file)
        self.position = 0

    def current(self):
        if self.position < len(self.lines):
            return self.lines[self.position]
        else:
            return None

    def next(self):
        line = self.current()
        while line and (line.strip().startswith("#") or len(line) == 0 or line.isspace()):
            self.position += 1
            line = self.current()
        return line

    def parse(self):
        entities = []
        while self.next():
            e = self.parse_entity()
            if e:
                entities.append(e)
        return entities

    def parse_entity(self):
        name = self.parse_name()
        if name:
            properties = self.parse_properties()
            return (name, properties)
        else:
            return None

    def parse_name(self):
        line = self.next()
        name = line.strip().split("{", 1)
        self.position += 1
        return name[0].strip()

    def parse_properties(self):
        line = self.next()
        if not line:
            raise Exception("Could not parse properties: %s" % self.lines[-1])
        properties = {}
        while not line.strip() == "}":
            (name, value) = line.split(":", 1)
            if not name.isspace() and not value.isspace():
                properties[name.strip()] = value.strip()
                self.position += 1
                line = self.next()
                if not line:
                    raise Exception("Could not find end of properties: %s" % self.lines[-1])
            else:
                raise Exception("Could not parse property (line %i): %s" % (self.position + 1, line))
        self.position += 1
        return properties


class JsonConfig:
    def __init__(self, filename):
        self.filename = filename
        with open(self.filename) as f:
            self.entities = json.load(f)

    def get_inter_router_properties(self):
        for e in self.entities:
            name = e[0]
            properties = e[1]
            if name == "listener" and properties and properties.get("role") == "inter-router":
                outval = {"port": properties.get("port", "55672")}
                add_sasl_properties(outval)
                if "sslProfile" in properties:
                    outval["sslProfile"] = properties["sslProfile"]
                    outval["verifyHostname"] = "no"
                return outval
        return {"port": "55672"}

    def append_connectors(self, connectors):
        properties = self.get_inter_router_properties()
        for c in connectors:
            c.update(properties)
            self.entities.append(['connector', c])
        with open(self.filename, 'w') as f:
            json.dump(self.entities, f, indent=4)


class SimpleConfig:
    def __init__(self, filename):
        self.filename = filename

    def get_inter_router_properties(self):
        entities = SimpleParser(self.filename).parse()
        for name, properties in entities:
            if name == "listener" and properties and properties.get("role") == "inter-router":
                outval = {"port": properties.get("port", "55672")}
                add_sasl_properties(outval)
                if "sslProfile" in properties:
                    outval["sslProfile"] = properties["sslProfile"]
                    outval["verifyHostname"] = "no"
                return outval
        return {"port": "55672"}

    def append_connectors(self, connectors):
        properties = self.get_inter_router_properties()
        with open(self.filename, "a") as f:
            print("", file=f)
            print("# generated by auto-mesh %s" % mode, file=f)
            for c in connectors:
                c.update(properties)
                print("connector {", file=f)
                for k, v in c.items():
                    print("    %s: %s" % (k, v), file=f)
                print("}", file=f)


def get_config(config_file):
    try:
        return JsonConfig(config_file)
    except:
        return SimpleConfig(config_file)


def query():
    host = os.environ['KUBERNETES_SERVICE_HOST']
    port = os.environ['KUBERNETES_PORT_443_TCP_PORT']
    ip = os.environ['POD_IP']
    ns = os.environ['POD_NAMESPACE']
    name = os.environ['APPLICATION_NAME']

    path = "/api/v1/namespaces/" + ns + "/pods"
    token = retrieve_token()

    while True:
        # Send REST requests until other routers are ready
        response = api_request(host, port, path, token)
        ip_list = extract_ips(response, name)
        if ip_list is not None:
            break
        # ip_list is empty, try again after one second
        time.sleep(1)

    # Get the ip addresses of the routers already running
    if ip_list:
        # Keep only pods created after current pod
        si = ip_list.index(ip)
        ip_list = ip_list[:si]
        return [{"role": "inter-router", "host": "config-sync-ignore-%s" % host} for host in ip_list]
    return ip_list


def infer():
    service_name = os.environ.get("QDROUTERD_AUTO_MESH_SERVICE_NAME", "%s-headless" % os.environ.get("APPLICATION_NAME", "amq-interconnect"))
    namespace = retrieve_namespace()
    (prefix, index) = os.environ["HOSTNAME"].rsplit("-", 1)
    return [{"role": "inter-router", "host": "%s-%s-%s.%s.%s.svc.cluster.local" % ("config-sync-ignore-", prefix, i, service_name, namespace)} for i in range(int(index))]


if __name__ == "__main__":
    mode = os.environ.get("QDROUTERD_AUTO_MESH_DISCOVERY")
    if mode and len(sys.argv) > 1:
        try:
            if mode.upper() == "QUERY":
                connectors = query()
            elif mode.upper() == "INFER":
                connectors = infer()
            else:
                raise Exception("Invalid value for QDROUTERD_AUTO_MESH_DISCOVERY, expected 'QUERY' or 'INFER'")
            get_config(sys.argv[1]).append_connectors(connectors)
        except Exception as e:
            traceback.print_exc()
            sys.exit("Error configuring automesh: %s" % e)
