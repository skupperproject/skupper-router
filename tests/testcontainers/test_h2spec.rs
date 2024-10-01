/*
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
 */

use std::env;
use std::io::{stdout, Write};
use std::time::Duration;

use bollard::Docker;
use bollard::container::{Config, KillContainerOptions, StopContainerOptions, WaitContainerOptions};
use bollard::models::{HostConfig, PortBinding};
use futures::StreamExt;
use futures::{FutureExt, pin_mut, select, stream_select};

use crate::collection;
use crate::testcontainers::docker::{create_and_start_container, find_or_pull_image, get_container_exit_code, get_container_hostname, H2SPEC_IMAGE, NETCAT_IMAGE, NGHTTP2_IMAGE, recreate_network, stream_container_logs, wait_for_port_using_nc};

const ROUTER_CONFIG: &str = r#"
router {
    mode: standalone
    id: QDR
}

listener {
    port: 24163
    role: normal
    host: 0.0.0.0
    saslMechanisms: ANONYMOUS
    idleTimeoutSeconds: 120
    authenticatePeer: no
}

tcpListener {
    port: 24162
    address: examples
    host: 0.0.0.0
}

tcpConnector {
    port: 8888
    address: examples
    host: nghttpd
    name: grpc-server
}

log {
    module: DEFAULT
    #enable: trace+
    #enable: info+
    includeSource: true
}

"#;

/**
 * Performs the DISPATCH-1940 reproducer steps using Container images. To run manually:
 *
 * 1. Write the `ROUTER_CONFIG` above to a file, say `h2spec_router.conf`
 *     - change `host: nghttpd` to `host: localhost`
 * 2. Run router, nghttpd and h2spec, each in its own terminal, using docker or (for skrouterd) without it
 *     - `docker run --rm -it --network=host nixery.dev/nghttp2:latest nghttpd -a 0.0.0.0 --no-tls -d /tmp 8888`
 *     - `skrouterd -c h2spec_router.conf`
 *     - `docker run --rm -it --network=host summerwind/h2spec:2.6.0 -h localhost -p 24162 --verbose --insecure --timeout 10`
 */
#[tokio::test(flavor = "multi_thread")]
async fn test_h2spec() {
    let skupper_router_image = &env::var("QDROUTERD_IMAGE").unwrap_or(String::from("quay.io/skupper/skupper-router:latest"));
    println!("Using router image: {}", skupper_router_image);

    // list all available tests by running `podman run --rm -it summerwind/h2spec:2.6.0 --dryrun`
    // TODO(ISSUE #371) DISPATCH-1940 [http2] Router HTTP2 adaptor should pass h2spec
    //  currently failing tests are generic/4, http2/5, http2/6, http2/7, which must be filtered out here
    let enabled_h2spec_tests = [
        vec!["--strict".to_string(), "hpack".to_string()],
        [2, 4, 5, 6].iter().filter(|i| **i != 4).map(|i| format!("generic/{}", i)).collect::<Vec<_>>(),
        (3..=8).filter(|i| *i != 5 && *i != 6 && *i != 7).map(|i| format!("http2/{}", i)).collect::<Vec<_>>(),
    ].concat();
    println!("Going to run these h2spec tests: {:?}", enabled_h2spec_tests);

    let docker = Docker::connect_with_local_defaults().unwrap();
    let network_name = "test_h2spec_network";
    let network = recreate_network(&docker, network_name).await;
    let network_id = network.id.unwrap();
    print!("Created network: {}, {}\n", network_name, network_id);

    let hostconfig: HostConfig = HostConfig {
        port_bindings: Some(collection! {
            "24162/tcp".to_string() => Some(vec![PortBinding{host_ip: None, host_port: None}]),
            "8888/tcp".to_string() => Some(vec![PortBinding{host_ip: None, host_port: None}]),
        }),
        publish_all_ports: Some(true),
        network_mode: Some(network_id),
        cap_add: Some(vec!["SYS_PTRACE".to_string()]),
        ..Default::default()
    };

    // prefetch all images before creating containers

    find_or_pull_image(&docker, NGHTTP2_IMAGE).await;
    find_or_pull_image(&docker, skupper_router_image).await;
    find_or_pull_image(&docker, NETCAT_IMAGE).await;
    find_or_pull_image(&docker, H2SPEC_IMAGE).await;

    // create container_nghttpd

    let container_nghttpd = create_and_start_container(
        &docker, NGHTTP2_IMAGE, "nghttpd",
        Config {
            host_config: Some(hostconfig.clone()),
            cmd: Some(vec!["nghttpd", "-a", "0.0.0.0", "--no-tls", "-d", "/tmp", "8888"]),
            ..Default::default()
        }).await;
    let logs_nghttpd = stream_container_logs(&docker, "nghttpd", &container_nghttpd);

    // container_skrouterd

    let container_skrouterd = create_and_start_container(
        &docker, skupper_router_image, "test_h2spec_skrouterd",
        Config {
            host_config: Some(hostconfig.clone()),
            env: Some(vec![
                format!("QDROUTERD_CONF={}", ROUTER_CONFIG).as_str(),
            ]),
            ..Default::default()
        }).await;
    let logs_skrouterd = stream_container_logs(&docker, "skrouterd", &container_skrouterd);

    let hostname = get_container_hostname(&docker, &container_skrouterd).await;

    wait_for_port_using_nc(&docker, hostconfig.clone(), &hostname, "24162").await.expect("Port was not open in time");

    // container_h2spec

    let h2args: Vec<&str> = [
        vec!["-h", &hostname, "-p", "24162", "--verbose", "--insecure", "--timeout", "10"],
        enabled_h2spec_tests.iter().map(String::as_str).collect()
    ].concat();
    let container_h2spec = create_and_start_container(
        &docker, H2SPEC_IMAGE, "h2spec",
        Config {
            host_config: Some(hostconfig.clone()),
            cmd: Some(h2args),
            ..Default::default()
        }).await;
    let logs_h2spec = stream_container_logs(&docker, "h2spec", &container_h2spec);

    // wait 5 seconds after h2spec finishes, keep printing logs
    let wait = async {
        docker.wait_container(&*container_h2spec.id, None::<WaitContainerOptions<String>>).next().await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    };

    let mut stream = stream_select!(logs_nghttpd, logs_skrouterd, logs_h2spec).fuse();
    let wait = wait.fuse();
    pin_mut!(wait);

    while select! {
        msg = stream.next() => match msg {
            Some(msg) => {
                let msg: String = msg;
                println!("{}", msg);
                true
            },
            None => false
        },
        _ = wait => false
    } {}

    let h2spec_exit_code = get_container_exit_code(&docker, &container_h2spec).await;
    println!("container h2spec finished with exit code {:?}", h2spec_exit_code);

    // stop the http2 server container so that router frees connection-related data
    docker.stop_container(&*container_nghttpd.id, None::<StopContainerOptions>).await.unwrap();
    // run the router container until completion; shutting router first would cause leaks report
    docker.kill_container(&*container_skrouterd.id, Some(KillContainerOptions { signal: "SIGTERM" })).await.unwrap();
    while let Some(msg) = stream.next().await {
        println!("{}", msg);
    }

    let router_exit_code = get_container_exit_code(&docker, &container_skrouterd).await;
    println!("container skrouterd finished with exit code {:?}", router_exit_code);

    stdout().flush().expect("failed to flush stdout");
    assert_eq!(0, h2spec_exit_code.unwrap());
    assert_eq!(0, router_exit_code.unwrap());
}
