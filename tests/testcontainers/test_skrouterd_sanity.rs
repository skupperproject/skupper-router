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
use std::time::Duration;

use bollard::Docker;
use bollard::container::{Config};
use bollard::models::{HostConfig, PortBinding};
use futures::StreamExt;
use speculoos::assert_that;
use speculoos::prelude::StrAssertions;

use crate::collection;
use crate::testcontainers::docker::LogPrinter;
use crate::testcontainers::docker::{create_and_start_container, docker_exec, docker_read_output, find_or_pull_image, get_container_exposed_ports, get_container_gateway, NETCAT_IMAGE, stream_container_logs, wait_for_port_using_nc};

/**
 * Starts skrouterd and queries it with skmanage from inside the same container.
 *
 * Issue #925: ModuleNotFoundError: No module named 'proton'
 */
#[tokio::test(flavor = "multi_thread")]
async fn test_skrouterd_sanity() -> Result<(), Box<dyn std::error::Error>> {
    let skupper_router_image = &env::var("QDROUTERD_IMAGE").unwrap_or(String::from("quay.io/skupper/skupper-router:latest"));
    println!("Using router image: {}", skupper_router_image);

    let hostconfig: HostConfig = HostConfig {
        cap_add: Some(vec!["SYS_PTRACE".to_string()]),
        port_bindings: Some(collection! {
            "5672/tcp".to_string() => Some(vec![PortBinding{host_ip: None, host_port: None}]),
        }),
        ..Default::default()
    };

    let docker = Docker::connect_with_local_defaults().unwrap();

    // prefetch all images before creating containers
    find_or_pull_image(&docker, skupper_router_image).await;
    find_or_pull_image(&docker, NETCAT_IMAGE).await;

    let skrouterd = create_and_start_container(
        &docker, skupper_router_image, "skrouterd_sanity",
        Config {
            host_config: Some(hostconfig.clone()),
            ..Default::default()
        }).await;

    let mut log_printer = LogPrinter::new();
    let logs_skrouterd = stream_container_logs(&docker, "skrouterd", &skrouterd);

    log_printer.print(logs_skrouterd.fuse());

    let gateway = get_container_gateway(&docker, &skrouterd).await;
    let ipport = get_container_exposed_ports(&docker, &skrouterd, "5672/tcp").await;
    wait_for_port_using_nc(&docker, hostconfig, &*gateway, &*ipport.host_port.unwrap()).await?;

    let skstat_command = vec!["skstat", "-l"];
    let (exec, result) = docker_exec(&docker, &skrouterd, skstat_command).await;
    let (stdout, stderr) = tokio::time::timeout(Duration::from_secs(10),docker_read_output(result)).await.unwrap();
    println!("{}", stdout);
    println!("{}", stderr);

    assert_that(&stdout).contains("Router Links");
    assert_that(&stdout).contains("$management");

    let exec_result = docker.inspect_exec(&*exec.id).await?;
    assert_eq!(exec_result.exit_code, Some(0));

    docker.stop_container(&*skrouterd.id, None).await.unwrap();

    return Ok(());
}
