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
use bollard::container::{Config, CreateContainerOptions, InspectContainerOptions, KillContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions, StartContainerOptions, StopContainerOptions, WaitContainerOptions};
use bollard::errors::Error::DockerResponseServerError;
use bollard::errors::Error;
use bollard::exec::{CreateExecOptions, CreateExecResults, StartExecResults};
use bollard::image::{CreateImageOptions, ListImagesOptions};
use bollard::models::{ContainerCreateResponse, ContainerWaitResponse, CreateImageInfo, HostConfig, NetworkCreateResponse, PortBinding};
use bollard::network::{CreateNetworkOptions, InspectNetworkOptions};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::{Fuse};
use futures::{FutureExt, pin_mut, select, Stream, stream, stream_select};
use tokio::task::JoinHandle;
use speculoos::assert_that;
use speculoos::prelude::StrAssertions;

// https://stackoverflow.com/questions/27582739/how-do-i-create-a-hashmap-literal
macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {{
        core::convert::From::from([$(($k, $v),)*])
    }};
    // set-like
    ($($v:expr),* $(,)?) => {{
        core::convert::From::from([$($v,)*])
    }};
}

// https://hub.docker.com/r/summerwind/h2spec
const H2SPEC_IMAGE: &str = "summerwind/h2spec:2.6.0";  // do not prefix with `docker.io/`, docker won't find it
// https://nixery.dev/
const NGHTTP2_IMAGE: &str = "nixery.dev/nghttp2:latest";
const NETCAT_IMAGE: &str = "nixery.dev/netcat:latest";

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
            env: Some(vec![
                format!("QDROUTERD_DEBUG={}", "asan").as_str(),
            ]),
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

httpListener {
    port: 24162
    address: examples
    host: 0.0.0.0
    protocolVersion: HTTP2
}

httpConnector {
    port: 8888
    address: examples
    host: nghttpd
    protocolVersion: HTTP2
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
                format!("QDROUTERD_DEBUG={}", "asan").as_str(),
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

struct LogPrinter {
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for LogPrinter {
    fn drop(&mut self) {
        for task in self.tasks.drain(..).rev() {
            let result = futures::executor::block_on(task);
            println!("Finalized LogPrinter {:?}", result);
        }
    }
}

impl LogPrinter {
    fn new() -> Self {
        Self {
            tasks: vec![],
        }
    }

    fn print(&mut self, log_stream: Fuse<(impl Stream<Item=String> + Unpin + Sized + Send + 'static)>) {
        let mut s = log_stream;
        let handle = tokio::spawn(async move {
            while select! {
                msg = s.next() => match msg {
                    Some(msg) => {
                        let msg: String = msg;
                        println!("{}", msg);
                        true
                    },
                    None => false
                },
            } {}
        });
        self.tasks.push(handle);
    }
}

async fn get_container_gateway(docker: &Docker, container: &ContainerCreateResponse) -> String {
    let inspection_skrouterd = docker.inspect_container(&*container.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let network_settings_skrouterd = inspection_skrouterd.network_settings.unwrap();
    let gateway = network_settings_skrouterd.gateway.unwrap();
    gateway
}

async fn get_container_exposed_ports(docker: &Docker, container: &ContainerCreateResponse, port: &str) -> PortBinding {
    let inspection_skrouterd = docker.inspect_container(&*container.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let network_settings_skrouterd = inspection_skrouterd.network_settings.unwrap();
    let ports = network_settings_skrouterd.ports.unwrap();
    let amqp = ports[port].clone().unwrap();
    let ipport = amqp.first().unwrap();
    return ipport.clone()
}

async fn get_container_hostname(docker: &Docker, container: &ContainerCreateResponse) -> String {
    let inspection_skrouterd = docker.inspect_container(&*container.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let network_settings_skrouterd = inspection_skrouterd.network_settings.unwrap();
    let hostname = network_settings_skrouterd.networks.unwrap().values().take(1).next().unwrap().ip_address.as_ref().unwrap().clone();
    hostname
}

/// Runs a nc container which tries to connect to the provided hostname and port.
async fn wait_for_port_using_nc(docker: &Docker, hostconfig: HostConfig, hostname: &str, port: &str) -> Result<(), Box<dyn std::error::Error>> {
    let retry_policy = again::RetryPolicy::exponential(Duration::from_secs(1))
        .with_jitter(true)
        .with_max_delay(Duration::from_secs(3))
        .with_max_retries(10);

    let container_netcat = create_and_start_container(
        &docker, NETCAT_IMAGE, "test_h2spec_netcat",
        Config {
            host_config: Some(hostconfig),
            cmd: Some(vec!["nc", "-zv", hostname, port]),
            ..Default::default()
        }).await;

    async fn try_connect(docker: &Docker, container: &ContainerCreateResponse) -> Result<(), Box<dyn std::error::Error>> {
        let result = get_container_exit_code(&docker, &container).await?;
        println!("Waiting for open port, nc exited with: {}", result);
        if result == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        docker.start_container(&*container.id, None::<StartContainerOptions<String>>).await.unwrap();

        return Err("".into());
    }
    let waiting_result = retry_policy.retry(|| { try_connect(&docker, &container_netcat) }).await;
    let mut logs_netcat = stream_container_logs(&docker, "netcat", &container_netcat).fuse();

    while select! {
        msg = logs_netcat.next() => match msg {
            Some(msg) => {
                let msg: String = msg;
                println!("{}", msg);
                true
            },
            None => false
        },
    } {}
    waiting_result
}

async fn find_or_pull_image(docker: &Docker, image: &str) -> String {
    if let Some(image_id) = docker_find_image(docker, image).await {
        return image_id;
    }
    let pull = docker_pull(&docker, image).await;
    return docker_find_image(docker, image).await.expect(&*format!("{:?}", pull));
}

async fn docker_find_image(docker: &Docker, image: &str) -> Option<String> {
    for found in docker.list_images(None::<ListImagesOptions<String>>).await.unwrap() {
        if found.repo_tags.iter().any(|t| t.contains(image)) {
            return Some(found.id);
        }
    }
    None
}

async fn docker_pull(docker: &Docker, image: &str) -> Vec<CreateImageInfo> {
    docker.create_image(Some(CreateImageOptions {
        from_image: image,
        ..Default::default()
    }), None, None).try_collect::<Vec<_>>().await.unwrap()
}

async fn create_and_start_container(docker: &Docker, image: &str, name: &str, config: Config<&str>) -> ContainerCreateResponse {
    let image_id = find_or_pull_image(docker, image).await;
    // to be extra sure container does not already exist
    delete_container(&docker, name).await;

    let container: ContainerCreateResponse = docker.create_container(
        Some(CreateContainerOptions { name, platform: Some("linux/amd64") }),
        Config::<&str> {
            image: Some(&*image_id),
            ..config
        }).await.unwrap();
    docker.start_container(&*container.id, None::<StartContainerOptions<String>>).await.unwrap();
    print!("started container {}, {}\n", name, container.id);

    container
}

async fn delete_container(docker: &Docker, name: &str) {
    match docker.remove_container(name, Some(RemoveContainerOptions {
        force: true,
        ..Default::default()
    })).await {
        Ok(_) => { print!("Deleted container {}\n", name) }
        Err(DockerResponseServerError { status_code: 404, message: m }) => { print!("got error 404, ignoring {:?}", m) }
        default => panic!("{:?}", default)
    };
}

async fn recreate_network(docker: &Docker, network_name: &str) -> NetworkCreateResponse {
    match docker.inspect_network(network_name, None::<InspectNetworkOptions<String>>).await {
        Ok(res) => {
            if let Some(cs) = res.containers {
                for (name, _) in cs.iter() {
                    docker.remove_container(name, Some(RemoveContainerOptions { force: true, ..Default::default() })).await.unwrap();
                }
            }
        }
        Err(_) => {}
    }

    match docker.remove_network(network_name).await {
        Ok(_) => {}
        Err(DockerResponseServerError { status_code: 404, message: m }) => { print!("ignoring {}", m); }
        default => panic!("failed to delete network: {:?}", default)
    }

    let net = docker.create_network(CreateNetworkOptions::<String> {
        name: network_name.parse().unwrap(),
        enable_ipv6: false,
        ..Default::default()
    }).await.unwrap();

    return net;
}

fn stream_container_logs(docker: &Docker, name: &'static str, container: &ContainerCreateResponse) -> impl Stream<Item=String> {
    // NOTE: containers that don't log anything will eventually (60+ sec) timeout on `RequestTimeoutError`
    let logs = docker.logs::<String>(
        &*container.id,
        Some(LogsOptions {
            stderr: true,
            stdout: true,
            follow: true,
            ..Default::default()
        }),
    );
    label_log(name, logs)
}

fn label_log(label: &'static str, stream: impl futures::Stream<Item=Result<LogOutput, Error>>) -> impl Stream<Item=String> {
    return stream
        .map(|f: Result<LogOutput, Error>|
            match f {
                Ok(log) => format!("{}", log),
                Err(err) => format!("{:?}", err)
            })
        // h2spec produces \r before checkmark unicode char.... split the hell out of it
        .map(|f| f.trim_end().split(&['\r', '\n']).map(str::to_owned).collect::<Vec<_>>())
        .flat_map(stream::iter)
        .map(move |line| format!("{}: {}", label, line));
}

async fn get_container_exit_code(docker: &Docker, container: &ContainerCreateResponse) -> Result<i64, Box<dyn std::error::Error>> {
    let result: Option<Result<ContainerWaitResponse, Error>> = docker.wait_container(&*container.id, None::<WaitContainerOptions<String>>).next().await;
    let exit_code = match result.ok_or("no result")? {
        Ok(c) => Ok(c.status_code),
        Err(Error::DockerContainerWaitError { error: _, code }) => Ok(code),
        Err(e) => Err(e.into())
    };
    exit_code
}

async fn docker_read_output(result: StartExecResults) -> (String, String) {
    let mut stdout = String::new();
    let mut stderr = String::new();
    match result {
        StartExecResults::Attached { mut output, .. } => {
            while let Some(msg) = output.next().await {
                match msg.unwrap() {
                    LogOutput::StdOut { message } => { stdout.push_str(std::str::from_utf8(&*message).unwrap()) }
                    LogOutput::StdErr { message } => { stderr.push_str(std::str::from_utf8(&*message).unwrap()) }
                    _ => {}
                }
            }
        }
        StartExecResults::Detached => {
            println!("detached");
        }
    }
    (stdout, stderr)
}

async fn docker_exec(docker: &Docker, container: &ContainerCreateResponse, command: Vec<&str>) -> (CreateExecResults, StartExecResults) {
    let exec = docker.create_exec(&*container.id, CreateExecOptions {
        cmd: Some(command),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        ..Default::default()
    }).await.unwrap();
    let result = docker.start_exec(&*exec.id, None).await.unwrap();
    (exec, result)
}
