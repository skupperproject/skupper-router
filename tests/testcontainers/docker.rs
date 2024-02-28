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

use std::time::Duration;

use bollard::Docker;
use bollard::container::{Config, CreateContainerOptions, InspectContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions, StartContainerOptions, WaitContainerOptions};
use bollard::errors::Error::DockerResponseServerError;
use bollard::errors::Error;
use bollard::exec::{CreateExecOptions, CreateExecResults, StartExecResults};
use bollard::image::{CreateImageOptions, ListImagesOptions};
use bollard::models::{ContainerCreateResponse, ContainerWaitResponse, CreateImageInfo, HostConfig, NetworkCreateResponse, PortBinding};
use bollard::network::{CreateNetworkOptions, InspectNetworkOptions};
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::{Fuse};
use futures::{select, Stream, stream};
use tokio::task::JoinHandle;

// how long to wait to print remaining container logs
const DRAIN_LOGS_TIMEOUT: Duration = Duration::from_secs(5);

// https://hub.docker.com/r/summerwind/h2spec
pub(crate) const H2SPEC_IMAGE: &str = "docker.io/summerwind/h2spec:2.6.0";  // do not prefix with `docker.io/` for docker; podman does not mind
// https://nixery.dev/
pub(crate) const NGHTTP2_IMAGE: &str = "nixery.dev/nghttp2:latest";
pub(crate) const NETCAT_IMAGE: &str = "nixery.dev/netcat:latest";

pub(crate) struct LogPrinter {
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for LogPrinter {
    fn drop(&mut self) {
        for task in self.tasks.drain(..).rev() {
            println!("Finalizing LogPrinter");
            let result = futures::executor::block_on(
                tokio::time::timeout(DRAIN_LOGS_TIMEOUT, task));
            println!("Finalized LogPrinter {:?}", result);
        }
    }
}

impl LogPrinter {
    pub(crate) fn new() -> Self {
        Self {
            tasks: vec![],
        }
    }

    pub(crate) fn print(&mut self, log_stream: Fuse<(impl Stream<Item=String> + Unpin + Sized + Send + 'static)>) {
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

pub(crate) async fn get_container_gateway(docker: &Docker, container: &ContainerCreateResponse) -> String {
    let inspection_skrouterd = docker.inspect_container(&*container.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let network_settings_skrouterd = inspection_skrouterd.network_settings.unwrap();
    let gateway = network_settings_skrouterd.gateway.unwrap();
    gateway
}

pub(crate) async fn get_container_exposed_ports(docker: &Docker, container: &ContainerCreateResponse, port: &str) -> PortBinding {
    let inspection_skrouterd = docker.inspect_container(&*container.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let network_settings_skrouterd = inspection_skrouterd.network_settings.unwrap();
    let ports = network_settings_skrouterd.ports.unwrap();
    let amqp = ports[port].clone().unwrap();
    let ipport = amqp.first().unwrap();
    return ipport.clone()
}

pub(crate) async fn get_container_hostname(docker: &Docker, container: &ContainerCreateResponse) -> String {
    let inspection_skrouterd = docker.inspect_container(&*container.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let network_settings_skrouterd = inspection_skrouterd.network_settings.unwrap();
    let hostname = network_settings_skrouterd.networks.unwrap().values().take(1).next().unwrap().ip_address.as_ref().unwrap().clone();
    hostname
}

/// Runs a nc container which tries to connect to the provided hostname and port.
pub(crate) async fn wait_for_port_using_nc(docker: &Docker, hostconfig: HostConfig, hostname: &str, port: &str) -> Result<(), Box<dyn std::error::Error>> {
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

pub(crate) async fn find_or_pull_image(docker: &Docker, image: &str) -> String {
    if let Some(image_id) = docker_find_image(docker, image).await {
        return image_id;
    }
    let pull = docker_pull(&docker, image).await;
    return docker_find_image(docker, image).await.expect(&*format!("{:?}", pull));
}

pub(crate) async fn docker_find_image(docker: &Docker, image: &str) -> Option<String> {
    for found in docker.list_images(None::<ListImagesOptions<String>>).await.unwrap() {
        if found.repo_tags.iter().any(|t| t.contains(image)) {
            return Some(found.id);
        }
    }
    None
}

pub(crate) async fn docker_pull(docker: &Docker, image: &str) -> Vec<CreateImageInfo> {
    docker.create_image(Some(CreateImageOptions {
        from_image: image,
        ..Default::default()
    }), None, None).try_collect::<Vec<_>>().await.unwrap()
}

pub(crate) async fn create_and_start_container(docker: &Docker, image: &str, name: &str, config: Config<&str>) -> ContainerCreateResponse {
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

pub(crate) async fn delete_container(docker: &Docker, name: &str) {
    match docker.remove_container(name, Some(RemoveContainerOptions {
        force: true,
        ..Default::default()
    })).await {
        Ok(_) => { print!("Deleted container {}\n", name) }
        Err(DockerResponseServerError { status_code: 404, message: m }) => { print!("got error 404, ignoring {:?}", m) }
        default => panic!("{:?}", default)
    };
}

pub(crate) async fn recreate_network(docker: &Docker, network_name: &str) -> NetworkCreateResponse {
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

pub(crate) fn stream_container_logs(docker: &Docker, name: &'static str, container: &ContainerCreateResponse) -> impl Stream<Item=String> {
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

pub(crate) fn label_log(label: &'static str, stream: impl futures::Stream<Item=Result<LogOutput, Error>>) -> impl Stream<Item=String> {
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

pub(crate) async fn get_container_exit_code(docker: &Docker, container: &ContainerCreateResponse) -> Result<i64, Box<dyn std::error::Error>> {
    let result: Option<Result<ContainerWaitResponse, Error>> = docker.wait_container(&*container.id, None::<WaitContainerOptions<String>>).next().await;
    let exit_code = match result.ok_or("no result")? {
        Ok(c) => Ok(c.status_code),
        Err(Error::DockerContainerWaitError { error: _, code }) => Ok(code),
        Err(e) => Err(e.into())
    };
    exit_code
}

pub(crate) async fn docker_read_output(result: StartExecResults) -> (String, String) {
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

pub(crate) async fn docker_exec(docker: &Docker, container: &ContainerCreateResponse, command: Vec<&str>) -> (CreateExecResults, StartExecResults) {
    let exec = docker.create_exec(&*container.id, CreateExecOptions {
        cmd: Some(command),
        attach_stdout: Some(true),
        attach_stderr: Some(true),
        ..Default::default()
    }).await.unwrap();
    let result = docker.start_exec(&*exec.id, None).await.unwrap();
    (exec, result)
}
