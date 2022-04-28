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

use bollard::container::{Config, CreateContainerOptions, InspectContainerOptions, KillContainerOptions, LogOutput, LogsOptions, RemoveContainerOptions, StartContainerOptions, StopContainerOptions, WaitContainerOptions};
use bollard::Docker;
use bollard::errors::Error;
use bollard::errors::Error::DockerResponseServerError;
use bollard::image::{CreateImageOptions, ListImagesOptions};
use bollard::models::{ContainerCreateResponse, CreateImageInfo, HostConfig, NetworkCreateResponse, PortBinding};
use bollard::network::{CreateNetworkOptions, InspectNetworkOptions};
use futures::{FutureExt, pin_mut, select, Stream, stream, stream_select};
use futures::StreamExt;
use futures::TryStreamExt;

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

#[tokio::test(flavor = "multi_thread")]
async fn test_h2spec() {
    let skupper_router_image = &env::var("QDROUTERD_IMAGE").unwrap_or(String::from("quay.io/skupper/skupper-router:latest"));

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

    let container_nghttpd = create_and_start_container(
        &docker, NGHTTP2_IMAGE, "nghttpd",
        Config {
            host_config: Some(hostconfig.clone()),
            cmd: Some(vec!["nghttpd", "-a", "0.0.0.0", "--no-tls", "-d", "/tmp", "8888"]),
            ..Default::default()
        }).await;
    let logs_nghttpd = stream_container_logs(&docker, "nghttpd", &container_nghttpd);

    let inspection = docker.inspect_container(&*container_skrouterd.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let hostname = inspection.network_settings.unwrap().networks.unwrap().values().take(1).next().unwrap().ip_address.as_ref().unwrap().clone();

    let container_h2spec = create_and_start_container(
        &docker, H2SPEC_IMAGE, "h2spec",
        Config {
            host_config: Some(hostconfig.clone()),
            cmd: Some(vec!["-h", &hostname, "-p", "24162", "--verbose", "--insecure", "--timeout", "10"]),
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

    // run the router container until completion
    docker.kill_container(&*container_skrouterd.id, Some(KillContainerOptions { signal: "SIGTERM" })).await.unwrap();
    // stop the http2 server container right away
    docker.stop_container(&*container_nghttpd.id, None::<StopContainerOptions>).await.unwrap();
    while let Some(msg) = stream.next().await {
        println!("{}", msg);
    }

    let router_exit_code = get_container_exit_code(&docker, &container_skrouterd).await;
    println!("container skrouterd finished with exit code {:?}", router_exit_code);

    stdout().flush().expect("failed to flush stdout");
    assert_eq!(Some(0), h2spec_exit_code);
    assert_eq!(Some(0), router_exit_code);
}

async fn find_or_pull_image(docker: &&Docker, image: &str) -> String {
    if let Some(image_id) = docker_find_image(docker, image).await {
        return image_id;
    }
    let pull = docker_pull(&docker, image).await;
    return docker_find_image(docker, image).await.expect(&*format!("{:?}", pull));
}

async fn docker_find_image(docker: &&Docker, image: &str) -> Option<String> {
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
    let image_id = find_or_pull_image(&docker, image).await;
    // to be extra sure container does not already exist
    delete_container(&docker, name).await;

    let container: ContainerCreateResponse = docker.create_container(
        Some(CreateContainerOptions { name }),
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
    // keep in mind that containers that don't log anything will eventually (60+ sec) timeout on `RequestTimeoutError`
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

async fn get_container_exit_code(docker: &Docker, container: &ContainerCreateResponse) -> Option<i64> {
    let final_inspect = docker.inspect_container(&*container.id, Some(InspectContainerOptions { size: false })).await.unwrap();
    let exit_code = final_inspect.state.unwrap().exit_code;
    exit_code
}
