use anyhow::{anyhow, Result};
use containerd_client::services::v1::snapshots::snapshots_client::SnapshotsClient;
use containerd_client::services::v1::snapshots::RemoveSnapshotRequest;
use futures::stream::TryStreamExt as _;
use nix::libc::{kill, SIGKILL};
use nix::unistd::getpid;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::fd::AsRawFd;
use std::time::Duration;
use tokio::fs::File;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{event, Level};
use uuid::Uuid;

// Maybe use OCI instead of containerd's specific API?
use containerd_client::services::v1::{
    containers_client::ContainersClient, tasks_client::TasksClient, CreateTaskRequest,
    DeleteContainerRequest, DeleteTaskRequest, StartRequest,
};
use containerd_client::tonic::Request;
use containerd_client::with_namespace;

use bismuth_common::{ContainerState, FunctionDefinition, InvokeMode};

use crate::consts::*;

pub struct ContainerRoot {
    pub directory: std::path::PathBuf,
}

impl ContainerRoot {
    pub fn cleanup(&mut self) -> Result<()> {
        nix::mount::umount(&self.directory)?;
        std::fs::remove_dir(&self.directory)?;
        Ok(())
    }
}

impl Drop for ContainerRoot {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

impl ContainerRoot {
    pub async fn new(definition: &FunctionDefinition, container_id: Uuid) -> Result<Self> {
        let path = std::path::PathBuf::from(ROOTFS_BASE_PATH).join(container_id.to_string());
        tokio::fs::create_dir(&path).await?;
        event!(Level::TRACE, "Created rootfs at {:?}", path);

        // TODO: also change this to use the (way too many) native RPCs
        // https://github.com/containerd/containerd/blob/643fa70a7d7716e1e8138a3f2b2ce0532886676c/cmd/ctr/commands/images/mount.go
        tokio::process::Command::new("ctr")
            .args(&[
                "-n",
                BISMUTH_CONTAINERD_NAMESPACE,
                "images",
                "mount",
                "--rw",
                &definition.image,
                &path.to_str().unwrap(),
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .status()
            .await?
            .code()
            .and_then(|c| if c == 0 { Some(()) } else { None })
            .ok_or(anyhow!("abnormal exit"))?;

        let c = Self { directory: path };

        let etc = c.directory.join("etc");
        let _ = tokio::fs::create_dir(&etc).await;
        tokio::fs::write(etc.join("hosts"), format!("127.0.0.1\tlocalhost\n")).await?;
        tokio::fs::write(etc.join("hostname"), container_id.to_string() + "\n").await?;
        tokio::fs::write(etc.join("resolv.conf"), "nameserver 8.8.8.8\n").await?;

        if let Some(repo) = &definition.repo {
            let repo = repo.clone();
            let dst = c.directory.join("repo");
            event!(Level::TRACE, "Cloning repo {:?} into {:?}", repo.0, dst);

            tokio::task::spawn_blocking(move || {
                let (url, branch) = repo;
                let mut fetch_opts = git2::FetchOptions::new();

                if url.password().is_some() {
                    fetch_opts.custom_headers(&[&format!(
                        "Authorization: Basic {}",
                        base64::encode(&format!(
                            "{}:{}",
                            url.username(),
                            url.password().unwrap_or("")
                        ))
                    )]);
                }

                let mut builder = git2::build::RepoBuilder::new();
                builder.branch(&branch);
                builder.fetch_options(fetch_opts);
                builder.clone(&url.to_string(), &dst)
            })
            .await??;

            tokio::process::Command::new("chown")
                .args(&["-R", "101000:101000", &c.directory.to_str().unwrap()])
                .status()
                .await?
                .code()
                .and_then(|c| if c == 0 { Some(()) } else { None })
                .ok_or(anyhow!("abnormal exit"))?;
        }

        Ok(c)
    }

    pub fn path(&self) -> &std::path::Path {
        self.directory.as_path()
    }
}

pub struct ContainerNodeSetupData {
    /// The rootfs of the container. Automatically destroyed when the container is dropped.
    pub rootfs: ContainerRoot,

    /// Auth token shared between svcprovider and the container.
    /// Each svcprovider instance should only be network accessible by the container it's for,
    /// but might as well add a second layer of security.
    pub auth_token: String,
}

pub struct ContainerNodeRuntimeData {
    /// The container's init PID.
    pub pid: u32,

    /// The container's 10.x.x.x IP. Technically redundant since it's based on the PID, but convenient.
    pub ip: Ipv4Addr,

    /// The host-side veth device's IP.
    pub host_ip: Ipv4Addr,

    /// Handle to the container's network namespace.
    pub netns: File,

    /// Handle to the service proxy process for this container.
    pub svcprovider: tokio::process::Child,

    /// The stdout log file.
    pub stdout: std::path::PathBuf,

    /// The stderr log file.
    pub stderr: std::path::PathBuf,

    /// The run state of the container.
    pub state: ContainerState,
}

pub struct ContainerNodeData {
    pub setup: ContainerNodeSetupData,

    pub runtime: Option<ContainerNodeRuntimeData>,
}

pub struct Container {
    /// UUID of the function this container is running.
    pub function_id: Uuid,

    /// UUID of the container instance, as used by the frontend.
    pub id: Uuid,

    /// The ID used by containerd.
    pub containerd_id: String,

    pub definition: FunctionDefinition,

    pub node_data: ContainerNodeData,

    pub containerd: containerd_client::tonic::transport::Channel,
}

impl Drop for Container {
    fn drop(&mut self) {
        // TODO: stop the container
    }
}

#[derive(Debug, Clone)]
pub struct SvcProviderOptions {
    pub path: std::path::PathBuf,
    pub args: Vec<String>,
}

pub async fn run_in_netns(netns: &File, cmd: &[&str]) -> Result<tokio::process::Child> {
    Ok(tokio::process::Command::new("nsenter")
        .arg(format!("--net=/proc/{}/fd/{}", getpid(), netns.as_raw_fd()))
        .args(cmd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?)
}

impl Container {
    /// Starts the given container running.
    pub async fn start(&mut self, svcprovider_opts: &SvcProviderOptions) -> Result<()> {
        let mut task_client = TasksClient::new(self.containerd.clone());
        let log_dir = std::path::Path::new(LOGS_BASE_PATH).join(&self.id.to_string());
        tokio::fs::create_dir_all(&log_dir).await?;
        let stdout = log_dir.join("stdout");
        tokio::fs::File::create(&stdout).await?;
        let stderr = log_dir.join("stderr");
        tokio::fs::File::create(&stderr).await?;

        let req = CreateTaskRequest {
            container_id: self.containerd_id.clone(),
            stdin: "/dev/null".to_string(),
            stdout: stdout.to_string_lossy().to_string(),
            stderr: stderr.to_string_lossy().to_string(),
            ..Default::default()
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);

        task_client.create(req).await?;
        event!(Level::TRACE, container_id = %self.containerd_id, "Created task");

        let req = StartRequest {
            container_id: self.containerd_id.clone(),
            ..Default::default()
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);

        let resp = task_client.start(req).await?;
        let pid = resp.into_inner().pid;
        event!(Level::TRACE, container_id = %self.containerd_id, pid = %pid, "Started task");

        let netns = File::open(format!("/proc/{}/ns/net", pid)).await?;

        let (connection, handle, _) = rtnetlink::new_connection()?;
        let _netlink_socket = tokio::spawn(connection);

        let host_link_name = format!("veth-{}", pid);
        let container_link_name = format!("ceth-{}", pid);

        // Create veth: ip link add veth-pid type veth peer name ceth-pid
        handle
            .link()
            .add()
            .veth(host_link_name.clone(), container_link_name.clone())
            .execute()
            .await?;
        // Get a handle to the links for later use
        let host_link = handle
            .link()
            .get()
            .match_name(host_link_name.clone())
            .execute()
            .try_next()
            .await?
            .ok_or(anyhow!("No such link (host)?"))?;
        let container_link = handle
            .link()
            .get()
            .match_name(container_link_name.clone())
            .execute()
            .try_next()
            .await?
            .ok_or(anyhow!("No such link (container)?"))?;
        // Move ceth into the container: ip link set ceth-pid netns pid
        handle
            .link()
            .set(container_link.header.index)
            .setns_by_fd(netns.as_raw_fd())
            .execute()
            .await?;
        // Set group on host-side veth (for iptables purposes): ip link set veth-pid group 100
        // Doesn't appear to be doable through the rtnetlink crate, so just use the command.
        tokio::process::Command::new("ip")
            .args(&[
                "link",
                "set",
                "dev",
                &host_link_name,
                "group",
                &BISMUTH_NET_LINK_GROUP.to_string(),
            ])
            .status()
            .await?
            .code()
            .and_then(|c| if c == 0 { Some(()) } else { None })
            .ok_or(anyhow!("abnormal exit"))?;
        // Throttle the link: tc qdisc add dev veth-pid root tbf rate 5mbit burst 32kbit latency 100ms
        tokio::process::Command::new("tc")
            .args(&[
                "qdisc",
                "add",
                "dev",
                &host_link_name,
                "root",
                "tbf",
                "rate",
                "5mbit",
                "burst",
                "32kbit",
                "latency",
                "100ms",
            ])
            .status()
            .await?
            .code()
            .and_then(|c| if c == 0 { Some(()) } else { None })
            .ok_or(anyhow!("abnormal exit"))?;
        // Set IPs on both ends of the veth: ip addr add ...
        // Each container is allocated a /30 in 10/8 based on its pid.
        // Host-side IP is .1, container-side is .2
        let host_ip = Ipv4Addr::from(10 << 24 | (pid * 4 + 1));
        let container_ip = Ipv4Addr::from(10 << 24 | (pid * 4 + 2));

        // ip address add 10.p.i.d+1/30 dev veth-pid
        handle
            .address()
            .add(host_link.header.index, host_ip.into(), 30)
            .execute()
            .await?;

        // ip address add 10.p.i.d+2/30 dev ceth-pid
        run_in_netns(
            &netns,
            &[
                "ip",
                "address",
                "add",
                &format!("{}/30", container_ip),
                "dev",
                &container_link_name,
            ],
        )
        .await?
        .wait()
        .await?
        .code()
        .and_then(|c| if c == 0 { Some(()) } else { None })
        .ok_or(anyhow!("abnormal exit"))?;

        // Bring up the container link: ip link set ceth-pid up
        run_in_netns(&netns, &["ip", "link", "set", &container_link_name, "up"])
            .await?
            .wait()
            .await?
            .code()
            .and_then(|c| if c == 0 { Some(()) } else { None })
            .ok_or(anyhow!("abnormal exit"))?;
        // Add the default route: ip route add default via 10.p.i.d-1 dev ceth-pid
        run_in_netns(
            &netns,
            &[
                "ip",
                "route",
                "add",
                "default",
                "via",
                &host_ip.to_string(),
                "dev",
                &container_link_name,
            ],
        )
        .await?
        .wait()
        .await?
        .code()
        .and_then(|c| if c == 0 { Some(()) } else { None })
        .ok_or(anyhow!("abnormal exit"))?;
        // And configure iptables to make svcproxy access easier
        // (DNAT 169.254.169.254 to the host-side IP)
        run_in_netns(
            &netns,
            &[
                "iptables",
                "-t",
                "nat",
                "-A",
                "OUTPUT",
                "-d",
                "169.254.169.254",
                "-j",
                "DNAT",
                "--to",
                &host_ip.to_string(),
            ],
        )
        .await?
        .wait()
        .await?
        .code()
        .and_then(|c| if c == 0 { Some(()) } else { None })
        .ok_or(anyhow!("abnormal exit"))?;

        // Finally bring up the host side link: ip link set veth-pid up
        handle
            .link()
            .set(host_link.header.index)
            .up()
            .execute()
            .await?;

        event!(Level::TRACE, container_id = %self.containerd_id, pid, host_ip = %host_ip, container_ip = %container_ip, "Network setup done");

        let mut svcprovider_args: Vec<String> = vec![
            "--bind".to_string(),
            host_ip.to_string(),
            "--function".to_string(),
            self.function_id.to_string(),
            "--auth-token".to_string(),
            self.node_data.setup.auth_token.to_string(),
        ];
        svcprovider_args.extend_from_slice(&svcprovider_opts.args);

        let svcprovider = tokio::process::Command::new(&svcprovider_opts.path)
            .args(&svcprovider_args)
            //.uid(1337)  // TODO: permission denied
            //.gid(1337)
            .stdin(std::process::Stdio::null())
            // TODO(ghost): redirect to internal logging
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()?;

        event!(Level::TRACE, container_id = %self.containerd_id, pid = %svcprovider.id().unwrap(), "Started svcprovider");

        self.node_data.runtime = Some(ContainerNodeRuntimeData {
            pid,
            ip: container_ip,
            host_ip,
            netns,
            svcprovider,
            stdout,
            stderr,
            state: ContainerState::Starting,
        });

        Ok(())
    }

    pub async fn terminate(&mut self) -> Result<()> {
        self.node_data
            .runtime
            .as_mut()
            .unwrap()
            .svcprovider
            .kill()
            .await?;

        let mut task_client = TasksClient::new(self.containerd.clone());

        unsafe {
            kill(self.node_data.runtime.as_ref().unwrap().pid as _, SIGKILL);
        }
        sleep(Duration::from_secs(1)).await;

        let req = DeleteTaskRequest {
            container_id: self.containerd_id.to_string(),
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        task_client.delete(req).await?;
        event!(Level::TRACE, container_id = %self.containerd_id, "Deleted task");

        let mut container_client = ContainersClient::new(self.containerd.clone());

        let req = DeleteContainerRequest {
            id: self.containerd_id.to_string(),
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        container_client.delete(req).await?;
        event!(Level::TRACE, container_id = %self.containerd_id, "Deleted container");

        let mut snapshots_client = SnapshotsClient::new(self.containerd.clone());
        let req = RemoveSnapshotRequest {
            snapshotter: std::env::var("CONTAINERD_SNAPSHOTTER").unwrap_or("overlayfs".to_string()),
            key: format!("{}/{}", ROOTFS_BASE_PATH, self.id),
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        snapshots_client.remove(req).await?;
        event!(Level::TRACE, container_id = %self.containerd_id, "Deleted snapshot");

        Ok(())
    }

    pub async fn healthcheck(&self) -> Result<bool> {
        let Some(runtime) = &self.node_data.runtime else {
            return Err(anyhow!("Container not running"));
        };
        match self.definition.invoke_mode {
            InvokeMode::Executable { .. } => Ok(true),
            InvokeMode::Server(_, dport) => {
                Ok(TcpStream::connect(SocketAddrV4::new(runtime.ip, dport))
                    .await
                    .is_ok())
            }
        }
    }
}
