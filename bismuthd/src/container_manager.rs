use anyhow::{anyhow, Context, Result};
use containerd_client::services::v1::snapshots::snapshots_client::SnapshotsClient;
use containerd_client::services::v1::snapshots::{ListSnapshotsRequest, RemoveSnapshotRequest};
use futures::StreamExt;
use nix::libc::{kill, SIGKILL};
use oci_spec::runtime::{get_default_mounts, get_default_namespaces};
use prost_types::Any;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::sleep;
use tracing::{event, instrument, Instrument, Level};
use uuid::Uuid;

// Maybe use OCI instead of containerd's specific API?
use containerd_client::services::v1::{
    container::Runtime, containers_client::ContainersClient, images_client::ImagesClient,
    tasks_client::TasksClient, CreateContainerRequest, DeleteContainerRequest, DeleteTaskRequest,
    ListContainersRequest, ListImagesRequest, ListTasksRequest,
};
use containerd_client::tonic::Request;
use containerd_client::with_namespace;

use bismuth_common::{
    ContainerState, FunctionDefinition, GenericError, InvokeMode, UUID_PACKED_LEN,
};

use crate::consts::*;
use crate::container::*;

pub struct ContainerManager {
    /// The internal IP of this node that the service binds to, and that is used for frontend<->backend communication.
    pub this_node: Ipv4Addr,

    pub svcprovider_opts: SvcProviderOptions,

    /// List of images that have been pulled on this node.
    pub pulled_images: RwLock<HashSet<String>>,

    /// Map of container instance IDs to container IDs.
    pub instance_map: RwLock<HashMap<Uuid, Arc<RwLock<Container>>>>,

    /// Channel to the containerd daemon.
    pub containerd: containerd_client::tonic::transport::Channel,
}

impl ContainerManager {
    pub async fn new(
        this_node: Ipv4Addr,
        zk_cluster: &str,
        zk_env: &str,
        svcprovider_opts: SvcProviderOptions,
    ) -> Result<Arc<Self>> {
        let containerd = containerd_client::connect("/run/containerd/containerd.sock")
            .await
            .context("Connecting to containerd")?;

        // Tear down any existing containers.
        // This also has the nice side effect of cleaning up stale veth devices.
        let mut task_client = TasksClient::new(containerd.clone());
        let req = ListTasksRequest {
            ..Default::default()
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        let resp = task_client.list(req).await?;

        futures::future::join_all(resp.into_inner().tasks.iter().map(|task| async {
            let mut task_client = TasksClient::new(containerd.clone());

            if task.status == containerd_client::types::v1::Status::Running as i32 {
                event!(Level::DEBUG, container_id = %task.id, pid = %task.pid, "Killing existing task");
                // KillRequest seems to be a bit janky at times (i.e. not work...), so just kill the PID ourselves.
                unsafe {
                    kill(task.pid as _, SIGKILL);
                }
            }

            // Give containerd a sec to figure out things have died
            sleep(std::time::Duration::from_secs(1)).await;

            event!(Level::DEBUG, container_id = %task.id, "Deleting existing task");
            let req = DeleteTaskRequest {
                container_id: task.id.clone(),
            };
            let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
            // Seems like the container object can be deleted even if the task hasn't,
            // which then makes the task deletion fail. Ignore the error here so we can continue launching,
            // even though when the task is attempted to be created later, it will fail.
            let _ = task_client.delete(req).await;
        })).await;

        let mut container_client = ContainersClient::new(containerd.clone());
        let req = ListContainersRequest {
            ..Default::default()
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        let resp = container_client.list(req).await?;
        for container in resp.into_inner().containers {
            event!(Level::DEBUG, container_id = %container.id, "Deleting existing container");
            let req = DeleteContainerRequest {
                id: container.id.clone(),
            };
            let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
            container_client.delete(req).await?;
        }

        let mut images_client = ImagesClient::new(containerd.clone());
        let req = ListImagesRequest { filters: vec![] };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        let resp = images_client.list(req).await?;
        let pulled_images = resp
            .into_inner()
            .images
            .into_iter()
            .map(|i| i.name)
            .collect();

        // Remove old rootfs's
        let snapshotter =
            std::env::var("CONTAINERD_SNAPSHOTTER").unwrap_or("overlayfs".to_string());
        let mut snapshots_client = SnapshotsClient::new(containerd.clone());
        let req = ListSnapshotsRequest {
            snapshotter: snapshotter.clone(),
            filters: vec![],
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        let mut resp = snapshots_client.list(req).await?.into_inner();
        while let Some(resp) = resp.next().await {
            for snapshot in resp.unwrap().info {
                if snapshot.name.starts_with(ROOTFS_BASE_PATH) {
                    event!(Level::DEBUG, snapshot = %snapshot.name, "Deleting existing snapshot");
                    let req = RemoveSnapshotRequest {
                        snapshotter: snapshotter.clone(),
                        key: snapshot.name,
                    };
                    let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
                    snapshots_client.remove(req).await?;
                }
            }
        }

        if tokio::fs::metadata(ROOTFS_BASE_PATH).await.is_ok() {
            for entry in std::fs::read_dir(ROOTFS_BASE_PATH)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    let _ = nix::mount::umount(&entry.path());
                }
            }
            tokio::fs::remove_dir_all(ROOTFS_BASE_PATH).await?;
        }
        tokio::fs::create_dir_all(ROOTFS_BASE_PATH).await?;

        let _ = tokio::fs::remove_dir_all(LOGS_BASE_PATH).await;
        tokio::fs::create_dir_all(LOGS_BASE_PATH).await?;

        // Reset iptables
        let ipt = iptables::new(false)
            .map_err(|e| anyhow!(e.to_string()).context("Connecting to iptables"))?;
        // Delete the FORWARD rule that jumps to our chain if it exists
        let _ = ipt.delete(
            "filter",
            "FORWARD",
            &format!("-j {}", BISMUTH_IPTABLES_CHAIN),
        );

        // Clear out and delete the BISMUTH chain
        let _ = ipt.flush_chain("filter", BISMUTH_IPTABLES_CHAIN);
        let _ = ipt.delete_chain("filter", BISMUTH_IPTABLES_CHAIN);

        // And re-create it
        ipt.new_chain("filter", BISMUTH_IPTABLES_CHAIN)
            .map_err(|e| anyhow!(e.to_string()).context("Creating BISMUTH chain"))?;

        // Allow INPUT traffic from container to svcprovider in the same subnet
        let _ = ipt.append_unique(
            "filter",
            "INPUT",
            &format!(
                // Allow INPUT if the source IP is in the same /30 subnet as the dst IP
                // nfbpf_compile RAW 'ip[12:4] & 0xfffffffc = ip[16:4] & 0xfffffffc'
                // Surely there's a better way to ensure we only allow traffic from the same subnet...
                // TODO: this also doesn't assure the src ip isn't spoofed. ideally there'd be a iptables/nftables rule to say "only allow if the dst is on the same interface."
                "-m devgroup --src-group {BISMUTH_NET_LINK_GROUP} -m bpf --bytecode '13,48 0 0 0,84 0 0 240,21 0 9 64,32 0 0 12,84 0 0 4294967292,2 0 0 2,32 0 0 16,84 0 0 4294967292,7 0 0 5,96 0 0 2,29 0 1 0,6 0 0 65535,6 0 0 0' -j ACCEPT",
                BISMUTH_NET_LINK_GROUP = BISMUTH_NET_LINK_GROUP
            ),
        );

        // Block traffic between containers
        ipt.append("filter", BISMUTH_IPTABLES_CHAIN, &format!("-m devgroup --src-group {BISMUTH_NET_LINK_GROUP} --dst-group {BISMUTH_NET_LINK_GROUP} -j DROP", BISMUTH_NET_LINK_GROUP = BISMUTH_NET_LINK_GROUP)).map_err(|e| anyhow!(e.to_string()).context("Adding inter-container DROP rule"))?;

        // and traffic from containers to other RFC1918/5735 addresses...
        let block_subnets = vec![
            "10.0.0.0/8",
            "169.254.0.0/16",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "224.0.0.0/4",
            "240.0.0.0/4",
        ];
        // ... on the INPUT chain...
        // (blocking established/related traffic would break responses to requests from the host)
        let _ = ipt.append_unique(
            "filter",
            "INPUT",
            &format!("-d {} -m devgroup --src-group {BISMUTH_NET_LINK_GROUP} -m state --state NEW -j DROP", block_subnets.join(","), BISMUTH_NET_LINK_GROUP = BISMUTH_NET_LINK_GROUP),
        );

        // ... and on the BISMUTH (FORWARD) chain
        // TODO: I'm like 99% sure this is redundant with the INPUT rule above?
        ipt.append(
            "filter",
            BISMUTH_IPTABLES_CHAIN,
            &format!(
                "-d {} -m devgroup --src-group {BISMUTH_NET_LINK_GROUP} -j DROP",
                block_subnets.join(","),
                BISMUTH_NET_LINK_GROUP = BISMUTH_NET_LINK_GROUP
            ),
        )
        .map_err(|e| anyhow!(e.to_string()).context("Adding private address DROP rule"))?;

        // Otherwise accept traffic from containers
        ipt.append(
            "filter",
            BISMUTH_IPTABLES_CHAIN,
            &format!(
                "-m devgroup --src-group {BISMUTH_NET_LINK_GROUP} -j ACCEPT",
                BISMUTH_NET_LINK_GROUP = BISMUTH_NET_LINK_GROUP
            ),
        )
        .map_err(|e| anyhow!(e.to_string()).context("Adding container ACCEPT rule"))?;
        // as well as any established/related traffic the other way
        ipt.append(
            "filter",
            BISMUTH_IPTABLES_CHAIN,
            "-m state --state RELATED,ESTABLISHED -j ACCEPT",
        )
        .map_err(|e| anyhow!(e.to_string()).context("Adding ESTABLISHED/RELATED rule"))?;

        // Now that the chain is built, add the jump to it
        ipt.append(
            "filter",
            "FORWARD",
            &format!("-j {}", BISMUTH_IPTABLES_CHAIN),
        )
        .map_err(|e| anyhow!(e.to_string()).context("Adding BISMUTH FORWARD rule"))?;

        // And lastly, setup NAT for traffic from containers to the outside world
        const NAT_POSTROUTING_RULE: &str = "-s 10.0.0.0/8 -j MASQUERADE";
        let _ = ipt.delete("nat", "POSTROUTING", NAT_POSTROUTING_RULE);
        ipt.append("nat", "POSTROUTING", NAT_POSTROUTING_RULE)
            .map_err(|e| anyhow!(e.to_string()).context("Adding NAT rule"))?;

        let cm = Arc::new(ContainerManager {
            this_node,
            svcprovider_opts,
            pulled_images: RwLock::new(pulled_images),
            instance_map: RwLock::new(HashMap::new()),
            containerd,
        });

        let cm_ = cm.clone();
        let zk_cluster_ = zk_cluster.to_string();
        let zk_env_ = zk_env.to_string();

        tokio::spawn(async move {
            loop {
                match Self::watch_zk(cm_.clone(), &zk_cluster_, &zk_env_, this_node).await {
                    Ok(_) => continue, // unreachable
                    Err(e) => {
                        event!(Level::ERROR, error = %e, "Error in ZooKeeper watch loop");
                    }
                }
                sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        let cm_ = cm.clone();
        let zk_cluster_ = zk_cluster.to_string();
        let zk_env_ = zk_env.to_string();

        tokio::spawn(async move {
            loop {
                match Self::watch_containerd(cm_.clone(), &zk_cluster_, &zk_env_, this_node).await {
                    Ok(_) => continue, // unreachable
                    Err(e) => {
                        event!(Level::ERROR, error = %e, "Error in containerd watch loop");
                    }
                }
                sleep(std::time::Duration::from_secs(1)).await;
            }
        });

        let zk = zookeeper_client::Client::connect(zk_cluster)
            .await
            .context("Connecting to ZooKeeper")?;
        let zk = zk
            .chroot(format!("/{}", zk_env))
            .map_err(|_| anyhow!("Failed to chroot to env {}", zk_env))?;
        event!(Level::TRACE, "Connected to ZooKeeper");

        cm.create_all(zk.clone()).await?;
        for container_id in cm.instance_map.read().await.keys() {
            let Ok(container) = cm.get_container(*container_id).await else {
                event!(Level::ERROR, container_id = %container_id, "Failed to get container");
                continue;
            };
            tokio::task::spawn(Self::watch_startup(cm.clone(), container, zk.clone()));
        }

        Ok(cm)
    }

    async fn watch_zk(
        cm: Arc<Self>,
        zk_cluster: &str,
        zk_env: &str,
        this_node: Ipv4Addr,
    ) -> Result<()> {
        let zk = zookeeper_client::Client::connect(&zk_cluster)
            .await
            .context("Error connecting to ZooKeeper")?;
        let zk = zk
            .chroot(format!("/{}", zk_env))
            .map_err(|_| anyhow!("Failed to chroot to env {}", zk_env))?;
        event!(Level::TRACE, "Connected to ZooKeeper");

        let mut watcher = zk
            .watch(
                &format!("/node/{}", this_node),
                zookeeper_client::AddWatchMode::PersistentRecursive,
            )
            .await?;

        loop {
            let event = watcher.changed().await;
            event!(Level::TRACE, "ZooKeeper event: {:?}", event);

            if event.event_type == zookeeper_client::EventType::Session
                && (event.session_state == zookeeper_client::SessionState::Disconnected
                    || event.session_state == zookeeper_client::SessionState::Expired
                    || event.session_state == zookeeper_client::SessionState::Closed)
            {
                event!(Level::ERROR, "ZooKeeper session disconnected or terminal");
                return Err(anyhow!("ZooKeeper session disconnected or terminal"));
            }

            // /node/{ip}/container/{container_id}
            let components = event.path.split('/').collect::<Vec<_>>();
            if components.len() != 5 {
                // We receive /{container_id}/status nodes events as well, so this is not unexpected.
                continue;
            }
            let container_id = Uuid::parse_str(components[4])?;

            match event.event_type {
                zookeeper_client::EventType::NodeCreated => {
                    let (container_data, _) = zk
                        .get_data(&format!("/node/{}/container/{}", this_node, container_id))
                        .await
                        .context("Error getting container data")?;

                    let function_id = Uuid::from_slice(&container_data[0..UUID_PACKED_LEN])?;
                    let (function_data, _) = zk
                        .get_data(&format!("/function/{}", function_id))
                        .await
                        .context("Error getting function data")?;

                    let definition: FunctionDefinition = serde_json::from_slice(&function_data)?;

                    event!(Level::DEBUG, function_id = %function_id, container_id = %container_id, "Creating new container");
                    let container = cm
                        .create_container(function_id, container_id, definition)
                        .await?;

                    tokio::task::spawn(Self::watch_startup(cm.clone(), container, zk.clone()));
                }
                zookeeper_client::EventType::NodeDeleted => {
                    event!(Level::DEBUG, container_id = %container_id, "Deleting container");
                    cm.delete_container(container_id).await?;
                }
                zookeeper_client::EventType::NodeDataChanged => {
                    event!(Level::DEBUG, container_id = %container_id, "Deleting container for update");
                    cm.delete_container(container_id).await?;
                }
                _ => {}
            }
        }
    }

    async fn watch_containerd(
        cm: Arc<Self>,
        zk_cluster: &str,
        zk_env: &str,
        this_node: Ipv4Addr,
    ) -> Result<()> {
        let zk = zookeeper_client::Client::connect(&zk_cluster)
            .await
            .context("Error connecting to ZooKeeper")?;
        let zk = zk
            .chroot(format!("/{}", zk_env))
            .map_err(|_| anyhow!("Failed to chroot to env {}", zk_env))?;
        event!(Level::TRACE, "Connected to ZooKeeper");

        let mut task_client = TasksClient::new(cm.containerd.clone());

        loop {
            let req = ListTasksRequest {
                ..Default::default()
            };
            let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);

            let resp = task_client.list(req).await?;
            for task in resp.into_inner().tasks {
                // Our status model (starting, running, paused, failed) distinguishes between
                // startup and steady-state, so we can't determine starting->running here.
                // We also know when a container is paused (since only we can pause it),
                // so we don't need to check for that either.
                if task.status != containerd_client::types::v1::Status::Stopped as i32 {
                    continue;
                }

                let Ok(container_id) = Uuid::parse_str(&task.id) else {
                    event!(Level::ERROR, container_id = %task.id, "Invalid container ID");
                    continue;
                };

                let Ok(container) = cm.get_container(container_id).await else {
                    event!(Level::ERROR, container_id = %container_id, "Found unknown container");
                    continue;
                };
                if container
                    .read()
                    .await
                    .node_data
                    .runtime
                    .as_ref()
                    .map(|r| r.state == ContainerState::Failed)
                    .unwrap_or_default()
                {
                    continue;
                };

                event!(Level::DEBUG, container_id = %container_id, "Container stopped");
                container.write().await.node_data.runtime.as_mut().map(|r| {
                    r.state = ContainerState::Failed;
                });
                zk.set_data(
                    &format!("/node/{}/container/{}/status", this_node, container_id),
                    &[ContainerState::Failed as u8],
                    None,
                )
                .await?;
            }

            sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// Create and start the container specified by the function id,
    /// the container id, and the function definition.
    #[instrument(skip(self, definition))]
    pub async fn create_container(
        &self,
        function_id: Uuid,
        container_id: Uuid,
        definition: FunctionDefinition,
    ) -> Result<Arc<RwLock<Container>>> {
        if !self.pulled_images.read().await.contains(&definition.image) {
            // TODO: RPCs - Transfer(OCIRegistry, ImageStore)
            tokio::process::Command::new("ctr")
                .args(&[
                    "-n",
                    BISMUTH_CONTAINERD_NAMESPACE,
                    "images",
                    "pull",
                    &definition.image,
                ])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .status()
                .instrument(tracing::info_span!("Pulling image"))
                .await
                .context("Pulling image")?
                .code()
                .and_then(|c| if c == 0 { Some(()) } else { None })
                .ok_or(anyhow!("abnormal exit"))?;
            self.pulled_images
                .write()
                .await
                .insert(definition.image.clone());
        }

        let rootfs = ContainerRoot::new(&definition, container_id).await?;
        event!(Level::TRACE, rootfs = %rootfs.path().display(), "Mounted container rootfs");

        let auth_token = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();

        let init_args = match &definition.invoke_mode {
            InvokeMode::Server(args, _) => args.clone(),
            InvokeMode::Executable(_) => vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "sleep infinity".to_string(),
            ],
        };

        let mut mounts = get_default_mounts();
        mounts.push(
            oci_spec::runtime::MountBuilder::default()
                .destination("/tmp")
                .typ("tmpfs")
                .options(vec![
                    "nosuid".to_string(),
                    "strictatime".to_string(),
                    "mode=1777".to_string(),
                    "size=65536k".to_string(),
                ])
                .build()?,
        );

        // New namespaces of every time
        let namespaces = vec![
            oci_spec::runtime::LinuxNamespaceBuilder::default()
                .typ(oci_spec::runtime::LinuxNamespaceType::Cgroup)
                .build()?,
            oci_spec::runtime::LinuxNamespaceBuilder::default()
                .typ(oci_spec::runtime::LinuxNamespaceType::Ipc)
                .build()?,
            oci_spec::runtime::LinuxNamespaceBuilder::default()
                .typ(oci_spec::runtime::LinuxNamespaceType::Network)
                .build()?,
            oci_spec::runtime::LinuxNamespaceBuilder::default()
                .typ(oci_spec::runtime::LinuxNamespaceType::Mount)
                .build()?,
            oci_spec::runtime::LinuxNamespaceBuilder::default()
                .typ(oci_spec::runtime::LinuxNamespaceType::Pid)
                .build()?,
            oci_spec::runtime::LinuxNamespaceBuilder::default()
                .typ(oci_spec::runtime::LinuxNamespaceType::User)
                .build()?,
            oci_spec::runtime::LinuxNamespaceBuilder::default()
                .typ(oci_spec::runtime::LinuxNamespaceType::Uts)
                .build()?,
        ];

        // Remap all container (U|G)IDs that might map to something in the hosts' used 64k id space.
        let id_mapping = vec![oci_spec::runtime::LinuxIdMappingBuilder::default()
            .container_id(0u32)
            .host_id(100_000u32)
            .size(65536u32)
            .build()?];

        let spec = Any {
            type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
            value: serde_json::to_string(
                &oci_spec::runtime::SpecBuilder::default()
                    .hostname(container_id.to_string())
                    .root(
                        oci_spec::runtime::RootBuilder::default()
                            .path(rootfs.path().to_str().unwrap().to_string())
                            .build()?,
                    )
                    .mounts(mounts)
                    .linux(
                        oci_spec::runtime::LinuxBuilder::default()
                            .namespaces(namespaces)
                            .uid_mappings(id_mapping.clone())
                            .gid_mappings(id_mapping.clone())
                            // capability defaults are fine (AUDIT_WRITE, KILL, NET_BIND_SERVICE)
                            .resources(
                                oci_spec::runtime::LinuxResourcesBuilder::default()
                                    .cpu(
                                        oci_spec::runtime::LinuxCpuBuilder::default()
                                            .quota((definition.cpu * 1_000_000f32) as i64) // usec/sec
                                            .period(1_000_000u64)
                                            .build()?,
                                    )
                                    .memory(
                                        oci_spec::runtime::LinuxMemoryBuilder::default()
                                            .limit(definition.memory as i64)
                                            .build()?,
                                    )
                                    .build()?,
                            )
                            .build()?,
                    )
                    .process(
                        oci_spec::runtime::ProcessBuilder::default()
                            .user(
                                oci_spec::runtime::UserBuilder::default()
                                    .uid(1000u32)
                                    .gid(1000u32)
                                    .build()?,
                            )
                            .args(init_args)
                            .env(vec![format!("BISMUTH_AUTH={}", auth_token)])
                            .build()?,
                    )
                    .build()?,
            )?
            .into(),
        };

        let container = containerd_client::services::v1::Container {
            id: container_id.to_string(),
            image: definition.image.clone(),
            runtime: Some(Runtime {
                name: "io.containerd.runc.v2".to_string(),
                options: None,
            }),
            spec: Some(spec),
            ..Default::default()
        };

        let mut container_client = ContainersClient::new(self.containerd.clone());

        let req = CreateContainerRequest {
            container: Some(container),
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);

        container_client.create(req).await?;
        event!(Level::TRACE, container_id = %container_id, "Created container");

        let container = Arc::new(RwLock::new(Container {
            function_id,
            id: container_id,
            containerd_id: container_id.to_string(),
            definition,
            node_data: ContainerNodeData {
                setup: ContainerNodeSetupData { rootfs, auth_token },
                runtime: None,
            },
            containerd: self.containerd.clone(),
        }));
        self.instance_map
            .write()
            .await
            .insert(container_id, container.clone());

        container
            .write()
            .await
            .start(&self.svcprovider_opts)
            .await?;

        Ok(container)
    }

    async fn create_all(&self, zk: zookeeper_client::Client) -> Result<()> {
        let containers = zk
            .get_children(&format!("/node/{}/container", &self.this_node))
            .await
            .context("Failed to read node container data")?
            .0
            .iter()
            .map(|i| Uuid::parse_str(i).unwrap())
            .collect::<Vec<_>>();

        for container_id in &containers {
            let status_path = format!(
                "/node/{}/container/{}/status",
                &self.this_node, &container_id
            );
            // Create if it doesn't exist, just for upgrade path.
            let _ = zk
                .create(
                    &status_path,
                    &[ContainerState::Starting as u8],
                    &zookeeper_client::CreateMode::Persistent
                        .with_acls(zookeeper_client::Acls::anyone_all()),
                )
                .await;
            zk.set_data(&status_path, &[ContainerState::Starting as u8], None)
                .await?;
        }

        let mut futures = vec![];
        for container_id in &containers {
            let (container_data, _) = zk
                .get_data(&format!(
                    "/node/{}/container/{}",
                    &self.this_node, &container_id
                ))
                .await
                .context("Failed to read container data")?;
            let function_id = Uuid::from_slice(&container_data[0..UUID_PACKED_LEN])?;
            let (function_data, _) = zk
                .get_data(&format!("/function/{}", &function_id))
                .await
                .context("Failed to read function data")?;
            let definition: FunctionDefinition = serde_json::from_slice(&function_data)?;

            futures.push(self.create_container(function_id, *container_id, definition));
        }

        for (container_id, result) in containers
            .iter()
            .zip(futures::future::join_all(futures).await)
        {
            if let Err(e) = result {
                event!(Level::ERROR, container_id = %container_id, error = %e, "Failed to create container");
            }
        }

        Ok(())
    }

    async fn watch_startup(
        cm: Arc<Self>,
        container: Arc<RwLock<Container>>,
        zk: zookeeper_client::Client,
    ) -> Result<()> {
        let container_id = container.read().await.id;

        for _ in 0..100 {
            sleep(std::time::Duration::from_millis(100)).await;
            if container
                .read()
                .await
                .node_data
                .runtime
                .as_ref()
                .map(|r| r.state != ContainerState::Starting)
                .unwrap_or_default()
            {
                break;
            }

            let healthcheck = container.read().await.healthcheck().await;
            if healthcheck.unwrap_or_default() {
                event!(Level::DEBUG, container_id = %container_id, "Container is healthy");
                container
                    .write()
                    .await
                    .node_data
                    .runtime
                    .as_mut()
                    .unwrap()
                    .state = ContainerState::Running;
                zk.set_data(
                    &format!("/node/{}/container/{}/status", cm.this_node, container_id,),
                    &[ContainerState::Running as u8],
                    None,
                )
                .await?;
                break;
            }
        }
        Ok(())
    }

    pub async fn get_container(&self, container_id: Uuid) -> Result<Arc<RwLock<Container>>> {
        // TODO: (re)set timer to suspend/remove container if not used for a while
        Ok(self
            .instance_map
            .read()
            .await
            .get(&container_id)
            .ok_or(GenericError::NotFound)?
            .clone())
    }

    #[instrument(skip(self))]
    pub async fn delete_container(&self, container_id: Uuid) -> Result<()> {
        let container = self.instance_map.write().await.remove(&container_id);
        if let Some(container) = container {
            let mut container = container.write().await;
            container.terminate().await?;
            // TODO: container isn't getting Drop'd properly, so we have to do this manually.
            container.node_data.setup.rootfs.cleanup();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bismuth_common::{pack_backends, Backend};
    use std::{
        fmt::Display,
        path::{Path, PathBuf},
        str::FromStr as _,
    };
    use tokio::fs::File;

    use super::*;
    // bismuthd relies on being a singleton process (tries to cleanup on startup), so have to run tests serially
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_container_root() {
        let container_id = Uuid::from_str("00000000-0000-4000-0000-000000000001").unwrap();
        let expected_path =
            std::path::PathBuf::from(ROOTFS_BASE_PATH).join(container_id.to_string());
        let _ = nix::mount::umount(&expected_path);
        let _ = tokio::fs::remove_dir(&expected_path).await;

        // Ensuring image is pulled is normally done by ContainerManager
        tokio::process::Command::new("ctr")
            .args(&[
                "-n",
                BISMUTH_CONTAINERD_NAMESPACE,
                "images",
                "pull",
                "docker.io/library/alpine:latest",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .unwrap();

        // TODO: we shouldn't need to do this when ContainerRoot::drop() properly does it
        let containerd = containerd_client::connect("/run/containerd/containerd.sock")
            .await
            .unwrap();
        let mut snapshots_client = SnapshotsClient::new(containerd);
        let req = RemoveSnapshotRequest {
            snapshotter: std::env::var("CONTAINERD_SNAPSHOTTER").unwrap_or("overlayfs".to_string()),
            key: format!("{}/{}", ROOTFS_BASE_PATH, container_id),
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        let _ = snapshots_client.remove(req).await;

        tokio::fs::create_dir_all(ROOTFS_BASE_PATH).await.unwrap();

        // ***
        // Test really begins here
        // ***
        let rootfs = ContainerRoot::new(
            &FunctionDefinition {
                image: "docker.io/library/alpine:latest".to_string(),
                invoke_mode: InvokeMode::Executable(vec!["/bin/ls".to_string()]),
                cpu: 1.0,
                memory: 128 * 1024 * 1024,
                repo: Some((
                    url::Url::parse("https://github.com/octocat/Hello-World").unwrap(),
                    "master".to_string(),
                )),
                max_instances: 1,
            },
            container_id,
        )
        .await
        .unwrap();

        let path = rootfs.path().to_path_buf();
        assert_eq!(path, expected_path);

        assert!(path.exists(), "Rootfs path does not exist");
        assert!(
            path.join("bin").exists(),
            "Image doesn't appear to be mounted"
        );
        assert!(path.join("repo/README").exists(), "Repo not cloned");

        // Test mount writability
        assert!(
            File::create(path.join("tmp/foo")).await.is_ok(),
            "Mount is not writable"
        );

        drop(rootfs);
        assert!(!path.exists(), "Rootfs not cleaned up");
    }

    #[tokio::test]
    #[serial]
    async fn test_container_create() {
        let zookeeper_cluster =
            std::env::var("ZOOKEEPER_CLUSTER").unwrap_or("zookeeper1:2181".to_string());
        let function_id = Uuid::from_str("00000000-0000-4000-0000-000000000002").unwrap();
        let container_id = Uuid::from_str("00000000-0000-4000-0000-000000000003").unwrap();
        let function_definition = FunctionDefinition {
            image: "docker.io/library/python:3.11".to_string(),
            invoke_mode: InvokeMode::Server(
                vec![
                    "/usr/local/bin/python3".to_string(),
                    "-m".to_string(),
                    "http.server".to_string(),
                ],
                8000,
            ),
            cpu: 1.0,
            memory: 128 * 1024 * 1024,
            repo: Some((
                url::Url::parse("https://github.com/octocat/Hello-World").unwrap(),
                "master".to_string(),
            )),
            max_instances: 1,
        };

        // Bootstrap ZK
        let zk_env = "test_container_create";
        let zk = bismuth_common::test::zk_bootstrap(&zookeeper_cluster, zk_env).await;
        let node_ip = Ipv4Addr::new(127, 0, 0, 1);
        let node_key = format!("/node/{}", node_ip);
        zk.create(
            &node_key,
            &[1u8],
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();
        zk.create(
            &format!("{}/container", &node_key),
            &b""[..],
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();
        // Add function
        zk.create(
            &format!("/function/{}", function_id),
            &serde_json::to_vec(&function_definition).unwrap(),
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();
        zk.create(
            &format!("/function/{}/backends", function_id),
            &pack_backends(&[Backend {
                ip: node_ip,
                container_id,
            }]),
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();
        // Assign container to node
        zk.create(
            &format!("{}/container/{}", &node_key, container_id),
            function_id.as_bytes(),
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();
        zk.create(
            &format!("{}/container/{}/status", &node_key, container_id),
            &[ContainerState::Starting as u8],
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .unwrap();
        println!("ZK setup done");

        let svcprovider_opts = SvcProviderOptions {
            path: PathBuf::from("/bin/true"),
            args: vec![],
        };

        // Ok NOW we can actually test

        let manager = ContainerManager::new(
            node_ip,
            &zookeeper_cluster,
            zk_env,
            svcprovider_opts.clone(),
        )
        .await
        .unwrap();
        println!("Created manager");

        let container = manager.get_container(container_id).await.unwrap();
        println!("Got container");

        // In block so we drop the read before trying to exit below
        {
            let container = container.read().await;
            let runtime_data = container.node_data.runtime.as_ref().unwrap();

            fn get_namespaces<S: ToString + Display>(proc_pid: S) -> HashMap<String, String> {
                std::fs::read_dir(format!("/proc/{}/ns", proc_pid))
                    .unwrap()
                    .map(|e| {
                        let entry = e.unwrap();
                        (
                            entry.file_name().into_string().unwrap(),
                            std::fs::read_link(entry.path())
                                .unwrap()
                                .as_os_str()
                                .to_str()
                                .unwrap()
                                .to_owned(),
                        )
                    })
                    .collect::<HashMap<String, String>>()
            }

            let host_namespaces = get_namespaces("self");
            let container_namespaces = get_namespaces(runtime_data.pid);

            // Check that the container has its own namespaces
            for ns in vec![
                "cgroup",
                "ipc",
                "mnt",
                "net",
                "pid",
                "pid_for_children",
                //"time",
                "user",
                "uts",
            ] {
                assert_ne!(
                    host_namespaces.get(ns).unwrap(),
                    container_namespaces.get(ns).unwrap(),
                    "{} namespace is shared",
                    ns
                );
            }
        }
        'healthy: {
            for _ in 0..100 {
                sleep(std::time::Duration::from_millis(100)).await;

                let container = container.read().await;
                let runtime_data = container.node_data.runtime.as_ref().unwrap();
                if runtime_data.state != ContainerState::Running {
                    continue;
                } else {
                    assert_eq!(
                        container.healthcheck().await.unwrap_or_default(),
                        true,
                        "Healthcheck failed"
                    );
                    assert_eq!(
                        zk.get_data(&format!("{}/container/{}/status", &node_key, container_id))
                            .await
                            .unwrap()
                            .0,
                        &[ContainerState::Running as u8],
                        "ZK status not updated"
                    );
                    break 'healthy;
                }
            }
            assert!(false, "Container did not become healthy");
        }

        container.write().await.terminate().await.unwrap();

        // Ensure cleanup was done
        let container = container.read().await;
        let runtime_data = container.node_data.runtime.as_ref().unwrap();
        assert!(
            !Path::new(&format!("/proc/{}", runtime_data.pid)).exists(),
            "Container process still running"
        );
        let containerd = containerd_client::connect("/run/containerd/containerd.sock")
            .await
            .unwrap();
        let mut snapshots_client = SnapshotsClient::new(containerd);
        let req = ListSnapshotsRequest {
            snapshotter: std::env::var("CONTAINERD_SNAPSHOTTER").unwrap_or("overlayfs".to_string()),
            filters: vec![],
        };
        let req = with_namespace!(req, BISMUTH_CONTAINERD_NAMESPACE);
        let mut resp = snapshots_client.list(req).await.unwrap().into_inner();
        let mut found = false;
        while let Some(resp) = resp.next().await {
            for snapshot in resp.unwrap().info {
                if snapshot.name == format!("{}/{}", ROOTFS_BASE_PATH, container_id) {
                    found = true;
                    break;
                }
            }
        }
        assert!(!found, "Rootfs snapshot still exists");
    }

    #[tokio::test]
    #[serial]
    async fn test_container_manager_pause() {
        // TODO
    }
}
