use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use uuid::Uuid;

use bismuth_common::{pack_backends, unpack_backends, Backend, FunctionDefinition, InvokeMode};

/// bismuthctl
#[derive(Debug, Parser)]
#[clap(name = "bismuthctl", version)]
struct Cli {
    #[clap(flatten)]
    global_opts: GlobalOpts,

    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    // TODO: split these into subcommands of "cluster" and "node"
    /// Bootstrap a new cluster, creating basic ZooKeeper nodes
    Bootstrap {},
    /// Perform a consistency check on the cluster
    Consistency {},

    /// Provision a server into the cluster
    Provision {
        node_ip: Ipv4Addr,
    },
    /// Fully deprovision a server from the cluster
    Deprovision {
        node_ip: Ipv4Addr,
    },
    /// Disable a node from scheduling and drain existing functions
    Drain {
        node_ip: Ipv4Addr,
    },
    /// Re-enable a node for scheduling
    Undrain {
        node_ip: Ipv4Addr,
    },
    ListNodes {},
    ListFunctions {},
    AddBackend {
        function_id: Uuid,
        new_backend: Ipv4Addr,
    },
    RemoveBackend {
        function_id: Uuid,
        remove_backend: Ipv4Addr,
        container_id: Uuid,
    },
}

#[derive(Debug, Args)]
struct GlobalOpts {
    #[command(flatten)]
    verbose: clap_verbosity_flag::Verbosity,

    /// ZooKeeper IP:port
    #[clap(long, global = true, default_value = "127.0.0.1:2181")]
    zookeeper: String,

    /// ZooKeeper environment name (e.g. "dev", "test", "default")
    #[clap(long, global = true, default_value = "default")]
    zookeeper_env: String,
}

async fn drain(zk: &zookeeper_client::Client, node_ip: &Ipv4Addr) -> Result<()> {
    let node_key = format!("/node/{}", node_ip);
    let exists = zk
        .check_stat(&node_key)
        .await
        .context("Error checking node presence")?;
    if exists.is_none() {
        return Err(anyhow!("Node not in cluster"));
    }
    let (node_data, stat) = zk
        .get_data(&node_key)
        .await
        .context("Error getting node data")?;

    // TODO: ensure /draining exists or /function is empty
    // This may need to be wrapped in a multi
    if node_data == vec![0u8] {
        info!("Node already disabled");
        return Ok(());
    }

    zk.set_data(&node_key, &[0u8], Some(stat.version))
        .await
        .context("Error disabling node")?;

    zk.create(
        &format!("{}/draining", &node_key),
        &b""[..],
        &zookeeper_client::CreateMode::Ephemeral.with_acls(zookeeper_client::Acls::anyone_all()),
    )
    .await
    .context("Error creating draining marker")?;

    let node_functions = zk
        .get_children(&format!("{}/function", &node_key))
        .await
        .context("Error listing node functions")?
        .0;

    for function in &node_functions {
        // Remove this node from the function's backend list
        let function_backends_key = format!("/function/{}/backends", &function);
        let (function_backends_raw, stat) = zk
            .get_data(&function_backends_key)
            .await
            .context("Error getting function backends")?;
        let mut function_backends = unpack_backends(&function_backends_raw)?;
        function_backends.retain(|b| b.ip != *node_ip);
        zk.set_data(
            &function_backends_key,
            &pack_backends(&function_backends),
            Some(stat.version),
        )
        .await
        .context("Error updating function backends")?;

        // And now delete the function entry in the node
        zk.delete(&format!("{}/function/{}", &node_key, &function), None)
            .await
            .context("Error deleting node function")?;
    }

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Cli::parse();
    env_logger::Builder::new()
        .filter_level(args.global_opts.verbose.log_level_filter())
        .init();

    let zk = zookeeper_client::Client::connect(&args.global_opts.zookeeper)
        .await
        .context("Failed to connect to zookeeper")?;

    if zk
        .check_stat(&format!("/{}", &args.global_opts.zookeeper_env))
        .await?
        .is_none()
    {
        zk.create(
            &format!("/{}", &args.global_opts.zookeeper_env),
            &b""[..],
            &zookeeper_client::CreateMode::Persistent
                .with_acls(zookeeper_client::Acls::anyone_all()),
        )
        .await
        .context("Error creating env znode")?;
    }

    let zk = zk
        .chroot(format!("/{}", &args.global_opts.zookeeper_env))
        .map_err(|_| {
            anyhow!(
                "Failed to chroot to env {}",
                &args.global_opts.zookeeper_env
            )
        })?;

    match &args.command {
        Command::Bootstrap {} => {
            // /node/1.2.3.4 has one byte value 0 or 1 with enabled status
            // /node/1.2.3.4/function has children for each function that the host serves
            zk.create(
                "/node",
                &b""[..],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .context("Error creating /node")?;

            // /function/name has data with function runtime details
            // /function/name/backends has array of ipv4 with the backends of this function
            zk.create(
                "/function",
                &b""[..],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .context("Error creating /function")?;

            // /route/hostname has data with function id
            zk.create(
                "/route",
                &b""[..],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .context("Error creating /route")?;

            info!("Cluster successfully bootstrapped");
        }
        Command::Consistency {} => {
            let node_ips = zk
                .get_children("/node")
                .await
                .context("Error listing nodes - cluster not bootstrapped?")?
                .0;

            // Map of IP to whether the node is enabled and serving
            let mut node_state: HashMap<Ipv4Addr, bool> = HashMap::new();
            // Map of IP to the list of containers that host is currently serving
            let mut node_containers: HashMap<Ipv4Addr, Vec<Uuid>> = HashMap::new();
            for node_ip in &node_ips {
                let node_key = format!("/node/{}", &node_ip);
                let ip = node_ip
                    .parse::<Ipv4Addr>()
                    .context("Node with non-IP name?")?;
                let (node_data, _) = zk
                    .get_data(&node_key)
                    .await
                    .context("Failed to read node data")?;
                node_state.insert(ip, node_data != vec![0u8]);

                let containers = zk
                    .get_children(&format!("{}/container", &node_key))
                    .await
                    .context("Failed to read node container data")?
                    .0;
                for container in containers {
                    node_containers
                        .entry(ip)
                        .or_default()
                        .push(Uuid::parse_str(&container)?);
                }
            }
            println!(
                "{} nodes in cluster, {} enabled, {} drained",
                node_ips.len(),
                node_state.values().filter(|v| **v).count(),
                node_state.values().filter(|v| !**v).count()
            );
            println!("{} total containers", node_containers.len());

            let function_ids = zk
                .get_children("/function")
                .await
                .context("Failed listing functions - cluster not bootstrapped?")?
                .0;
            println!("{} functions in cluster", function_ids.len());

            for function_id in &function_ids {
                let (function_backends_raw, _) = zk
                    .get_data(&format!("/function/{}/backends", &function_id))
                    .await
                    .context("Failed to read function backend data")?;

                let backends = unpack_backends(&function_backends_raw)?;

                for backend in backends {
                    match node_state.get(&backend.ip) {
                        Some(node_enabled) => {
                            if !node_enabled {
                                return Err(anyhow!(
                                    "Function {} contains drained backend {}",
                                    &function_id,
                                    &backend.ip
                                ));
                            }
                        }
                        None => {
                            return Err(anyhow!(
                                "Function {} contains nonexistent backend {}",
                                &function_id,
                                &backend.ip
                            ));
                        }
                    }
                    match node_containers.get(&backend.ip) {
                        Some(containers) => {
                            if !containers.contains(&backend.container_id) {
                                return Err(anyhow!(
                                    "Function {} points to node {} which isn't serving it",
                                    &function_id,
                                    &backend.ip
                                ));
                            }
                        }
                        None => {
                            return Err(anyhow!(
                                "Function {} points to nonexistent node {}",
                                &function_id,
                                &backend.ip
                            ));
                        }
                    }
                }
            }
        }
        Command::Provision { node_ip } => {
            let node_key = format!("/node/{}", node_ip);
            let exists = zk
                .check_stat(&node_key)
                .await
                .context("Error checking node presence")?;
            if exists.is_some() {
                return Err(anyhow!("Node already joined to cluster"));
            }
            zk.create(
                &node_key,
                &[1u8],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .context("Error creating node znode")?;

            zk.create(
                &format!("{}/container", &node_key),
                &b""[..],
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .context("Error creating node container znode")?;
        }
        Command::Drain { node_ip } => {
            drain(&zk, node_ip).await?;
        }
        Command::Undrain { node_ip } => {
            let node_key = format!("/node/{}", node_ip);
            let exists = zk
                .check_stat(&node_key)
                .await
                .context("Error checking node presence")?;
            if exists.is_none() {
                return Err(anyhow!("Node not in cluster"));
            }
            let (node_data, stat) = zk
                .get_data(&node_key)
                .await
                .context("Failed to read node data")?;

            if node_data == vec![1u8] {
                return Err(anyhow!("Node already enabled"));
            }

            let draining = zk
                .check_stat(&format!("{}/draining", &node_key))
                .await
                .context("Failed to read node draining status")?;
            if draining.is_some() {
                return Err(anyhow!("Node currently draining"));
            }

            zk.set_data(&node_key, &[1u8], Some(stat.version))
                .await
                .context("Error enabling node")?;
        }
        Command::Deprovision { node_ip } => {
            drain(&zk, node_ip).await?;
            // At this point, node is disabled and all function child nodes should be gone
            let node_key = format!("/node/{}", node_ip);
            zk.delete(&format!("{}/container", &node_key), None)
                .await
                .context("Error deleting node container znode")?;
            zk.delete(&node_key, None)
                .await
                .context("Error deleting node znode")?;
        }
        Command::ListNodes {} => {
            let node_ips = zk
                .get_children("/node")
                .await
                .context("Error listing nodes - cluster not bootstrapped?")?
                .0;
            for node_ip in &node_ips {
                println!("{}", node_ip);
            }
        }
        Command::ListFunctions {} => {
            let function_ids = zk
                .get_children("/function")
                .await
                .context("Failed listing functions - cluster not bootstrapped?")?
                .0;
            for function_id in &function_ids {
                println!("{}", function_id);
            }
        }

        // JUST FOR DEV
        Command::AddBackend {
            function_id,
            new_backend,
        } => {
            let container_id = Uuid::new_v4();

            let node_key = format!("/node/{}", new_backend);
            zk.create(
                &format!("{}/container/{}", &node_key, &container_id),
                function_id.as_bytes(),
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .context("Error creating container znode")?;
            zk.create(
                &format!("{}/container/{}/status", &node_key, &container_id),
                function_id.as_bytes(),
                &zookeeper_client::CreateMode::Persistent
                    .with_acls(zookeeper_client::Acls::anyone_all()),
            )
            .await
            .context("Error creating container status znode")?;

            let backends_key = format!("/function/{}/backends", function_id);
            let (backends_raw, stat) = zk
                .get_data(&backends_key)
                .await
                .context("Error getting function backends")?;

            let mut backends = unpack_backends(&backends_raw)?;
            for backend in &backends {
                if backend.ip == *new_backend {
                    warn!(
                        "Function {} already contains backend IP {}",
                        function_id, new_backend
                    );
                }
            }
            backends.push(Backend {
                ip: *new_backend,
                container_id,
            });

            let backends_raw = pack_backends(&backends);
            zk.set_data(&backends_key, &backends_raw, Some(stat.version))
                .await
                .context("Error updating function backends")?;
            println!("{}", container_id);
        }
        Command::RemoveBackend {
            function_id,
            remove_backend,
            container_id,
        } => {
            let node_key = format!("/node/{}", remove_backend);
            zk.delete(
                &format!("{}/container/{}/status", &node_key, &container_id),
                None,
            )
            .await
            .context("Error deleting container status znode")?;
            zk.delete(&format!("{}/container/{}", &node_key, &container_id), None)
                .await
                .context("Error deleting container znode")?;

            let backends_key = format!("/function/{}/backends", function_id);
            let (backends_raw, stat) = zk
                .get_data(&backends_key)
                .await
                .context("Error getting function backends")?;
            let mut backends = unpack_backends(&backends_raw)?;
            backends.retain(|b| b.ip != *remove_backend && b.container_id != *container_id);
            zk.set_data(&backends_key, &pack_backends(&backends), Some(stat.version))
                .await
                .context("Error updating function backends")?;
        }
    }

    Ok(())
}
