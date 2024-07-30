use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;
use std::str::FromStr;
use url::Url;
use uuid::Uuid;

mod api_error;
pub use api_error::*;
mod metrics;
pub use metrics::*;
mod tracing;
pub use tracing::*;

pub mod test;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum InvokeMode {
    /// The container runs executables, one per request.
    /// Requests are sent to the program's stdin, and the response is read from its stdout.
    Executable(Vec<String>),

    /// The container is running the specified program (a web server) on the specified port.
    /// Requests are proxied to the container's HTTP server, and the response is proxied back.
    Server(Vec<String>, u16),
}

impl FromStr for InvokeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        match parts.as_slice() {
            ["exec", args] => Ok(InvokeMode::Executable(
                args.split(' ').map(str::to_string).collect(),
            )),
            ["server", port_and_exec] => {
                let parts: Vec<&str> = port_and_exec.splitn(2, ':').collect();
                let port = parts[0]
                    .parse()
                    .map_err(|e| format!("Invalid port: {}", e))?;
                let args = parts[1].split(' ').map(str::to_string).collect();
                Ok(InvokeMode::Server(args, port))
            }
            _ => Err(
                "Invoke mode must be one of 'exec:args' or 'server:listen_port:args'".to_string(),
            ),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionRoute {
    pub hostname: String,
    //pub path_prefix: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FunctionDefinition {
    /// Image to use for the container.
    pub image: String,

    /// URL of the git repository and branch to clone into the container.
    pub repo: Option<(Url, String)>,

    /// CPU limit, as a fraction of a core.
    pub cpu: f32,

    /// Memory limit, in bytes.
    pub memory: u64,

    /// How to communicate with the container.
    pub invoke_mode: InvokeMode,

    /// Maximum number of instances of this function to run.
    pub max_instances: u32,

    #[serde(default)]
    pub routes: Vec<FunctionRoute>,
}

pub const BACKEND_PORT: u16 = 8001;
pub const SVCPROVIDER_PORT: u16 = 9000;
pub const UUID_PACKED_LEN: usize = 16;
pub const UUID_STR_LEN: usize = 36;

#[derive(Clone, Debug, Serialize)]
pub struct Backend {
    pub ip: Ipv4Addr,
    pub container_id: Uuid,
}

impl conhash::Node for Backend {
    fn name(&self) -> String {
        format!("{}:{}", self.ip, self.container_id)
    }
}

pub fn unpack_backends(data: &[u8]) -> Result<Vec<Backend>> {
    if data.len() % (4 + UUID_PACKED_LEN) != 0 {
        return Err(anyhow!("Invalid backend data length: {}", data.len()));
    }

    let mut backends = Vec::new();
    // 4 = size of an IPv4 address
    for chunk in data.chunks(4 + UUID_PACKED_LEN) {
        let backend_ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let container_id = Uuid::from_slice(&chunk[4..])?;
        backends.push(Backend {
            ip: backend_ip,
            container_id,
        });
    }
    Ok(backends)
}

pub fn pack_backends(backends: &[Backend]) -> Vec<u8> {
    let mut data = Vec::new();
    for backend in backends {
        data.extend(backend.ip.octets().iter());
        data.extend(backend.container_id.as_bytes());
    }
    data
}

#[derive(Debug, Serialize, PartialEq)]
#[repr(u8)]
pub enum ContainerState {
    Invalid = 0,
    Starting = 1,
    Running = 2,
    Paused = 3,
    Failed = 4,
}

pub fn init_sentry() -> Option<sentry::ClientInitGuard> {
    if let Ok(dsn) = std::env::var("SENTRY_DSN") {
        eprintln!("Sentry DSN found, initializing Sentry");
        Some(sentry::init((
            dsn,
            sentry::ClientOptions {
                release: sentry::release_name!(),
                ..Default::default()
            },
        )))
    } else {
        None
    }
}
