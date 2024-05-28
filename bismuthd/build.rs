use std::io::Result;

fn main() -> Result<()> {
    prost_build::compile_protos(&["io.containerd.cgroups.v2.proto"], &["vendor/"])?;
    Ok(())
}
