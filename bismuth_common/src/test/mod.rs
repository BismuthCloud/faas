use anyhow::Result;

pub async fn zk_bootstrap(cluster: &str, zk_env: &str) -> zookeeper_client::Client {
    let env_node = &format!("/{}", zk_env);

    let zk = zookeeper_client::Client::connect(cluster).await.unwrap();
    let _ = delete_all(&zk, env_node).await;
    // Ephemeral znodes can't have children, so the chroot znode must be persistent even for tests :(
    zk.create(
        env_node,
        &b""[..],
        &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all()),
    )
    .await
    .unwrap();
    let zk = zk.chroot(env_node).unwrap();

    // Bootstrap
    zk.create(
        "/node",
        &b""[..],
        &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all()),
    )
    .await
    .unwrap();
    zk.create(
        "/function",
        &b""[..],
        &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all()),
    )
    .await
    .unwrap();

    zk
}

pub async fn delete_all(zk: &zookeeper_client::Client, path: &str) -> Result<()> {
    let mut queue = vec![path.to_string()];
    let mut stack = vec![];
    while let Some(path) = queue.pop() {
        stack.push(path.clone());
        let (children, _) = zk.get_children(&path).await?;
        for child in children {
            queue.push(format!("{}/{}", path, child));
        }
    }

    for path in stack.iter().rev() {
        zk.delete(path, None).await?;
    }

    Ok(())
}
