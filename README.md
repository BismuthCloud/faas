# Bismuth FaaS

This is the container orchestration system (+ platform service stub) used by [BismuthOS](https://www.bismuthos.com).

## Architecture

The frontend (`bismuthfe`) is the external-facing entrypoint of the service, responsible for routing requests to assigned backends.

The backend (`bismuthd`) runs on worker nodes, starting containers with function code as necessary, receiving requests from the frontend, and forwarding the requests to the containers.

The API is the control plane for the service, where other applications can specify containers to be created, check status, fetch logs, etc.

Finally, `svcprovider` provides a HTTP interface for a common set of basic services that the function may need to interact with: K/V, blob storage, and secrets.

### ZooKeeper

ZK is where function definitions and routing information is persisted.
This gives us a consistency-guaranteed data store, additionally with the ability to set watches to ensure caches and similar don't go stale.

#### Data Model

Everything is chrooted into an arbitrary "environment" (default is "default") so that multiple instances can be running on the same ZK cluster (e.g. so testing locally can be in its own isolated environment and not destroy anything in dev).

* `/function`
  * `/function/{id}` is a znode with function runtime details in its data (a serialized `FunctionDefinition`)
    * `/function/{id}/backends` is a znode with a packed array of backend ipv4 addresses and container ids which serve this function
* `/node`
  * `/node/{ip}` has one byte value 0 or 1 with enabled status (whether this node should be considered for scheduling)
  * `/node/{ip}/container` has children znodes for each container that the host serves
  * `/node/{ip}/container/{id}` is a znode with the function id and any associated metadata set on the node (e.g. host-internal IP)


## Basic Setup

### Developing
#### VSCode Devcontainer
* Open this workspace in a devcontainer
* `cargo build`
* `./bootstrap.sh`, or the equivalent:
    * Bootstrap zookeeper: `./target/debug/bismuthctl --zookeeper zookeeper1:2181 bootstrap`
    * Provision the node you're on, e.g. `./target/debug/bismuthctl --zookeeper zookeeper1:2181 provision 127.0.0.1`
    * Create a function to run:
      * `./target/debug/bismuthctl --zookeeper zookeeper1:2181 create-function docker.io/library/alpine:latest 'exec:/bin/ls'` to run /bin/ls on each request
      * `./target/debug/bismuthctl --zookeeper zookeeper1:2181 create-function docker.io/library/python:3.11 'server:8000:/usr/local/bin/python3 -m http.server'` to run python's web server as init and proxy to it for requests.
    * Mark the current node as a backend for the function: `./target/debug/bismuthctl --zookeeper zookeeper1:2181 add-backend {id from create-function above} 127.0.0.1`

### Using
* Run `./start_all.sh` to start frontend, backend, and API.
* Now you should be able to `curl http://localhost:8000/invoke/{function id}`

### In case of containerd issues

If you need to manually remove a container (everything should be cleaned up on startup but something will always go wrong in a new and unexpected way):
* `sudo ctr -n=bismuth task list` to get the PID of the init task
* `sudo kill -9` it
* `sudo ctr -n=bismuth container rm {container_id}`
* `sudo ctr -n=bismuth snapshots rm /run/bismuthd/roots/{container_id}`

### In case of zookeeper issues (cluster consistency, old function specs, etc.)

* Easiest is to just destroy the cluster and re-create per setup above.
  * `/usr/share/zookeeper/bin/zkCli.sh`
  * `deleteall /default`


## Why no k8s?

There's a few FaaS runtimes (basically all running on top of k8s) which Bismuth could have used instead. However:

1) That's no fun
2) They actually have some limitations/reasons not to use

In no particular order:

### [Knative](https://knative.dev/)
* Must be run on k8s, and scaling k8s gets kinda ugly
  * Especially since we want to be able to run on our own metal, so no "just use hosted control plane"
  * And we also don't need any other features of k8s so it's super overkill
* Not really a complete thing on its own

### [OpenFaaS](https://www.openfaas.com/)
* Not really "open" because [$$$$](https://www.openfaas.com/pricing/)

### [OpenWhisk](https://openwhisk.apache.org/)
* Quite a few complex dependencies to run/scale (couchdb, kafka, redis, zookeeper)
* Can be run outside of k8s, but definitely not happy path
  * With that comes the same k8s scaling "fun" along with everything else above

### [Fn](https://fnproject.io/)
* Not sure if this is actually deployed by anyone outside oracle...
