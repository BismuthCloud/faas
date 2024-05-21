FROM ubuntu:22.04
RUN apt-get update && apt-get install -y curl make clang pkg-config libssl-dev libfmt-dev protobuf-compiler containerd iptables iproute2 tmux

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
RUN echo 'source $HOME/.cargo/env' >> $HOME/.bashrc

# containerd uses overlayfs by default, but this doesn't work nested.
# also can't easily mount in the host's /run/containerd/containerd.sock
# as we can't share a directory between container and docker VM on mac.
ENV CONTAINERD_SNAPSHOTTER=native
