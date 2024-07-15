#!/bin/bash
set -xeuo pipefail

SESSION=bismuth

tmux new-session -s $SESSION -d
tmux send-keys -t $SESSION:0 'RUST_BACKTRACE=1 RUST_LOG=faas-api=TRACE ./target/debug/api --zookeeper zookeeper1:2181 --hosted-db postgresql://postgres:password@hosteddb:5432/postgres' C-m

tmux split-window -t $SESSION:0 -h
tmux send-keys -t $SESSION:0 'RUST_BACKTRACE=1 RUST_LOG=bismuthfe=TRACE ./target/debug/bismuthfe --zookeeper zookeeper1:2181' C-m

tmux split-window -t $SESSION:0 -h
tmux send-keys -t $SESSION:0 'RUST_BACKTRACE=1 RUST_LOG=bismuthd=TRACE ./target/debug/bismuthd --zookeeper zookeeper1:2181 --bind 127.0.0.1' C-m

tmux attach -t $SESSION
