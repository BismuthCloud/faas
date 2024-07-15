#!/bin/bash
set -xeuo pipefail

./target/debug/bismuthctl --zookeeper zookeeper1:2181 bootstrap
./target/debug/bismuthctl --zookeeper zookeeper1:2181 provision 127.0.0.1