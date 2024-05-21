#!/bin/bash
set -xeuo pipefail

./target/debug/bismuthctl --zookeeper zookeeper1:2181 bootstrap
./target/debug/bismuthctl --zookeeper zookeeper1:2181 provision 127.0.0.1
FUNC_ID=$(./target/debug/bismuthctl --zookeeper zookeeper1:2181 create-function docker.io/library/python:3.11 'server:8000:/usr/local/bin/python3 -m http.server')
CT_ID=$(./target/debug/bismuthctl --zookeeper zookeeper1:2181 add-backend ${FUNC_ID} 127.0.0.1)

echo "Function ID: ${FUNC_ID}"
echo "Container ID: ${CT_ID}"