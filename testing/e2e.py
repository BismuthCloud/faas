import os
import signal
import subprocess
from typing import List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
import requests
import time
from uuid import UUID

FRONTEND_URL = "http://localhost:8000"
BACKEND_URL = "http://localhost:8001"
API_URL = "http://localhost:8002"


@dataclass
class ExecutableInvokeMode:
    cmd: List[str]


@dataclass
class ServerInvokeMode:
    entrypoint: str
    port: int


@dataclass
class FunctionDefinition:
    image: str
    repo: Optional[Tuple[str, str]]
    cpu: float
    memory: int
    invoke_mode: ServerInvokeMode | ExecutableInvokeMode
    max_instances: int = 1

    def json(self):
        match self.invoke_mode:
            case ServerInvokeMode(entrypoint, port):
                invoke_mode = {"Server": [entrypoint, port]}
            case ExecutableInvokeMode(cmd):
                invoke_mode = {"Executable": cmd}

        return {
            "image": self.image,
            "repo": self.repo,
            "cpu": self.cpu,
            "memory": self.memory,
            "invoke_mode": invoke_mode,
            "max_instances": self.max_instances,
        }


class ContainerState(Enum):
    Invalid = 0
    Starting = 1
    Running = 2
    Paused = 3
    Failed = 4


class Function:
    url: str

    def __init__(self, url: str):
        self.url = url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logging.info(f"Attempting to DELETE {self.url}")
        resp = requests.delete(self.url)
        logging.info("OK" if resp.ok else f"Failed ({resp.status_code})")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.info("Creating function")
    function_definition = FunctionDefinition(
        image="docker.io/library/python:3.11",
        repo=None,
        cpu=0.5,
        memory=128 * 1024 * 1024,
        invoke_mode=ServerInvokeMode(["/usr/local/bin/python3", "-m", "http.server"], 8000),
    )
    function_resp = requests.post(
        f"{API_URL}/function", json=function_definition.json()
    )
    assert (
        function_resp.status_code == 200
    ), f"Failed to create function ({function_resp.status_code}): {function_resp.text}"
    function_id = UUID(function_resp.json()["id"])
    logging.info(f"Function {function_id} created")

    with Function(f"{API_URL}/function/{function_id}") as function:
        logging.info("Waiting for function to ready")
        for _ in range(30):
            resp = requests.get(function.url)
            logging.debug(resp.json())
            assert (
                resp.status_code == 200
            ), f"Failed to get function ({resp.status_code}): {resp.text}"
            status = ContainerState[resp.json()["backends"][0]["status"]]
            if status == ContainerState.Running:
                break
            logging.debug(f"Function not ready ({status})")
            time.sleep(1)
        else:
            assert False, "Function failed to start"

        logging.info("Checking function is reachable")
        resp = requests.get(f"{FRONTEND_URL}/invoke/{function_id}")
        logging.debug(resp.text)
        assert (
            resp.status_code == 200
        ), f"Failed to invoke function ({resp.status_code}): {resp.text}"

        logging.info("Ensuring response debug headers are present")
        assert resp.headers.get("X-Bismuth-Container-ID"), f"Container ID not present in response headers ({resp.headers})"
        assert resp.headers.get("traceparent"), f"Traceparent not present in response headers ({resp.headers})"

        logging.info("Checking logs")
        resp = requests.get(f"{function.url}/logs")
        logging.debug(resp.text)
        assert (
            resp.status_code == 200
        ), f"Failed to get logs ({resp.status_code}): {resp.text}"

        logging.info("Checking streaming logs")
        with requests.get(
            f"{function.url}/logs?follow=true", stream=True
        ) as resp:
            assert (
                resp.status_code == 200
            ), f"Failed to get logs ({resp.status_code}): {resp.text}"

            # Invoke function again and ensure new logs are streamed
            requests.get(f"{FRONTEND_URL}/invoke/{function_id}/again")
            time.sleep(1)

            for line in resp.iter_lines():
                line = line.decode("utf-8")
                logging.debug(line)
                if "/again" in line:
                    break
            else:
                assert False, "New request not in streamed logs"

        logging.info("Checking new container on update")
        resp = requests.get(f"{FRONTEND_URL}/invoke/{function_id}")
        container_before = resp.headers["X-Bismuth-Container-ID"]
        logging.debug(f"Container ID before update: {container_before}")
        resp = requests.put(function.url, data="")
        logging.debug(resp.json())
        assert (
            resp.status_code == 200
        ), f"Failed to update function ({resp.status_code}): {resp.text}"
        for _ in range(30):
            resp = requests.get(function.url)
            logging.debug(resp.json())
            assert (
                resp.status_code == 200
            ), f"Failed to get function ({resp.status_code}): {resp.text}"
            status = ContainerState[resp.json()["backends"][0]["status"]]
            if status == ContainerState.Running:
                break
            logging.debug(f"Function not ready ({status})")
            time.sleep(1)
        else:
            assert False, "Function failed to start"
        resp = requests.get(f"{FRONTEND_URL}/invoke/{function_id}")
        container_after = resp.headers["X-Bismuth-Container-ID"]
        logging.debug(f"Container ID after update: {container_after}")
        assert (
            container_before != container_after
        ), "Container ID did not change after update"

        logging.info("Checking old container was removed")
        cts = [l.decode("utf-8") for l in subprocess.check_output(
            ["ctr", "-n", "bismuth", "task", "ls"]
        ).splitlines()]
        for line in cts:
            if container_before in line:
                assert False, f"Old container {container_before} still exists"

        logging.info("Checking function status update on kill")
        for line in cts:
            if container_after in line:
                pid = int(line.split()[1])
                logging.debug(f"Resolved container {container_after} to PID {pid}")
                break
        else:
            assert False, f"Failed to resolve container {container_after} to PID"

        os.kill(pid, signal.SIGKILL)
        time.sleep(1)

        resp = requests.get(function.url)
        logging.debug(resp.json())
        assert (
            resp.status_code == 200
        ), f"Failed to get function ({resp.status_code}): {resp.text}"
        status = ContainerState[resp.json()["backends"][0]["status"]]
        assert status == ContainerState.Failed, f"Expected status Failed, got {status}"
