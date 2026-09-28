#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging
import subprocess
import uuid
from typing import Dict, TypedDict

import lightkube
from lightkube.core.exceptions import ApiError
from lightkube.resources.core_v1 import Pod

logger = logging.getLogger(__name__)
CURL_IMAGE = "curlimages/curl:8.10.1"


class ContainerSecurityContext(TypedDict, total=False):
    """Kubernetes container security context UID/GID settings."""

    runAsUser: int | None  # noqa N815
    runAsGroup: int | None  # noqa N815
    runAsNonRoot: bool | None  # noqa N815


def assert_security_context(
    lightkube_client: lightkube.Client,
    pod_name: str,
    container_name: str,
    container_securitycontext_map: Dict[str, ContainerSecurityContext],
    model_name: str,
) -> None:
    """Assert a container's security context matches expected UID/GID settings."""
    pod_spec = lightkube_client.get(Pod, pod_name, namespace=model_name).spec
    assert pod_spec is not None
    containers: list = pod_spec.containers
    container = next((c for c in containers if c.name == container_name), None)
    assert container is not None, f"Container {container_name} not found in pod {pod_name}"
    security_context = container.securityContext
    # assert user ID is the one defined in metadata.yaml
    for key, value in container_securitycontext_map[container_name].items():
        assert getattr(security_context, key) == value


def generate_container_securitycontext_map(
    metadata_yaml: dict, juju_user_id: int = 170
) -> dict[str, ContainerSecurityContext]:
    """Build a map of container names to expected security context UID/GID settings.

    The map is derived from the ``uid``/``gid`` values in the ``containers`` section
    of ``metadata.yaml``, plus a ``charm`` entry for the Juju agent container.
    """
    c_uid_map: dict[str, ContainerSecurityContext] = {}
    for k, v in metadata_yaml.get("containers", {}).items():
        c_uid_map[k] = ContainerSecurityContext(
            runAsUser=v["uid"],
            runAsGroup=v["gid"],
        )
    c_uid_map["charm"] = {"runAsUser": juju_user_id, "runAsGroup": juju_user_id}
    return c_uid_map


def pod_has_labels(
    namespace: str,
    pod_name: str,
    labels: dict[str, str],
) -> bool:
    """Verify and return bool whether the given pod has all the given labels."""
    client = lightkube.Client()
    try:
        pod = client.get(Pod, name=pod_name, namespace=namespace)
        if pod.metadata is None or pod.metadata.labels is None:
            return False
        return all(pod.metadata.labels.get(k) == v for k, v in labels.items())
    except ApiError as e:
        logger.error(f"Failed to get pod {pod_name} in namespace {namespace}: {e}")
        return False


def curl_using_pod(
    namespace: str,
    url: str,
    labels: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a curl command from a temporary pod in the specified namespace."""
    pod_name = f"curl-{uuid.uuid4()}"

    labels_args = []
    if labels:
        # kubectl run --labels accepts a single comma-separated k=v list.
        labels_value = ",".join(f"{key}={value}" for key, value in labels.items())
        labels_args = ["--labels", labels_value]

    return subprocess.run(
        [
            "kubectl",
            "-n",
            namespace,
            "run",
            pod_name,
            "--rm",
            "-i",
            "--quiet",
            "--restart=Never",
            f"--image={CURL_IMAGE}",
            *labels_args,
            "--",
            "curl",
            "-sS",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            url,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
