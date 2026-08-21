#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import subprocess
from pathlib import Path
from typing import Dict, TypedDict, cast

import jubilant
import lightkube
import requests
import yaml
from lightkube.resources.core_v1 import Pod

from constants import JMX_EXPORTER_PORT

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

COS_METRICS_PORT = 10019


logger = logging.getLogger(__name__)


class ContainerSecurityContext(TypedDict, total=False):
    """Kubernetes container security context UID/GID settings."""

    runAsUser: int | None  # noqa N815
    runAsGroup: int | None  # noqa N815
    runAsNonRoot: bool | None  # noqa N815


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


def get_pod_names(model: str, application_name: str) -> list[str]:
    """Retrieve names of all pods belonging to a specific Juju application."""
    cmd = [
        "kubectl",
        "get",
        "pods",
        f"-n{model}",
        f"-lapp.kubernetes.io/name={application_name}",
        "--no-headers",
        "-o=custom-columns=NAME:.metadata.name",
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
    )
    stdout = proc.stdout.decode("utf8")
    return stdout.split()


def assert_security_context(
    lightkube_client: lightkube.Client,
    pod_name: str,
    container_name: str,
    container_securitycontext_map: Dict[str, ContainerSecurityContext],
    model_name: str,
) -> None:
    """Assert a container's security context matches expected UID/GID settings."""
    containers: list = lightkube_client.get(Pod, pod_name, namespace=model_name).spec.containers
    container = next((c for c in containers if c.name == container_name), None)
    assert container is not None, f"Container {container_name} not found in pod {pod_name}"
    security_context = container.securityContext
    # assert user ID is the one defined in metadata.yaml
    for key, value in container_securitycontext_map[container_name].items():
        assert getattr(security_context, key) == value


def set_s3_credentials(
    juju: jubilant.Juju,
    s3_app_name: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Use the charm action to start a password rotation."""
    params = {
        "access-key": access_key,
        "secret-key": secret_key,
    }
    secret_uri = juju.add_secret("s3-credentials", params)
    juju.grant_secret(secret_uri, s3_app_name)
    juju.config(s3_app_name, {"credentials": secret_uri})


def delete_azure_container(container: str):
    """Delete azure container."""
    command = ["azcli", "storage", "container", "delete", "--name", container]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        return output.stdout.decode(), output.stderr.decode(), output.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.decode(), e.stderr.decode(), e.returncode


def get_certificate_from_file(filename: str) -> str:
    """Returns the certificate as a string."""
    with open(filename, "r") as file:
        certificate = file.read()
    return certificate


def prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has metric service available and it is publishing."""
    url = f"http://{host}:{JMX_EXPORTER_PORT}/metrics"
    try:
        response = requests.get(url)
        logger.info(f"Response: {response.text}")
        print(response)
    except requests.exceptions.RequestException:
        return None

    if response.status_code == 200:
        return response.text

    return None


def all_prometheus_exporters_data(juju: jubilant.Juju, check_field) -> bool:
    """Check if a all units has metric service available and publishing."""
    result = True
    status = juju.status()
    for unit in status.apps[APP_NAME].units.values():
        unit_ip = unit.address
        result = result and check_field in (prometheus_exporter_data(unit_ip) or "")
    return result


def published_prometheus_alerts(juju: jubilant.Juju, host: str) -> dict | None:
    """Retrieve all Prometheus Alert rules that have been published."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{cast(str, juju.model)}-prometheus-0/api/v1/rules"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return None

    if response.status_code == 200:
        return response.json()

    return None


def published_prometheus_data(juju: jubilant.Juju, host: str, field: str) -> dict | None:
    """Check the existence of field among Prometheus published data."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{cast(str, juju.model)}-prometheus-0/api/v1/query?query={field}"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return None

    if response.status_code == 200:
        return response.json()

    return None


def published_grafana_dashboards(juju: jubilant.Juju) -> dict | None:
    """Get the list of dashboards published to Grafana."""
    base_url, pw = get_grafana_access(juju)
    url = f"{base_url}/api/search?query=&starred=false"

    try:
        session = requests.Session()
        session.auth = ("admin", pw)
        response = session.get(url)
    except requests.exceptions.RequestException:
        return None

    if response.status_code == 200:
        return response.json()

    return None


def get_cos_address(juju: jubilant.Juju) -> str:
    """Retrieve the URL where COS services are available."""
    task = juju.run("traefik/0", "show-proxied-endpoints")
    assert task.return_code == 0
    endpoints = task.results["proxied-endpoints"]
    return json.loads(endpoints)["traefik"]["url"]


def get_grafana_access(juju: jubilant.Juju) -> tuple[str, str]:
    """Get Grafana URL and password."""
    task = juju.run("grafana/0", "get-admin-password")
    assert task.return_code == 0
    return task.results["url"], task.results["admin-password"]
