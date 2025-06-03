#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import subprocess
from pathlib import Path
from typing import cast

import boto3
import jubilant
import requests
import yaml
from botocore.client import Config

from constants import JMX_EXPORTER_PORT

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

COS_METRICS_PORT = 10019


logger = logging.getLogger(__name__)


def set_s3_credentials(
    juju: jubilant.Juju,
    access_key: str,
    secret_key: str,
) -> None:
    """Use the charm action to start a password rotation."""
    params = {
        "access-key": access_key,
        "secret-key": secret_key,
    }

    task = juju.run("s3-integrator/0", "sync-s3-credentials", params)
    assert task.return_code == 0


def setup_s3_bucket_for_history_server(
    endpoint_url: str, aws_access_key: str, aws_secret_key: str, bucket_str: str, verify=False
):
    config = Config(
        connect_timeout=60,
        retries={"max_attempts": 0},
        response_checksum_validation="when_supported",
        request_checksum_calculation="when_supported",
    )
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )
    s3 = session.client("s3", endpoint_url=endpoint_url, config=config, verify=verify)
    # delete test bucket and its content if it already exist
    buckets = s3.list_buckets()
    for bucket in buckets["Buckets"]:
        bucket_name = bucket["Name"]
        if bucket_name == bucket_str:
            logger.info(f"Deleting bucket: {bucket_name}")
            objects = s3.list_objects_v2(Bucket=bucket_str)["Contents"]
            objs = [x["Key"] for x in objects]
            for obj in objs:
                s3.delete_object(Bucket=bucket_str, Key=obj)
            s3.delete_bucket(Bucket=bucket_str)


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
        return
    if response.status_code == 200:
        return response.text


def all_prometheus_exporters_data(juju: jubilant.Juju, check_field) -> bool:
    """Check if a all units has metric service available and publishing."""
    result = True
    status = juju.status()
    for unit in status.apps[APP_NAME].units.values():
        unit_ip = unit.address
        result = result and check_field in prometheus_exporter_data(unit_ip)
    return result


def published_prometheus_alerts(juju: jubilant.Juju, host: str) -> dict | None:
    """Retrieve all Prometheus Alert rules that have been published."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{cast(str, juju.model)}-prometheus-0/api/v1/rules"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


def published_prometheus_data(juju: jubilant.Juju, host: str, field: str) -> dict | None:
    """Check the existence of field among Prometheus published data."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{cast(str, juju.model)}-prometheus-0/api/v1/query?query={field}"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


def published_grafana_dashboards(juju: jubilant.Juju) -> dict | None:
    """Get the list of dashboards published to Grafana."""
    base_url, pw = get_grafana_access(juju)
    url = f"{base_url}/api/search?query=&starred=false"

    try:
        session = requests.Session()
        session.auth = ("admin", pw)
        response = session.get(url)
    except requests.exceptions.RequestException:
        return
    if response.status_code == 200:
        return response.json()


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
