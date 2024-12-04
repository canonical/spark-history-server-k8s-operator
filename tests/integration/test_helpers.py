#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import subprocess
from pathlib import Path
from subprocess import PIPE, check_output
from typing import Dict

import boto3
import requests
import yaml
from botocore.client import Config
from juju.unit import Unit
from pytest_operator.plugin import OpsTest

from constants import JMX_EXPORTER_PORT

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]

COS_METRICS_PORT = 10019


logger = logging.getLogger(__name__)


async def fetch_action_sync_s3_credentials(unit: Unit, access_key: str, secret_key: str) -> Dict:
    """Helper to run an action to sync credentials.

    Args:
        unit: The juju unit on which to run the get-password action for credentials
        access_key: the access_key to access the s3 compatible endpoint
        secret_key: the secret key to access the s3 compatible endpoint
    Returns:
        A dictionary with the server config username and password
    """
    parameters = {"access-key": access_key, "secret-key": secret_key}
    action = await unit.run_action(action_name="sync-s3-credentials", **parameters)
    result = await action.wait()

    return result.results


def setup_s3_bucket_for_history_server(
    endpoint_url: str, aws_access_key: str, aws_secret_key: str, bucket_str: str, verify=False
):
    config = Config(connect_timeout=60, retries={"max_attempts": 0})
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
            objs = [{"Key": x["Key"]} for x in objects]
            s3.delete_objects(Bucket=bucket_str, Delete={"Objects": objs})
            s3.delete_bucket(Bucket=bucket_str)

    # logger.info("create bucket in minio")
    # for i in range(0, 30):
    #     try:
    #         s3.create_bucket(Bucket=bucket_str)
    #         break
    #     except Exception as e:
    #         if i >= 30:
    #             logger.error(f"create bucket failed....exiting....\n{str(e)}")
    #             raise
    #         else:
    #             logger.warning(f"create bucket failed....retrying in 10 secs.....\n{str(e)}")
    #             sleep(10)
    #             continue

    # s3.put_object(Bucket=bucket_str, Key=("spark-events/"))
    # logger.debug(s3.list_buckets())


def setup_azure_container_for_history_server(container: str, path: str) -> None:
    """Setup azure container."""
    logger.info(f"Create container: {container}")
    create_azure_container(container)
    logger.info(f"Create folder {path}")
    create_folder_in_container(container, path)
    logger.info("Setup of azure storage done!")


def create_folder_in_container(container: str, path: str):
    """Setup required folder in azure container."""
    command = [
        "azcli",
        "storage",
        "blob",
        "upload",
        "--container-name",
        container,
        "--name",
        f"{path}/a.tmp",
        "-f",
        "/dev/null",
    ]

    try:
        output = subprocess.run(command, check=True, capture_output=True)
        return output.stdout.decode(), output.stderr.decode(), output.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.decode(), e.stderr.decode(), e.returncode


def create_azure_container(container: str):
    """Create Azure container."""
    command = ["azcli", "storage", "container", "create", "--fail-on-exist", "--name", container]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        return output.stdout.decode(), output.stderr.decode(), output.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.decode(), e.stderr.decode(), e.returncode


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


async def get_juju_secret(ops_test: OpsTest, secret_uri: str) -> Dict[str, str]:
    """Retrieve juju secret."""
    secret_unique_id = secret_uri.split("/")[-1]
    complete_command = f"show-secret {secret_uri} --reveal --format=json"
    _, stdout, _ = await ops_test.juju(*complete_command.split())
    return json.loads(stdout)[secret_unique_id]["content"]["Data"]


async def add_juju_secret(
    ops_test: OpsTest, charm_name: str, secret_label: str, data: Dict[str, str]
) -> str:
    """Add a new juju secret."""
    logger.info(f"Data keys to insert: {data.keys()}")
    key_values = " ".join([f"{key}={value}" for key, value in data.items()])
    command = f"add-secret {secret_label} {key_values}"
    _, stdout, _ = await ops_test.juju(*command.split())
    logger.info(f"Add secret output: {stdout}")
    secret_uri = stdout.strip()
    logger.info(f"Secret uri: {secret_uri}")
    command = f"grant-secret {secret_label} {charm_name}"
    _, stdout, _ = await ops_test.juju(*command.split())
    return secret_uri


async def update_juju_secret(
    ops_test: OpsTest, charm_name: str, secret_label: str, data: Dict[str, str]
) -> None:
    """Update the given juju secret."""
    key_values = " ".join([f"{key}={value}" for key, value in data.items()])
    command = f"update-secret {secret_label} {key_values}"
    retcode, stdout, stderr = await ops_test.juju(*command.split())
    if retcode != 0:
        logger.warning(
            f"Update Juju secret exited with non zero status. \nSTDOUT: {stdout.strip()} \nSTDERR: {stderr.strip()}"
        )


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


async def all_prometheus_exporters_data(ops_test: OpsTest, check_field) -> bool:
    """Check if a all units has metric service available and publishing."""
    result = True
    for unit in ops_test.model.applications[APP_NAME].units:
        unit_ip = await get_address(ops_test, unit.name)
        result = result and check_field in prometheus_exporter_data(unit_ip)
    return result


def published_prometheus_alerts(ops_test: OpsTest, host: str) -> dict | None:
    """Retrieve all Prometheus Alert rules that have been published."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{ops_test.model.name}-prometheus-0/api/v1/rules"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


def published_prometheus_data(ops_test: OpsTest, host: str, field: str) -> dict | None:
    """Check the existence of field among Prometheus published data."""
    if "http://" in host:
        host = host.split("//")[1]
    url = f"http://{host}/{ops_test.model.name}-prometheus-0/api/v1/query?query={field}"
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        return

    if response.status_code == 200:
        return response.json()


async def published_grafana_dashboards(ops_test: OpsTest) -> str | None:
    """Get the list of dashboards published to Grafana."""
    base_url, pw = await get_grafana_access(ops_test)
    url = f"{base_url}/api/search?query=&starred=false"

    try:
        session = requests.Session()
        session.auth = ("admin", pw)
        response = session.get(url)
    except requests.exceptions.RequestException:
        return
    if response.status_code == 200:
        return response.json()


async def get_cos_address(ops_test: OpsTest) -> str:
    """Retrieve the URL where COS services are available."""
    cos_addr_res = check_output(
        f"JUJU_MODEL={ops_test.model.name} juju run traefik/0 show-proxied-endpoints --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    try:
        cos_addr = json.loads(cos_addr_res)
    except json.JSONDecodeError:
        raise ValueError

    endpoints = cos_addr["traefik/0"]["results"]["proxied-endpoints"]
    return json.loads(endpoints)["traefik"]["url"]


async def get_grafana_access(ops_test: OpsTest) -> tuple[str, str]:
    """Get Grafana URL and password."""
    grafana_res = check_output(
        f"JUJU_MODEL={ops_test.model.name} juju run grafana/0 get-admin-password --format json",
        stderr=PIPE,
        shell=True,
        universal_newlines=True,
    )

    try:
        grafana_data = json.loads(grafana_res)
    except json.JSONDecodeError:
        raise ValueError

    url = grafana_data["grafana/0"]["results"]["url"]
    password = grafana_data["grafana/0"]["results"]["admin-password"]
    return url, password


async def get_address(ops_test: OpsTest, unit_name: str) -> str:
    """Get the address for a unit."""
    status = await ops_test.model.get_status()  # noqa: F821
    address = status["applications"][APP_NAME]["units"][f"{unit_name}"]["address"]
    return address
