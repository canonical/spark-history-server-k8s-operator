#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging
import subprocess
from pathlib import Path
from time import sleep

import jubilant
import yaml

from ..types import AzureInfo, IntegrationTestsCharms

logger = logging.getLogger(__name__)
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def prepare_azure_storage_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    azure_storage_credentials: AzureInfo,
):
    """Prepare the Azure storage setup for the history server.

    Args:
        juju: The Juju client instance.
        charm_versions: The versions of the charms to deploy.
        azure_storage_credentials: The Azure storage credentials information.
    """
    juju.deploy(**charm_versions.azure_storage.deploy_dict())

    logger.info("Adding Juju secret for secret-key config option for azure-storage-integrator")
    secret_id = juju.add_secret(
        "iamsecret",
        {"secret-key": azure_storage_credentials["secret-key"]},
    )
    juju.cli("grant-secret", "iamsecret", charm_versions.azure_storage.application_name)

    # create azure container
    configuration_parameters = {
        "container": azure_storage_credentials["container"],
        "path": azure_storage_credentials["path"],
        "storage-account": azure_storage_credentials["storage-account"],
        "connection-protocol": azure_storage_credentials["connection-protocol"],
        "credentials": secret_id,
    }

    logger.info(
        f"Creating container {azure_storage_credentials['container']} with path {azure_storage_credentials['path']}"
    )
    # First delete container
    delete_azure_container(azure_storage_credentials["container"])
    sleep(10)

    # apply new configuration options
    logger.info("Setting up configuration for azure-storage-integrator charm...")
    juju.config(charm_versions.azure_storage.application_name, configuration_parameters)
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.azure_storage.application_name)
    )

    logger.info("Relating history server charm with azure-storage-integrator charm")

    juju.integrate(charm_versions.azure_storage.application_name, APP_NAME)
    juju.wait(jubilant.all_active, delay=5)


def delete_azure_container(container: str):
    """Delete azure container."""
    command = ["azcli", "storage", "container", "delete", "--name", container]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        return output.stdout.decode(), output.stderr.decode(), output.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.decode(), e.stderr.decode(), e.returncode
