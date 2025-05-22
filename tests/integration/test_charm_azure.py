#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import json
import logging
import subprocess
import urllib.request
from pathlib import Path
from time import sleep

import jubilant
import yaml

from .test_helpers import (
    delete_azure_container,
)
from .types import AzureInfo, IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"


def test_build_and_deploy(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    azure_storage_credentials: AzureInfo,
    history_server_charm: Path,
) -> None:
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    image_metadata = json.loads(
        subprocess.check_output(
            f"./tests/integration/setup/get_image_metadata.sh {image_version}",
            shell=True,
            stderr=None,
        ).decode("utf-8")
    )

    spark_version = image_metadata["org.opencontainers.image.version"]

    logger.info(f"Spark version: {spark_version}")

    resources = {"spark-history-server-image": image_version}

    logger.info("Deploying charm")

    # Deploy the charm and wait for waiting status
    juju.deploy(**charm_versions.azure_storage.deploy_dict())
    juju.deploy(
        history_server_charm,
        resources=resources,
        app=APP_NAME,
        num_units=1,
        base="ubuntu@22.04",
    )

    juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.azure_storage.application_name),
        delay=5,
    )

    logger.info("Adding Juju secret for secret-key config option for azure-storage-integrator")
    secret_id = juju.add_secret(
        "iamsecret",
        {"secret-key": azure_storage_credentials["secret-key"]},
    )
    logger.info(f"Created secret {secret_id}")
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
    status = juju.wait(jubilant.all_active, delay=5)

    logger.info("Verifying history server has no app entries")

    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address

    apps = []
    for _ in range(0, 5):
        try:
            apps = json.loads(
                urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read()
            )
        except Exception:
            apps = []

        if len(apps) == 0:
            break
        else:
            sleep(5)

    assert len(apps) == 0

    logger.info("Setting up spark")

    setup_spark_output = subprocess.check_output(
        (
            f"./tests/integration/setup/setup_spark_azure.sh "
            f"{azure_storage_credentials['container']} "
            f"{azure_storage_credentials['path']} "
            f"{azure_storage_credentials['storage-account']} "
            f"{azure_storage_credentials['secret-key']} {image_version}"
        ),
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Executing Spark job")

    run_spark_output = subprocess.check_output(
        f"./tests/integration/setup/run_spark_job.sh {spark_version} {image_version}",
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Run spark output:\n{run_spark_output}")

    logger.info("Verifying history server has 1 app entry")

    for _ in range(0, 5):
        try:
            apps = json.loads(
                urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read()
            )
        except Exception as e:
            logger.warning(f"Exception e: {e}")
            apps = []

        if len(apps) > 0:
            break
        else:
            sleep(30)

    assert len(apps) == 1

    logger.info("Delete azure container!")
    delete_azure_container(azure_storage_credentials["container"])
