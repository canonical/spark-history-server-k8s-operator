#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import asyncio
import json
import logging
import subprocess
import urllib.request
from pathlib import Path
from time import sleep

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .test_helpers import (
    add_juju_secret,
    delete_azure_container,
    setup_azure_container_for_history_server,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, charm_versions, azure_credentials):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    logger.info("Setting up azure storage container.....")

    logger.info("Building charm")
    # Build and deploy charm from local source folder

    charm = await ops_test.build_charm(".")

    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    resources = {"spark-history-server-image": image_version}

    logger.info("Deploying charm")

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(**charm_versions.azure_storage.deploy_dict()),
        ops_test.model.deploy(
            charm, resources=resources, application_name=APP_NAME, num_units=1, series="jammy"
        ),
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name], timeout=1000
    )

    logger.info("Adding Juju secret for secret-key config option for azure-storage-integrator")
    credentials_secret_uri = await add_juju_secret(
        ops_test,
        charm_versions.azure_storage.application_name,
        "iamsecret",
        {"secret-key": azure_credentials["secret-key"]},
    )
    logger.info(
        f"Juju secret for secret-key config option for azure-storage-integrator added. Secret URI: {credentials_secret_uri}"
    )

    configuration_parameters = {
        "container": azure_credentials["container"],
        "path": azure_credentials["path"],
        "storage-account": azure_credentials["storage-account"],
        "connection-protocol": azure_credentials["connection-protocol"],
        "credentials": credentials_secret_uri,
    }

    # create azure container
    logger.info(
        f"Creating container {azure_credentials['container']} with path {azure_credentials['path']}"
    )
    # First delete container
    delete_azure_container(azure_credentials["container"])
    sleep(10)
    # Setup container
    setup_azure_container_for_history_server(
        azure_credentials["container"], azure_credentials["path"]
    )
    logger.info("Azure container and path correctly setup!")

    # apply new configuration options
    logger.info("Setting up configuration for azure-storage-integrator charm...")
    await ops_test.model.applications[charm_versions.azure_storage.application_name].set_config(
        configuration_parameters
    )

    logger.info(
        "Waiting for azure-storage-integrator and history-server charm to be idle and active..."
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[
                charm_versions.azure_storage.application_name,
            ],
            status="active",
        )

    logger.info("Relating history server charm with azure-storage-integrator charm")

    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name], timeout=1000
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    logger.info("Verifying history server has no app entries")

    status = await ops_test.model.get_status()
    address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/0"]["address"]

    for i in range(0, 5):
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
        f"./tests/integration/setup/setup_spark_azure.sh {azure_credentials['container']} {azure_credentials['path']} {azure_credentials['storage-account']} {azure_credentials['secret-key']}",
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Executing Spark job")

    run_spark_output = subprocess.check_output(
        "./tests/integration/setup/run_spark_job.sh", shell=True, stderr=None
    ).decode("utf-8")

    logger.info(f"Run spark output:\n{run_spark_output}")

    logger.info("Verifying history server has 1 app entry")

    for i in range(0, 5):
        try:
            apps = json.loads(
                urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read()
            )
        except Exception:
            apps = []

        if len(apps) > 0:
            break
        else:
            sleep(3)

    assert len(apps) == 1

    logger.info("Delete azure container!")
    delete_azure_container(azure_credentials["container"])
