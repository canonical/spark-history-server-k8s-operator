#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import logging
from pathlib import Path

import jubilant
import yaml

from .helpers import (
    assert_jobs_in_history_server,
    delete_azure_container,
    deploy_history_server_setup,
    run_spark_job,
    setup_spark_job,
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
    deploy_history_server_setup(
        juju=juju,
        charm_versions=charm_versions,
        history_server_charm=history_server_charm,
        azure_storage_credentials=azure_storage_credentials,
    )
    status = juju.wait(jubilant.all_active, delay=5)

    logger.info("Verifying history server has no app entries")

    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    server_url = f"http://{address}:18080"
    assert_jobs_in_history_server(server_url=server_url, expected_count=0)

    setup_spark_job(azure_storage_credentials=azure_storage_credentials)
    run_spark_job()
    assert_jobs_in_history_server(server_url=server_url, expected_count=1)

    logger.info("Delete azure container!")
    delete_azure_container(azure_storage_credentials["container"])
