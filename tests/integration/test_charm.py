#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import asyncio
import json
import logging
import os
import urllib.request
from pathlib import Path
from time import sleep

import pytest
import yaml
from pytest_operator.plugin import OpsTest
from src.constants import S3_INTEGRATOR_CHARM_NAME
from test_helpers import fetch_action_sync_s3_credentials

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    # Build and deploy charm from local source folder
    charm = await ops_test.build_charm(".")
    resources = {
        "spark-history-server-image": METADATA["resources"]["spark-history-server-image"][
            "upstream-source"
        ]
    }

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(
            S3_INTEGRATOR_CHARM_NAME,
            channel="edge",
            application_name=S3_INTEGRATOR_CHARM_NAME,
            num_units=1,
            series="jammy",
        ),
        ops_test.model.deploy(
            charm, resources=resources, application_name=APP_NAME, num_units=1, series="jammy"
        ),
    )

    await ops_test.model.wait_for_idle(apps=[APP_NAME, S3_INTEGRATOR_CHARM_NAME], timeout=1000)

    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")
    s3_integrator_unit = ops_test.model.applications[S3_INTEGRATOR_CHARM_NAME].units[0]

    await fetch_action_sync_s3_credentials(
        s3_integrator_unit, access_key=access_key, secret_key=secret_key
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[S3_INTEGRATOR_CHARM_NAME], status="active")

    configuration_parameters = {
        "bucket": "history-server",
        "path": "spark-events",
        "endpoint": os.getenv("MINIO_ENDPOINT", default="http://127.0.0.1:9000"),
    }
    # apply new configuration options
    await ops_test.model.applications[S3_INTEGRATOR_CHARM_NAME].set_config(
        configuration_parameters
    )

    await ops_test.model.add_relation(S3_INTEGRATOR_CHARM_NAME, APP_NAME)

    await ops_test.model.wait_for_idle(apps=[APP_NAME, S3_INTEGRATOR_CHARM_NAME], timeout=1000)

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    status = await ops_test.model.get_status()
    address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/0"]["address"]

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
