#!/usr/bin/env python3
# Copyright 2023 Abhishek Verma
# See LICENSE file for licensing details.

import asyncio
import logging
from pathlib import Path

import pytest
import yaml
from pytest_operator.plugin import OpsTest
from src.constants import  *

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
        "sparkhistoryserver-image": METADATA["resources"]["sparkhistoryserver-image"][
            "upstream-source"
        ]
    }

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(
            charm, resources=resources, application_name=APP_NAME, num_units=1, series="jammy"
        ),
        ops_test.model.wait_for_idle(
            apps=[APP_NAME], status="waiting", raise_on_blocked=True, timeout=1000
        ),
    )

    # add config parameters via a file
    app = ops_test.model.applications.get(APP_NAME)
    await app.set_config(
        {
            CONFIG_KEY_S3_ENDPOINT: "http://192.168.1.8:9000",
            CONFIG_KEY_S3_ACCESS_KEY: "5mHwHrovJXVTMsQV",
            CONFIG_KEY_S3_SECRET_KEY: "dKQ9DyhcPltC3U4jSqlHsBhykiflT5kR",
            CONFIG_KEY_S3_LOGS_DIR: "s3a://history-server/spark-events/",
        }
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", raise_on_blocked=True, timeout=1000
    )
