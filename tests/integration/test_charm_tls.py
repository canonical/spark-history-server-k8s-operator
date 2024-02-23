#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import asyncio
import base64
import json
import logging
import os
import subprocess
import sys
import urllib.request
from pathlib import Path
from time import sleep

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .test_helpers import (
    fetch_action_sync_s3_credentials,
    get_certificate_from_file,
    setup_s3_bucket_for_history_server,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, charm_versions):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    logger.info("Setting up minio.....")

    setup_env = (
        subprocess.check_output("source microceph.source; env", shell=True, stderr=None)
        .decode("utf-8")
        .strip()
    )
    logger.info(f"Env variable:\n{setup_env}")
    endpoint_url = os.environ["S3_SERVER_URL"]
    access_key = os.environ["S3_ACCESS_KEY"]
    secret_key = os.environ["S3_SECRET_KEY"]
    tls_ca_chain_path = os.environ["S3_CA_BUNDLE_PATH"]

    logger.info(
        f"Setting up s3 bucket with endpoint_url={endpoint_url}, access_key={access_key}, secret_key={secret_key}"
    )

    setup_s3_bucket_for_history_server(endpoint_url, access_key, secret_key, BUCKET_NAME)

    logger.info("Bucket setup complete")

    logger.info("Building charm")
    # Build and deploy charm from local source folder

    charm = await ops_test.build_charm(".")

    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    resources = {"spark-history-server-image": image_version}

    logger.info("Deploying charm")

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(**charm_versions.s3.deploy_dict()),
        ops_test.model.deploy(
            charm, resources=resources, application_name=APP_NAME, num_units=1, series="jammy"
        ),
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
    )

    s3_integrator_unit = ops_test.model.applications[charm_versions.s3.application_name].units[0]

    logger.info("Setting up s3 credentials in s3-integrator charm")

    await fetch_action_sync_s3_credentials(
        s3_integrator_unit, access_key=access_key, secret_key=secret_key
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[charm_versions.s3.application_name], status="active"
        )

    ca = get_certificate_from_file(tls_ca_chain_path)
    ca_b64 = base64.b64encode(ca.encode("utf-8")).decode("utf-8")
    configuration_parameters = {
        "bucket": "history-server",
        "path": "spark-events",
        "endpoint": endpoint_url,
        "tls_ca_chain": ca_b64,
    }
    # apply new configuration options
    await ops_test.model.applications[charm_versions.s3.application_name].set_config(
        configuration_parameters
    )

    logger.info("Relating history server charm with s3-integrator charm")

    await ops_test.model.add_relation(charm_versions.s3.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.s3.application_name], timeout=1000
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

    apps = json.loads(urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read())

    assert len(apps) == 0
    sys.exit(1)
    logger.info("Setting up spark")

    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {endpoint_url} {access_key} {secret_key}",
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
