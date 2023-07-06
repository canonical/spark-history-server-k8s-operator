#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import asyncio
import json
import logging
import subprocess
import urllib.request
from pathlib import Path
from time import sleep

import boto3
import pytest
import yaml
from botocore.client import Config
from pytest_operator.plugin import OpsTest

from constants import INGRESS_CHARM, S3_INTEGRATOR_CHARM_NAME

from .test_helpers import fetch_action_sync_s3_credentials

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def setup_s3_bucket_for_history_server(
    endpoint_url: str, aws_access_key: str, aws_secret_key: str
):
    config = Config(connect_timeout=60, retries={"max_attempts": 0})
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )
    s3 = session.client("s3", endpoint_url=endpoint_url, config=config)
    logger.info("create bucket in minio")
    for i in range(0, 30):
        try:
            s3.create_bucket(Bucket="history-server")
            break
        except Exception as e:
            if i >= 30:
                logger.error(f"create bucket failed....exiting....\n{str(e)}")
                raise
            else:
                logger.warning(f"create bucket failed....retrying in 10 secs.....\n{str(e)}")
                sleep(10)
                continue

    s3.put_object(Bucket="history-server", Key=("spark-events/"))
    logger.debug(s3.list_buckets())


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    logger.info("Setting up minio.....")

    setup_minio_output = (
        subprocess.check_output(
            "./tests/integration/setup/setup_minio.sh | tail -n 1", shell=True, stderr=None
        )
        .decode("utf-8")
        .strip()
    )

    logger.info(f"Minio output:\n{setup_minio_output}")

    s3_params = setup_minio_output.strip().split(",")
    endpoint_url = s3_params[0]
    access_key = s3_params[1]
    secret_key = s3_params[2]

    logger.info(
        f"Setting up s3 bucket with endpoint_url={endpoint_url}, access_key={access_key}, secret_key={secret_key}"
    )

    setup_s3_bucket_for_history_server(endpoint_url, access_key, secret_key)

    logger.info("Bucket setup complete")

    logger.info("Building charm")
    # Build and deploy charm from local source folder

    charm = await ops_test.build_charm(".")
    resources = {
        "spark-history-server-image": METADATA["resources"]["spark-history-server-image"][
            "upstream-source"
        ]
    }

    logger.info("Deploying charm")

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

    s3_integrator_unit = ops_test.model.applications[S3_INTEGRATOR_CHARM_NAME].units[0]

    logger.info("Setting up s3 credentials in s3-integrator charm")

    await fetch_action_sync_s3_credentials(
        s3_integrator_unit, access_key=access_key, secret_key=secret_key
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(apps=[S3_INTEGRATOR_CHARM_NAME], status="active")

    configuration_parameters = {
        "bucket": "history-server",
        "path": "spark-events",
        "endpoint": endpoint_url,
    }
    # apply new configuration options
    await ops_test.model.applications[S3_INTEGRATOR_CHARM_NAME].set_config(
        configuration_parameters
    )

    logger.info("Relating history server charm with s3-integrator charm")

    await ops_test.model.add_relation(S3_INTEGRATOR_CHARM_NAME, APP_NAME)

    await ops_test.model.wait_for_idle(apps=[APP_NAME, S3_INTEGRATOR_CHARM_NAME], timeout=1000)

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


@pytest.mark.abort_on_fail
async def test_ingress(ops_test: OpsTest):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    # Deploy the charm and wait for waiting status
    _ = await ops_test.model.deploy(
        INGRESS_CHARM,
        channel="edge",
        num_units=1,
        series="focal",
    )

    logger.info("Relating history server charm with ingress")

    await ops_test.model.add_relation(INGRESS_CHARM, APP_NAME)

    await ops_test.model.wait_for_idle(apps=[APP_NAME, INGRESS_CHARM], timeout=300)

    action = await ops_test.model.units.get(f"{INGRESS_CHARM}/0").run_action(
        "show-proxied-endpoints",
    )

    ingress_endpoint = (await action.wait()).results[APP_NAME]["url"]

    apps = json.loads(
        urllib.request.urlopen(f"http://{ingress_endpoint}/api/v1/applications").read()
    )

    assert len(apps) == 1
