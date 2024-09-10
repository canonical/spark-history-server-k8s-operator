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
from urllib.parse import urlencode

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from .test_helpers import fetch_action_sync_s3_credentials, setup_s3_bucket_for_history_server

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
        ops_test.model.deploy(**charm_versions.loki.deploy_dict()),
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

    configuration_parameters = {
        "bucket": "history-server",
        "path": "spark-events",
        "endpoint": endpoint_url,
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

    # loki

    await ops_test.model.integrate(charm_versions.loki.application_name, APP_NAME)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.loki.application_name], timeout=1000
    )

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

    logger.info("Verifying Loki has received the logs.")
    loki_app_name = charm_versions.loki.application_name
    status = await ops_test.model.get_status()
    loki_address = status["applications"][loki_app_name]["units"][f"{loki_app_name}/0"]["address"]

    # check labels existence
    try:
        labels = json.loads(
            urllib.request.urlopen(f"http://{loki_address}:3100/loki/api/v1/labels").read()
        )
    except Exception:
        labels = {}

    logger.info(f"Labels: {labels}")
    assert "success" == labels["status"]
    assert "juju_unit" in labels["data"]

    # check label juju_unit contains spark-history-server-k8s application
    try:
        units = json.loads(
            urllib.request.urlopen(
                f"http://{loki_address}:3100/loki/api/v1/label/juju_unit/values"
            ).read()
        )
    except Exception:
        units = {}

    logger.info(f"units: {units}")
    assert "success" == units["status"]
    assert f"{APP_NAME}/0" in units["data"][0]

    # check for history server logs in loki
    url = f"http://{loki_address}:3100/loki/api/v1/query_range"
    keys = {"query": f'{{juju_unit="{APP_NAME}/0"}}'}
    data = urlencode(keys).encode()

    try:
        query = json.loads(urllib.request.urlopen(url, data).read().decode())
        logger.info(query)
    except Exception:
        query = {}

    assert "success" == query["status"]
    assert "stream" in query["data"]["result"][0]
    assert f"{APP_NAME}" == query["data"]["result"][0]["stream"]["charm"]
    assert f"{APP_NAME}/0" == query["data"]["result"][0]["stream"]["juju_unit"]

    logs = query["data"]["result"][0]["values"]
    logger.info(f"Retrieved logs: {logs}")
    # check for non empty logs
    assert len(logs) > 0
    # check if startup messages are there
    c = 0
    for log_line in logs:
        if "INFO HistoryServer" in log_line[1]:
            c = c + 1
    logger.info(f"Number of line found: {c}")
    assert c > 0
