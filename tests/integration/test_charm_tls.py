#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import base64
import json
import logging
import subprocess
import urllib.request
from pathlib import Path
from time import sleep

import jubilant
import yaml

from .test_helpers import (
    get_certificate_from_file,
    set_s3_credentials,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"


def test_build_and_deploy(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    history_server_charm: Path,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    bucket = s3_bucket_and_creds["bucket"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    endpoint = s3_bucket_and_creds["endpoint"]
    path = s3_bucket_and_creds["path"]
    tls_ca_chain_path = s3_bucket_and_creds["ca_bundle_path"]

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
    juju.deploy(**charm_versions.s3.deploy_dict())
    juju.deploy(
        history_server_charm, resources=resources, app=APP_NAME, num_units=1, base="ubuntu@22.04"
    )
    juju.wait(jubilant.all_agents_idle, timeout=1000)

    logger.info("Setting up s3 credentials in s3-integrator charm")
    set_s3_credentials(juju, access_key, secret_key)

    ca = get_certificate_from_file(tls_ca_chain_path)
    ca_b64 = base64.b64encode(ca.encode("utf-8")).decode("utf-8")
    configuration_parameters = {
        "bucket": bucket,
        "path": path,
        "endpoint": endpoint,
        "tls-ca-chain": ca_b64,
    }
    # apply new configuration options
    juju.config(charm_versions.s3.application_name, configuration_parameters)

    logger.info("Relating history server charm with s3-integrator charm")

    juju.integrate(charm_versions.s3.application_name, APP_NAME)
    status = juju.wait(jubilant.all_active, delay=5)

    logger.info("Verifying history server has no app entries")

    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    apps = json.loads(urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read())

    assert len(apps) == 0

    logger.info("Setting up spark")

    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {endpoint} {access_key} {secret_key} {image_version}",
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Executing Spark job")

    run_spark_output = subprocess.check_output(
        f"./tests/integration/setup/run_spark_job_tls.sh  {spark_version} {tls_ca_chain_path}",
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
            logger.debug(f"apps: {apps}")
        except Exception:
            apps = []

        if len(apps) > 0:
            break
        else:
            sleep(30)

    assert len(apps) == 1
