#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

from .helpers import (
    assert_jobs_in_history_server,
    deploy_history_server_setup,
    get_unit_address,
    run_spark_job,
    setup_spark_job,
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
    deploy_history_server_setup(
        juju=juju,
        charm_versions=charm_versions,
        history_server_charm=history_server_charm,
        s3_bucket_and_creds=s3_bucket_and_creds,
        s3_tls=True,
    )

    address = get_unit_address(juju, APP_NAME)
    server_url = f"http://{address}:18080"
    assert_jobs_in_history_server(server_url=server_url, expected_count=0)

    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    run_spark_job()
    assert_jobs_in_history_server(server_url=server_url, expected_count=1)
