#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

from .helpers.history_server import assert_jobs_in_history_server, deploy_history_server_setup
from .helpers.juju import get_unit_address
from .helpers.spark import run_spark_job, setup_spark_job
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
    """Deploy history-server charm along with S3 integrator relation, with TLS enabled."""
    deploy_history_server_setup(
        juju=juju,
        charm_versions=charm_versions,
        history_server_charm=history_server_charm,
        s3_bucket_and_creds=s3_bucket_and_creds,
        s3_tls=True,
    )
    juju.wait(jubilant.all_active)


def test_spark_job_logs_in_history_server(
    juju: jubilant.Juju,
    s3_bucket_and_creds: S3Info,
):
    """Run a Spark job and verify that Spark job logs appear in the history server."""
    address = get_unit_address(juju, APP_NAME)
    server_url = f"http://{address}:18080"
    assert_jobs_in_history_server(server_url=server_url, expected_count=0)

    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    run_spark_job(tls_ca=s3_bucket_and_creds["ca_bundle_path"])
    assert_jobs_in_history_server(server_url=server_url, expected_count=1)
