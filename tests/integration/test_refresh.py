# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

from pathlib import Path

import jubilant
import yaml

from .helpers.history_server import assert_jobs_in_history_server
from .helpers.s3 import prepare_s3_storage_setup
from .helpers.spark import (
    run_spark_job,
    setup_spark_job,
)
from .types import IntegrationTestsCharms, S3Info

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_deploy(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Deploy history-server stable risk charm along with S3 integrator relation."""
    juju.deploy(
        APP_NAME,
        app=APP_NAME,
        channel="4/stable",
        # FIXME: Once 26.04 is released to stable, we should replace those 22.04 values
        base="ubuntu@22.04",
        num_units=1,
        trust=True,
    )
    prepare_s3_storage_setup(juju, charm_versions, s3_bucket_and_creds, s3_tls=False)
    base = juju.status().apps[APP_NAME].base
    assert base is not None
    assert base.name == "ubuntu"
    assert base.channel == "22.04"


def test_spark_job_logs_in_history_server(
    juju: jubilant.Juju,
    s3_bucket_and_creds: S3Info,
):
    """Run a Spark job and verify that Spark job logs appear in the history server."""
    status = juju.status()
    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    server_url = f"http://{address}:18080"
    assert_jobs_in_history_server(server_url=server_url, expected_count=0)

    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    run_spark_job()
    assert_jobs_in_history_server(server_url=server_url, expected_count=1)


def test_refresh_from_previous_stable(
    juju: jubilant.Juju,
    history_server_charm: Path,
) -> None:
    """Test refreshing from stable risk to the current charm."""
    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]
    juju.refresh(
        APP_NAME,
        path=history_server_charm,
        base="ubuntu@26.04",
        resources={"integration-hub-image": image_version},
        trust=True,
    )
    status = juju.wait(jubilant.all_active, delay=10)
    base = status.apps[APP_NAME].base
    assert base is not None
    assert base.name == "ubuntu"
    assert base.channel == "26.04"

    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    server_url = f"http://{address}:18080"
    assert_jobs_in_history_server(server_url=server_url, expected_count=1)
