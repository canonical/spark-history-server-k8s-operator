#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml

from .helpers import (
    assert_grafana_dashboards_published,
    assert_jobs_in_history_server,
    assert_logs_published_in_loki,
    assert_prometheus_alerts_published,
    assert_prometheus_data_exported,
    assert_prometheus_data_published,
    deploy_history_server_setup,
    deploy_o11y_setup,
    get_unit_address,
    run_spark_job,
    setup_spark_job,
)
from .types import IntegrationTestsCharms, S3Info, TelemetryAgent

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def test_build_and_deploy(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    history_server_charm: Path,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the output of collected Loki labels and logs.
    """
    deploy_history_server_setup(
        juju=juju,
        charm_versions=charm_versions,
        history_server_charm=history_server_charm,
        s3_bucket_and_creds=s3_bucket_and_creds,
    )
    juju.wait(
        lambda status: jubilant.all_active(status),
        delay=5,
    )


def test_loki_integration(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Check that logs are forwarded to Loki.

    Assert on the unit status before any relations/configurations take place.
    """
    logger.info("Verifying history server has no app entries")
    address = get_unit_address(juju, APP_NAME)
    history_server_url = f"http://{address}:18080"
    assert_jobs_in_history_server(server_url=history_server_url, expected_count=0)

    deploy_o11y_setup(
        juju=juju, charm_versions=charm_versions, telemetry_agent=TelemetryAgent.GRAFANA_AGENT
    )
    juju.wait(jubilant.all_active, delay=10)

    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    run_spark_job()

    logger.info("Verifying history server has 1 app entry")
    assert_jobs_in_history_server(server_url=history_server_url, expected_count=1)

    assert_logs_published_in_loki(
        juju=juju,
        app_name=charm_versions.loki.application_name,
        filter_by_label={"juju_unit": f"{APP_NAME}/0"},
        search_phrase="INFO HistoryServer",
    )


def test_history_server_cos_integration(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Check that the integration with cos work correctly.

    Assert on absences of labels/dashboards/alert rules.
    """
    assert_prometheus_data_exported(juju, check_field="jmx_scrape_duration_seconds")
    assert_prometheus_data_published(juju, check_field="jmx_scrape_duration_seconds")
    assert_prometheus_alerts_published(juju)
    assert_grafana_dashboards_published(juju)
    logger.info("End of the tests")
