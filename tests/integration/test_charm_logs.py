#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml
from tenacity import Retrying, stop_after_attempt, wait_fixed

from .helpers import (
    assert_jobs_in_history_server,
    deploy_history_server_setup,
    get_logs_in_loki,
    get_unit_address,
    run_spark_job,
    setup_spark_job,
)
from .test_helpers import (
    all_prometheus_exporters_data,
    get_cos_address,
    published_grafana_dashboards,
    published_prometheus_alerts,
    published_prometheus_data,
)
from .types import IntegrationTestsCharms, S3Info

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

    logger.info("Integrate spark-history-server with Loki-k8s ")

    juju.integrate(charm_versions.loki.application_name, APP_NAME)
    juju.wait(jubilant.all_agents_idle)

    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    run_spark_job()

    logger.info("Verifying history server has 1 app entry")
    assert_jobs_in_history_server(server_url=history_server_url, expected_count=1)

    logs = get_logs_in_loki(
        juju=juju, app_name=APP_NAME, filter_by_label={"juju_unit": f"{APP_NAME}/0"}
    )
    # check for non empty logs
    assert len(logs) > 0
    # check if startup messages are there
    c = 0
    for log_line in logs:
        if "INFO HistoryServer" in log_line[1]:
            c = c + 1
    logger.info(f"Number of line found: {c}")
    assert c > 0


def test_history_server_cos_integration(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms
) -> None:
    """Check that the integration with cos work correctly.

    Assert on absences of labels/dashboards/alert rules.
    """
    # Prometheus data is being published by the app
    assert all_prometheus_exporters_data(juju, check_field="jmx_scrape_duration_seconds")

    # Deploying and relating to grafana-agent
    logger.info("Deploying grafana-agent-k8s charm...")
    juju.deploy(**charm_versions.grafana_agent.deploy_dict())

    logger.info("Waiting for test charm to be idle...")
    juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.grafana_agent.application_name)
    )

    juju.integrate(charm_versions.grafana_agent.name, f"{APP_NAME}:metrics-endpoint")
    juju.integrate(charm_versions.grafana_agent.name, f"{APP_NAME}:grafana-dashboard")
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)
    juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.grafana_agent.application_name),
        delay=10,
    )

    juju.cli("deploy", "cos-lite", "--trust")

    juju.wait(
        lambda status: jubilant.all_active(
            status, "prometheus", "alertmanager", "loki", "grafana"
        ),
        delay=10,
    )
    juju.wait(
        lambda status: jubilant.all_blocked(status, charm_versions.grafana_agent.application_name),
        delay=10,
    )

    juju.integrate(f"{charm_versions.grafana_agent.name}:grafana-dashboards-provider", "grafana")
    juju.integrate(f"{charm_versions.grafana_agent.name}:send-remote-write", "prometheus")

    juju.wait(jubilant.all_active, delay=10)

    # We should leave time for Prometheus data to be published
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(30)):
        with attempt:
            # Data got published to Prometheus
            cos_address = get_cos_address(juju)
            assert published_prometheus_data(juju, cos_address, "jmx_scrape_duration_seconds")

            # Alerts got published to Prometheus
            alerts_data = published_prometheus_alerts(juju, cos_address)
            assert alerts_data is not None
            logger.info(f"Alerts data: {alerts_data}")

            logger.info("Rules: ")
            for group in alerts_data["data"]["groups"]:
                for rule in group["rules"]:
                    logger.info(f"Rule: {rule['name']}")
            logger.info("End of rules.")

            for alert in [
                "Spark History Server Missing",
                "Spark History Server Threads Dead Locked",
            ]:
                assert any(
                    rule["name"] == alert
                    for group in alerts_data["data"]["groups"]
                    for rule in group["rules"]
                )

            # Grafana dashboard got published
            dashboards_info = published_grafana_dashboards(juju)
            logger.info(f"Dashboard info {dashboards_info}")
            assert dashboards_info is not None
            assert any(
                board["title"] == "Spark History Server JMX Dashboard" for board in dashboards_info
            )

    logger.info("End of the tests")
