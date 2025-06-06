#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
import subprocess
import urllib.request
from pathlib import Path
from time import sleep
from urllib.parse import urlencode

import jubilant
import yaml
from tenacity import Retrying, stop_after_attempt, wait_fixed

from .test_helpers import (
    all_prometheus_exporters_data,
    get_cos_address,
    published_grafana_dashboards,
    published_prometheus_alerts,
    published_prometheus_data,
    set_s3_credentials,
    setup_s3_bucket_for_history_server,
)
from .types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"


def test_build_and_deploy(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, history_server_charm: Path
) -> None:
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the output of collected Loki labels and logs.
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

    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    resources = {"spark-history-server-image": image_version}

    logger.info("Deploying charms")
    # Deploy the charm and wait for waiting status
    juju.deploy(**charm_versions.s3.deploy_dict())
    juju.deploy(**charm_versions.loki.deploy_dict())
    juju.deploy(
        history_server_charm, resources=resources, app=APP_NAME, num_units=1, base="ubuntu@22.04"
    )
    juju.wait(jubilant.all_agents_idle, timeout=1000)

    logger.info("Setting up s3 credentials in s3-integrator charm")
    set_s3_credentials(juju, access_key, secret_key)

    configuration_parameters = {
        "bucket": "history-server",
        "path": "spark-events",
        "endpoint": endpoint_url,
    }
    # apply new configuration options
    juju.config(charm_versions.s3.application_name, configuration_parameters)
    juju.wait(jubilant.all_agents_idle)

    logger.info("Relating history server charm with s3-integrator charm")

    juju.integrate(charm_versions.s3.application_name, APP_NAME)
    juju.wait(
        lambda status: jubilant.all_active(status, APP_NAME, charm_versions.s3.application_name),
        delay=5,
    )


def test_loki_integration(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Check that logs are forwarded to Loki.

    Assert on the unit status before any relations/configurations take place.
    """
    # Get minio credentials
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

    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

    image_metadata = json.loads(
        subprocess.check_output(
            f"./tests/integration/setup/get_image_metadata.sh {image_version}",
            shell=True,
            stderr=None,
        ).decode("utf-8")
    )

    spark_version = image_metadata["org.opencontainers.image.version"]

    logger.info(f"Spark version: {spark_version}")

    logger.info("Verifying history server has no app entries")
    status = juju.status()
    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address

    apps = json.loads(urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read())

    assert len(apps) == 0

    logger.info("Integrate spark-history-server with Loki-k8s ")

    juju.integrate(charm_versions.loki.application_name, APP_NAME)
    juju.wait(jubilant.all_agents_idle)

    logger.info("Setup a spark to run job")

    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {endpoint_url} {access_key} {secret_key} {image_version}",
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Executing Spark job")

    run_spark_output = subprocess.check_output(
        f"./tests/integration/setup/run_spark_job.sh {spark_version}", shell=True, stderr=None
    ).decode("utf-8")

    logger.info(f"Run spark output:\n{run_spark_output}")

    logger.info("Verifying history server has 1 app entry")

    for _ in range(0, 5):
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
    status = juju.status()
    loki_address = status.apps[loki_app_name].units[f"{loki_app_name}/0"].address

    logger.info("Verifying loki labels")
    try:
        labels = json.loads(
            urllib.request.urlopen(f"http://{loki_address}:3100/loki/api/v1/labels").read()
        )
    except Exception:
        labels = {}

    logger.info(f"Labels: {labels}")
    assert "success" == labels["status"]
    assert "juju_unit" in labels["data"]

    try:
        units = json.loads(
            urllib.request.urlopen(
                f"http://{loki_address}:3100/loki/api/v1/label/juju_unit/values"
            ).read()
        )
    except Exception:
        units = {}

    # check label juju_unit contains spark-history-server-k8s application
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
