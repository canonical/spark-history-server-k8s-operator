#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path
from typing import cast

import jubilant
import yaml
from playwright.sync_api import BrowserContext, Page

from constants import HISTORY_SERVER_PORT

from .helpers.auth import ExternalIdpService, complete_authentication_flow, deploy_identity_setup
from .helpers.cos import (
    assert_grafana_dashboards_published,
    assert_logs_published_in_loki,
    assert_prometheus_alerts_published,
    assert_prometheus_data_exported,
    assert_prometheus_data_published,
    deploy_observability_setup,
)
from .helpers.history_server import (
    assert_jobs_in_history_server,
    deploy_history_server_setup,
    get_ingress_url,
)
from .helpers.juju import get_pod_names, get_unit_address
from .helpers.k8s import curl_using_pod, pod_has_labels
from .helpers.spark import run_spark_job, setup_spark_job
from .types import IngressMode, IntegrationTestsCharms, S3Info, TelemetryAgent

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
AMBIENT_MESH_POD_LABEL_KEY = "istio.io/dataplane-mode"
AMBIENT_MESH_POD_LABEL_VALUE = "ambient"


def test_deploy_history_server_setup_with_istio_ingress(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    history_server_charm: Path,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Test deploying the History Server setup with Istio ingress."""
    deploy_history_server_setup(
        juju=juju,
        charm_versions=charm_versions,
        history_server_charm=history_server_charm,
        s3_bucket_and_creds=s3_bucket_and_creds,
        ingress_mode=IngressMode.ISTIO_INGRESS,
        trust=True,
    )
    juju.wait(lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status))


def test_run_spark_job_before_meshing(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
) -> None:
    """Test running a Spark job before enabling the ambient mesh."""
    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.ISTIO_INGRESS)
    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    assert_jobs_in_history_server(server_url=ingress_url, expected_count=0, verify_tls=False)
    run_spark_job()
    assert_jobs_in_history_server(server_url=ingress_url, expected_count=1, verify_tls=False)


def test_access_from_unmeshed_pod_before_meshing(
    juju: jubilant.Juju,
) -> None:
    """Test the access to the History Server from an unmeshed pod before enabling the ambient mesh."""
    pod_ip = get_unit_address(juju, APP_NAME)
    pod_url = f"http://{pod_ip}:{HISTORY_SERVER_PORT}"
    curl_process = curl_using_pod(namespace=juju.model or "default", url=pod_url)
    assert curl_process.returncode == 0
    assert curl_process.stdout.endswith("200")


def test_enable_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test enabling the ambient mesh."""
    logger.info("Deploying istio beacon charm")
    juju.deploy(**charm_versions.istio_beacon.deploy_dict())
    juju.wait(lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status))

    logger.info("Integrating history server charm with istio beacon charm")
    juju.integrate(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=5
    )
    for pod_name in get_pod_names(cast(str, juju.model), APP_NAME):
        assert pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={AMBIENT_MESH_POD_LABEL_KEY: AMBIENT_MESH_POD_LABEL_VALUE},
        )


def test_blocked_access_from_unmeshed_pod_after_meshing(
    juju: jubilant.Juju,
) -> None:
    """Test access to the History Server from an unmeshed pod is blocked after enabling the ambient mesh."""
    pod_ip = get_unit_address(juju, APP_NAME)
    pod_url = f"http://{pod_ip}:{HISTORY_SERVER_PORT}"
    curl_process = curl_using_pod(namespace=juju.model or "default", url=pod_url)
    assert curl_process.returncode != 0


def test_blocked_access_from_meshed_pod_but_no_policy_after_meshing(
    juju: jubilant.Juju,
) -> None:
    """Test access to the History Server from a meshed pod without an appropriate policy is blocked."""
    pod_ip = get_unit_address(juju, APP_NAME)
    pod_url = f"http://{pod_ip}:{HISTORY_SERVER_PORT}"
    curl_process = curl_using_pod(
        namespace=juju.model or "default",
        url=pod_url,
        labels={"istio.io/dataplane-mode": "ambient"},
    )
    assert curl_process.returncode != 0


def test_access_via_ingress_after_meshing(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test access to the History Server via the ingress is successful after enabling the ambient mesh."""
    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.ISTIO_INGRESS)
    assert_jobs_in_history_server(server_url=ingress_url, expected_count=1, verify_tls=False)


def test_auth_login_with_istio_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
):
    """Test history server authentication with Istio mesh enabled."""
    deploy_identity_setup(
        juju=juju,
        charm_versions=charm_versions,
        external_idp_service=external_idp_service,
        ingress_mode=IngressMode.ISTIO_INGRESS,
    )
    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.ISTIO_INGRESS)
    session_cookie = complete_authentication_flow(
        external_idp_service=external_idp_service,
        page=page,
        context=context,
        history_server_url=ingress_url,
    )
    assert session_cookie is not None
    assert_jobs_in_history_server(
        server_url=ingress_url,
        expected_count=1,
        session_cookie=session_cookie,
        verify_tls=False,
    )


def test_observability_with_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
) -> None:
    """Test observability features with the ambient mesh enabled."""
    deploy_observability_setup(
        juju=juju, charm_versions=charm_versions, telemetry_agent=TelemetryAgent.OTEL_COLLECTOR
    )

    logger.info("Putting opentelemetry-collector-k8s into ambient mesh...")
    juju.integrate(
        f"{charm_versions.otel_collector.application_name}:service-mesh",
        f"{charm_versions.istio_beacon.application_name}:service-mesh",
    )
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=30
    )

    run_spark_job()

    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.ISTIO_INGRESS)
    session_cookie = complete_authentication_flow(
        external_idp_service=external_idp_service,
        page=page,
        context=context,
        history_server_url=ingress_url,
    )
    assert session_cookie is not None
    assert_jobs_in_history_server(
        server_url=ingress_url,
        expected_count=2,
        session_cookie=session_cookie,
        verify_tls=False,
    )

    assert_logs_published_in_loki(
        juju=juju,
        app_name=charm_versions.loki.application_name,
        filter_by_label={"juju_unit": f"{APP_NAME}/0"},
        search_phrase="INFO HistoryServer",
    )
    assert_prometheus_data_exported(juju, check_field="jmx_scrape_duration_seconds")
    assert_prometheus_data_published(juju, check_field="jmx_scrape_duration_seconds")
    assert_prometheus_alerts_published(juju)
    assert_grafana_dashboards_published(juju)


def test_disable_ambient_mesh(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Test disabling the ambient mesh for the history server charm."""
    logger.info("Disabling ambient mesh for history server charm")
    juju.remove_relation(
        f"{APP_NAME}:service-mesh", f"{charm_versions.istio_beacon.application_name}:service-mesh"
    )
    juju.wait(
        lambda status: jubilant.all_agents_idle(status) and jubilant.all_active(status), delay=5
    )
    for pod_name in get_pod_names(cast(str, juju.model), APP_NAME):
        assert not pod_has_labels(
            namespace=cast(str, juju.model),
            pod_name=pod_name,
            labels={AMBIENT_MESH_POD_LABEL_KEY: AMBIENT_MESH_POD_LABEL_VALUE},
        )


def test_access_from_unmeshed_pod_after_unmeshing(
    juju: jubilant.Juju,
) -> None:
    """Test accessing the History Server from an unmeshed pod after the ambient mesh has been disabled."""
    pod_ip = get_unit_address(juju, APP_NAME)
    pod_url = f"http://{pod_ip}:{HISTORY_SERVER_PORT}"
    curl_process = curl_using_pod(namespace=juju.model or "default", url=pod_url)
    assert curl_process.returncode == 0
    assert curl_process.stdout.endswith("500")  # pod is reachable, but auth is failing


def test_access_via_ingress_after_unmeshing(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
) -> None:
    """Test accessing the History Server via ingress after the ambient mesh has been disabled."""
    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.ISTIO_INGRESS)
    session_cookie = complete_authentication_flow(
        external_idp_service=external_idp_service,
        page=page,
        context=context,
        history_server_url=ingress_url,
    )
    assert session_cookie is not None
    assert_jobs_in_history_server(
        server_url=ingress_url, expected_count=1, session_cookie=session_cookie, verify_tls=False
    )
