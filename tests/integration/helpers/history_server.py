#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
from pathlib import Path

import jubilant
import requests
import yaml
from tenacity import Retrying, stop_after_attempt, wait_fixed

from ..types import AzureInfo, IngressMode, IntegrationTestsCharms, S3Info
from .azure_storage import prepare_azure_storage_setup
from .istio import deploy_istio_control_plane
from .juju import get_application_data
from .s3 import prepare_s3_storage_setup

logger = logging.getLogger(__name__)
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]


def get_history_server_image_version():
    """Get the image version of the Spark History Server from the metadata."""
    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]
    return image_version


def deploy_history_server_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    history_server_charm: Path,
    s3_bucket_and_creds: S3Info | None = None,
    azure_storage_credentials: AzureInfo | None = None,
    ingress_mode: IngressMode = IngressMode.NONE,
    trust: bool = False,
    s3_tls: bool = False,
    kubernetes_provider: str = "microk8s",
) -> None:
    """Deploy the Spark History Server along with optional storage and ingress setups."""
    image_version = get_history_server_image_version()
    resources = {"spark-history-server-image": image_version}
    logger.info("Deploying Spark History Server charm")
    juju.deploy(
        history_server_charm,
        resources=resources,
        app=APP_NAME,
        num_units=1,
        base="ubuntu@22.04",
        trust=trust,
    )

    if s3_bucket_and_creds is not None:
        logger.info("Using S3 object storage with Spark History Server")
        prepare_s3_storage_setup(juju, charm_versions, s3_bucket_and_creds, s3_tls=s3_tls)
    elif azure_storage_credentials is not None:
        logger.info("Using Azure object storage with Spark History Server")
        prepare_azure_storage_setup(juju, charm_versions, azure_storage_credentials)

    if ingress_mode is IngressMode.NONE:
        logger.info("Ingress is not enabled for Spark History Server")
        return

    ingress_app_name = None
    ingress_deploy_args = {}
    if ingress_mode is IngressMode.TRAEFIK:
        logger.info("Ingress is enabled for Spark History Server using Traefik")
        ingress_app_name = charm_versions.ingress.application_name
        ingress_deploy_args = charm_versions.ingress.deploy_dict()
    elif ingress_mode is IngressMode.ISTIO_INGRESS:
        logger.info("Ingress is enabled for Spark History Server using Istio")
        deploy_istio_control_plane(juju, charm_versions, kubernetes_provider)
        ingress_app_name = charm_versions.istio_ingress.application_name
        ingress_deploy_args = charm_versions.istio_ingress.deploy_dict()

    logger.info(f"Deploying ingress: {ingress_app_name}")
    juju.deploy(**ingress_deploy_args)

    logger.info(f"Integrating history server with ingress: {ingress_app_name}")
    juju.integrate(f"{APP_NAME}:ingress", f"{ingress_app_name}:ingress")
    juju.wait(jubilant.all_active, delay=5)
    logger.info("History Server setup with Ingress completed.")


def get_ingress_url(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, ingress_mode: IngressMode
) -> str:
    """Retrieve the ingress URL for the History Server based on the ingress mode."""
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        app_data = get_application_data(juju, APP_NAME, "ingress")
        ingress_data = next(iter(app_data.values()), None)
        if not ingress_data:
            raise ValueError("No ingress data found for the application.")
        return json.loads(ingress_data["ingress"])["url"]
    elif ingress_mode == IngressMode.TRAEFIK:
        traefik_unit = f"{charm_versions.ingress.application_name}/0"
        action = juju.run(traefik_unit, "show-proxied-endpoints")
        assert action.return_code == 0, "Failed to get proxied endpoints from Traefik"
        ingress_data = action.results
        ingress_url = json.loads(ingress_data["proxied-endpoints"]).get(APP_NAME, {}).get("url")
        logger.info(f"Ingress URL for {APP_NAME}: {ingress_url}")
        if not ingress_url:
            raise ValueError("No ingress URL found for the application.")
        return ingress_url
    raise ValueError(f"Unsupported ingress mode: {ingress_mode}")


def assert_jobs_in_history_server(
    server_url: str,
    expected_count: int = 1,
    session_cookie: str | None = None,
    verify_tls: bool = True,
) -> None:
    """Assert that the History Server has the expected number of jobs."""
    applications_url = f"{server_url.rstrip('/')}/api/v1/applications"
    cookies = {"_oauth2_proxy": session_cookie} if session_cookie is not None else None

    for attempt in Retrying(
        stop=stop_after_attempt(5),
        wait=wait_fixed(3),
        reraise=True,
    ):
        with attempt:
            response = requests.get(
                applications_url,
                cookies=cookies,
                verify=verify_tls,
                timeout=30,
            )
            assert response.status_code == 200, (
                f"History Server returned HTTP {response.status_code}: {response.text[:500]}"
            )
            apps = response.json()
            assert len(apps) >= expected_count, (
                f"Expected at least {expected_count} applications, got {len(apps)}"
            )
