#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import logging
from pathlib import Path

import jubilant
import yaml
from playwright.sync_api import BrowserContext, Page

from .helpers import (
    assert_jobs_in_history_server,
    complete_authentication_flow,
    deploy_history_server_setup,
    deploy_identity_setup,
    get_ingress_url,
    run_spark_job,
    setup_spark_job,
)
from .oauth_tools.external_idp import ExternalIdpService
from .types import IngressMode, IntegrationTestsCharms, S3Info

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
        ingress_mode=IngressMode.TRAEFIK,
    )
    status = juju.wait(jubilant.all_active)

    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    server_url = f"http://{address}:18080"
    logger.info("Verifying history server has no app entries")
    assert_jobs_in_history_server(server_url=server_url, expected_count=0)

    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    run_spark_job()

    logger.info("Verifying history server has 1 app entry")
    assert_jobs_in_history_server(server_url=server_url, expected_count=1)


def test_login_flow(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
) -> None:
    """Deploy the iam bundle."""
    deploy_identity_setup(
        juju=juju,
        charm_versions=charm_versions,
        external_idp_service=external_idp_service,
        ingress_mode=IngressMode.TRAEFIK,
    )
    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.TRAEFIK)
    session_cookie = complete_authentication_flow(
        juju=juju,
        charm_versions=charm_versions,
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
