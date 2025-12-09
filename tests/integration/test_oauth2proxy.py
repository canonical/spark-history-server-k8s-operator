#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
import os
import subprocess
import urllib.request
from pathlib import Path
from time import sleep
from typing import Optional

import jubilant
import requests
import yaml
from playwright.async_api._generated import BrowserContext, Page

from .oauth_tools.external_idp import ExternalIdpService
from .test_helpers import (
    set_s3_credentials,
)
from .types import IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"


async def verify_page_loads(page: Page, url: str):
    """Verify that the correct url has been loaded.

    Args:
        page (page): The page fixture.
        url (str): The url to go to.
    """
    await page.wait_for_url(url)


async def click_on_sign_in_button_by_text(page: Page, text: str):
    """Find and click on a button by its displayed text.

    Args:
        page (page): The page fixture.
        text (str): The button's text to search for.
    """
    async with page.expect_navigation():
        await page.get_by_text(text).click()


async def get_cookie_from_browser_by_name(
    browser_context: BrowserContext, name: str
) -> Optional[str]:
    """Retrieve a cookie by name.

    Args:
        browser_context (BrowserContext): The browser_context fixture.
        name (str): The cookie name.
    """
    cookies = await browser_context.cookies()
    for cookie in cookies:
        if cookie["name"] == name:
            return cookie["value"]
    return None


async def complete_auth_code_login(
    page: Page,
    external_idp_service: Optional[ExternalIdpService],
) -> None:
    """Take a page that is in the identity-platform's login page and login the user.

    Args:
        page (page): The page fixture.
        identity_platform_login_ui_operator_url (str): The identity platform login UI operator URL.
        external_idp_service (ExternalIdpService): The external IdP service.
    """
    async with page.expect_navigation():
        await external_idp_service.complete_user_login(page)
    logger.info(f"Login flow completed: {page.url}")


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

    # Deploy charm from local source folder

    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    shell_output = subprocess.check_output(
        f"./tests/integration/setup/get_image_metadata.sh {image_version}", shell=True
    ).decode("utf-8")

    logger.info(shell_output)

    image_metadata = json.loads(shell_output)

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

    juju.wait(lambda status: jubilant.all_active(status, charm_versions.s3.application_name))

    configuration_parameters = {
        "bucket": bucket,
        "path": path,
        "endpoint": endpoint,
    }
    # apply new configuration options
    juju.config(charm_versions.s3.application_name, configuration_parameters)
    juju.wait(jubilant.all_agents_idle)

    logger.info("Relating history server charm with s3-integrator charm")

    juju.integrate(APP_NAME, charm_versions.s3.application_name)

    status = juju.wait(jubilant.all_active)

    logger.info("Verifying history server has no app entries")

    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    apps = None

    for _ in range(0, 5):
        try:
            apps = json.loads(
                urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read()
            )
        except Exception:
            sleep(3)

    assert apps is not None and len(apps) == 0

    logger.info("Setting up spark")

    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark.sh {endpoint} {access_key} {secret_key} {image_version}",
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


def test_deploy_iam_bundle(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
) -> None:
    """Deploy the iam bundle."""
    # Deploy all charms necessary for Oauth2proxy integration
    juju.deploy(**charm_versions.ingress.deploy_dict())
    juju.deploy(**charm_versions.postgresql.deploy_dict())
    juju.deploy(**charm_versions.self_signed_certificate.deploy_dict())
    juju.deploy(**charm_versions.hydra.deploy_dict())
    juju.deploy(**charm_versions.kratos.deploy_dict())
    juju.deploy(**charm_versions.identity_platform_login_ui_operator.deploy_dict())
    juju.deploy(**charm_versions.kratos_external_idp_integrator.deploy_dict())

    juju.deploy(**charm_versions.oauth2proxy.deploy_dict())

    juju.integrate(
        charm_versions.self_signed_certificate.application_name,
        f"{charm_versions.ingress.application_name}:certificates",
    )

    # hydra integrations
    juju.integrate(
        charm_versions.hydra.application_name, charm_versions.postgresql.application_name
    )
    juju.integrate(
        f"{charm_versions.hydra.application_name}:public-route",
        charm_versions.ingress.application_name,
    )

    # kratos integrations
    juju.integrate(
        charm_versions.kratos.application_name, charm_versions.postgresql.application_name
    )
    juju.integrate(
        f"{charm_versions.kratos.application_name}:public-route",
        charm_versions.ingress.application_name,
    )
    juju.integrate(
        charm_versions.kratos.application_name,
        f"{charm_versions.hydra.application_name}:hydra-endpoint-info",
    )

    # login ui integrations
    juju.integrate(
        charm_versions.hydra.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:ui-endpoint-info",
    )
    juju.integrate(
        charm_versions.hydra.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:hydra-endpoint-info",
    )

    juju.integrate(
        charm_versions.kratos.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:ui-endpoint-info",
    )
    juju.integrate(
        charm_versions.kratos.application_name,
        f"{charm_versions.identity_platform_login_ui_operator.application_name}:kratos-info",
    )

    juju.integrate(
        charm_versions.identity_platform_login_ui_operator.application_name,
        charm_versions.ingress.application_name,
    )

    juju.integrate(
        charm_versions.kratos.application_name,
        charm_versions.kratos_external_idp_integrator.application_name,
    )

    # wait for all charms to be active/blocking
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.postgresql.application_name,
            charm_versions.self_signed_certificate.application_name,
            charm_versions.hydra.application_name,
            charm_versions.kratos.application_name,
            charm_versions.identity_platform_login_ui_operator.application_name,
        ),
        delay=10,
        timeout=2000,
    )

    juju.wait(
        lambda status: jubilant.all_blocked(
            status,
            charm_versions.kratos_external_idp_integrator.application_name,
        ),
        delay=10,
        timeout=1000,
    )
    # configure external idp integrator with external idp service (dex)
    juju.config(
        charm_versions.kratos_external_idp_integrator.application_name,
        {
            "client_id": external_idp_service.client_id,
            "client_secret": external_idp_service.client_secret,
            "provider": "generic",
            "issuer_url": external_idp_service.issuer_url,
            "scope": "profile email",
            "provider_id": "Dex",
        },
    )

    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.postgresql.application_name,
            charm_versions.self_signed_certificate.application_name,
            charm_versions.hydra.application_name,
            charm_versions.kratos.application_name,
            charm_versions.identity_platform_login_ui_operator.application_name,
            charm_versions.kratos_external_idp_integrator.application_name,
        ),
        delay=10,
        timeout=600,
    )

    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:ingress",
        charm_versions.ingress.application_name,
    )
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:oauth",
        charm_versions.hydra.application_name,
    )
    juju.config(
        charm_versions.ingress.application_name, {"enable_experimental_forward_auth": "True"}
    )
    juju.integrate(
        f"{charm_versions.ingress.application_name}:experimental-forward-auth",
        f"{charm_versions.oauth2proxy.application_name}:forward-auth",
    )
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:receive-ca-cert",
        charm_versions.self_signed_certificate.application_name,
    )
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.oauth2proxy.application_name,
            charm_versions.ingress.application_name,
        ),
        delay=10,
        timeout=200,
    )

    juju.integrate(charm_versions.oauth2proxy.application_name, f"{APP_NAME}:oauth2-proxy")
    juju.integrate(f"{APP_NAME}:ingress", charm_versions.ingress.application_name)

    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.oauth2proxy.application_name,
            charm_versions.ingress.application_name,
            charm_versions.postgresql.application_name,
            charm_versions.self_signed_certificate.application_name,
            charm_versions.hydra.application_name,
            charm_versions.kratos.application_name,
            charm_versions.identity_platform_login_ui_operator.application_name,
            charm_versions.kratos_external_idp_integrator.application_name,
        ),
        delay=10,
        timeout=600,
    )

    task = juju.run(
        f"{charm_versions.kratos_external_idp_integrator.application_name}/0", "get-redirect-uri"
    )
    assert task.return_code == 0

    logger.info("Configuring the external provider")
    external_idp_service.update_redirect_uri(redirect_uri=task.results["redirect-uri"])

    logger.info("IAM bundle deployed successfully.")


async def test_login(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
) -> None:
    """Test deploying the identity platform with external IdP and logging into the application."""
    # get proxied endpoint
    task = juju.run(f"{charm_versions.ingress.application_name}/0", "show-proxied-endpoints")
    assert task.return_code == 0
    history_server_proxy_endpoint = json.loads(task.results["proxied-endpoints"])[APP_NAME]["url"]

    logger.info(f"History server proxy endpoint: {history_server_proxy_endpoint}")

    await page.goto(history_server_proxy_endpoint)
    logger.info(f"Navigated to {history_server_proxy_endpoint}")

    logger.info("Clicking on Sign in with Generic identity provider.")
    await click_on_sign_in_button_by_text(page=page, text="Sign in with Generic")

    # complete login in the external identity provider
    await complete_auth_code_login(page=page, external_idp_service=external_idp_service)

    # verify the correct redirect after login
    await verify_page_loads(page=page, url=history_server_proxy_endpoint)

    # Verifying that the login flow was successful is application specific.
    # The test uses Spark history server's /api/user endpoint to verify the session cookie is valid
    history_server_session_cookie = await get_cookie_from_browser_by_name(
        browser_context=context, name="_oauth2_proxy"
    )
    request = requests.get(
        os.path.join(history_server_proxy_endpoint, "api/v1/applications"),
        headers={"Cookie": f"_oauth2_proxy={history_server_session_cookie}"},
        verify=False,
    )
    assert request.status_code == 200
    apps = request.json()
    logger.info(f"Response JSON from application: {request.json()}")
    assert len(apps) == 1
