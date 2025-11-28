#!/usr/bin/env python3
# Copyright 2025 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
import os
import re
import subprocess
import urllib.request
from pathlib import Path
from time import sleep
from typing import Any, AsyncGenerator, Callable, Coroutine, Dict, Generator, Optional

import jubilant
import pytest
import pytest_asyncio
import requests
import yaml
from lightkube import Client, KubeConfig
from oauth_tools import (
    ExternalIdpService,
    click_on_sign_in_button_by_text,
    verify_page_loads,
    # get_cookie_from_browser_by_name,
)
from oauth_tools.external_idp import DexIdpService
from playwright.async_api import async_playwright, expect
from playwright.async_api._generated import Browser, BrowserContext, BrowserType, Page
from playwright.async_api._generated import Playwright as AsyncPlaywright

from .test_helpers import (
    set_s3_credentials,
    setup_s3_bucket_for_history_server,
)
from .types import IntegrationTestsCharms

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"

KUBECONFIG = os.environ.get("TESTING_KUBECONFIG", "~/.kube/config")


@pytest.fixture(scope="session")
def client() -> Client:
    return Client(config=KubeConfig.from_file(KUBECONFIG), field_manager="dex-test")


# @pytest_asyncio.fixture
# async def page(context: BrowserContext) -> AsyncGenerator[Page, None]:
#     page = await context.new_page()
#     yield page
#     await page.close()

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
        logger.info(f"Cookie found: {cookie['name']}")
        if cookie["name"] == name:
            return cookie["value"]
    return None

async def access_application_login_page(
    page: Page, url: str, redirect_login_url: Optional[str] = None
):
    """Navigate the browser to the login page.

    If the url of the application redirects to a login page, pass the application's url as url,
    and a pattern string for the login page as redirect_login_url.
    Otherwise pass the url of the application's login page as url, and leave redirect_login_url
    empty.

    Args:
        page (page): The page fixture.
        url (str): The url to go to.
        redirect_login_url (str): The redirect to which the browser will get redirected to.
    """
    await page.goto(url)
    logger.info(f"Navigated to {url}")
    logger.info(f"Page URL after navigation: {page.url}")
    logger.info(f"Page content after navigation: {await page.content()}")
    # if redirect_login_url:
    #     await expect(page).to_have_url(re.compile(rf"{redirect_login_url}*"))

async def complete_auth_code_login(
    page: Page, identity_platform_login_ui_operator_url: str, external_idp_service: ExternalIdpService
) -> None:
    """Take a page that is in the identity-platform's login page and login the user.

    Args:
        page (page): The page fixture.
        ops_test (OpsTest): The ops_test fixture.
        ext_idp_service (ExternalIdpService): The ExternalIdpService.
    """
    if not isinstance(external_idp_service, ExternalIdpService):
        raise ValueError(
            f"Invalid ext_idp_service type: {type(external_idp_service)}, MUST be ExternalIdpManager or None"
        )

    expected_url = os.path.join(
        # await get_reverse_proxy_app_url(
        #     ops_test, APPS.TRAEFIK_PUBLIC, APPS.IDENTITY_PLATFORM_LOGIN_UI_OPERATOR
        # ),
        identity_platform_login_ui_operator_url,
        "ui/login",
    )
    logger.info(f"Expected URL for identity platform login UI: {expected_url}")
    logger.info("Choose external provider")
    logger.info(f"Current page URL before clicking sign-in button: {page.url}")
    logger.info(f"Current page content before clicking sign-in button: {await page.content()}")
    # await expect(page).to_have_url(re.compile(rf"{expected_url}*"))
    async with page.expect_navigation():
        # await page.get_by_role("button")
        await page.get_by_role("button", name="Login").click()

    logger.info("Completing the login flow on the external provider")
    await external_idp_service.complete_user_login(page)

@pytest.fixture(scope="module")
def external_idp_service(
    request: pytest.FixtureRequest, client: Client
) -> Generator[DexIdpService, None, None]:
    """Deploy and manage the lifecycle of an Dex service."""
    logger.info("Deploying dex resources")
    ext_idp_manager = DexIdpService(client=client)
    try:
        yield ext_idp_manager
    finally:
        keep_models = bool(request.config.getoption("--keep-models"))
        if keep_models:
            return
        logger.info("Deleting dex resources")
        ext_idp_manager.remove_idp_service()


@pytest.fixture(scope="module")
def launch_arguments(pytestconfig: Any) -> Dict:
    return {
        "headless": not (pytestconfig.getoption("--headed") or os.getenv("HEADFUL", False)),
        "channel": pytestconfig.getoption("--browser-channel"),
    }


@pytest_asyncio.fixture(scope="module")
async def playwright() -> AsyncGenerator[AsyncPlaywright, None]:
    async with async_playwright() as playwright_object:
        yield playwright_object


@pytest.fixture(scope="module")
def browser_type(playwright: AsyncPlaywright, browser_name: str) -> BrowserType:
    if browser_name == "firefox":
        return playwright.firefox
    if browser_name == "webkit":
        return playwright.webkit
    return playwright.chromium


@pytest_asyncio.fixture(scope="module")
async def browser_factory(
    launch_arguments: Dict, browser_type: BrowserType
) -> AsyncGenerator[Callable[..., Coroutine[Any, Any, Browser]], None]:
    browsers = []

    async def launch(**kwargs: Any) -> Browser:
        browser = await browser_type.launch(**launch_arguments, **kwargs)
        browsers.append(browser)
        return browser

    yield launch
    for browser in browsers:
        await browser.close()


@pytest_asyncio.fixture(scope="module")
async def browser(
    browser_factory: Callable[..., Coroutine[Any, Any, Browser]],
) -> AsyncGenerator[Browser, None]:
    browser = await browser_factory()
    yield browser
    await browser.close()


@pytest_asyncio.fixture
async def context_factory(
    browser: Browser,
) -> AsyncGenerator[Callable[..., Coroutine[Any, Any, BrowserContext]], None]:
    contexts = []

    async def launch(**kwargs: Any) -> BrowserContext:
        context = await browser.new_context(**kwargs)
        contexts.append(context)
        return context

    yield launch
    for context in contexts:
        await context.close()


@pytest_asyncio.fixture
async def context(
    context_factory: Callable[..., Coroutine[Any, Any, BrowserContext]],
) -> AsyncGenerator[BrowserContext, None]:
    context = await context_factory(ignore_https_errors=True)
    yield context
    await context.close()


@pytest_asyncio.fixture
async def page(context: BrowserContext) -> AsyncGenerator[Page, None]:
    page = await context.new_page()
    yield page
    await page.close()


def test_build_and_deploy(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, history_server_charm: Path
) -> None:
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
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
        "bucket": "history-server",
        "path": "spark-events",
        "endpoint": endpoint_url,
    }
    # apply new configuration options
    juju.config(charm_versions.s3.application_name, configuration_parameters)

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


async def test_deploy_iam_bundle(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
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
        timeout=1000,
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
        timeout=200,
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

    juju.integrate(charm_versions.oauth2proxy.application_name, f"{APP_NAME}:auth-proxy")
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
        timeout=200,
    )

    task = juju.run(f"{charm_versions.kratos_external_idp_integrator.application_name}/0", "get-redirect-uri")
    assert task.return_code == 0

    logger.info("Configuring the external provider")
    external_idp_service.update_redirect_uri(redirect_uri=
                                        task.results["redirect-uri"])

    logger.info("IAM bundle deployed successfully.")
    # get proxied endpoint
    task = juju.run(f"{charm_versions.ingress.application_name}/0", "show-proxied-endpoints")
    assert task.return_code == 0
    history_server_proxy_endpoint = json.loads(task.results["proxied-endpoints"])[APP_NAME]["url"]
    logger.info(f"History server proxy endpoint: {history_server_proxy_endpoint}")
    # grafana_proxy = await get_reverse_proxy_app_url(ops_test, public_traefik_app_name, "grafana")
    redirect_login = os.path.join(history_server_proxy_endpoint, "login")
    logger.info(f"Redirect login url: {redirect_login}")

    # sleep(360)
    # variable = input('input something!: ')
    await access_application_login_page(
        page=page, url=history_server_proxy_endpoint, redirect_login_url=redirect_login
    )
    logger.info("Application login page accessed successfully.")
    logger.info(page.url)
    logger.info(await page.content())
    await click_on_sign_in_button_by_text(
        page=page, text="Sign in with Generic"
    )

    # HERE
    task = juju.run(f"{charm_versions.ingress.application_name}/0", "show-proxied-endpoints")
    assert task.return_code == 0
    logger.info(f"Proxied endpoints: {task.results['proxied-endpoints']}")

    # a
    variable = input('input something!: ')
    status = juju.status()
    identity_platform_login_ui_operator_url =json.loads(task.results["proxied-endpoints"])[charm_versions.ingress.application_name]["url"]
    # json.loads(task.results["proxied-endpoints"])[charm_versions.identity_platform_login_ui_operator.application_name]["url"]
    logger.info(f"Identity platform login ui operator url: {identity_platform_login_ui_operator_url}")
    await complete_auth_code_login(page=page,
                                   identity_platform_login_ui_operator_url=identity_platform_login_ui_operator_url, external_idp_service=external_idp_service)

    redirect_url = os.path.join(history_server_proxy_endpoint, "?*")
    await verify_page_loads(page=page, url=redirect_url)

    # Verifying that the login flow was successful is application specific.
    # The test uses Grafana's /api/user endpoint to verify the session cookie is valid
    history_server_session_cookie = await get_cookie_from_browser_by_name(
        browser_context=context, name="history_server_session"
    )
    request = requests.get(
        os.path.join(history_server_proxy_endpoint, "api/v1/applications"),
        headers={"Cookie": f"history_server_session={history_server_session_cookie}"},
        verify=False,
    )
    assert request.status_code == 200
    # assert request.json()["email"] == user_email


# def test_ingress(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
#     """Build the charm-under-test and deploy it together with related charms.

#     Assert on the unit status before any relations/configurations take place.
#     """
#     # Deploy the charm and wait for waiting status
#     juju.deploy(**charm_versions.ingress.deploy_dict())
#     juju.wait(
#         lambda status: jubilant.all_active(status, charm_versions.ingress.application_name),
#         delay=10,
#     )

#     logger.info("Relating history server charm with ingress")

#     juju.integrate(charm_versions.ingress.application_name, APP_NAME)
#     juju.wait(
#         lambda status: jubilant.all_active(
#             status, APP_NAME, charm_versions.ingress.application_name
#         ),
#         delay=10,
#     )

#     task = juju.run(f"{charm_versions.ingress.application_name}/0", "show-proxied-endpoints")
#     assert task.return_code == 0

#     ingress_endpoint = json.loads(task.results["proxied-endpoints"])[APP_NAME]["url"]

#     logger.info(f"Querying endpoint: {ingress_endpoint}/api/v1/applications")

#     apps = json.loads(urllib.request.urlopen(f"{ingress_endpoint}/api/v1/applications").read())

#     assert len(apps) == 1

#     logger.info(f"Number of apps: {len(apps)}")


# def test_oauth2proxy(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
#     """Test the integration of the spark history server with Oauth2proxy.

#     Assert that the proxied-enpoints of the ingress are protected (err code 401).
#     """
#     # remove relation between ingress and spark-history server
#     juju.remove_relation(
#         f"{APP_NAME}:ingress", f"{charm_versions.ingress.application_name}:ingress"
#     )
#     juju.wait(jubilant.all_active, delay=10)

#     # Deploy the self-signed-certificates charm
#     juju.deploy(**charm_versions.self_signed_certificate.deploy_dict())
#     juju.wait(jubilant.all_active, delay=10)

#     # Deploy the oauth2proxy charm and wait for waiting status
#     juju.deploy(**charm_versions.oauth2proxy.deploy_dict())
#     juju.wait(jubilant.all_active, delay=10)

#     # configure Oauth2proxy charm
#     oauth2proxy_configuration_parameters = {"dev": "True"}
#     juju.config(charm_versions.oauth2proxy.application_name, oauth2proxy_configuration_parameters)

#     juju.wait(jubilant.all_active, delay=5)

#     # configure ingress to work with Oauth2proxy
#     ingress_configuration_parameters = {"enable_experimental_forward_auth": "True"}
#     # apply new configuration options
#     juju.config(charm_versions.ingress.application_name, ingress_configuration_parameters)

#     juju.wait(jubilant.all_active, delay=5)

#     # relate ingress with self-signed-certificates
#     juju.integrate(
#         charm_versions.self_signed_certificate.application_name,
#         f"{charm_versions.ingress.application_name}:certificates",
#     )

#     # Relate Oauth2proxy with the Spark history server charm
#     logger.info("Relating the spark history server charm with Oauth2proxy.")
#     juju.integrate(charm_versions.oauth2proxy.application_name, APP_NAME)

#     juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)

#     # relate spark-history-server and ingress
#     juju.integrate(charm_versions.ingress.application_name, APP_NAME)
#     juju.wait(
#         lambda status: jubilant.all_active(
#             status, APP_NAME, charm_versions.ingress.application_name
#         ),
#         delay=5,
#     )

#     # Relate Oauth2proxy with the Ingress charm
#     logger.info("Relating the oauth2proxy charm with the ingress.")

#     juju.integrate(
#         f"{charm_versions.ingress.application_name}:experimental-forward-auth",
#         charm_versions.oauth2proxy.application_name,
#     )

#     # juju integrate oauth2-proxy-k8s:receive-ca-cert self-signed-certificates
#     juju.integrate(
#         f"{charm_versions.oauth2proxy.application_name}:receive-ca-cert",
#         charm_versions.self_signed_certificate.application_name,
#     )

#     juju.wait(
#         lambda status: jubilant.all_active(
#             status,
#             charm_versions.oauth2proxy.application_name,
#             charm_versions.ingress.application_name,
#         ),
#         delay=10,
#     )

#     # get proxied endpoint
#     task = juju.run(f"{charm_versions.ingress.application_name}/0", "show-proxied-endpoints")
#     assert task.return_code == 0
#     ingress_endpoint = json.loads(task.results["proxied-endpoints"])[APP_NAME]["url"]

#     # ignore SSL certificate verification
#     ssl_context = ssl._create_unverified_context()

#     # check that the ingress endpoint is not authorized!
#     logger.info(f"Querying endpoint: {ingress_endpoint}")
#     try:
#         _ = urllib.request.urlopen(ingress_endpoint, context=ssl_context)
#         raise Exception(
#             "Successful request.... something is wrong with the protection of the endpoints."
#         )
#     except urllib.error.HTTPError as e:  # type: ignore
#         # Return code error (e.g. 404, 501, ...)
#         logger.info("HTTPError: {}".format(e.code))
#         # check that the endopoint respond with code 403
#         assert e.code == 403

#     logger.info(f"Endpoint: {ingress_endpoint} successfully protected.")

#     # check that servlet filter is enabled on the unit endpoint
#     status = juju.status()
#     address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
#     try:
#         _ = urllib.request.urlopen(f"http://{address}:18080/api/v1/applications")
#         raise Exception(
#             "Successful request.... something is wrong with the servlet filter configuration..."
#         )

#     except urllib.error.HTTPError as e:  # type: ignore
#         # Return code error (e.g. 404, 501, ...)
#         logger.info("HTTPError: {}".format(e.code))
#         # check that the endopoint respond with code 500
#         assert e.code == 500

#     req = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
#     req.add_header(AUTH_PROXY_HEADERS[1], "xxx")
#     apps = json.loads(urllib.request.urlopen(req).read())
#     assert len(apps) == 1

#     # configure the history server charm with a new authorized user yyy
#     authorized_user = "test-user"
#     config = {"authorized-users": authorized_user}
#     juju.config(APP_NAME, config)

#     juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

#     # check that user admin is not authorized
#     try:
#         req = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
#         req.add_header(AUTH_PROXY_HEADERS[1], "admin")
#         _ = urllib.request.urlopen(req)
#         raise Exception(
#             "Successful request.... something is wrong with the servlet filter configuration..."
#         )

#     except urllib.error.HTTPError as e:  # type: ignore
#         # Return code error (e.g. 404, 501, ...)
#         logger.info("HTTPError: {}".format(e.code))
#         # check that the endopoint respond with code 401
#         assert e.code == 401

#     # check that user is authorized
#     req1 = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
#     req1.add_header(AUTH_PROXY_HEADERS[1], authorized_user)
#     apps = json.loads(urllib.request.urlopen(req1).read())
#     assert len(apps) == 1


# @pytest.mark.skip
# def test_remove_oauth2proxy(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
#     """Test the removal of integration between the spark history server and Oauth2proxy.

#     Assert that the proxied-enpoints of the ingress are not protected.
#     """
#     # Remove of the relation between oauth2proxy and spark-history server
#     juju.remove_relation(
#         f"{APP_NAME}:auth-proxy", f"{charm_versions.oauth2proxy.application_name}:auth-proxy"
#     )

#     juju.wait(
#         lambda status: jubilant.all_active(
#             status, APP_NAME, charm_versions.oauth2proxy.application_name
#         ),
#         delay=10,
#     )

#     try:
#         for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(30)):
#             with attempt:
#                 task = juju.run(
#                     f"{charm_versions.ingress.application_name}/0", "show-proxied-endpoints"
#                 )
#                 assert task.return_code == 0
#                 ingress_endpoint = task.results["proxied-endpoints"][APP_NAME]["url"]

#                 logger.info(f"Trying to querying endpoint: {ingress_endpoint}/api/v1/applications")

#                 apps = json.loads(
#                     urllib.request.urlopen(f"{ingress_endpoint}/api/v1/applications").read()
#                 )

#                 assert len(apps) == 1

#                 logger.info(f"Number of apps: {len(apps)}")
#     except RetryError:
#         raise Exception("Failed to reach the endpoint!")
