#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import base64
import json
import logging
import subprocess
import urllib.request
import uuid
from pathlib import Path
from time import sleep
from typing import Dict, TypedDict, cast
from urllib.parse import urlencode

import jubilant
import lightkube
import requests
import yaml
from lightkube.resources.core_v1 import Pod
from playwright.sync_api import BrowserContext, Page
from tenacity import Retrying, stop_after_attempt, wait_fixed

from constants import JMX_EXPORTER_PORT

from .oauth_tools.external_idp import ExternalIdpService
from .types import AzureInfo, IngressMode, IntegrationTestsCharms, S3Info

logger = logging.getLogger(__name__)
METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
CURL_IMAGE = "curlimages/curl:8.10.1"


def _run_command(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=check, capture_output=True, text=True)


def _prepare_s3_storage_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    s3_bucket_and_creds: S3Info,
    s3_tls: bool = False,
):
    bucket = s3_bucket_and_creds["bucket"]
    access_key = s3_bucket_and_creds["access_key"]
    secret_key = s3_bucket_and_creds["secret_key"]
    endpoint = s3_bucket_and_creds["endpoint"]
    path = s3_bucket_and_creds["path"]
    tls_ca_chain_path = s3_bucket_and_creds["ca_bundle_path"]

    logger.info("Deploying S3 Integrator charm")
    juju.deploy(**charm_versions.s3.deploy_dict())

    logger.info("Setting up s3 credentials in s3-integrator charm")
    set_s3_credentials(juju, charm_versions.s3.application_name, access_key, secret_key)

    configuration_parameters = {
        "bucket": bucket,
        "path": path,
        "endpoint": endpoint,
    }
    if s3_tls:
        ca = get_certificate_from_file(tls_ca_chain_path)
        ca_b64 = base64.b64encode(ca.encode("utf-8")).decode("utf-8")
        configuration_parameters["tls-ca-chain"] = ca_b64

    juju.config(charm_versions.s3.application_name, configuration_parameters)
    juju.wait(
        lambda status: (
            jubilant.all_active(status, charm_versions.s3.application_name)
            and jubilant.all_agents_idle(status)
        )
    )

    logger.info("Relating history server charm with s3-integrator charm")
    juju.integrate(APP_NAME, charm_versions.s3.application_name)

    juju.wait(
        lambda status: (
            jubilant.all_agents_idle(status)
            and jubilant.all_active(status, APP_NAME, charm_versions.s3.application_name)
        )
    )
    logger.info("S3 storage setup completed")


def _prepare_azure_storage_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    azure_storage_credentials: AzureInfo,
):
    juju.deploy(**charm_versions.azure_storage.deploy_dict())

    logger.info("Adding Juju secret for secret-key config option for azure-storage-integrator")
    secret_id = juju.add_secret(
        "iamsecret",
        {"secret-key": azure_storage_credentials["secret-key"]},
    )
    juju.cli("grant-secret", "iamsecret", charm_versions.azure_storage.application_name)

    # create azure container
    configuration_parameters = {
        "container": azure_storage_credentials["container"],
        "path": azure_storage_credentials["path"],
        "storage-account": azure_storage_credentials["storage-account"],
        "connection-protocol": azure_storage_credentials["connection-protocol"],
        "credentials": secret_id,
    }

    logger.info(
        f"Creating container {azure_storage_credentials['container']} with path {azure_storage_credentials['path']}"
    )
    # First delete container
    delete_azure_container(azure_storage_credentials["container"])
    sleep(10)

    # apply new configuration options
    logger.info("Setting up configuration for azure-storage-integrator charm...")
    juju.config(charm_versions.azure_storage.application_name, configuration_parameters)
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.azure_storage.application_name)
    )

    logger.info("Relating history server charm with azure-storage-integrator charm")

    juju.integrate(charm_versions.azure_storage.application_name, APP_NAME)
    juju.wait(jubilant.all_active, delay=5)


def set_s3_credentials(
    juju: jubilant.Juju,
    s3_app_name: str,
    access_key: str,
    secret_key: str,
) -> None:
    """Use the charm action to start a password rotation."""
    params = {
        "access-key": access_key,
        "secret-key": secret_key,
    }
    secret_uri = juju.add_secret("s3-credentials", params)
    juju.grant_secret(secret_uri, s3_app_name)
    juju.config(s3_app_name, {"credentials": secret_uri})


def delete_azure_container(container: str):
    """Delete azure container."""
    command = ["azcli", "storage", "container", "delete", "--name", container]
    try:
        output = subprocess.run(command, check=True, capture_output=True)
        return output.stdout.decode(), output.stderr.decode(), output.returncode
    except subprocess.CalledProcessError as e:
        return e.stdout.decode(), e.stderr.decode(), e.returncode


def get_certificate_from_file(filename: str) -> str:
    """Returns the certificate as a string."""
    with open(filename, "r") as file:
        certificate = file.read()
    return certificate


def get_history_server_image_version():
    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]
    return image_version


def get_spark_version():
    image_version = get_history_server_image_version()
    logger.info(f"Spark History Server image version: {image_version}")

    shell_output = subprocess.check_output(
        f"./tests/integration/setup/get_image_metadata.sh {image_version}", shell=True
    ).decode("utf-8")
    logger.info(shell_output)

    image_metadata = json.loads(shell_output)
    spark_version = image_metadata["org.opencontainers.image.version"]
    logger.info(f"Spark version: {spark_version}")
    return spark_version


def _deploy_istio_control_plane(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
):
    logger.info("Deploying Istio control plane")
    juju.deploy(**charm_versions.istio.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.istio.application_name), delay=5
    )


def deploy_history_server_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    history_server_charm: Path,
    s3_bucket_and_creds: S3Info | None = None,
    azure_storage_credentials: AzureInfo | None = None,
    ingress_mode: IngressMode = IngressMode.NONE,
    trust: bool = False,
    s3_tls: bool = False,
) -> None:
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
        _prepare_s3_storage_setup(juju, charm_versions, s3_bucket_and_creds, s3_tls=s3_tls)
    elif azure_storage_credentials is not None:
        logger.info("Using Azure object storage with Spark History Server")
        _prepare_azure_storage_setup(juju, charm_versions, azure_storage_credentials)

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
        _deploy_istio_control_plane(juju, charm_versions)
        ingress_app_name = charm_versions.istio_ingress.application_name
        ingress_deploy_args = charm_versions.istio_ingress.deploy_dict()

    logger.info(f"Deploying ingress: {ingress_app_name}")
    juju.deploy(**ingress_deploy_args)

    # logger.info("Deploying self-signed-certificates for ingress")
    # juju.deploy(**charm_versions.self_signed_certificate.deploy_dict())

    # juju.wait(
    #     lambda status: jubilant.all_active(
    #         status, ingress_app_name, charm_versions.self_signed_certificate.application_name
    #     ),
    #     delay=5,
    # )

    # logger.info(f"Integrating certificates for ingress with application: {ingress_app_name}")
    # juju.integrate(
    #     charm_versions.self_signed_certificate.application_name,
    #     ingress_app_name,
    # )

    logger.info(f"Integrating history server with ingress: {ingress_app_name}")
    juju.integrate(f"{APP_NAME}:ingress", f"{ingress_app_name}:ingress")
    juju.wait(jubilant.all_active, delay=5)
    logger.info("History Server setup with Ingress completed.")


def deploy_identity_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    ingress_mode: IngressMode = IngressMode.TRAEFIK,
):
    # Deploy all charms necessary for Oauth2proxy integration
    juju.deploy(**charm_versions.ingress.deploy_dict())
    juju.deploy(**charm_versions.oauth2proxy.deploy_dict())
    juju.deploy(**charm_versions.postgresql.deploy_dict())
    juju.deploy(**charm_versions.self_signed_certificate.deploy_dict())
    juju.deploy(**charm_versions.hydra.deploy_dict())
    juju.deploy(**charm_versions.kratos.deploy_dict())
    juju.deploy(**charm_versions.identity_platform_login_ui_operator.deploy_dict())
    juju.deploy(**charm_versions.kratos_external_idp_integrator.deploy_dict())
    hserver_ingress_charm = charm_versions.ingress
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        hserver_ingress_charm = charm_versions.istio_ingress

    juju.integrate(
        charm_versions.self_signed_certificate.application_name,
        f"{charm_versions.ingress.application_name}:certificates",
    )
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        juju.integrate(
            charm_versions.self_signed_certificate.application_name,
            f"{hserver_ingress_charm.application_name}:certificates",
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
        lambda status: (
            jubilant.all_active(
                status,
                charm_versions.postgresql.application_name,
                charm_versions.self_signed_certificate.application_name,
                charm_versions.hydra.application_name,
                charm_versions.kratos.application_name,
                charm_versions.identity_platform_login_ui_operator.application_name,
            )
            and jubilant.all_blocked(
                status,
                charm_versions.kratos_external_idp_integrator.application_name,
            )
        ),
        delay=10,
        timeout=2000,
    )

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

    # oauth2proxy integrations
    oauth2proxy_ingress_relation_name = "ingress"
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        oauth2proxy_ingress_relation_name = "ingress-unauthenticated"
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:ingress",
        f"{hserver_ingress_charm.application_name}:{oauth2proxy_ingress_relation_name}",
    )
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:oauth",
        charm_versions.hydra.application_name,
    )
    juju.integrate(
        f"{charm_versions.oauth2proxy.application_name}:receive-ca-cert",
        charm_versions.self_signed_certificate.application_name,
    )
    forward_auth_relation = "forward-auth"
    if ingress_mode == IngressMode.TRAEFIK:
        juju.config(
            hserver_ingress_charm.application_name, {"enable_experimental_forward_auth": "True"}
        )
        forward_auth_relation = "experimental-forward-auth"
    juju.integrate(
        f"{hserver_ingress_charm.application_name}:{forward_auth_relation}",
        f"{charm_versions.oauth2proxy.application_name}:forward-auth",
    )
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        juju.integrate(
            f"{hserver_ingress_charm.application_name}:istio-ingress-config",
            f"{charm_versions.istio.application_name}:istio-ingress-config",
        )
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.oauth2proxy.application_name,
            hserver_ingress_charm.application_name,
        ),
        delay=10,
        timeout=200,
    )

    juju.integrate(charm_versions.oauth2proxy.application_name, f"{APP_NAME}:oauth2-proxy")
    # juju.integrate(f"{APP_NAME}:ingress", f"{hserver_ingress_charm.application_name}:ingress")
    juju.wait(
        lambda status: jubilant.all_active(
            status,
            charm_versions.oauth2proxy.application_name,
            charm_versions.ingress.application_name,
            hserver_ingress_charm.application_name,
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


def deploy_o11y_setup(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    logger.info("Deploying opentelemetery-collector-k8s charm...")
    juju.deploy(**charm_versions.otel_collector.deploy_dict())

    logger.info("Waiting for test charm to be idle...")
    juju.wait(jubilant.all_agents_idle, delay=5)

    juju.integrate(charm_versions.otel_collector.application_name, f"{APP_NAME}:metrics-endpoint")
    juju.integrate(charm_versions.otel_collector.application_name, f"{APP_NAME}:grafana-dashboard")
    juju.integrate(charm_versions.otel_collector.application_name, f"{APP_NAME}:logging")
    juju.wait(jubilant.all_agents_idle, delay=5)
    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    juju.cli("deploy", "cos-lite", "--trust")
    juju.wait(
        lambda status: jubilant.all_active(
            status, "prometheus", "alertmanager", "loki", "grafana"
        ),
        delay=10,
    )
    juju.wait(
        lambda status: jubilant.all_blocked(
            status, charm_versions.otel_collector.application_name
        ),
        delay=10,
    )
    juju.integrate(
        f"{charm_versions.otel_collector.application_name}:grafana-dashboards-provider", "grafana"
    )
    juju.integrate(
        f"{charm_versions.otel_collector.application_name}:send-remote-write", "prometheus"
    )
    juju.integrate(f"{charm_versions.otel_collector.application_name}:send-loki-logs", "loki")

    juju.wait(jubilant.all_active, delay=10)
    logger.info("Observability setup deployed successfully.")


def complete_authentication_flow(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
    external_idp_service: ExternalIdpService,
    page: Page,
    context: BrowserContext,
    history_server_url: str,
):
    logger.info(f"Navigating to {history_server_url}")
    page.goto(history_server_url)

    logger.info("Clicking on Sign in with Generic identity provider...")
    with page.expect_navigation():
        page.get_by_text("Sign in with Generic").click()

    logger.info("Completing login in the external identity provider...")
    with page.expect_navigation():
        external_idp_service.complete_user_login(page)
    logger.info(f"Login flow completed: {page.url}")

    logger.info("Verifying the correct redirect after login")
    page.wait_for_url(history_server_url)

    logger.info("Verifying that the login flow was successful...")
    # The test uses Spark history server's /api/user endpoint to verify the session cookie is valid
    history_server_session_cookie = next(
        iter(
            [cookie for cookie in context.cookies() if cookie.get("name", None) == "_oauth2_proxy"]
        ),
        None,
    )
    assert history_server_session_cookie is not None, "Session cookie '_oauth2_proxy' not found"

    return history_server_session_cookie.get("value")


def setup_spark_job(
    s3_bucket_and_creds: S3Info | None = None,
    azure_storage_credentials: AzureInfo | None = None,
):
    image_version = get_history_server_image_version()
    if s3_bucket_and_creds is not None:
        logger.info("Setting up Spark job with S3 storage")
        access_key = s3_bucket_and_creds["access_key"]
        secret_key = s3_bucket_and_creds["secret_key"]
        endpoint = s3_bucket_and_creds["endpoint"]
        setup_spark_output = subprocess.check_output(
            f"./tests/integration/setup/setup_spark.sh {endpoint} {access_key} {secret_key} {image_version}",
            shell=True,
            stderr=None,
        ).decode("utf-8")
    elif azure_storage_credentials is not None:
        logger.info("Setting up Spark job with Azure storage")
        container = azure_storage_credentials["container"]
        path = azure_storage_credentials["path"]
        storage_account = azure_storage_credentials["storage-account"]
        secret_key = azure_storage_credentials["secret-key"]
        setup_spark_output = subprocess.check_output(
            (
                f"./tests/integration/setup/setup_spark_azure.sh {container} {path} {storage_account} {secret_key} {image_version}"
            ),
            shell=True,
            stderr=None,
        ).decode("utf-8")
    else:
        raise ValueError(
            "Either s3_bucket_and_creds or azure_storage_credentials must be provided."
        )
    return setup_spark_output


def run_spark_job(tls_ca: str | None = None):
    logger.info("Executing Spark job...")
    spark_version = get_spark_version()
    output = ""
    if tls_ca:
        output = subprocess.check_output(
            f"./tests/integration/setup/run_spark_job_tls.sh  {spark_version} {tls_ca}",
            shell=True,
            stderr=None,
        ).decode("utf-8")
    else:
        output = subprocess.check_output(
            f"./tests/integration/setup/run_spark_job.sh {spark_version}", shell=True, stderr=None
        ).decode("utf-8")
    logger.info(f"Run spark output:\n{output}")


def _get_application_data(juju: jubilant.Juju, app_name: str, relation_name: str) -> dict:
    """Retrieves the application data from a specific relation.

    Args:
        juju: The Juju client object used to execute CLI commands.
        app_name: The name of the Juju application.
        relation_name: The name of the relation endpoint to query.

    Returns:
        A dictionary containing the application data for the specified relation.

    Raises:
        ValueError: If no relation data can be found for the specified
            relation endpoint.
    """
    unit_name = f"{app_name}/0"
    command_stdout = juju.cli("show-unit", unit_name, "--format=json")
    result = json.loads(command_stdout)
    relation_data = [
        v for v in result[unit_name]["relation-info"] if v["endpoint"] == relation_name
    ]
    if len(relation_data) == 0:
        raise ValueError(
            f"No relation data could be grabbed on relation with endpoint {relation_name}"
        )
    return {relation["relation-id"]: relation["application-data"] for relation in relation_data}


def _get_prometheus_exporter_data(host: str) -> str | None:
    """Check if a given host has metric service available and it is publishing."""
    url = f"http://{host}:{JMX_EXPORTER_PORT}/metrics"
    try:
        response = requests.get(url)
        logger.info(f"Response: {response.text}")
        print(response)
    except requests.exceptions.RequestException:
        return None

    if response.status_code == 200:
        return response.text

    return None


def _get_cos_address(juju: jubilant.Juju) -> str:
    """Retrieve the URL where COS services are available."""
    task = juju.run("traefik/0", "show-proxied-endpoints")
    assert task.return_code == 0
    endpoints = task.results["proxied-endpoints"]
    return json.loads(endpoints)["traefik"]["url"]


def get_unit_address(
    juju: jubilant.Juju,
    app_name: str,
    unit_number: int = 0,
) -> str:
    status = juju.status()
    address = status.apps[app_name].units[f"{app_name}/{unit_number}"].address
    return address


def _get_grafana_access(juju: jubilant.Juju) -> tuple[str, str]:
    """Get Grafana URL and password."""
    task = juju.run("grafana/0", "get-admin-password")
    assert task.return_code == 0
    return task.results["url"], task.results["admin-password"]


def get_ingress_url(
    juju: jubilant.Juju, charm_versions: IntegrationTestsCharms, ingress_mode: IngressMode
) -> str:
    if ingress_mode == IngressMode.ISTIO_INGRESS:
        app_data = _get_application_data(juju, APP_NAME, "ingress")
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


def get_logs_in_loki(juju: jubilant.Juju, app_name: str, filter_by_label: dict[str, str]):
    loki_address = get_unit_address(juju, app_name)
    try:
        response = json.loads(
            urllib.request.urlopen(f"http://{loki_address}:3100/loki/api/v1/labels").read()
        )
    except Exception:
        response = {}
    assert "success" == response["status"], "Failed to get labels from Loki"
    labels = response["data"]
    for key in filter_by_label:
        assert key in labels, f"Log label '{key}' not found in Loki labels: {labels}"

    for key, value in filter_by_label.items():
        try:
            values = json.loads(
                urllib.request.urlopen(
                    f"http://{loki_address}:3100/loki/api/v1/label/{key}/values"
                ).read()
            )
        except Exception:
            values = {}
        logger.info(f"Values for label '{key}': {values}")
        assert "success" == values["status"]
        assert value in values["data"][0], (
            f"Expected value '{value}' for label '{key}' not found in Loki"
        )

    # check for history server logs in loki
    url = f"http://{loki_address}:3100/loki/api/v1/query_range"
    query = ",".join([f"{key}={value}" for key, value in filter_by_label.items()])
    keys = {"query": f"{{{query}}}"}
    data = urlencode(keys).encode()

    try:
        query = json.loads(urllib.request.urlopen(url, data).read().decode())
        logger.info(query)
    except Exception:
        query = {}

    assert "success" == query["status"]
    assert "stream" in query["data"]["result"][0]
    for key, value in filter_by_label.items():
        assert value == query["data"]["result"][0]["stream"].get(key), (
            f"Expected value '{value}' for label '{key}' not found in Loki stream"
        )

    logs = query["data"]["result"][0]["values"]
    logger.info(f"Retrieved logs: {logs}")
    return logs

    # check if startup messages are there
    c = 0
    for log_line in logs:
        if "INFO HistoryServer" in log_line[1]:
            c = c + 1
    logger.info(f"Number of line found: {c}")

    return logs


def assert_jobs_in_history_server(
    server_url: str,
    expected_count: int = 1,
    session_cookie: str | None = None,
    verify_tls: bool = True,
) -> None:
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


def assert_prometheus_data_exported(
    juju: jubilant.Juju,
    check_field: str = "jmx_scrape_duration_seconds",
):
    result = True
    status = juju.status()
    for unit in status.apps[APP_NAME].units.values():
        unit_ip = unit.address
        result = result and check_field in (_get_prometheus_exporter_data(unit_ip) or "")
    assert result is True, f"Prometheus data for field '{check_field}' not exported by all units"


def assert_prometheus_data_published(
    juju: jubilant.Juju,
    check_field: str = "jmx_scrape_duration_seconds",
):
    # We should leave time for Prometheus data to be published
    cos_address = _get_cos_address(juju)
    if "http://" in cos_address:
        cos_address = cos_address.split("//")[1]
    url = f"http://{cos_address}/{cast(str, juju.model)}-prometheus-0/api/v1/query?query={check_field}"
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(30)):
        with attempt:
            # Data got published to Prometheus
            response = requests.get(url).json()
            assert "data" in response, (
                f"Prometheus query for field '{check_field}' failed: {response}"
            )
            assert "result" in response["data"], (
                f"Prometheus query for field '{check_field}' returned no results: {response}"
            )
            assert len(response["data"]["result"]) > 0, (
                f"Prometheus query for field '{check_field}' returned empty result: {response}"
            )


def assert_prometheus_alerts_published(
    juju: jubilant.Juju,
):
    # We should leave time for Prometheus data to be published
    cos_address = _get_cos_address(juju)
    if "http://" in cos_address:
        cos_address = cos_address.split("//")[1]
    url = f"http://{cos_address}/{cast(str, juju.model)}-prometheus-0/api/v1/rules"
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(30)):
        with attempt:
            # Alerts got published to Prometheus
            response = requests.get(url).json()
            assert response is not None
            assert "data" in response, f"Prometheus query for alerts failed: {response}"
            assert "groups" in response["data"], (
                f"Prometheus query for alerts returned no groups: {response}"
            )
            assert len(response["data"]["groups"]) > 0, (
                f"Prometheus query for alerts returned empty groups: {response}"
            )

            for alert in [
                "Spark History Server Missing",
                "Spark History Server Threads Dead Locked",
            ]:
                assert any(
                    rule["name"] == alert
                    for group in response["data"]["groups"]
                    for rule in group["rules"]
                ), f"Prometheus query for alert '{alert}' returned no matching rules: {response}"


def assert_grafana_dashboards_published(
    juju: jubilant.Juju,
):
    base_url, pw = _get_grafana_access(juju)
    url = f"{base_url}/api/search?query=&starred=false"
    for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(30)):
        with attempt:
            session = requests.Session()
            session.auth = ("admin", pw)
            response = session.get(url).json()
            assert response is not None, f"Failed to get Grafana dashboards: {response}"
            assert any(
                board["title"] == "Spark History Server JMX Dashboard" for board in response
            ), (
                f"Grafana dashboard 'Spark History Server JMX Dashboard' not found in response: {response}"
            )


def assert_logs_published_in_loki(
    juju: jubilant.Juju, app_name: str, filter_by_label: dict[str, str], search_phrase: str
) -> None:
    logs = get_logs_in_loki(juju=juju, app_name=app_name, filter_by_label=filter_by_label)
    assert len(logs) > 0, f"No logs found for app '{app_name}' with labels '{filter_by_label}'"

    c = 0
    for log_line in logs:
        if search_phrase in log_line[1]:
            c = c + 1
    logger.info(f"Number of line found: {c}")
    assert c > 0, (
        f"No logs found containing the phrase '{search_phrase}' with labels '{filter_by_label}'"
    )


def curl_using_pod(
    namespace: str,
    url: str,
    labels: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    pod_name = f"ambient-curl-{uuid.uuid4()}"

    labels_args = []
    if labels:
        # kubectl run --labels accepts a single comma-separated k=v list.
        labels_value = ",".join(f"{key}={value}" for key, value in labels.items())
        labels_args = ["--labels", labels_value]

    return _run_command(
        [
            "kubectl",
            "-n",
            namespace,
            "run",
            pod_name,
            "--rm",
            "-i",
            "--quiet",
            "--restart=Never",
            f"--image={CURL_IMAGE}",
            *labels_args,
            "--",
            "curl",
            "-sS",
            "--max-time",
            "10",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            url,
        ],
        check=False,
    )


class ContainerSecurityContext(TypedDict, total=False):
    """Kubernetes container security context UID/GID settings."""

    runAsUser: int | None  # noqa N815
    runAsGroup: int | None  # noqa N815
    runAsNonRoot: bool | None  # noqa N815


def assert_security_context(
    lightkube_client: lightkube.Client,
    pod_name: str,
    container_name: str,
    container_securitycontext_map: Dict[str, ContainerSecurityContext],
    model_name: str,
) -> None:
    """Assert a container's security context matches expected UID/GID settings."""
    containers: list = lightkube_client.get(Pod, pod_name, namespace=model_name).spec.containers
    container = next((c for c in containers if c.name == container_name), None)
    assert container is not None, f"Container {container_name} not found in pod {pod_name}"
    security_context = container.securityContext
    # assert user ID is the one defined in metadata.yaml
    for key, value in container_securitycontext_map[container_name].items():
        assert getattr(security_context, key) == value


def generate_container_securitycontext_map(
    metadata_yaml: dict, juju_user_id: int = 170
) -> dict[str, ContainerSecurityContext]:
    """Build a map of container names to expected security context UID/GID settings.

    The map is derived from the ``uid``/``gid`` values in the ``containers`` section
    of ``metadata.yaml``, plus a ``charm`` entry for the Juju agent container.
    """
    c_uid_map: dict[str, ContainerSecurityContext] = {}
    for k, v in metadata_yaml.get("containers", {}).items():
        c_uid_map[k] = ContainerSecurityContext(
            runAsUser=v["uid"],
            runAsGroup=v["gid"],
        )
    c_uid_map["charm"] = {"runAsUser": juju_user_id, "runAsGroup": juju_user_id}
    return c_uid_map


def get_pod_names(model: str, application_name: str) -> list[str]:
    """Retrieve names of all pods belonging to a specific Juju application."""
    cmd = [
        "kubectl",
        "get",
        "pods",
        f"-n{model}",
        f"-lapp.kubernetes.io/name={application_name}",
        "--no-headers",
        "-o=custom-columns=NAME:.metadata.name",
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
    )
    stdout = proc.stdout.decode("utf8")
    return stdout.split()
