#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import json
import logging
import ssl
import urllib.request
from pathlib import Path

import jubilant
import pytest
import yaml
from tenacity import RetryError, Retrying, stop_after_attempt, wait_fixed

from core.context import OAUTH2_PROXY_HEADERS

from .helpers import (
    assert_jobs_in_history_server,
    deploy_history_server_setup,
    get_ingress_url,
    run_spark_job,
    setup_spark_job,
)
from .types import IngressMode, IntegrationTestsCharms, S3Info

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

    Assert on the unit status before any relations/configurations take place.
    """
    deploy_history_server_setup(
        juju=juju,
        charm_versions=charm_versions,
        history_server_charm=history_server_charm,
        s3_bucket_and_creds=s3_bucket_and_creds,
    )

    status = juju.status()
    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    server_url = f"http://{address}:18080"
    assert_jobs_in_history_server(server_url=server_url, expected_count=0)

    setup_spark_job(s3_bucket_and_creds=s3_bucket_and_creds)
    run_spark_job()
    assert_jobs_in_history_server(server_url=server_url, expected_count=1)


def test_ingress(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    # Deploy the charm and wait for waiting status
    juju.deploy(**charm_versions.ingress.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.ingress.application_name),
        delay=10,
    )

    logger.info("Relating history server charm with ingress")
    juju.integrate(charm_versions.ingress.application_name, APP_NAME)
    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.ingress.application_name
        ),
        delay=10,
    )

    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.TRAEFIK)
    assert_jobs_in_history_server(server_url=ingress_url, expected_count=1)


def test_oauth2proxy(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test the integration of the spark history server with Oauth2proxy.

    Assert that the proxied-enpoints of the ingress are protected (err code 401).
    """
    # remove relation between ingress and spark-history server
    juju.remove_relation(
        f"{APP_NAME}:ingress", f"{charm_versions.ingress.application_name}:ingress"
    )
    juju.wait(jubilant.all_active, delay=10)

    # Deploy the self-signed-certificates charm
    juju.deploy(**charm_versions.self_signed_certificate.deploy_dict())
    juju.wait(jubilant.all_active, delay=10)

    # Deploy the oauth2proxy charm and wait for waiting status
    juju.deploy(**charm_versions.oauth2proxy.deploy_dict())
    juju.wait(jubilant.all_active, delay=10)

    # configure Oauth2proxy charm
    oauth2proxy_configuration_parameters = {"dev": "True"}
    juju.config(charm_versions.oauth2proxy.application_name, oauth2proxy_configuration_parameters)

    juju.wait(jubilant.all_active, delay=5)

    # configure ingress to work with Oauth2proxy
    ingress_configuration_parameters = {"enable_experimental_forward_auth": "True"}
    # apply new configuration options
    juju.config(charm_versions.ingress.application_name, ingress_configuration_parameters)

    juju.wait(jubilant.all_active, delay=5)

    # relate ingress with self-signed-certificates
    juju.integrate(
        charm_versions.self_signed_certificate.application_name,
        f"{charm_versions.ingress.application_name}:certificates",
    )

    # Relate Oauth2proxy with the Spark history server charm
    logger.info("Relating the spark history server charm with Oauth2proxy.")
    juju.integrate(charm_versions.oauth2proxy.application_name, f"{APP_NAME}:oauth2-proxy")

    juju.wait(lambda status: jubilant.all_blocked(status, APP_NAME), delay=5)

    # relate spark-history-server and ingress
    juju.integrate(charm_versions.ingress.application_name, APP_NAME)
    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.ingress.application_name
        ),
        delay=5,
    )

    # Relate Oauth2proxy with the Ingress charm
    logger.info("Relating the oauth2proxy charm with the ingress.")

    juju.integrate(
        f"{charm_versions.ingress.application_name}:experimental-forward-auth",
        charm_versions.oauth2proxy.application_name,
    )

    # juju integrate oauth2-proxy-k8s:receive-ca-cert self-signed-certificates
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
    )

    # get proxied endpoint
    task = juju.run(f"{charm_versions.ingress.application_name}/0", "show-proxied-endpoints")
    assert task.return_code == 0
    ingress_endpoint = json.loads(task.results["proxied-endpoints"])[APP_NAME]["url"]

    # ignore SSL certificate verification
    ssl_context = ssl._create_unverified_context()

    # check that the ingress endpoint is not authorized!
    logger.info(f"Querying endpoint: {ingress_endpoint}")
    try:
        _ = urllib.request.urlopen(ingress_endpoint, context=ssl_context)
        raise Exception(
            "Successful request.... something is wrong with the protection of the endpoints."
        )
    except urllib.error.HTTPError as e:  # type: ignore
        # Return code error (e.g. 404, 501, ...)
        logger.info("HTTPError: {}".format(e.code))
        # check that the endopoint respond with code 403
        assert e.code == 403

    logger.info(f"Endpoint: {ingress_endpoint} successfully protected.")

    # check that servlet filter is enabled on the unit endpoint
    status = juju.status()
    address = status.apps[APP_NAME].units[f"{APP_NAME}/0"].address
    try:
        _ = urllib.request.urlopen(f"http://{address}:18080/api/v1/applications")
        raise Exception(
            "Successful request.... something is wrong with the servlet filter configuration..."
        )

    except urllib.error.HTTPError as e:  # type: ignore
        # Return code error (e.g. 404, 501, ...)
        logger.info("HTTPError: {}".format(e.code))
        # check that the endopoint respond with code 500
        assert e.code == 500

    req = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
    req.add_header(OAUTH2_PROXY_HEADERS[1], "xxx")
    apps = json.loads(urllib.request.urlopen(req).read())
    assert len(apps) == 1

    # configure the history server charm with a new authorized user yyy
    authorized_user = "test-user"
    config = {"authorized-users": authorized_user}
    juju.config(APP_NAME, config)

    juju.wait(lambda status: jubilant.all_active(status, APP_NAME), delay=10)

    # check that user admin is not authorized
    try:
        req = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
        req.add_header(OAUTH2_PROXY_HEADERS[1], "admin")
        _ = urllib.request.urlopen(req)
        raise Exception(
            "Successful request.... something is wrong with the servlet filter configuration..."
        )

    except urllib.error.HTTPError as e:  # type: ignore
        # Return code error (e.g. 404, 501, ...)
        logger.info("HTTPError: {}".format(e.code))
        # check that the endopoint respond with code 401
        assert e.code == 401

    # check that user is authorized
    req1 = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
    req1.add_header(OAUTH2_PROXY_HEADERS[1], authorized_user)
    apps = json.loads(urllib.request.urlopen(req1).read())
    assert len(apps) == 1


@pytest.mark.skip
def test_remove_oauth2proxy(juju: jubilant.Juju, charm_versions: IntegrationTestsCharms) -> None:
    """Test the removal of integration between the spark history server and Oauth2proxy.

    Assert that the proxied-enpoints of the ingress are not protected.
    """
    # Remove of the relation between oauth2proxy and spark-history server
    juju.remove_relation(
        f"{APP_NAME}:auth-proxy", f"{charm_versions.oauth2proxy.application_name}:auth-proxy"
    )

    juju.wait(
        lambda status: jubilant.all_active(
            status, APP_NAME, charm_versions.oauth2proxy.application_name
        ),
        delay=10,
    )
    ingress_url = get_ingress_url(juju, charm_versions, IngressMode.TRAEFIK)
    try:
        for attempt in Retrying(stop=stop_after_attempt(10), wait=wait_fixed(30)):
            with attempt:
                assert_jobs_in_history_server(server_url=ingress_url, expected_count=1)
    except RetryError:
        raise Exception("Failed to reach the endpoint!")
