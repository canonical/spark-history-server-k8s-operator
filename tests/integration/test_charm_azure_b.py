#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

# Integration Tests TBD separately in next pulse

import asyncio
import json
import logging
import subprocess
import urllib.request
from pathlib import Path
from time import sleep

import pytest
import yaml
from pytest_operator.plugin import OpsTest

from core.context import AUTH_PROXY_HEADERS

from .test_helpers import (
    add_juju_secret,
    delete_azure_container,
    setup_azure_container_for_history_server,
)

logger = logging.getLogger(__name__)

METADATA = yaml.safe_load(Path("./metadata.yaml").read_text())
APP_NAME = METADATA["name"]
BUCKET_NAME = "history-server"


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest, charm_versions, azure_credentials):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    logger.info("Setting up azure storage container.....")

    logger.info("Building charm")
    # Build and deploy charm from local source folder

    charm = await ops_test.build_charm(".")

    image_version = METADATA["resources"]["spark-history-server-image"]["upstream-source"]

    logger.info(f"Image version: {image_version}")

    resources = {"spark-history-server-image": image_version}

    logger.info("Deploying charm")

    # Deploy the charm and wait for waiting status
    await asyncio.gather(
        ops_test.model.deploy(**charm_versions.azure_storage.deploy_dict()),
        ops_test.model.deploy(
            charm, resources=resources, application_name=APP_NAME, num_units=1, series="jammy"
        ),
        ops_test.model.deploy(**charm_versions.ingress.deploy_dict()),
        ops_test.model.deploy(**charm_versions.oathkeeper.deploy_dict()),
    
    )
    
    await ops_test.model.add_relation(charm_versions.azure_storage.application_name, APP_NAME)
    await ops_test.model.add_relation(charm_versions.ingress.application_name, APP_NAME)
    await ops_test.model.add_relation(charm_versions.oathkeeper.application_name, APP_NAME)
    await ops_test.model.add_relation(charm_versions.ingress.application_name, APP_NAME)
    await ops_test.model.add_relation(
        f"{charm_versions.ingress.application_name}:experimental-forward-auth",
        charm_versions.oathkeeper.application_name,
    )


    await ops_test.model.wait_for_idle(
        apps=[charm_versions.azure_storage.application_name,charm_versions.oathkeeper.application_name, charm_versions.ingress.application_name],
        timeout=1000,
        status="blocked",
        idle_period=30,
    )

    logger.info("Adding Juju secret for secret-key config option for azure-storage-integrator")
    credentials_secret_uri = await add_juju_secret(
        ops_test,
        charm_versions.azure_storage.application_name,
        "iamsecret",
        {"secret-key": azure_credentials["secret-key"]},
    )
    logger.info(
        f"Juju secret for secret-key config option for azure-storage-integrator added. Secret URI: {credentials_secret_uri}"
    )

    configuration_parameters = {
        "container": azure_credentials["container"],
        "path": azure_credentials["path"],
        "storage-account": azure_credentials["storage-account"],
        "connection-protocol": azure_credentials["connection-protocol"],
        "credentials": credentials_secret_uri,
    }

    # create azure container
    logger.info(
        f"Creating container {azure_credentials['container']} with path {azure_credentials['path']}"
    )
    # First delete container
    delete_azure_container(azure_credentials["container"])
    sleep(10)
    # Setup container
    setup_azure_container_for_history_server(
        azure_credentials["container"], azure_credentials["path"]
    )
    logger.info("Azure container and path correctly setup!")

    # apply new configuration options
    logger.info("Setting up configuration for azure-storage-integrator charm...")
    await ops_test.model.applications[charm_versions.azure_storage.application_name].set_config(
        configuration_parameters
    )

    logger.info(
        "Waiting for azure-storage-integrator and history-server charm to be idle and active..."
    )
    async with ops_test.fast_forward():
        await ops_test.model.wait_for_idle(
            apps=[
                charm_versions.azure_storage.application_name,
            ],
            status="active",
        )

    logger.info("Relating history server charm with azure-storage-integrator charm")


    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.azure_storage.application_name], timeout=1000
    )

    # wait for active status
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=1000,
    )

    logger.info("Verifying history server has no app entries")

    status = await ops_test.model.get_status()
    address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/0"]["address"]

    for i in range(0, 5):
        try:
            apps = json.loads(
                urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read()
            )
        except Exception:
            apps = []

        if len(apps) == 0:
            break
        else:
            sleep(5)

    assert len(apps) == 0

    logger.info("Setting up spark")

    setup_spark_output = subprocess.check_output(
        f"./tests/integration/setup/setup_spark_azure.sh {azure_credentials['container']} {azure_credentials['path']} {azure_credentials['storage-account']} {azure_credentials['secret-key']}",
        shell=True,
        stderr=None,
    ).decode("utf-8")

    logger.info(f"Setup spark output:\n{setup_spark_output}")

    logger.info("Executing Spark job")

    run_spark_output = subprocess.check_output(
        "./tests/integration/setup/run_spark_job.sh", shell=True, stderr=None
    ).decode("utf-8")

    logger.info(f"Run spark output:\n{run_spark_output}")

    logger.info("Verifying history server has 1 app entry")

    for i in range(0, 5):
        try:
            apps = json.loads(
                urllib.request.urlopen(f"http://{address}:18080/api/v1/applications").read()
            )
        except Exception:
            apps = []

        if len(apps) > 0:
            break
        else:
            sleep(10)

    assert len(apps) == 1


@pytest.mark.abort_on_fail
async def test_ingress(ops_test: OpsTest, charm_versions):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    # Deploy the charm and wait for waiting status
   
    await ops_test.model.wait_for_idle(
        apps=[charm_versions.ingress.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    logger.info("Relating history server charm with ingress")

    

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.ingress.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    action = await ops_test.model.units.get(
        f"{charm_versions.ingress.application_name}/0"
    ).run_action(
        "show-proxied-endpoints",
    )

    ingress_endpoint = json.loads((await action.wait()).results["proxied-endpoints"])[APP_NAME][
        "url"
    ]

    logger.info(f"Querying endpoint: {ingress_endpoint}/api/v1/applications")

    apps = json.loads(urllib.request.urlopen(f"{ingress_endpoint}/api/v1/applications").read())

    assert len(apps) == 1

    logger.info(f"Number of apps: {len(apps)}")


@pytest.mark.abort_on_fail
async def test_oathkeeper(ops_test: OpsTest, charm_versions, azure_credentials):
    """Test the integration of the spark history server with Oathkeeper.

    Assert that the proxied-enpoints of the ingress are protected (err code 401).
    """
    # remove relation between ingress and spark-history server
    await ops_test.model.applications[APP_NAME].remove_relation(
        f"{APP_NAME}:ingress", f"{charm_versions.ingress.application_name}:ingress"
    )

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.ingress.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    # Deploy the oathkeeper charm and wait for waiting status
   

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.oathkeeper.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    # configure Oathkeeper charm
    oathkeeper_configuration_parameters = {"dev": "True"}
    await ops_test.model.applications[charm_versions.oathkeeper.application_name].set_config(
        oathkeeper_configuration_parameters
    )

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.oathkeeper.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )
    # configure ingress to work with Oathkeeper
    ingress_configuration_parameters = {"enable_experimental_forward_auth": "True"}
    # apply new configuration options
    await ops_test.model.applications[charm_versions.ingress.application_name].set_config(
        ingress_configuration_parameters
    )

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.ingress.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )
    # Relate Oathkeeper with the Spark history server charm
    logger.info("Relating the spark history server charm with oathkeeper.")
    # await ops_test.model.add_relation(charm_versions.oathkeeper.application_name, APP_NAME)

    # await ops_test.model.wait_for_idle(
    #     apps=[APP_NAME],
    #     status="blocked",
    #     timeout=300,
    #     idle_period=30,
    # )
    # # relate spark-history-server and ingress
    # await ops_test.model.add_relation(charm_versions.ingress.application_name, APP_NAME)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME, charm_versions.ingress.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    # Relate Oathkeeper with the Ingress charm
    logger.info("Relating the oathkeeper charm with the ingress.")
    # await ops_test.model.add_relation(
    #     f"{charm_versions.ingress.application_name}:experimental-forward-auth",
    #     charm_versions.oathkeeper.application_name,
    # )

    await ops_test.model.wait_for_idle(
        apps=[charm_versions.oathkeeper.application_name, charm_versions.ingress.application_name],
        status="active",
        timeout=300,
        idle_period=30,
    )

    # get proxied endpoint
    action = await ops_test.model.units.get(
        f"{charm_versions.ingress.application_name}/0"
    ).run_action(
        "show-proxied-endpoints",
    )

    ingress_endpoint = json.loads((await action.wait()).results["proxied-endpoints"])[APP_NAME][
        "url"
    ]

    # check that the ingress endpoint is not authorized!
    logger.info(f"Querying endpoint: {ingress_endpoint}")
    try:
        _ = urllib.request.urlopen(ingress_endpoint)
        raise Exception(
            "Successful request.... something is wrong with the protection of the endpoints."
        )
    except urllib.error.HTTPError as e:  # type: ignore
        # Return code error (e.g. 404, 501, ...)
        logger.info("HTTPError: {}".format(e.code))
        # check that the endopoint respond with code 401
        assert e.code == 401

    logger.info(f"Endpoint: {ingress_endpoint} successfully protected.")

    # check that servlet filter is enabled on the unit endpoint
    status = await ops_test.model.get_status()
    address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/0"]["address"]
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

    status = await ops_test.model.get_status()
    address = status["applications"][APP_NAME]["units"][f"{APP_NAME}/0"]["address"]
    req = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
    req.add_header(AUTH_PROXY_HEADERS[1], "xxx")
    apps = json.loads(urllib.request.urlopen(req).read())
    assert len(apps) == 1

    # configure the history server charm with a new authorized user yyy
    authorized_user = "test-user"
    authorized_users = {"authorized-users": authorized_user}
    await ops_test.model.applications[APP_NAME].set_config(authorized_users)

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        timeout=300,
        idle_period=30,
    )

    # check that user admin is not authorized
    try:
        req = urllib.request.Request(f"http://{address}:18080/api/v1/applications")
        req.add_header(AUTH_PROXY_HEADERS[1], "admin")
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
    req1.add_header(AUTH_PROXY_HEADERS[1], authorized_user)
    apps = json.loads(urllib.request.urlopen(req1).read())
    assert len(apps) == 1

    logger.info("Delete azure container!")
    delete_azure_container(azure_credentials["container"])
