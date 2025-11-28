#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import os
import subprocess
from pathlib import Path
from typing import Iterable

import boto3
import boto3.session
import jubilant
import pytest
from botocore.client import Config
from dotenv import load_dotenv

from .types import AzureInfo, CharmVersion, IntegrationTestsCharms, S3Info

load_dotenv("microceph.source")
load_dotenv()


logger = logging.getLogger(__name__)
BUCKET_NAME = "history-server"
PATH_NAME = "spark-events"


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    keep_models = bool(request.config.getoption("--keep-models"))

    with jubilant.temp_model(keep=keep_models) as juju:
        juju.wait_timeout = 10 * 60

        yield juju  # run the test

        if request.session.testsfailed:
            log = juju.debug_log(limit=30)
            print(log, end="")


# def pytest_addoption(parser):
#     parser.addoption(
#         "--keep-models",
#         action="store_true",
#         default=False,
#         help="keep temporarily-created models",
#     )


@pytest.fixture
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        s3=CharmVersion(
            name="s3-integrator",
            channel="edge",
            base="ubuntu@22.04",
        ),
        ingress=CharmVersion(
            name="traefik-k8s",
            channel="latest/edge",
            base="ubuntu@20.04",
            alias="traefik-k8s",
            trust=True,
        ),
        oauth2proxy=CharmVersion(
            name="oauth2-proxy-k8s",
            channel="latest/stable",
            base="ubuntu@22.04",
        ),
        azure_storage=CharmVersion(
            name="azure-storage-integrator",
            channel="edge",
            base="ubuntu@22.04",
            alias="azure-storage",
        ),
        loki=CharmVersion(
            name="loki-k8s",
            channel="1/stable",
            base="ubuntu@20.04",
            alias="loki",
            trust=True,
        ),
        grafana_agent=CharmVersion(
            name="grafana-agent-k8s",
            channel="1/stable",
            base="ubuntu@22.04",
            alias="grafana-agent-k8s",
            trust=True,
        ),
        self_signed_certificate=CharmVersion(
            name="self-signed-certificates",
            channel="1/stable",
            base="ubuntu@24.04",
            alias="self-signed-certificates",
            trust=True,
        ),
        postgresql=CharmVersion(
            name="postgresql-k8s",
            channel="14/stable",
            base="ubuntu@22.04",
            alias="postgresql",
            trust=True,
        ),
        hydra=CharmVersion(
            name="hydra",
            channel="0.5/edge",
            base="ubuntu@22.04",
            alias="hydra",
            trust=True,
        ),
        kratos=CharmVersion(
            name="kratos",
            channel="0.5/edge",
            base="ubuntu@22.04",
            alias="kratos",
            trust=True,
        ),
        identity_platform_login_ui_operator=CharmVersion(
            name="identity-platform-login-ui-operator",
            channel="0.5/edge",
            base="ubuntu@22.04",
            alias="identity-platform-login-ui-operator",
            trust=True,
        ),
        kratos_external_idp_integrator=CharmVersion(
            name="kratos-external-idp-integrator",
            channel="latest/edge",
            base="ubuntu@22.04",
            alias="kratos-external-idp-integrator",
            trust=True,
        ),
    )


@pytest.fixture(scope="module")
def azure_storage_credentials() -> AzureInfo:
    return {
        "container": "test-container",
        "path": "spark-events",
        "storage-account": os.environ["AZURE_STORAGE_ACCOUNT"],
        "connection-protocol": "abfss",
        "secret-key": os.environ["AZURE_STORAGE_KEY"],
    }


@pytest.fixture(scope="module")
def s3_bucket_and_creds(request: pytest.FixtureRequest) -> Iterable[S3Info]:
    keep_models = bool(request.config.getoption("--keep-models"))

    if any(
        (
            (access_key := os.environ.get("S3_ACCESS_KEY", None)) is None,
            (secret_key := os.environ.get("S3_SECRET_KEY", None)) is None,
            (endpoint_url := os.environ.get("S3_SERVER_URL", None)) is None,
        )
    ):
        logger.info("Cannot find object storage information in environment, looking into minio.")
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

    session = boto3.session.Session(aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    s3 = session.resource(
        service_name="s3",
        endpoint_url=endpoint_url,
        verify=False,
        config=Config(
            connect_timeout=60,
            retries={"max_attempts": 4},
            request_checksum_calculation="when_supported",
            response_checksum_validation="when_supported",
        ),
    )
    test_bucket = s3.Bucket(BUCKET_NAME)

    # Delete test bucket if it exists
    if test_bucket in s3.buckets.all():
        logger.info(f"The bucket {BUCKET_NAME} already exists. Deleting it...")
        for obj in test_bucket.objects.all():
            # We need to iterate over keys because delete_objects (plural) has mandatory checksum
            obj.delete()
        test_bucket.delete()

    # Create the test bucket
    s3.create_bucket(Bucket=BUCKET_NAME)
    logger.info(f"Created bucket: {BUCKET_NAME}")
    test_bucket.put_object(Key=os.path.join(PATH_NAME, "touch"))
    yield {
        "endpoint": str(endpoint_url),
        "access_key": str(access_key),
        "secret_key": str(secret_key),
        "bucket": BUCKET_NAME,
        "path": PATH_NAME,
        "ca_bundle_path": os.environ.get("S3_CA_BUNDLE_PATH", ""),
    }

    if not keep_models:
        logger.info("Tearing down test bucket...")
        for obj in test_bucket.objects.all():
            # We need to iterate over keys because delete_objects (plural) has mandatory checksum
            obj.delete()

        test_bucket.delete()


@pytest.fixture(scope="module")
def history_server_charm() -> Path:
    """Path to the packed history server charm."""
    if not (path := next(iter(Path.cwd().glob("*.charm")), None)):
        raise FileNotFoundError("Could not find packed history server charm.")

    return path
