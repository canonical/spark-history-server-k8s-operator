#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import os
from pathlib import Path

import jubilant
import pytest

from .types import AzureInfo, CharmVersion, IntegrationTestsCharms


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    keep_models = bool(request.config.getoption("--keep-models"))

    with jubilant.temp_model(keep=keep_models) as juju:
        juju.wait_timeout = 10 * 60

        yield juju  # run the test

        if request.session.testsfailed:
            log = juju.debug_log(limit=30)
            print(log, end="")


def pytest_addoption(parser):
    parser.addoption(
        "--keep-models",
        action="store_true",
        default=False,
        help="keep temporarily-created models",
    )


@pytest.fixture
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        s3=CharmVersion(
            name="s3-integrator",
            channel="edge",
            base="ubuntu@22.04",
        ),
        ingress=CharmVersion(name="traefik-k8s", channel="edge", base="ubuntu@20.04", trust=True),
        oathkeeper=CharmVersion(
            name="oathkeeper",
            channel="edge",
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
            channel="latest/stable",
            base="ubuntu@22.04",
            alias="grafana-agent-k8s",
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
def history_server_charm() -> Path:
    """Path to the packed kyuubi charm."""
    if not (path := next(iter(Path.cwd().glob("*.charm")), None)):
        raise FileNotFoundError("Could not find packed kyuubi charm.")

    return path
