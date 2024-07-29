#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.
import os
from typing import Optional

import pytest
from pydantic import BaseModel
from pytest_operator.plugin import OpsTest


class CharmVersion(BaseModel):
    """Identifiable for specifying a version of a charm to be deployed.

    Attrs:
        name: str, representing the charm to be deployed
        channel: str, representing the channel to be used
        series: str, representing the series of the system for the container where the charm
            is deployed to
        num_units: int, number of units for the deployment
    """

    name: str
    channel: str
    series: str
    num_units: int = 1
    alias: Optional[str] = None

    @property
    def application_name(self) -> str:
        return self.alias or self.name

    def deploy_dict(self):
        return {
            "entity_url": self.name,
            "channel": self.channel,
            "series": self.series,
            "num_units": self.num_units,
            "application_name": self.application_name,
        }


class IntegrationTestsCharms(BaseModel):
    s3: CharmVersion
    ingress: CharmVersion
    oathkeeper: CharmVersion
    azure_storage: CharmVersion


@pytest.fixture
def charm_versions() -> IntegrationTestsCharms:
    return IntegrationTestsCharms(
        s3=CharmVersion(
            **{"name": "s3-integrator", "channel": "edge", "series": "jammy", "alias": "s3"}
        ),
        ingress=CharmVersion(
            **{
                "name": "traefik-k8s",
                "channel": "edge",
                "series": "focal",
            }
        ),
        oathkeeper=CharmVersion(
            **{
                "name": "oathkeeper",
                "channel": "edge",
                "series": "jammy",
            }
        ),
        azure_storage=CharmVersion(
            **{
                "name": "azure-storage-integrator",
                "channel": "edge",
                "series": "jammy",
                "alias": "azure-storage",
            }
        ),
    )


@pytest.fixture(scope="module")
def azure_credentials(ops_test: OpsTest):
    return {
        "container": "test-container",
        "path": "spark-events",
        "storage-account": os.environ["AZURE_STORAGE_ACCOUNT"],
        "connection-protocol": "abfss",
        "secret-key": os.environ["AZURE_STORAGE_KEY"],
    }
