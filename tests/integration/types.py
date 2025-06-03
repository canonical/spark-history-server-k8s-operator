#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.
from typing import TypedDict

from pydantic import BaseModel


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
    base: str
    num_units: int = 1
    alias: str | None = None
    trust: bool = False

    @property
    def application_name(self) -> str:
        return self.alias or self.name

    def deploy_dict(self):
        return {
            "charm": self.name,
            "channel": self.channel,
            "base": self.base,
            "num_units": self.num_units,
            "app": self.application_name,
            "trust": self.trust,
        }


class IntegrationTestsCharms(BaseModel):
    s3: CharmVersion
    ingress: CharmVersion
    oathkeeper: CharmVersion
    azure_storage: CharmVersion
    loki: CharmVersion
    grafana_agent: CharmVersion


AzureInfo = TypedDict(
    "AzureInfo",
    {
        "container": str,
        "path": str,
        "storage-account": str,
        "connection-protocol": str,
        "secret-key": str,
    },
)
