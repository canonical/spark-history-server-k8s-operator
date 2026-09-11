#!/usr/bin/env python3
# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

from enum import Enum
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
    istio: CharmVersion
    istio_beacon: CharmVersion
    istio_ingress: CharmVersion
    oathkeeper: CharmVersion
    oauth2proxy: CharmVersion
    azure_storage: CharmVersion
    loki: CharmVersion
    grafana_agent: CharmVersion
    otel_collector: CharmVersion
    self_signed_certificate: CharmVersion
    postgresql: CharmVersion
    hydra: CharmVersion
    kratos: CharmVersion
    identity_platform_login_ui_operator: CharmVersion
    kratos_external_idp_integrator: CharmVersion


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


S3Info = TypedDict(
    "S3Info",
    {
        "endpoint": str,
        "access_key": str,
        "secret_key": str,
        "bucket": str,
        "path": str,
        "ca_bundle_path": str,
    },
)


class IngressMode(Enum):
    NONE = "none"
    TRAEFIK = "traefik"
    ISTIO_INGRESS = "istio-ingress"
