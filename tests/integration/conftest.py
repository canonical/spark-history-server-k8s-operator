#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import os
from pathlib import Path
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


@pytest.fixture
def ops_test(ops_test: OpsTest) -> OpsTest:
    if os.environ.get("CI") == "true":
        # Running in GitHub Actions; skip build step
        # (GitHub Actions uses a separate, cached build step. See .github/workflows/ci.yaml)
        packed_charms = json.loads(os.environ["CI_PACKED_CHARMS"])

        async def build_charm(charm_path, bases_index: int = None) -> Path:
            for charm in packed_charms:
                if Path(charm_path) == Path(charm["directory_path"]):
                    if bases_index is None or bases_index == charm["bases_index"]:
                        return charm["file_path"]
            raise ValueError(f"Unable to find .charm file for {bases_index=} at {charm_path=}")

        ops_test.build_charm = build_charm
    return ops_test


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
    )
