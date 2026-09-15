#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import logging

import jubilant

from ..types import IntegrationTestsCharms

logger = logging.getLogger(__name__)


def deploy_istio_control_plane(
    juju: jubilant.Juju,
    charm_versions: IntegrationTestsCharms,
) -> None:
    """Deploy the Istio control plane."""
    logger.info("Deploying Istio control plane")
    juju.deploy(**charm_versions.istio.deploy_dict())
    juju.wait(
        lambda status: jubilant.all_active(status, charm_versions.istio.application_name), delay=5
    )
