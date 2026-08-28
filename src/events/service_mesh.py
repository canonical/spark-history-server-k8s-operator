#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Service Mesh Integration related event handlers."""

from ops import CharmBase

from common.utils import WithLogging
from core.context import Context
from core.workload import SparkHistoryWorkloadBase
from events.base import BaseEventHandler

from charms.istio_beacon_k8s.v0.service_mesh import ServiceMeshConsumer

class ServiceMeshEvents(BaseEventHandler, WithLogging):
    """Class implementing Ambient Service Mesh event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: SparkHistoryWorkloadBase):
        super().__init__(charm, "service-mesh")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.service_mesh = ServiceMeshConsumer(self.charm)
