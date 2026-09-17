#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

"""Service Mesh Integration related event handlers."""

from charms.istio_beacon_k8s.v0.service_mesh import ServiceMeshConsumer, UnitPolicy
from ops import CharmBase

from common.utils import WithLogging
from constants import (
    JMX_CC_PORT,
    JMX_EXPORTER_PORT,
    METRICS_RELATION_NAME,
    SERVICE_MESH_RELATION_NAME,
)
from core.context import Context
from core.workload import SparkHistoryWorkloadBase
from events.base import BaseEventHandler


class ServiceMeshEvents(BaseEventHandler, WithLogging):
    """Class implementing Ambient Service Mesh event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: SparkHistoryWorkloadBase):
        super().__init__(charm, "service-mesh")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.service_mesh = ServiceMeshConsumer(
            self.charm,
            mesh_relation_name=SERVICE_MESH_RELATION_NAME,
            policies=[
                UnitPolicy(relation=METRICS_RELATION_NAME, ports=[JMX_CC_PORT, JMX_EXPORTER_PORT])
            ],
        )
