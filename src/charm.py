#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""

from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v1.loki_push_api import LogForwarder
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from data_platform_helpers.advanced_statuses.handler import StatusHandler
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from ops import CharmBase
from ops.main import main

from common.utils import WithLogging
from constants import (
    CONTAINER,
    HISTORY_SERVER_PORT,
    JMX_CC_PORT,
    JMX_EXPORTER_PORT,
    METRICS_RULES_DIR,
    PEBBLE_USER,
)
from core.context import Context
from core.domain import User
from core.workload import SparkHistoryWorkloadBase
from events.azure_storage import AzureStorageEvents
from events.history_server import CharmStatuses, HistoryServerEvents
from events.ingress import IngressEvents
from events.s3 import S3Events
from events.service_mesh import ServiceMeshEvents
from workload import SparkHistoryServer


class HistoryServerWorkloadStatus(ManagerStatusProtocol):
    """Report generic low-priority history server workload statuses."""

    def __init__(
        self,
        context: Context,
        workload: SparkHistoryWorkloadBase,
    ) -> None:
        self.name = "polaris-workload"
        self.state = context
        self.workload = workload

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return low-priority workload statuses."""
        statuses: list[StatusObject] = [CharmStatuses.ACTIVE_IDLE]
        if not self.workload.active():
            statuses.append(CharmStatuses.NOT_RUNNING)

        return statuses


class SparkHistoryServerCharm(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        self._log_forwarder = LogForwarder(
            self,
            relation_name="logging",  # optional, defaults to "logging"
        )

        self.metrics_endpoint = MetricsEndpointProvider(
            self,
            jobs=[
                {"static_configs": [{"targets": [f"*:{JMX_EXPORTER_PORT}", f"*:{JMX_CC_PORT}"]}]}
            ],
            alert_rules_path=METRICS_RULES_DIR,
        )
        self.grafana_dashboards = GrafanaDashboardProvider(self)

        context = Context(self)

        workload = SparkHistoryServer(
            self.unit.get_container(CONTAINER), User(name=PEBBLE_USER[0], group=PEBBLE_USER[1])
        )

        self.ingress = IngressEvents(self, context, workload)
        self.s3 = S3Events(self, context, workload)
        self.azure_storage = AzureStorageEvents(self, context, workload)
        self.history_server = HistoryServerEvents(self, context, workload)
        self.service_mesh = ServiceMeshEvents(self, context, workload)

        self.unit.set_ports(HISTORY_SERVER_PORT)
        self.history_server_workload_status = HistoryServerWorkloadStatus(context, workload)

        self.status = StatusHandler(
            self,
            self.history_server,
            self.s3,
            self.azure_storage,
            self.ingress,
            self.history_server_workload_status,
        )


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
