#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""

from charms.grafana_k8s.v0.grafana_dashboard import GrafanaDashboardProvider
from charms.loki_k8s.v1.loki_push_api import LogForwarder
from charms.prometheus_k8s.v0.prometheus_scrape import MetricsEndpointProvider
from ops import CharmBase
from ops.main import main

from common.utils import WithLogging
from constants import CONTAINER, JMX_CC_PORT, JMX_EXPORTER_PORT, METRICS_RULES_DIR, PEBBLE_USER
from core.context import Context
from core.domain import User
from events.azure_storage import AzureStorageEvents
from events.history_server import HistoryServerEvents
from events.ingress import IngressEvents
from events.s3 import S3Events
from workload import SparkHistoryServer


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


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
