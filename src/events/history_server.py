#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""

from ops.charm import (
    CharmBase,
    InstallEvent
)

from common.utils import WithLogging
from core.state import State
from core.workload import SparkHistoryWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.history_server import HistoryServerManager


class HistoryServerEvents(BaseEventHandler, WithLogging):
    """Charm the service."""

    def __init__(self, charm: CharmBase,
                 state: State, workload: SparkHistoryWorkloadBase
                 ):
        super().__init__(charm, "ingress")

        self.charm = charm
        self.state = state
        self.workload = workload

        self.history_server = HistoryServerManager(self.workload)

        self.framework.observe(self.on.install, self._update_event)

        self.framework.observe(
            self.on.spark_history_server_pebble_ready,
            self._on_spark_history_server_pebble_ready,
        )
        self.framework.observe(self.on.update_status, self._update_event)
        self.framework.observe(self.on.install, self._on_install)

    @compute_status
    def _on_spark_history_server_pebble_ready(self, event):
        """Handle on Pebble ready event."""
        self.logger.info("Pebble ready")
        self.history_server.update(self.state.s3, self.state.ingress)

    @compute_status
    def _on_install(self, event: InstallEvent) -> None:
        pass

    @compute_status
    def _update_event(self, _):
        pass
