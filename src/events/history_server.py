#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark History Server workload related event handlers."""

import ops
from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from ops import ConfigChangedEvent
from ops.charm import CharmBase

from common.utils import WithLogging
from core.context import Context
from core.workload import SparkHistoryWorkloadBase
from managers.history_server import HistoryServerManager


class _CharmStatuses:
    """Generic status objects related to the charm."""

    ACTIVE_IDLE = StatusObject(status="active", message="")

    MISSING_STORAGE_RELATION = StatusObject(
        status="blocked",
        message="Missing relation with object storage",
        action="Integrate with S3 or Azure Storage",
    )

    MULTIPLE_AUTH_PROXY_RELATIONS = StatusObject(
        status="blocked",
        message="Too many auth proxy integrations",
        action="Keep only one Oauth2proxy or Authkeeper integration",
    )

    MULTIPLE_STORAGE_RELATIONS = StatusObject(
        status="blocked",
        message="Too many object storages",
        action="Keep only one object storage integration",
    )

    NOT_RUNNING = StatusObject(status="waiting", message="History server is not serving running")

    WAITING_PEBBLE = StatusObject(status="maintenance", message="Waiting for Pebble")


CharmStatuses = _CharmStatuses()


class HistoryServerEvents(ops.Object, WithLogging, ManagerStatusProtocol):
    """Class implementing Spark History Server event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: SparkHistoryWorkloadBase):
        super().__init__(charm, "history-server")

        self.name = "history-server"
        self.state = context

        self.charm = charm
        self.context = context
        self.workload = workload

        self.history_server = HistoryServerManager(self.context, self.workload)

        self.framework.observe(
            self.charm.on.spark_history_server_pebble_ready,
            self._on_spark_history_server_pebble_ready,
        )
        self.framework.observe(self.charm.on.update_status, self._update_event)
        self.framework.observe(self.charm.on.install, self._update_event)
        self.framework.observe(self.charm.on.config_changed, self._on_config_changed)

    def _on_spark_history_server_pebble_ready(self, event):
        """Handle on Pebble ready event."""
        self.logger.info("Pebble ready")
        self.history_server.update()

    def _update_event(self, _) -> None:
        self.history_server.update()

    def _on_config_changed(self, _: ConfigChangedEvent):
        """Handle the on config changed event."""
        self.logger.info("On config changed event.")
        self.history_server.update()

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        statuses = []

        if not self.workload.ready():
            statuses.append(CharmStatuses.WAITING_PEBBLE)

        if not self.context.s3_relation and not self.context.azure_storage_relation:
            statuses.append(CharmStatuses.MISSING_STORAGE_RELATION)

        if self.context.s3_relation and self.context.azure_storage_relation:
            statuses.append(CharmStatuses.MULTIPLE_STORAGE_RELATIONS)

        if self.context.oauth2_proxy_relation and self.context.oathkeeper_relation:
            statuses.append(CharmStatuses.MULTIPLE_AUTH_PROXY_RELATIONS)

        return statuses
