#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""

from object_storage import StorageConnectionInfoChangedEvent, StorageConnectionInfoGoneEvent
from ops import CharmBase

from common.utils import WithLogging
from core.context import Context
from core.workload import SparkHistoryWorkloadBase
from events.base import BaseEventHandler, compute_status, defer_when_not_ready
from managers.history_server import HistoryServerManager


class S3Events(BaseEventHandler, WithLogging):
    """Class implementing S3 Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: SparkHistoryWorkloadBase):
        super().__init__(charm, "s3")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.history_server = HistoryServerManager(self.context, self.workload)

        self.s3_requirer = self.context.s3_requirer
        self.framework.observe(
            self.s3_requirer.on.storage_connection_info_changed, self._on_s3_credential_changed
        )
        self.framework.observe(
            self.s3_requirer.on.storage_connection_info_gone, self._on_s3_credential_gone
        )

    @compute_status
    @defer_when_not_ready
    def _on_s3_credential_changed(self, _: StorageConnectionInfoChangedEvent):
        """Handle the `StorageConnectionInfoChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.history_server.update(
            self.context.s3,
            self.context.azure_storage,
            self.context.ingress,
            self.context.authorized_users,
        )

    @defer_when_not_ready
    def _on_s3_credential_gone(self, _: StorageConnectionInfoGoneEvent):
        """Handle the `StorageConnectionInfoGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.history_server.update(
            None,
            self.context.azure_storage,
            self.context.ingress,
            self.context.authorized_users,
        )

        self.charm.unit.status = self.get_app_status(
            None,
            self.context.azure_storage,
            self.context.ingress,
            self.context.auth_proxy_config,
            self.context.oauth2_proxy_config,
        )
        if self.charm.unit.is_leader():
            self.charm.app.status = self.get_app_status(
                None,
                self.context.azure_storage,
                self.context.ingress,
                self.context.auth_proxy_config,
                self.context.oauth2_proxy_config,
            )
