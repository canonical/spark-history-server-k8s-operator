#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 Integration related event handlers."""

from data_platform_helpers.advanced_statuses.models import StatusObject
from data_platform_helpers.advanced_statuses.protocol import ManagerStatusProtocol
from data_platform_helpers.advanced_statuses.types import Scope
from object_storage import StorageConnectionInfoChangedEvent, StorageConnectionInfoGoneEvent
from ops import CharmBase

from common.utils import WithLogging
from core.context import Context
from core.workload import SparkHistoryWorkloadBase
from events.base import BaseEventHandler, defer_when_not_ready
from managers.history_server import HistoryServerManager


class _AzureStorageStatuses:
    """Status objects related to the object storage integration."""

    OBJECT_STORAGE_NOT_READY = StatusObject(
        status="waiting",
        message="Waiting for object storage relation data",
    )

    @staticmethod
    def missing_parameters(fields: list[str]) -> StatusObject:
        """Return a status for missing object storage relation data."""
        fields_str = ", ".join(f"'{field}'" for field in fields)
        return StatusObject(
            status="waiting",
            message=f"Missing object storage parameter(s): {fields_str}",
            action=f"Set object storage parameter(s): {fields_str}",
        )


AzureStorageStatuses = _AzureStorageStatuses()


class AzureStorageEvents(BaseEventHandler, WithLogging, ManagerStatusProtocol):
    """Class implementing Azure Integration event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: SparkHistoryWorkloadBase):
        super().__init__(charm, "azure-storage")

        self.name = "azure-storage"
        self.state = context

        self.charm = charm
        self.context = context
        self.workload = workload

        self.history_server = HistoryServerManager(self.context, self.workload)

        self.azure_storage_requirer = self.context.azure_storage_requirer
        self.framework.observe(
            self.azure_storage_requirer.on.storage_connection_info_changed,
            self._on_azure_storage_connection_info_changed,
        )
        self.framework.observe(
            self.azure_storage_requirer.on.storage_connection_info_gone,
            self._on_azure_storage_connection_info_gone,
        )

    @defer_when_not_ready
    def _on_azure_storage_connection_info_changed(self, _: StorageConnectionInfoChangedEvent):
        """Handle the `StorageConnectionInfoChangedEvent` event from Object Storage integrator."""
        self.logger.info("Azure Storage connection info changed")
        self.history_server.update(
            self.context.s3,
            self.context.azure_storage,
            self.context.ingress,
            self.context.authorized_users,
        )

    @defer_when_not_ready
    def _on_azure_storage_connection_info_gone(self, _: StorageConnectionInfoGoneEvent):
        """Handle the `StorageConnectionInfoGoneEvent` event for Object Storage integrator."""
        self.logger.info("Azure Storage connection info gone")
        self.history_server.update(
            self.context.s3,
            None,
            self.context.ingress,
            self.context.authorized_users,
        )

    def get_statuses(self, scope: Scope, recompute: bool = False) -> list[StatusObject]:
        """Return the list of statuses for this component."""
        if not self.context.azure_storage_relation:
            return []

        if not (azure_storage_info := self.context.azure_storage):
            return [AzureStorageStatuses.OBJECT_STORAGE_NOT_READY]

        if not azure_storage_info.path:
            return [AzureStorageStatuses.missing_parameters(fields=["path"])]

        return []
