#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from functools import wraps
from typing import Callable

from ops import CharmBase, EventBase, Object, StatusBase

from core.context import AuthProxyConfig, Context, IngressUrl, S3ConnectionInfo, Status
from core.domain import AzureStorageConnectionInfo
from core.workload import SparkHistoryWorkloadBase
from managers.s3 import S3Manager


class BaseEventHandler(Object):
    """Base class for all Event Handler classes in the Spark History Server."""

    workload: SparkHistoryWorkloadBase
    charm: CharmBase
    context: Context

    def get_app_status(
        self,
        s3: S3ConnectionInfo | None,
        azure: AzureStorageConnectionInfo | None,
        ingress: IngressUrl | None,
        oathkeeper: AuthProxyConfig | None,
    ) -> StatusBase:
        """Return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        s3 = s3

        if not s3 and not azure:
            return Status.MISSING_STORAGE_RELATION.value

        if s3 and azure:
            return Status.MULTIPLE_OBJECT_STORAGE_RELATIONS.value

        if s3:
            s3_manager = S3Manager(s3)
            if not s3_manager.verify():
                return Status.INVALID_S3_CREDENTIALS.value

        if not self.workload.active():
            return Status.NOT_RUNNING.value

        if oathkeeper and not ingress:
            return Status.MISSING_INGRESS_RELATION.value

        return Status.ACTIVE.value


def compute_status(
    hook: Callable[[BaseEventHandler, EventBase], None]
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to automatically compute statuses at the end of the hook."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return output after resetting statuses."""
        res = hook(event_handler, event)
        if event_handler.charm.unit.is_leader():
            event_handler.charm.app.status = event_handler.get_app_status(
                event_handler.context.s3,
                event_handler.context.azure_storage,
                event_handler.context.ingress,
                event_handler.context.auth_proxy_config,
            )
        event_handler.charm.unit.status = event_handler.get_app_status(
            event_handler.context.s3,
            event_handler.context.azure_storage,
            event_handler.context.ingress,
            event_handler.context.auth_proxy_config,
        )
        return res

    return wrapper_hook


def defer_when_not_ready(
    hook: Callable[[BaseEventHandler, EventBase], None]
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to automatically compute statuses at the end of the hook."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return output after resetting statuses."""
        if not event_handler.workload.ready():
            event.defer()
            return None
        return hook(event_handler, event)

    return wrapper_hook
