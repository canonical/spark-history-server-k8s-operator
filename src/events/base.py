#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from functools import wraps
from typing import Callable

from ops import CharmBase, EventBase, Object, StatusBase

from core.context import (
    AuthProxyConfig,
    Context,
    IngressUrl,
    OathkeeperAuthProxyConfig,
    S3ConnectionInfo,
    Status,
)
from core.domain import AzureStorageConnectionInfo
from core.workload import SparkHistoryWorkloadBase
from managers.azure_storage import AzureStorageManager
from managers.s3 import S3Manager, S3VerificationResult


class BaseEventHandler(Object):
    """Base class for all Event Handler classes in the Spark History Server."""

    workload: SparkHistoryWorkloadBase
    charm: CharmBase
    context: Context

    def _get_s3_status(self, s3: S3ConnectionInfo | None) -> StatusBase | None:
        """Return a status for S3-specific errors when present."""
        if not s3:
            return None

        status_by_result = {
            S3VerificationResult.MISSING_PATH: Status.MISSING_STORAGE_PATH.value,
            S3VerificationResult.INVALID_CREDENTIALS: Status.INVALID_STORAGE_CREDENTIALS.value,
            S3VerificationResult.SSL_ERROR: Status.OBJECT_STORAGE_SSL_ERROR.value,
            S3VerificationResult.PROXY_ERROR: Status.OBJECT_STORAGE_PROXY_ERROR.value,
            S3VerificationResult.ENDPOINT_UNREACHABLE: Status.OBJECT_STORAGE_ENDPOINT_UNREACHABLE.value,
            S3VerificationResult.UNKNOWN_ERROR: Status.OBJECT_STORAGE_UNKNOWN_ERROR.value,
        }
        return status_by_result.get(S3Manager(s3).verify_result())

    def get_app_status(
        self,
        s3: S3ConnectionInfo | None,
        azure: AzureStorageConnectionInfo | None,
        ingress: IngressUrl | None,
        auth_proxy: OathkeeperAuthProxyConfig | None,
        oauth2_proxy: AuthProxyConfig | None,
    ) -> StatusBase:
        """Return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        if not s3 and not azure:
            return Status.MISSING_STORAGE_RELATION.value

        if s3 and azure:
            return Status.MULTIPLE_OBJECT_STORAGE_RELATIONS.value

        if not getattr(s3, "path", None) and not getattr(azure, "path", None):
            # We already assessed that one of the two is present
            return Status.MISSING_STORAGE_PATH.value

        if s3_status := self._get_s3_status(s3):
            return s3_status

        if azure and not AzureStorageManager(azure).verify():
            return Status.INVALID_STORAGE_CREDENTIALS.value

        if not self.workload.active():
            return Status.NOT_RUNNING.value

        if auth_proxy and oauth2_proxy:
            return Status.MULTIPLE_AUTH_PROXY_RELATIONS.value

        if (auth_proxy or oauth2_proxy) and not ingress:
            return Status.MISSING_INGRESS_RELATION.value

        return Status.ACTIVE.value


def compute_status(hook: Callable) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to automatically compute statuses at the end of the hook."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return output after resetting statuses."""
        res = hook(event_handler, event)
        status = event_handler.get_app_status(
            event_handler.context.s3,
            event_handler.context.azure_storage,
            event_handler.context.ingress,
            event_handler.context.auth_proxy_config,
            event_handler.context.oauth2_proxy_config,
        )
        if event_handler.charm.unit.is_leader():
            event_handler.charm.app.status = status
        event_handler.charm.unit.status = status
        return res

    return wrapper_hook


def defer_when_not_ready(
    hook: Callable,
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
