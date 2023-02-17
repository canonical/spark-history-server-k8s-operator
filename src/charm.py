#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""


import errno
import os
from typing import Optional

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from ops.charm import (
    CharmBase,
    HookEvent,
    InstallEvent,
)
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, Relation, WaitingStatus

from config import SparkHistoryServerConfig
from constants import (
    CONTAINER,
    CONTAINER_LAYER,
    S3_INTEGRATOR_REL,
    SPARK_HISTORY_SERVER_LAUNCH_CMD,
    SPARK_PROPERTIES_FILE,
    SPARK_USER,
    SPARK_USER_GID,
    SPARK_USER_GROUP,
    SPARK_USER_UID,
    SPARK_USER_WORKDIR,
)
from utils import WithLogging


class SparkHistoryServerCharm(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(
            self.on.spark_history_server_pebble_ready, self._on_spark_history_server_pebble_ready
        )
        self.framework.observe(self.on.install, self._on_install)
        self.s3_creds_client = S3Requirer(self, S3_INTEGRATOR_REL)
        self.framework.observe(
            self.s3_creds_client.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(
            self.s3_creds_client.on.credentials_gone, self._on_s3_credential_gone
        )
        self.framework.observe(self.on.config_changed, self._on_model_config_changed)

        self.spark_config = SparkHistoryServerConfig(self.s3_creds_client, self.model.config)

    def _on_spark_history_server_pebble_ready(self, event):
        """Define and start a workload using the Pebble API."""
        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
        container.push(SPARK_PROPERTIES_FILE, self.spark_config.contents, make_dirs=True)
        # Add initial Pebble config layer using the Pebble API
        container.add_layer(CONTAINER, self._spark_history_server_layer, combine=True)
        # Make Pebble reevaluate its plan, ensuring any services are started if enabled.
        container.replan()
        self.unit.status = BlockedStatus("Pebble ready, waiting for Spark Configuration")

    def apply_s3_credentials(self) -> None:
        """Apply s3 credentials to container."""
        container = self.unit.get_container(CONTAINER)

        container.push(
            SPARK_PROPERTIES_FILE,
            self.spark_config.contents,
            user_id=SPARK_USER_UID,
            group_id=SPARK_USER_GID,
            make_dirs=True,
        )

        if not container.exists(SPARK_PROPERTIES_FILE):
            self.logger.error(f"{SPARK_PROPERTIES_FILE} not found")
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), SPARK_PROPERTIES_FILE)

        # Push an updated layer with the new config
        container.add_layer(CONTAINER_LAYER, self._spark_history_server_layer, combine=True)
        container.restart(CONTAINER)

    def push_s3_credentials_to_container(self, event: HookEvent) -> None:
        """Apply s3 credentials to container if pebble is ready."""
        container = self.unit.get_container(CONTAINER)
        if container.can_connect():
            try:
                self.apply_s3_credentials()
            except FileNotFoundError:
                self.unit.status = BlockedStatus("Missing service configuration")
                return

            self.unit.status = ActiveStatus()
        else:
            # We were unable to connect to the Pebble API, so we defer this event
            event.defer()
            self.unit.status = WaitingStatus("Waiting for Pebble API")

    def refresh_cached_s3_credentials(self, event: HookEvent) -> None:
        """Refresh cached credentials."""
        self.push_s3_credentials_to_container(event)

    def verify_s3_credentials_in_relation(self) -> bool:
        """Verify cached credentials coming from relation."""
        if not self.s3_relation:
            return False

        return self.spark_config.verify_conn_config()

    @property
    def _spark_history_server_layer(self):
        """Return a dictionary representing a Pebble layer."""
        return {
            "summary": "spark history server layer",
            "description": "pebble config layer for spark history server",
            "services": {
                CONTAINER: {
                    "override": "replace",
                    "summary": "spark history server",
                    "command": SPARK_HISTORY_SERVER_LAUNCH_CMD,
                    "user": SPARK_USER,
                    "group": SPARK_USER_GROUP,
                    "working_dir": SPARK_USER_WORKDIR,
                    "startup": "enabled",
                    "environment": {"SPARK_NO_DAEMONIZE": "true"},
                }
            },
        }

    def _on_install(self, event: InstallEvent) -> None:
        """Handle the `on_install` event."""
        self.unit.status = WaitingStatus("Waiting for Pebble")

    def _on_model_config_changed(self, event: HookEvent) -> None:
        """Handle the `on_config_changed` event."""
        self.refresh_cached_s3_credentials(event)

    def _on_s3_credential_changed(self, event: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.refresh_cached_s3_credentials(event)

    def _on_s3_credential_gone(self, event: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.refresh_cached_s3_credentials(event)
        self.unit.status = BlockedStatus("Pebble ready, waiting for Spark Configuration")

    @property
    def s3_relation(self) -> Optional[Relation]:
        """The cluster peer relation."""
        return self.model.get_relation(S3_INTEGRATOR_REL)


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
