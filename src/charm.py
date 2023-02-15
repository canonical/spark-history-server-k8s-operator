#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""


import errno
import os
from typing import MutableMapping, Optional

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from ops.charm import (
    CharmBase,
    HookEvent,
    InstallEvent,
    RelationBrokenEvent,
    RelationChangedEvent,
    RelationDepartedEvent,
    RelationJoinedEvent,
)
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, Relation, WaitingStatus

from config import SparkHistoryServerConfig
from constants import (
    CONTAINER,
    CONTAINER_LAYER,
    PEER,
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
        self.spark_config = SparkHistoryServerConfig(self.app_peer_data, self.model.config)

        self.s3_creds_client = S3Requirer(self, S3_INTEGRATOR_REL)
        self.framework.observe(
            self.s3_creds_client.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(
            self.s3_creds_client.on.credentials_gone, self._on_s3_credential_gone
        )
        self.framework.observe(
            self.on[S3_INTEGRATOR_REL].relation_joined, self._on_s3_credential_relation_joined
        )
        self.framework.observe(
            self.on[S3_INTEGRATOR_REL].relation_departed, self._on_s3_credential_relation_departed
        )
        self.framework.observe(self.on[PEER].relation_changed, self._on_peer_relation_changed)
        self.framework.observe(self.on[PEER].relation_departed, self._on_peer_relation_departed)
        self.framework.observe(self.on[PEER].relation_broken, self._on_peer_relation_broken)
        self.framework.observe(self.on.config_changed, self._on_model_config_changed)

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

        self.logger.debug(
            "Spark configuration changed to '%s'",
            self.spark_config.contents,
        )

    def push_s3_credentials_to_container(self, event: HookEvent) -> None:
        """Apply s3 credentials to container if pebble is ready."""
        container = self.unit.get_container(CONTAINER)
        if container.can_connect():
            try:
                self.apply_s3_credentials()
            except FileNotFoundError:
                self.unit.status = BlockedStatus("Missing service configuration")
                return

            self.unit.status = ActiveStatus(f"Spark log directory: {self.spark_config.s3_log_dir}")
        else:
            # We were unable to connect to the Pebble API, so we defer this event
            event.defer()
            self.unit.status = WaitingStatus("Waiting for Pebble API")

    def refresh_cached_s3_credentials(self) -> None:
        """Refresh cached credentials."""
        if not self.unit.is_leader():
            return

        credentials = self.s3_creds_client.get_s3_connection_info()
        if not self.peers:
            return
        self.spark_config.update_conn_config(credentials)
        self.logger.debug(f"Updated s3 relation credentials: {self.spark_config.spark_conf}")

    def purge_cached_s3_credentials(self) -> None:
        """Purge cached credentials."""
        if not self.unit.is_leader():
            return

        if not self.peers:
            return
        self.spark_config.purge_conn_config()
        self.logger.debug(f"Purged s3 relation credentials: {self.spark_config.contents}")

    def verify_s3_credentials_in_relation(self) -> bool:
        """Verify cached credentials coming from relation."""
        credentials = self.s3_creds_client.get_s3_connection_info()

        if not self.peers:
            return False

        return self.spark_config.verify_conn_config(credentials)

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
        self.logger.info("===============================================")
        self.logger.info(self.spark_config.contents)
        self.logger.info("===============================================")

    def _on_peer_relation_changed(self, event: RelationChangedEvent):
        """Handle the `RelationChangedEvent` event for History Server peers."""
        if not self.verify_s3_credentials_in_relation():
            self.logger.warning("Configuration change received but incomplete...")

        self.push_s3_credentials_to_container(event)

        if not self.s3_relation:
            self.logger.debug(self.spark_config.contents)
            self.unit.status = BlockedStatus("Pebble ready, waiting for Spark Configuration")
            return

    def _on_peer_relation_departed(self, event: RelationDepartedEvent):
        """Handle the `RelationDepartedEvent` event for History Server peers."""
        pass

    def _on_peer_relation_broken(self, event: RelationBrokenEvent):
        """Handle the `RelationBrokenEvent` event for History Server peers."""
        pass

    def _on_s3_credential_changed(self, event: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.refresh_cached_s3_credentials()

    def _on_s3_credential_gone(self, event: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event from S3 integrator."""
        self.purge_cached_s3_credentials()

    def _on_s3_credential_relation_joined(self, event: RelationJoinedEvent):
        """Handle the `RelationJoinedEvent` event for S3 integrator."""
        if not self.verify_s3_credentials_in_relation():
            self.logger.warning("S3 credentials not yet populated!")
            return
        else:
            self.refresh_cached_s3_credentials()

    def _on_s3_credential_relation_departed(self, event: RelationDepartedEvent):
        """Handle the `RelationDepartedEvent` event for S3 integrator."""
        self.purge_cached_s3_credentials()

    @property
    def peers(self) -> Optional[Relation]:
        """The cluster peer relation."""
        return self.model.get_relation(PEER)

    @property
    def app_peer_data(self) -> MutableMapping[str, str]:
        """Application peer relation data object."""
        if not self.peers:
            return {}

        return self.peers.data[self.app]

    @property
    def unit_peer_data(self) -> MutableMapping[str, str]:
        """Unit peer relation data object."""
        if not self.peers:
            return {}

        return self.peers.data[self.unit]

    @property
    def s3_relation(self) -> Optional[Relation]:
        """The cluster peer relation."""
        return self.model.get_relation(S3_INTEGRATOR_REL)


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
