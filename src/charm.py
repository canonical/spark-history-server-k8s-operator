#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Charmed Kubernetes Operator for Apache Spark History Server."""

import logging

from constants import (
    CONFIG_KEY_S3_CREDS_PROVIDER,
    CONFIG_KEY_S3_ACCESS_KEY,
    CONFIG_KEY_S3_ENDPOINT,
    CONFIG_KEY_S3_LOGS_DIR,
    CONFIG_KEY_S3_SECRET_KEY,
    CONTAINER,
    CONTAINER_LAYER,
    SPARK_HISTORY_SERVER_LAUNCH_CMD,
    SPARK_PROPERTIES_FILE,
    SPARK_USER,
    SPARK_USER_GID,
    SPARK_USER_GROUP,
    SPARK_USER_UID,
    SPARK_USER_WORKDIR,
)
from ops.charm import (
    CharmBase,
    InstallEvent,
)
from ops.main import main
from ops.model import ActiveStatus, BlockedStatus, WaitingStatus

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


class SparkHistoryServerCharm(CharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(
            self.on.spark_history_server_pebble_ready, self._on_spark_history_server_pebble_ready
        )
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.install, self._on_install)
        self.spark_config = None

    def _on_spark_history_server_pebble_ready(self, event):
        """Define and start a workload using the Pebble API.

        Change this example to suit your needs. You'll need to specify the right entrypoint and
        environment configuration for your specific workload.

        Learn more about interacting with Pebble at at https://juju.is/docs/sdk/pebble.
        """
        # Get a reference the container attribute on the PebbleReadyEvent
        container = event.workload
        contents = f"spark.eventLog.enabled=false"
        container.push(SPARK_PROPERTIES_FILE, contents, make_dirs=True)
        # Add initial Pebble config layer using the Pebble API
        container.add_layer(CONTAINER, self._spark_history_server_layer, combine=True)
        # Make Pebble reevaluate its plan, ensuring any services are started if enabled.
        container.replan()
        # Learn more about statuses in the SDK docs:
        # https://juju.is/docs/sdk/constructs#heading--statuses
        self.unit.status = WaitingStatus("Pebble ready, waiting for Spark Configuration")

    def _on_config_changed(self, event):
        """Handle changed configuration.

        Change this example to suit your needs. If you don't need to handle config, you can remove
        this method.

        Learn more about config at https://juju.is/docs/sdk/config
        """
        # The config is good, so update the configuration of the workload
        container = self.unit.get_container(CONTAINER)
        # Verify that we can connect to the Pebble API in the workload container
        if container.can_connect():
            s3_endpoint = self.model.config[CONFIG_KEY_S3_ENDPOINT]
            s3_access_key = self.model.config[CONFIG_KEY_S3_ACCESS_KEY]
            s3_secret_key = self.model.config[CONFIG_KEY_S3_SECRET_KEY]
            spark_logs_dir = self.model.config[CONFIG_KEY_S3_LOGS_DIR]
            s3_creds_provider = self.model.config[CONFIG_KEY_S3_CREDS_PROVIDER]

            self.spark_config = f"spark.hadoop.fs.s3a.endpoint={s3_endpoint}"
            self.spark_config += "\n"
            self.spark_config += f"spark.hadoop.fs.s3a.access.key={s3_access_key}"
            self.spark_config += "\n"
            self.spark_config += f"spark.hadoop.fs.s3a.secret.key={s3_secret_key}"
            self.spark_config += "\n"
            self.spark_config += f"spark.eventLog.dir={spark_logs_dir}"
            self.spark_config += "\n"
            self.spark_config += f"spark.history.fs.logDirectory={spark_logs_dir}"
            self.spark_config += "\n"
            self.spark_config += (
                f"spark.hadoop.fs.s3a.aws.credentials.provider={s3_creds_provider}"
            )
            self.spark_config += "\n"
            self.spark_config += "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
            self.spark_config += "\n"
            self.spark_config += "spark.hadoop.fs.s3a.path.style.access=true"
            self.spark_config += "\n"
            self.spark_config += "spark.eventLog.enabled=true"

            container.push(
                SPARK_PROPERTIES_FILE,
                self.spark_config,
                user_id=SPARK_USER_UID,
                group_id=SPARK_USER_GID,
                make_dirs=True,
            )

            if not container.exists(SPARK_PROPERTIES_FILE):
                self.unit.status = BlockedStatus(
                    "Spark configuration push error. Please retry configuration..."
                )
                return

            # Push an updated layer with the new config
            container.add_layer(CONTAINER_LAYER, self._spark_history_server_layer, combine=True)
            container.replan()

            logger.debug("Spark configuration changed to '%s'", spark_logs_dir)
            self.unit.status = ActiveStatus(f"{spark_logs_dir}")
        else:
            # We were unable to connect to the Pebble API, so we defer this event
            event.defer()
            self.unit.status = WaitingStatus("Waiting for Pebble API")

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


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
