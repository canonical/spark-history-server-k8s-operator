#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Module containing all business logic related to the workload."""

from ops.model import Container

from common.k8s import K8sWorkload
from common.utils import WithLogging
from core.domain import User
from core.workload import SparkHistoryWorkloadBase, HistoryServerPaths


class SparkHistoryServer(SparkHistoryWorkloadBase, K8sWorkload, WithLogging):
    """Class representing Workload implementation for Spark History server on K8s."""

    CONTAINER = "spark-history-server"
    CONTAINER_LAYER = "spark-history-server"

    HISTORY_SERVER_SERVICE = "history-server"

    def __init__(self, container: Container, user: User):
        self.container = container
        self.user = user

        self.paths = HistoryServerPaths(
            "/etc/spark/conf", "keytool"
        )

        self.spark_history_server_java_config = ""

    @property
    def _spark_history_server_layer(self):
        """Return a dictionary representing a Pebble layer."""
        return {
            "summary": "spark history server layer",
            "description": "pebble config layer for spark history server",
            "services": {
                self.HISTORY_SERVER_SERVICE: {
                    "override": "merge",
                    "summary": "spark history server",
                    "startup": "enabled",
                    "environment": {
                        "SPARK_PROPERTIES_FILE": self.paths.spark_properties,
                        # "SPARK_HISTORY_OPTS": self.spark_history_server_java_config,
                    },
                }
            },
        }

    def start(self):
        """Execute business-logic for starting the workload."""
        services = self.container.get_plan().services

        # ===============
        # THIS IS WORKING
        # ===============
        if services[self.HISTORY_SERVER_SERVICE].startup != "enabled":
            self.logger.info("Adding layer...")
            self.container.add_layer(
                self.CONTAINER_LAYER, self._spark_history_server_layer,
                combine=True
            )
        # ===============

        # ===============
        # THIS WOULD NOT BE WORKING
        # ===============
        # self.container.add_layer(
        #     self.CONTAINER_LAYER, self._spark_history_server_layer, combine=True
        # )
        # ===============

        if not self.exists(str(self.paths.spark_properties)):
            self.logger.error(f"{self.paths.spark_properties} not found")
            raise FileNotFoundError(self.paths.spark_properties)

        # Push an updated layer with the new config
        # self.container.replan()
        self.container.restart(self.HISTORY_SERVER_SERVICE)

    def stop(self):
        """Execute business-logic for stopping the workload."""
        self.container.stop(self.HISTORY_SERVER_SERVICE)

    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()

    def active(self) -> bool:
        """Return the health of the service."""
        return self.container.get_service(
            self.HISTORY_SERVER_SERVICE).is_running()
        # We could use pebble health checks here
