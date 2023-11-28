#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Module containing all business logic related to the workload."""

from abc import ABC, abstractmethod
from enum import Enum
from io import IOBase, StringIO

from ops.model import Container

from models import User
from utils import WithLogging


class IOMode(str, Enum):
    """Class representing the modes to open file resources."""

    READ = "r"
    WRITE = "w"


class AbstractWorkload(ABC):
    """Abstract class representing general API of a workload, irrespective on the substrate (VM or K8s)."""

    @abstractmethod
    def start(self):
        """Execute business-logic for starting the workload."""
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """Execute business-logic for stopping the workload."""
        raise NotImplementedError

    @abstractmethod
    def health(self) -> bool:
        """Return the health of the service."""
        raise NotImplementedError

    @abstractmethod
    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        raise NotImplementedError

    @abstractmethod
    def get_spark_configuration_file(self, mode: IOMode) -> IOBase:
        """Return the configuration file for Spark History server."""
        raise NotImplementedError


class ContainerFile(StringIO):
    """Class representing a file in the workload container to be read/written.

    The operations will be mediated by Pebble, but this should be abstracted away such
    that the same API can also be used for files in local file systems. This allows to
    create some context where handling read/write independently from the substrate:

    ```python
    file = ContainerFile(container, user, IOMode.READ)
    # or open("local-file", IOMode.READ)

    with file as fid:
        fid.read()
    ```
    """

    def __init__(self, container: Container, user: User, path: str, mode: IOMode):
        super().__init__()
        self.container = container
        self.user = user
        self.path = path
        self._mode = mode

    def exists(self):
        """Check whether the file exists."""
        return self.container.exists(self.path)

    def open(self):
        """Execute business logic on context creation."""
        if self._mode is IOMode.READ:
            self.write(self.container.pull(self.path).read().decode("utf-8"))

    def close(self):
        """Execute business logic on context destruction."""
        if self._mode is IOMode.WRITE:
            self.container.push(
                self.path,
                self.getvalue(),
                user=self.user.name,
                group=self.user.group,
                make_dirs=True,
                permissions=0o640,
            )


class SparkHistoryServer(AbstractWorkload, WithLogging):
    """Class representing Workload implementation for Spark History server on K8s."""

    SPARK_WORKDIR = "/opt/spark"
    CONTAINER_LAYER = "charm-layer"
    HISTORY_SERVER_SERVICE = "history-server"
    SPARK_PROPERTIES = f"{SPARK_WORKDIR}/conf/spark-properties.conf"

    def __init__(self, container: Container, user: User = User()):
        self.container = container
        self.user = user

    def get_spark_configuration_file(self, mode: IOMode) -> ContainerFile:
        """Return the configuration file for Spark History server."""
        return ContainerFile(self.container, self.user, self.SPARK_PROPERTIES, mode)

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
                    "environment": {"SPARK_PROPERTIES_FILE": self.SPARK_PROPERTIES},
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
                self.CONTAINER_LAYER, self._spark_history_server_layer, combine=True
            )
        # ===============

        # ===============
        # THIS WOULD NOT BE WORKING
        # ===============
        # self.container.add_layer(
        #     self.CONTAINER_LAYER, self._spark_history_server_layer, combine=True
        # )
        # ===============

        spark_configuration_file = self.get_spark_configuration_file(IOMode.READ)

        if not spark_configuration_file.exists():
            self.logger.error(f"{spark_configuration_file.path} not found")
            raise FileNotFoundError(spark_configuration_file.path)

        # Push an updated layer with the new config
        # self.container.replan()
        self.container.restart(self.HISTORY_SERVER_SERVICE)

    def stop(self):
        """Execute business-logic for stopping the workload."""
        self.container.stop(self.HISTORY_SERVER_SERVICE)

    def ready(self) -> bool:
        """Check whether the service is ready to be used."""
        return self.container.can_connect()

    def health(self) -> bool:
        """Return the health of the service."""
        return True  # We could use pebble health checks here
