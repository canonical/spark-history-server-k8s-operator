from abc import ABC, abstractmethod
from enum import Enum
from io import IOBase, StringIO

from ops.model import Container

from models import User
from utils import WithLogging


class IOMode(str, Enum):
    READ = "r"
    WRITE = "w"


class AbstractWorkload(ABC):

    @abstractmethod
    def start(self):
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        raise NotImplementedError

    @abstractmethod
    def health(self):
        raise NotImplementedError

    @abstractmethod
    def ready(self):
        raise NotImplementedError

    @abstractmethod
    def get_spark_configuration_file(self, mode: IOMode) -> IOBase:
        raise NotImplementedError


class ContainerFile(StringIO):
    def __init__(self, container: Container, user: User, path: str, mode: IOMode):
        super().__init__()
        self.container = container
        self.user = user
        self.path = path
        self._mode = mode

    def exists(self):
        return self.container.exists(self.path)

    def open(self):
        if self._mode is IOMode.READ:
            self.write(self.container.pull(self.path).read().decode("utf-8"))

    def close(self):
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
    SPARK_WORKDIR = "/opt/spark"
    CONTAINER_LAYER = "charm-layer"
    HISTORY_SERVER_SERVICE = "history-server"
    SPARK_PROPERTIES = f"{SPARK_WORKDIR}/conf/spark-properties.conf"

    def __init__(self, container: Container, user: User = User()):
        self.container = container
        self.user = user

    def get_spark_configuration_file(self, mode: IOMode) -> ContainerFile:
        return ContainerFile(
            self.container,
            self.user,
            self.SPARK_PROPERTIES,
            mode
        )

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
        services = self.container.get_plan().services

        if services[self.HISTORY_SERVER_SERVICE].startup != "enabled":
            self.logger.info("Adding layer...")
            self.container.add_layer(
                self.CONTAINER_LAYER, self._spark_history_server_layer, combine=True
            )

        spark_configuration_file = self.get_spark_configuration_file(IOMode.READ)

        if not spark_configuration_file.exists():
            self.logger.error(f"{spark_configuration_file.path} not found")
            raise FileNotFoundError(spark_configuration_file.path)

        # Push an updated layer with the new config
        # self.container.replan()
        self.container.restart(self.HISTORY_SERVER_SERVICE)

    def stop(self):
        self.container.stop(self.HISTORY_SERVER_SERVICE)

    def ready(self) -> bool:
        return self.container.can_connect()

    def health(self) -> bool:
        return True  # We could use pebble health checks here
