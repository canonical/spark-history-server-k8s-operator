import secrets
import string
from abc import abstractmethod
from pathlib import Path

from common.workload import AbstractWorkload
from core.domain import User


class HistoryServerPaths:
    """Object to store common paths for Kafka."""

    def __init__(self, conf_path: Path | str, keytool: str):
        self.conf_path = conf_path if isinstance(conf_path, Path) \
            else Path(conf_path)
        self.keytool = keytool

    @property
    def spark_properties(self) -> Path:
        return self.conf_path / "spark-properties.conf"

    @property
    def cert(self):
        return self.conf_path / "ca.pem"

    @property
    def truststore(self):
        return self.conf_path / "truststore.jks"


class SparkHistoryWorkloadBase(AbstractWorkload):
    """Base interface for common workload operations."""

    paths: HistoryServerPaths
    user: User

    def restart(self) -> None:
        """Restarts the workload service."""
        self.stop()
        self.start()

    @abstractmethod
    def active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @abstractmethod
    def ready(self) -> bool:
        """Checks that the container/snap is ready."""
        ...

    @staticmethod
    def generate_password() -> str:
        """Creates randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join(
            [secrets.choice(string.ascii_letters + string.digits) for _ in
             range(32)])
