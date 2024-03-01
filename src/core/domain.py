from dataclasses import dataclass, fields
from typing import List
from common.models import DataDict

@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str
    group: str


class S3ConnectionInfo:
    """Class representing credentials and endpoints to connect to S3."""

    def __init__(self, data: dict):
        self._data = data

    @property
    def endpoint(self) -> str | None:
        return self._data.get("endpoint", None)

    @property
    def access_key(self) -> str:
        return self._data["access-key"]

    @property
    def secret_key(self) -> str:
        return self._data["secret-key"]

    @property
    def path(self) -> str:
        return self._data["path"]

    @property
    def bucket(self) -> str:
        return self._data["bucket"]

    @property
    def tls_ca_chain(self) -> List[str] | None:
        return self._data["tls-ca-chain"]

    @property
    def log_dir(self) -> str:
        """Return the path to the bucket."""
        return f"s3a://{self.bucket}/{self.path}"

    @classmethod
    def from_dict(cls, data_dict: dict) -> "S3ConnectionInfo":
        """Return instance of S3ConnectionInfo from a dictionary."""
        return S3ConnectionInfo(data_dict)
