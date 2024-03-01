#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Domain object of the Spark History Server charm."""

from dataclasses import dataclass
from typing import List
import json

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
        """Return endpoint of the S3 bucket."""
        return self._data.get("endpoint", None)

    @property
    def access_key(self) -> str:
        """Return the access key."""
        return self._data["access-key"]

    @property
    def secret_key(self) -> str:
        """Return the secret key."""
        return self._data["secret-key"]

    @property
    def path(self) -> str:
        """Return the path in the S3 bucket."""
        return self._data["path"]

    @property
    def bucket(self) -> str:
        """Return the name of the S3 bucket."""
        return self._data["bucket"]

    @property
    def tls_ca_chain(self) -> List[str] | None:
        """Return the CA chain (when applicable)."""
        return json.loads(ca_chain) \
            if (ca_chain := self._data.get("tls-ca-chain", "")) else None

    @property
    def log_dir(self) -> str:
        """Return the full path to the object."""
        return f"s3a://{self.bucket}/{self.path}"

    @classmethod
    def from_dict(cls, data_dict: dict) -> "S3ConnectionInfo":
        """Return instance of S3ConnectionInfo from a dictionary."""
        return S3ConnectionInfo(data_dict)
