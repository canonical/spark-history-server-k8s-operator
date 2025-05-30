#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Domain object of the Spark History Server charm."""

import json
from dataclasses import dataclass
from typing import List, MutableMapping

from ops import Application, Relation, Unit


class StateBase:
    """Base state object."""

    def __init__(self, relation: Relation | None, component: Unit | Application):
        self.relation = relation
        self.component = component

    @property
    def relation_data(self) -> MutableMapping[str, str]:
        """The raw relation data."""
        if not self.relation:
            return {}

        return self.relation.data[self.component]

    def update(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        if not self.relation:
            return

        self.relation_data.update(items)


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str
    group: str


class S3ConnectionInfo(StateBase):
    """Class representing credentials and endpoints to connect to S3."""

    def __init__(self, relation: Relation, component: Application):
        super().__init__(relation, component)

    @property
    def endpoint(self) -> str | None:
        """Return endpoint of the S3 bucket."""
        return self.relation_data.get("endpoint", None)

    @property
    def access_key(self) -> str:
        """Return the access key."""
        return self.relation_data.get("access-key", "")

    @property
    def secret_key(self) -> str:
        """Return the secret key."""
        return self.relation_data.get("secret-key", "")

    @property
    def path(self) -> str:
        """Return the path in the S3 bucket."""
        return self.relation_data["path"]

    @property
    def bucket(self) -> str:
        """Return the name of the S3 bucket."""
        return self.relation_data["bucket"]

    @property
    def tls_ca_chain(self) -> List[str] | None:
        """Return the CA chain (when applicable)."""
        return (
            json.loads(ca_chain)
            if (ca_chain := self.relation_data.get("tls-ca-chain", ""))
            else None
        )

    @property
    def log_dir(self) -> str:
        """Return the full path to the object."""
        return f"s3a://{self.bucket}/{self.path}"


class AzureStorageConnectionInfo:
    """Class representing credentials and endpoints to connect to S3."""

    def __init__(self, relation_data):
        self.relation_data = relation_data

    @property
    def endpoint(self) -> str | None:
        """Return endpoint of the Azure storage container."""
        if self.connection_protocol in ("abfs", "abfss"):
            return f"{self.connection_protocol}://{self.container}@{self.storage_account}.dfs.core.windows.net"
        elif self.connection_protocol in ("wasb", "wasbs"):
            return f"{self.connection_protocol}://{self.container}@{self.storage_account}.blob.core.windows.net"
        return ""

    @property
    def endpoint_http(self) -> str:
        """Endpoint of the Azure storage container to be used with the SDK."""
        return f"https://{self.storage_account}.blob.core.windows.net/"

    @property
    def secret_key(self) -> str:
        """Return the secret key."""
        return self.relation_data["secret-key"]

    @property
    def path(self) -> str:
        """Return the path in the Azure Storage container."""
        return self.relation_data.get("path", "")

    @property
    def container(self) -> str:
        """Return the name of the Azure Storage container."""
        return self.relation_data["container"]

    @property
    def connection_protocol(self) -> str:
        """Return the protocol to be used to access files."""
        return self.relation_data["connection-protocol"].lower()

    @property
    def storage_account(self) -> str:
        """Return the name of the Azure Storage account."""
        return self.relation_data["storage-account"]

    @property
    def log_dir(self) -> str:
        """Return the full path to the object."""
        if self.endpoint:
            return f"{self.endpoint}/{self.path}"
        return ""

    @property
    def connection_string(self) -> str:
        """Return the connection string that can be used to connect to storage account."""
        protocol = "http" if self.connection_protocol in ("wasb", "abfs") else "https"
        return (
            f"DefaultEndpointsProtocol={protocol};"
            f"AccountName={self.storage_account};"
            f"AccountKey={self.secret_key};"
            "EndpointSuffix=core.windows.net"
        )
