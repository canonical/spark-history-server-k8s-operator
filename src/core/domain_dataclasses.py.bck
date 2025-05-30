#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Domain object of the Spark History Server charm."""

from dataclasses import dataclass, fields
from typing import List


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str
    group: str


@dataclass
class S3ConnectionInfo:
    """Class representing credentials and endpoints to connect to S3."""

    endpoint: str | None
    access_key: str
    secret_key: str
    path: str
    bucket: str
    tls_ca_chain: List[str] | None

    @classmethod
    def from_dict(cls, d: dict) -> "S3ConnectionInfo":
        """Return instance of S3ConnectionInfo from a dictionary."""
        field_names = {field.name for field in fields(cls)}
        data = {k: v for k, v in d.items() if k in field_names}
        missing_fields = field_names.difference(set(data.keys()))
        for missing_field in missing_fields:
            data[missing_field] = None
        return cls(**data)

    @property
    def log_dir(self) -> str:
        """Return the path to the bucket."""
        return f"s3a://{self.bucket}/{self.path}"
