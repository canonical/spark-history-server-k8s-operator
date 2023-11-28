#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Module with all domain specific objects used by the charm."""

from dataclasses import dataclass
from enum import Enum
from functools import cached_property

import boto3
from botocore.exceptions import ClientError
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus

from utils import WithLogging


@dataclass
class User:
    """Class representing the user running the Pebble workload services."""

    name: str = "spark"
    group: str = "spark"


@dataclass
class S3ConnectionInfo(WithLogging):
    """Class representing credentials and endpoints to connect to S3."""

    endpoint: str | None
    access_key: str
    secret_key: str
    path: str
    bucket: str

    @property
    def log_dir(self) -> str:
        """Return the path to the bucket."""
        return f"s3a://{self.bucket}/{self.path}"

    @cached_property
    def session(self):
        """Return the S3 session to be used when connecting to S3."""
        return boto3.session.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def verify(self) -> bool:
        """Verify S3 credentials."""
        s3 = self.session.client("s3", endpoint_url=self.endpoint or "https://s3.amazonaws.com")

        try:
            s3.list_buckets()
        except ClientError:
            self.logger.error("Invalid S3 credentials...")
            return False

        return True


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    MISSING_S3_RELATION = BlockedStatus("Missing S3 relation")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    ACTIVE = ActiveStatus("")
