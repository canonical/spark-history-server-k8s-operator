#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Module with all domain specific objects used by the charm."""

import tempfile
from dataclasses import dataclass, fields
from enum import Enum
from functools import cached_property
from typing import List

import boto3
from botocore.exceptions import ClientError, SSLError
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
    tls_ca_chain: List[str] | None

    @classmethod
    def from_dict(cls, d: dict) -> "S3ConnectionInfo":
        """Return the instance of S3ConnectionInfo dataclass from a dictionary."""
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

    @cached_property
    def session(self):
        """Return the S3 session to be used when connecting to S3."""
        return boto3.session.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

    def verify(self) -> bool:
        """Verify S3 credentials."""
        tls_ca_chain_file = None
        if self.tls_ca_chain:
            ca_file = tempfile.NamedTemporaryFile()
            ca = "\n".join(self.tls_ca_chain)
            ca_file.write(ca.encode())
            ca_file.flush()
            tls_ca_chain_file = ca_file.name

        s3 = self.session.client(
            "s3",
            endpoint_url=self.endpoint or "https://s3.amazonaws.com",
            verify=tls_ca_chain_file,
        )

        try:
            s3.list_buckets()
        except ClientError:
            self.logger.error("Invalid S3 credentials...")
            return False
        except SSLError:
            self.logger.error("SSL validation failed...")
            return False
        except Exception as e:
            self.logger.error(f"S3 related error {e}")
            return False
        finally:
            if self.tls_ca_chain:
                ca_file.close()

        return True


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    MISSING_S3_RELATION = BlockedStatus("Missing S3 relation")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    MISSING_INGRESS_RELATION = BlockedStatus("Missing INGRESS relation")
    ACTIVE = ActiveStatus("")
