#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

import tempfile
from functools import cached_property

import boto3
from botocore.exceptions import ClientError, SSLError

from common.utils import WithLogging
from core.domain import S3ConnectionInfo


class S3Manager(WithLogging):
    """Class exposing business logic for interacting with S3 service."""

    def __init__(self, config: S3ConnectionInfo):
        self.config = config

    @cached_property
    def session(self):
        """Return the S3 session to be used when connecting to S3."""
        return boto3.session.Session(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
        )

    def verify(self) -> bool:
        """Verify S3 credentials."""
        with tempfile.NamedTemporaryFile() as ca_file:

            if config := self.config.tls_ca_chain:
                ca_file.write("\n".join(config).encode())
                ca_file.flush()

            s3 = self.session.client(
                "s3",
                endpoint_url=self.config.endpoint or "https://s3.amazonaws.com",
                verify=ca_file.name if self.config.tls_ca_chain else None,
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

        return True
