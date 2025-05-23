#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

from __future__ import annotations

import os
import tempfile
from functools import cached_property
from typing import TYPE_CHECKING

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, SSLError
from tenacity import retry, retry_if_exception_cause_type, stop_after_attempt, wait_fixed

from common.utils import WithLogging
from core.domain import S3ConnectionInfo

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


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

    def get_or_create_bucket(self, client: S3Client) -> bool:
        """Create bucket if it does not exists."""
        bucket_name = self.config.bucket
        bucket_exists = True

        try:
            client.head_bucket(Bucket=bucket_name)
        except ClientError as ex:
            if "(403)" in ex.args[0]:
                self.logger.error("Wrong credentials or access to bucket is forbidden")
                return False
            elif "(404)" in ex.args[0]:
                bucket_exists = False
        else:
            self.logger.info(f"Using existing bucket {bucket_name}")

        if not bucket_exists:
            client.create_bucket(Bucket=bucket_name)
            self._wait_until_exists(client)
            self.logger.info(f"Created bucket {bucket_name}")

        client.put_object(Bucket=bucket_name, Key=os.path.join(self.config.path, ""))

        return True

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(20),
        retry=retry_if_exception_cause_type(ClientError),
        reraise=True,
    )
    def _wait_until_exists(self, client: S3Client):
        """Poll s3 API until resource is found."""
        client.head_bucket(Bucket=self.config.bucket)

    def verify(self) -> bool:
        """Verify S3 credentials and configuration."""
        with tempfile.NamedTemporaryFile() as ca_file:
            if config := self.config.tls_ca_chain:
                ca_file.write("\n".join(config).encode())
                ca_file.flush()

            s3 = self.session.client(
                "s3",
                endpoint_url=self.config.endpoint or "https://s3.amazonaws.com",
                verify=ca_file.name if self.config.tls_ca_chain else None,
                config=Config(
                    request_checksum_calculation="when_supported",
                    response_checksum_validation="when_supported",
                ),
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

            if not self.get_or_create_bucket(s3):
                return False

        return True
