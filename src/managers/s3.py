#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

import os
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

    def get_or_create_bucket(self, ca_file_name: str) -> bool:
        """Create bucket if it does not exists."""
        s3 = boto3.resource(
            "s3",
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            region_name=None,
            endpoint_url=self.config.endpoint or "https://s3.amazonaws.com",
            verify=ca_file_name if self.config.tls_ca_chain else None,
        )
        bucket_name = self.config.bucket
        bucket_exists = True

        bucket = s3.Bucket(bucket_name)  # pyright: ignore [reportAttributeAccessIssue]

        try:
            bucket.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as ex:
            if "(403)" in ex.args[0]:
                self.logger.error("Wrong credentials or access to bucket is forbidden")
                return False
            elif "(404)" in ex.args[0]:
                bucket_exists = False
        else:
            self.logger.info(f"Using existing bucket {bucket_name}")

        if not bucket_exists:
            bucket.create()
            bucket.wait_until_exists()
            self.logger.info(f"Created bucket {bucket_name}")

        bucket.put_object(Key=os.path.join(self.config.path, ""))

        return True

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

            if not self.get_or_create_bucket(ca_file_name=ca_file.name):
                return False

        return True
