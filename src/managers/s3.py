#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

from __future__ import annotations

import ipaddress
import os
import tempfile
from functools import cached_property
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, ProxyConnectionError, SSLError
from tenacity import retry, retry_if_exception_cause_type, stop_after_attempt, wait_fixed

from common.utils import WithLogging
from core.domain import S3ConnectionInfo

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


def is_proxy_skipped(endpoint: str) -> bool:
    """Determine if proxy should not be applied for the given endpoint."""
    no_proxy_list = os.environ.get("JUJU_CHARM_NO_PROXY", "")
    if not no_proxy_list:
        return False

    host = urlparse(endpoint).hostname
    if not host:
        return False
    no_proxy_entries = [
        entry.strip().lower() for entry in no_proxy_list.split(",") if entry.strip()
    ]
    for entry in no_proxy_entries:
        if host == entry:
            return True
        elif entry.startswith(".") and host.endswith(
            entry
        ):  # abc.example.com matches .example.com
            return True
        elif host.endswith("." + entry):  # abc.example.com matches example.com
            return True
        try:
            if ipaddress.ip_address(host) in ipaddress.ip_network(
                entry, strict=False
            ):  # CIDR match
                return True
        except (AttributeError, ValueError):
            continue

    return False


class S3Manager(WithLogging):
    """Class exposing business logic for interacting with S3 service."""

    def __init__(self, connection_info: S3ConnectionInfo):
        self.connection_info = connection_info

    @cached_property
    def session(self):
        """Return the S3 session to be used when connecting to S3."""
        return boto3.session.Session(
            aws_access_key_id=self.connection_info.access_key,
            aws_secret_access_key=self.connection_info.secret_key,
        )

    def get_or_create_bucket(self, client: S3Client) -> bool:
        """Create bucket if it does not exists."""
        bucket_name = self.connection_info.bucket
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

        client.put_object(Bucket=bucket_name, Key=os.path.join(self.connection_info.path, ""))

        return True

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(20),
        retry=retry_if_exception_cause_type(ClientError),
        reraise=True,
    )
    def _wait_until_exists(self, client: S3Client):
        """Poll s3 API until resource is found."""
        client.head_bucket(Bucket=self.connection_info.bucket)

    def verify(self) -> bool:
        """Verify S3 credentials and configuration."""
        proxy_config: dict[str, str] = {}

        if not is_proxy_skipped(self.connection_info.endpoint or ""):
            if os.environ.get("JUJU_CHARM_HTTPS_PROXY"):
                proxy_config["https"] = os.environ["JUJU_CHARM_HTTPS_PROXY"]
            if os.environ.get("JUJU_CHARM_HTTP_PROXY"):
                proxy_config["http"] = os.environ["JUJU_CHARM_HTTP_PROXY"]

        with tempfile.NamedTemporaryFile() as ca_file:
            if tls_ca_chain := self.connection_info.tls_ca_chain:
                ca_file.write("\n".join(tls_ca_chain).encode())
                ca_file.flush()

            s3 = self.session.client(
                "s3",
                region_name=self.connection_info.region or "us-east-1",
                endpoint_url=self.connection_info.endpoint or "https://s3.amazonaws.com",
                verify=ca_file.name if self.connection_info.tls_ca_chain else None,
                config=Config(
                    request_checksum_calculation="when_supported",
                    response_checksum_validation="when_supported",
                    proxies=proxy_config,
                ),
            )

            try:
                s3.list_buckets()
            except ClientError as client_error:
                self.logger.error(f"Invalid S3 credentials...{client_error}")
                return False
            except SSLError as ssl_error:
                self.logger.error(f"SSL validation failed... {ssl_error}")
                return False
            except ProxyConnectionError as proxy_error:
                self.logger.error(f"Could not communicate with/through proxy {proxy_error}")
            except Exception as e:
                self.logger.error(f"S3 related error {e}")
                return False

            if not self.get_or_create_bucket(s3):
                return False

        return True
