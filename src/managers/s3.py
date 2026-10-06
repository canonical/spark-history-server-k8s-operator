#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from enum import Enum, auto
from functools import cached_property
from typing import TYPE_CHECKING

import boto3
from botocore.client import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ProxyConnectionError,
    ReadTimeoutError,
    SSLError,
)
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from common.utils import WithLogging, is_proxy_skipped
from core.domain import S3ConnectionInfo

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import ListObjectsV2OutputTypeDef

WRONG_CREDENTIALS_CODES = {
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "AuthorizationHeaderMalformed",
    "ExpiredToken",
}
MISMATCH_CODES = {"AccessDenied", "NoSuchBucket", "PermanentRedirect"}


class S3VerifyCode(Enum):
    """Broad categories for S3 verification results."""

    OK = auto()
    WRONG_CREDENTIALS = auto()
    CONFIGURATION_MISMATCH = auto()
    ACTIONABLE_CONNECTIVITY = auto()
    OTHER_ISSUE = auto()


@dataclass(frozen=True)
class S3VerifyResult:
    """Result of S3 verification."""

    ok: bool
    code: S3VerifyCode


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

    def _get_proxy_config(self) -> dict[str, str]:
        """Return proxy configuration based on charm environment variables."""
        proxy_config: dict[str, str] = {}

        if not is_proxy_skipped(self.connection_info.endpoint or ""):
            if os.environ.get("JUJU_CHARM_HTTPS_PROXY"):
                proxy_config["https"] = os.environ["JUJU_CHARM_HTTPS_PROXY"]
            if os.environ.get("JUJU_CHARM_HTTP_PROXY"):
                proxy_config["http"] = os.environ["JUJU_CHARM_HTTP_PROXY"]

        return proxy_config

    def _client(self, ca_file_path: str | None = None) -> S3Client:
        """Build the S3 client."""
        return self.session.client(
            "s3",
            region_name=self.connection_info.region or "us-east-1",
            endpoint_url=self.connection_info.endpoint or "https://s3.amazonaws.com",
            verify=ca_file_path,
            config=Config(
                # "when_supported" (the boto3 >= 1.36 default) makes every write use
                # aws-chunked encoding with a trailing CRC32 checksum. Several S3-compatible
                # backends (e.g. Ceph radosgw behind an Apache proxy) don't support that and
                # reject the request with XAmzContentSHA256Mismatch, so only compute/validate
                # checksums when the S3 API actually requires them.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                proxies=self._get_proxy_config(),
            ),
        )

    @retry(
        wait=wait_fixed(2),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(
            (
                ProxyConnectionError,
                EndpointConnectionError,
                ConnectTimeoutError,
                ReadTimeoutError,
                ConnectionClosedError,
            )
        ),
        reraise=True,
    )
    def _check_bucket_and_path(self, client: S3Client) -> ListObjectsV2OutputTypeDef:
        """Check the existence of the configured bucket and path."""
        normalized_prefix = f"{self.connection_info.path.rstrip('/')}/"
        # Note: list_objects_v2() demands fewer permissions than the previous
        # list_buckets() implementation.
        return client.list_objects_v2(
            Bucket=self.connection_info.bucket,
            Prefix=normalized_prefix,
            MaxKeys=1,
        )

    def _classify_client_error(self, error: ClientError) -> S3VerifyResult:
        """Map client errors to broad verification categories."""
        code = error.response.get("Error", {}).get("Code", "")

        if code in WRONG_CREDENTIALS_CODES:
            self.logger.error(f"Invalid S3 credentials... {error}")
            return S3VerifyResult(False, S3VerifyCode.WRONG_CREDENTIALS)

        if code in MISMATCH_CODES:
            self.logger.error(f"S3 configuration mismatch... {error}")
            return S3VerifyResult(False, S3VerifyCode.CONFIGURATION_MISMATCH)

        self.logger.error(f"S3 related error {error}")
        return S3VerifyResult(False, S3VerifyCode.OTHER_ISSUE)

    def verify(self) -> S3VerifyResult:
        """Verify S3 credentials and configuration."""
        if not self.connection_info.path:
            return S3VerifyResult(False, S3VerifyCode.CONFIGURATION_MISMATCH)

        try:
            with tempfile.NamedTemporaryFile() as ca_file:
                ca_file_path = None
                if tls_ca_chain := self.connection_info.tls_ca_chain:
                    ca_file.write("\n".join(tls_ca_chain).encode())
                    ca_file.flush()
                    ca_file_path = ca_file.name

                response = self._check_bucket_and_path(self._client(ca_file_path=ca_file_path))
        except ClientError as error:
            return self._classify_client_error(error)
        except (
            SSLError,
            ProxyConnectionError,
            EndpointConnectionError,
            ConnectTimeoutError,
            ReadTimeoutError,
            ConnectionClosedError,
        ) as error:
            self.logger.error(f"Could not reach S3... {error}")
            return S3VerifyResult(False, S3VerifyCode.ACTIONABLE_CONNECTIVITY)
        except Exception as error:
            self.logger.error(f"S3 related error {error}")
            return S3VerifyResult(False, S3VerifyCode.OTHER_ISSUE)

        if response.get("KeyCount", 0) == 0 and not response.get("Contents"):
            self.logger.error(
                "S3 configuration mismatch: no objects found under bucket %s and path %s",
                self.connection_info.bucket,
                self.connection_info.path,
            )
            return S3VerifyResult(False, S3VerifyCode.CONFIGURATION_MISMATCH)

        return S3VerifyResult(True, S3VerifyCode.OK)
