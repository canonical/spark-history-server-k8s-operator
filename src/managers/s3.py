#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""S3 manager."""

from __future__ import annotations

import os
import tempfile
from enum import Enum, auto
from functools import cached_property
from typing import TYPE_CHECKING, Literal

import boto3
from botocore.client import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    ProxyConnectionError,
    ReadTimeoutError,
    SSLError,
)
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_exception_cause_type,
    stop_after_attempt,
    wait_fixed,
)

from common.utils import WithLogging, is_proxy_skipped
from core.domain import S3ConnectionInfo

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


TRANSIENT_S3_ERRORS = (
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    HTTPClientError,
    OSError,
    ReadTimeoutError,
    TimeoutError,
)

AUTHENTICATION_ERROR_CODES = {
    "401",
    "403",
    "AccessDenied",
    "ExpiredToken",
    "ExpiredTokenException",
    "InvalidAccessKeyId",
    "InvalidClientTokenId",
    "InvalidSecretAccessKey",
    "InvalidToken",
    "SignatureDoesNotMatch",
    "TokenRefreshRequired",
}

NOT_FOUND_ERROR_CODES = {
    "404",
    "NoSuchBucket",
    "NoSuchKey",
    "NotFound",
}

RETRYABLE_CLIENT_ERROR_CODES = {
    "InternalError",
    "RequestTimeout",
    "RequestTimeoutException",
    "ServiceUnavailable",
    "SlowDown",
}

IDEMPOTENT_BUCKET_CREATE_ERROR_CODES = {
    "BucketAlreadyExists",
    "BucketAlreadyOwnedByYou",
}


def _client_error_code(error: ClientError) -> str:
    """Return the AWS error code for a botocore ClientError."""
    return str(error.response.get("Error", {}).get("Code", ""))


def _client_error_status(error: ClientError) -> str:
    """Return the HTTP status code for a botocore ClientError."""
    return str(error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", ""))


def _is_auth_or_permission_error(error: ClientError) -> bool:
    """Return whether the client error indicates invalid credentials or access."""
    return _client_error_status(error) in {"401", "403"} or _client_error_code(
        error
    ) in AUTHENTICATION_ERROR_CODES


def _is_not_found_error(error: ClientError) -> bool:
    """Return whether the client error indicates the bucket or object is missing."""
    return _client_error_status(error) == "404" or _client_error_code(error) in NOT_FOUND_ERROR_CODES


def _is_retryable_client_error(error: ClientError) -> bool:
    """Return whether the client error likely reflects a transient endpoint problem."""
    return _client_error_status(error) in {"500", "502", "503", "504"} or _client_error_code(
        error
    ) in RETRYABLE_CLIENT_ERROR_CODES


def _should_retry_verification_error(error: BaseException) -> bool:
    """Return whether S3 verification should retry this failure."""
    return (
        isinstance(error, TRANSIENT_S3_ERRORS)
        and not isinstance(error, (ProxyConnectionError, SSLError))
    ) or (
        isinstance(error, ClientError)
        and _is_retryable_client_error(error)
    )


class S3VerificationResult(Enum):
    """Verification result for S3 connectivity and access checks."""

    SUCCESS = auto()
    MISSING_PATH = auto()
    INVALID_CREDENTIALS = auto()
    SSL_ERROR = auto()
    PROXY_ERROR = auto()
    ENDPOINT_UNREACHABLE = auto()
    UNKNOWN_ERROR = auto()


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

    def get_or_create_bucket(self, client: S3Client) -> S3VerificationResult:
        """Create bucket if it does not exists."""
        bucket_name = self.connection_info.bucket
        bucket_exists = True

        try:
            client.head_bucket(Bucket=bucket_name)
        except ClientError as ex:
            if _is_not_found_error(ex):
                bucket_exists = False
            else:
                return self._verification_error_result(ex)

        if not bucket_exists:
            try:
                client.create_bucket(Bucket=bucket_name)
                self._wait_until_exists(client, "bucket")
            except ClientError as ex:
                if _client_error_code(ex) in IDEMPOTENT_BUCKET_CREATE_ERROR_CODES:
                    self._wait_until_exists(client, "bucket")
                    return S3VerificationResult.SUCCESS
                self.logger.error(f"Could not create bucket {bucket_name}: {ex}")
                return self._verification_error_result(ex)
            self.logger.info(f"Created bucket {bucket_name}")

        return S3VerificationResult.SUCCESS

    def ensure_path(self, client: S3Client) -> S3VerificationResult:
        """Create path if it does not exists."""
        path = self.connection_info.path
        path_exists = True
        if not path:
            return S3VerificationResult.MISSING_PATH
        try:
            client.head_object(
                Bucket=self.connection_info.bucket,
                Key=os.path.join(path, ".keep"),
            )
        except ClientError as ex:
            if _is_not_found_error(ex):
                path_exists = False
            else:
                return self._verification_error_result(ex)

        if not path_exists:
            try:
                client.put_object(
                    Bucket=self.connection_info.bucket,
                    Key=os.path.join(path, ".keep"),
                )
                self._wait_until_exists(client, "key")
            except ClientError as ex:
                self.logger.error(
                    f"Could not create path {path} in bucket {self.connection_info.bucket}: {ex}"
                )
                return self._verification_error_result(ex)
            self.logger.info(f"Created path {path} in bucket {self.connection_info.bucket}")

        return S3VerificationResult.SUCCESS

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(20),
        retry=retry_if_exception_cause_type(ClientError),
        reraise=True,
    )
    def _wait_until_exists(
        self, client: S3Client, resource_type: Literal["bucket", "key"]
    ) -> None:
        """Poll s3 API until resource is found."""
        if resource_type == "bucket":
            client.head_bucket(Bucket=self.connection_info.bucket)
        else:
            client.head_object(
                Bucket=self.connection_info.bucket,
                Key=os.path.join(self.connection_info.path, ".keep"),
            )

    def _proxy_config(self) -> dict[str, str]:
        """Return proxy configuration for the S3 client."""
        if is_proxy_skipped(self.connection_info.endpoint or ""):
            return {}

        proxy_config: dict[str, str] = {}
        if os.environ.get("JUJU_CHARM_HTTPS_PROXY"):
            proxy_config["https"] = os.environ["JUJU_CHARM_HTTPS_PROXY"]
        if os.environ.get("JUJU_CHARM_HTTP_PROXY"):
            proxy_config["http"] = os.environ["JUJU_CHARM_HTTP_PROXY"]
        return proxy_config

    def _client(self, ca_file) -> S3Client:
        """Build the S3 client used for verification."""
        return self.session.client(
            "s3",
            region_name=self.connection_info.region or "us-east-1",
            endpoint_url=self.connection_info.endpoint or "https://s3.amazonaws.com",
            verify=ca_file.name if self.connection_info.tls_ca_chain else None,
            config=Config(
                # "when_supported" (the boto3 >= 1.36 default) makes every write use
                # aws-chunked encoding with a trailing CRC32 checksum. Several S3-compatible
                # backends (e.g. Ceph radosgw behind an Apache proxy) don't support that and
                # reject the request with XAmzContentSHA256Mismatch, so only compute/validate
                # checksums when the S3 API actually requires them.
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
                proxies=self._proxy_config(),
            ),
        )

    def _verification_error_result(self, error: Exception) -> S3VerificationResult:
        """Classify S3 verification failures."""
        if isinstance(error, ClientError):
            if _is_auth_or_permission_error(error):
                self.logger.error(f"Invalid S3 credentials or permissions issue: {error}")
                return S3VerificationResult.INVALID_CREDENTIALS
            if _is_retryable_client_error(error):
                self.logger.error(f"Could not reach the S3 endpoint after 5 attempts: {error}")
                return S3VerificationResult.ENDPOINT_UNREACHABLE
            self.logger.error(f"Unexpected S3 client error: {error}")
            return S3VerificationResult.UNKNOWN_ERROR
        if isinstance(error, SSLError):
            self.logger.error(f"SSL validation failed when contacting the S3 endpoint: {error}")
            return S3VerificationResult.SSL_ERROR
        if isinstance(error, ProxyConnectionError):
            self.logger.error(f"Could not communicate with/through proxy: {error}")
            return S3VerificationResult.PROXY_ERROR
        if isinstance(error, TRANSIENT_S3_ERRORS):
            self.logger.error(f"Could not reach the S3 endpoint after 5 attempts: {error}")
            return S3VerificationResult.ENDPOINT_UNREACHABLE

        self.logger.error(f"Unexpected S3 verification error: {error}")
        return S3VerificationResult.UNKNOWN_ERROR

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(5),
        retry=retry_if_exception(_should_retry_verification_error),
        reraise=True,
    )
    def _list_buckets(self, client: S3Client) -> None:
        """Verify S3 connectivity by listing buckets, retrying transient errors."""
        client.list_buckets()

    def verify_result(self) -> S3VerificationResult:
        """Verify S3 credentials and configuration, returning a classified result."""
        with tempfile.NamedTemporaryFile() as ca_file:
            if tls_ca_chain := self.connection_info.tls_ca_chain:
                ca_file.write("\n".join(tls_ca_chain).encode())
                ca_file.flush()

            s3 = self._client(ca_file)

            try:
                self._list_buckets(s3)
            except Exception as error:
                return self._verification_error_result(error)

            try:
                bucket_result = self.get_or_create_bucket(s3)
                if bucket_result != S3VerificationResult.SUCCESS:
                    return bucket_result
                path_result = self.ensure_path(s3)
                if path_result != S3VerificationResult.SUCCESS:
                    return path_result
            except Exception as error:
                return self._verification_error_result(error)

        return S3VerificationResult.SUCCESS

    def verify(self) -> bool:
        """Verify S3 credentials and configuration."""
        return self.verify_result() is S3VerificationResult.SUCCESS
