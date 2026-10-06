from __future__ import annotations

import os
from typing import TYPE_CHECKING
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError, SSLError
from moto import mock_aws

from core.domain import S3ConnectionInfo
from managers.s3 import S3Manager, S3VerifyCode, is_proxy_skipped

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Return a mocked S3 client.

    All boto3 call will be mocked from this point.
    """
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


def _connection_info(path: str = "path") -> Mock:
    connection_info = Mock(spec=S3ConnectionInfo)
    connection_info.endpoint = ""
    connection_info.access_key = ""
    connection_info.secret_key = ""
    connection_info.bucket = "test-bucket"
    connection_info.path = path
    connection_info.tls_ca_chain = []
    connection_info.region = ""
    return connection_info


def test_verify_ok_when_path_exists(s3: S3Client) -> None:
    """Verification succeeds when the configured prefix contains an object."""
    # Given
    connection_info = _connection_info()
    s3_manager = S3Manager(connection_info)

    s3.create_bucket(Bucket=connection_info.bucket)
    s3.put_object(Bucket=connection_info.bucket, Key="path/eventlog", Body=b"data")

    # When
    result = s3_manager.verify()

    # Then
    assert result.ok is True
    assert result.code == S3VerifyCode.OK


def test_verify_configuration_mismatch_when_path_missing(s3: S3Client) -> None:
    """Verification fails when the configured prefix has no objects."""
    # Given
    connection_info = _connection_info()
    s3_manager = S3Manager(connection_info)

    s3.create_bucket(Bucket=connection_info.bucket)

    # When
    result = s3_manager.verify()

    # Then
    assert result.ok is False
    assert result.code == S3VerifyCode.CONFIGURATION_MISMATCH


def test_verify_uses_when_required_checksum_config(s3: S3Client, monkeypatch) -> None:
    """verify() must not opt in to flexible checksums (aws-chunked + trailing CRC)."""
    # Given
    connection_info = _connection_info()
    s3_manager = S3Manager(connection_info)

    s3.create_bucket(Bucket=connection_info.bucket)
    s3.put_object(Bucket=connection_info.bucket, Key="path/eventlog", Body=b"data")

    captured_configs = []
    original_client = s3_manager.session.client

    def capturing_client(*args, **kwargs):
        captured_configs.append(kwargs["config"])
        return original_client(*args, **kwargs)

    monkeypatch.setattr(s3_manager.session, "client", capturing_client)

    # When
    s3_manager.verify()

    # Then
    assert captured_configs
    assert captured_configs[0].request_checksum_calculation == "when_required"
    assert captured_configs[0].response_checksum_validation == "when_required"


@pytest.mark.parametrize(
    "error_code, expected",
    [
        ("InvalidAccessKeyId", S3VerifyCode.WRONG_CREDENTIALS),
        ("NoSuchBucket", S3VerifyCode.CONFIGURATION_MISMATCH),
        ("AccessDenied", S3VerifyCode.CONFIGURATION_MISMATCH),
        ("PermanentRedirect", S3VerifyCode.CONFIGURATION_MISMATCH),
        ("InternalError", S3VerifyCode.OTHER_ISSUE),
    ],
)
def test_verify_classifies_client_errors(error_code: str, expected: S3VerifyCode) -> None:
    """Client errors are mapped to broad verification categories."""
    # Given
    s3_manager = S3Manager(_connection_info())
    client = Mock()
    client.list_objects_v2.side_effect = ClientError(
        {"Error": {"Code": error_code, "Message": "boom"}}, "ListObjectsV2"
    )
    s3_manager.__dict__["session"] = Mock(client=Mock(return_value=client))

    # When
    code = s3_manager.verify().code

    # Then
    assert code == expected


@pytest.mark.parametrize(
    "error",
    [
        SSLError(endpoint_url="https://s3.amazonaws.com", error="certificate verify failed"),
        EndpointConnectionError(endpoint_url="https://s3.amazonaws.com"),
    ],
)
def test_verify_classifies_connectivity_errors(error) -> None:
    """Connectivity errors are grouped together."""
    # Given
    s3_manager = S3Manager(_connection_info())
    client = Mock()
    client.list_objects_v2.side_effect = error
    s3_manager.__dict__["session"] = Mock(client=Mock(return_value=client))

    # When
    code = s3_manager.verify().code

    # Then
    assert code == S3VerifyCode.ACTIONABLE_CONNECTIVITY


@pytest.mark.parametrize(
    "no_proxy_env, endpoint, expected",
    [
        # Exact hostname match
        ("example.com", "https://example.com", True),
        # Domain suffix match
        (".example.com", "https://sub.example.com", True),
        # Subdomain match
        ("example.com", "https://sub.example.com", True),
        # Host not in no_proxy
        ("example.com", "https://other.com", False),
        # IP exact match
        ("10.1.1.1", "https://10.1.1.1", True),
        # IP in CIDR
        ("10.0.0.0/8", "https://10.152.183.1", True),
        # IP outside CIDR
        ("10.0.0.0/8", "https://192.168.1.1", False),
        # Multiple entries in no_proxy
        ("127.0.0.1,example.com,10.0.0.0/8", "https://10.5.5.5", True),
        ("127.0.0.1,example.com,10.0.0.0/8", "https://192.168.1.1", False),
        # Empty no_proxy
        ("", "https://anything.com", False),
        # Localhost and loopback IPs
        ("127.0.0.1,localhost,::1", "http://127.0.0.1", True),
        ("127.0.0.1,localhost,::1", "http://localhost", True),
        ("127.0.0.1,localhost,::1", "http://[::1]", True),
        ("127.0.0.1,localhost,::1", "http://10.0.0.1", False),
        # Empty endpoint or missing hostname
        ("example.com", "", False),
        ("example.com", "file:///tmp/file.txt", False),
        # Wildcard subdomain edge
        (".example.com", "https://deep.sub.example.com", True),
        # Multiple domains / IPs with whitespace
        (" example.com , 10.0.0.0/8 ,localhost ", "https://10.12.34.56", True),
        (" example.com , 10.0.0.0/8 ,localhost ", "https://otherhost.com", False),
        # Invalid CIDR entries (should be ignored)
        ("10.0.0.0/8,invalid_cidr,example.com", "https://10.1.2.3", True),
        ("10.0.0.0/8,invalid_cidr,example.com", "https://notexample.com", False),
        # Endpoint with port number
        ("example.com", "https://example.com:8080/path", True),
        ("example.com", "https://other.com:443/path", False),
        # Mixed case domain (should be case-insensitive)
        ("EXAMPLE.COM", "https://example.com", True),
        ("EXAMPLE.COM", "https://Sub.Example.Com", True),
    ],
)
def test_skip_proxy(no_proxy_env, endpoint, expected, monkeypatch):
    """Test that we are properly detecting that we should skip domains given a NO_PROXY env var."""
    # Given
    monkeypatch.setenv("JUJU_CHARM_NO_PROXY", no_proxy_env)

    # When
    should_skip_proxy = is_proxy_skipped(endpoint)

    # Then
    assert should_skip_proxy == expected
