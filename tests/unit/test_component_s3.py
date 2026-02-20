from __future__ import annotations

import os
from typing import TYPE_CHECKING
from unittest.mock import Mock

import boto3
import pytest
from moto import mock_aws

from core.domain import S3ConnectionInfo
from managers.s3 import S3Manager, is_proxy_skipped

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


def test_bucket_created_on_verify(s3: S3Client) -> None:
    """If the bucket does not exist, we create it."""
    # Given
    bucket_name = "test_bucket"
    connection_info = Mock(spec=S3ConnectionInfo)
    connection_info.endpoint = ""
    connection_info.access_key = ""
    connection_info.secret_key = ""
    connection_info.bucket = bucket_name
    connection_info.path = "path"
    connection_info.tls_ca_chain = []
    connection_info.region = ""
    s3_manager = S3Manager(connection_info)

    assert not len(s3.list_buckets()["Buckets"])

    # When
    s3_manager.verify()

    # Then
    assert len(buckets := s3.list_buckets()["Buckets"]) == 1
    assert buckets[0].get("Name", "") == bucket_name
    # Note that the path provided as been transformed into a directory structure
    assert "Contents" in s3.list_objects_v2(Bucket=bucket_name, Prefix="path/", MaxKeys=1)


def test_bucket_existing_path_created_on_verify(s3: S3Client) -> None:
    """If the bucket does exist, we use it and add the path."""
    # Given
    bucket_name = "test_bucket"
    connection_info = Mock(spec=S3ConnectionInfo)
    connection_info.endpoint = ""
    connection_info.access_key = ""
    connection_info.secret_key = ""
    connection_info.bucket = bucket_name
    connection_info.path = "path"
    connection_info.tls_ca_chain = []
    connection_info.region = ""
    s3_manager = S3Manager(connection_info)

    s3.create_bucket(Bucket=bucket_name)
    assert len(buckets := s3.list_buckets()["Buckets"]) == 1
    assert buckets[0].get("Name", "") == bucket_name

    # When
    s3_manager.verify()

    # Then
    assert len(buckets := s3.list_buckets()["Buckets"]) == 1
    # Note that the path provided as been transformed into a directory structure
    assert "Contents" in s3.list_objects_v2(Bucket=bucket_name, Prefix="path/", MaxKeys=1)


def test_path_existing_still_ok_on_verify(s3: S3Client) -> None:
    """If the path already exists, safe to overwrite it."""
    # Given
    bucket_name = "test_bucket"
    connection_info = Mock(spec=S3ConnectionInfo)
    connection_info.endpoint = ""
    connection_info.access_key = ""
    connection_info.secret_key = ""
    connection_info.bucket = bucket_name
    connection_info.path = "path"
    connection_info.tls_ca_chain = []
    connection_info.region = ""
    s3_manager = S3Manager(connection_info)

    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key="path/")
    assert len(buckets := s3.list_buckets()["Buckets"]) == 1
    assert buckets[0].get("Name", "") == bucket_name

    # When
    s3_manager.verify()

    # Then
    assert len(buckets := s3.list_buckets()["Buckets"]) == 1
    # Note that the path provided as been transformed into a directory structure
    assert "Contents" in s3.list_objects_v2(Bucket=bucket_name, Prefix="path/", MaxKeys=3)


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
    # Patch JUJU_CHARM_NO_PROXY env var
    monkeypatch.setenv("JUJU_CHARM_NO_PROXY", no_proxy_env)

    # When
    should_skip_proxy = is_proxy_skipped(endpoint)

    # Then
    assert should_skip_proxy == expected
