import os
from unittest.mock import Mock

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_s3.client import S3Client

from core.domain import S3ConnectionInfo
from managers.s3 import S3Manager


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
