from __future__ import annotations

from typing import Iterable, cast
from unittest.mock import Mock, patch

import pytest
from azure.core.pipeline.transport import HttpTransport
from azure.storage.blob import ContainerClient

from core.domain import AzureStorageConnectionInfo
from managers.azure_storage import AzureStorageManager


class MockTransport(HttpTransport):
    pass


@pytest.fixture()
def connection_info() -> AzureStorageConnectionInfo:
    connection_info = Mock(spec=AzureStorageConnectionInfo)
    connection_info.endpoint = ""
    connection_info.storage_account = ""
    connection_info.secret_key = ""
    connection_info.container = "test_container"
    connection_info.path = "path"
    return cast(AzureStorageConnectionInfo, connection_info)


@pytest.fixture()
def container_client() -> ContainerClient:
    return cast(ContainerClient, Mock(spec=ContainerClient))


@pytest.fixture()
def manager(
    connection_info: AzureStorageConnectionInfo, container_client: ContainerClient
) -> Iterable[AzureStorageManager]:
    with patch("managers.azure_storage.AzureStorageManager.container", container_client):
        yield AzureStorageManager(connection_info)


def test_container_created_on_verify(
    connection_info: AzureStorageConnectionInfo,
    manager: AzureStorageManager,
    container_client: ContainerClient,
) -> None:
    """If the container does not exist, we create it."""
    # Given
    setattr(container_client.exists, "side_effect", [False, True])
    blob_client = container_client.get_blob_client(connection_info.path)
    setattr(blob_client.exists, "side_effect", [False, True])

    # When
    manager.verify()

    # Then
    getattr(container_client.create_container, "assert_called_once")()
    getattr(blob_client.create_append_blob, "assert_called_once")()


def test_container_existing_path_created_on_verify(
    connection_info: AzureStorageConnectionInfo,
    manager: AzureStorageManager,
    container_client: ContainerClient,
) -> None:
    """If the container does exist, we use it and add the path."""
    # Given
    setattr(container_client.exists, "side_effect", [True])
    blob_client = container_client.get_blob_client(connection_info.path)
    setattr(blob_client.exists, "side_effect", [False, True])

    # When
    manager.verify()

    # Then
    getattr(container_client.create_container, "assert_not_called")()
    getattr(blob_client.create_append_blob, "assert_called_once")()


def test_path_existing_still_ok_on_verify(
    connection_info: AzureStorageConnectionInfo,
    manager: AzureStorageManager,
    container_client: ContainerClient,
) -> None:
    """If the path already exists, safe to overwrite it."""
    # Given
    setattr(container_client.exists, "side_effect", [True])
    blob_client = container_client.get_blob_client(connection_info.path)
    setattr(blob_client.exists, "side_effect", [True])

    # When
    manager.verify()

    # Then
    getattr(container_client.create_container, "assert_not_called")()
    getattr(blob_client.create_append_blob, "assert_not_called")()
