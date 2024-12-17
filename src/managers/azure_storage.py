#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Azure storage manager."""


import os
from functools import cached_property

from azure.core.exceptions import ClientAuthenticationError, ResourceExistsError
from azure.storage.blob import BlobClient, ContainerClient
from tenacity import (
    RetryError,
    retry,
    retry_if_result,
    stop_after_attempt,
    wait_fixed,
)

from common.utils import WithLogging
from core.domain import AzureStorageConnectionInfo


class AzureStorageManager(WithLogging):
    """Class exposing business logic for interacting with Azure Storage service."""

    def __init__(self, config: AzureStorageConnectionInfo):
        self.config = config

    @cached_property
    def container_client(self) -> ContainerClient:
        """Azure container client session."""
        return ContainerClient(
            account_url=self.config.endpoint_http,
            container_name=self.config.container,
            credential=self.config.secret_key,
        )

    def get_or_create_container(self) -> bool:
        """Create container if it does not exists."""
        if not self.container_client.exists():
            try:
                self.container_client.create_container()
            except ResourceExistsError:
                # In case of race condition between multiple units
                pass

            try:
                self._wait_until_exists(self.container_client)
            except RetryError:
                return False

        blob_client = self.container_client.get_blob_client(
            os.path.join(self.config.path, ".keep")
        )
        if not blob_client.exists():
            blob_client.create_append_blob()
            try:
                self._wait_until_exists(blob_client)
            except RetryError:
                return False

        return True

    @retry(
        wait=wait_fixed(5),
        stop=stop_after_attempt(20),
        retry=retry_if_result(lambda res: not res),
    )
    def _wait_until_exists(self, resource: ContainerClient | BlobClient) -> bool:
        """Poll azure API until resource is found."""
        return resource.exists()

    def verify(self) -> bool:
        """Verify Azure credentials and configuration."""
        try:
            self.container_client.get_account_information()
        except ClientAuthenticationError:
            self.logger.error("Invalid Azure credentials")
            return False
        except Exception as e:
            self.logger.error(f"Azure related error {e}")
            return False

        if not self.get_or_create_container():
            self.logger.error("Could not create container or path.")
            return False

        return True
