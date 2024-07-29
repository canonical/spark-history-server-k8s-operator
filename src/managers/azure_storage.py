#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Azure storage manager."""


from common.utils import WithLogging
from core.domain import AzureStorageConnectionInfo


class AzureStorageManager(WithLogging):
    """Class exposing business logic for interacting with Azure Storage service."""

    def __init__(self, config: AzureStorageConnectionInfo):
        self.config = config
