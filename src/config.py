#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Spark History Server configuration."""

from abc import ABC, abstractmethod
from typing import Dict

from constants import (
    CONFIG_KEY_S3_ACCESS_KEY,
    CONFIG_KEY_S3_CREDS_PROVIDER,
    CONFIG_KEY_S3_ENDPOINT,
    CONFIG_KEY_S3_LOGS_DIR,
    CONFIG_KEY_S3_SECRET_KEY,
)
from utils import WithLogging


class AbstractSparkHistoryServerConfig(WithLogging, ABC):
    """Abstraction for Spark History Server Configuration."""

    def __init__(self):
        self.config = {}

    @abstractmethod
    def get(self) -> Dict[str, str]:
        """Get Config."""
        pass

    @abstractmethod
    def populate(self, conf: Dict[str, str]) -> None:
        """Populate Config From Model or Relation etc."""
        pass

    def contents_before_init(self) -> str:
        """Return dummy contents to initialize pebble before admin configures the history server."""
        return "spark.eventLog.enabled=false"

    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        spark_config = f"spark.hadoop.fs.s3a.endpoint={self.get()[CONFIG_KEY_S3_ENDPOINT]}"
        spark_config += "\n"
        spark_config += f"spark.hadoop.fs.s3a.access.key={self.get()[CONFIG_KEY_S3_ACCESS_KEY]}"
        spark_config += "\n"
        spark_config += f"spark.hadoop.fs.s3a.secret.key={self.get()[CONFIG_KEY_S3_SECRET_KEY]}"
        spark_config += "\n"
        spark_config += f"spark.eventLog.dir={self.get()[CONFIG_KEY_S3_LOGS_DIR]}"
        spark_config += "\n"
        spark_config += f"spark.history.fs.logDirectory={self.get()[CONFIG_KEY_S3_LOGS_DIR]}"
        spark_config += "\n"
        spark_config += f"spark.hadoop.fs.s3a.aws.credentials.provider={self.get()[CONFIG_KEY_S3_CREDS_PROVIDER]}"
        spark_config += "\n"
        spark_config += "spark.hadoop.fs.s3a.connection.ssl.enabled=false"
        spark_config += "\n"
        spark_config += "spark.hadoop.fs.s3a.path.style.access=true"
        spark_config += "\n"
        spark_config += "spark.eventLog.enabled=true"

        return spark_config


class SparkHistoryServerConfigModel(AbstractSparkHistoryServerConfig):
    """Spark History Server Configuration coming from Model config file."""

    def get(self) -> Dict[str, str]:
        """Get Config."""
        return self.config

    def populate(self, conf: Dict[str, str]) -> None:
        """Populate Config From Model or Relation etc."""
        self.config[CONFIG_KEY_S3_ENDPOINT] = conf[CONFIG_KEY_S3_ENDPOINT]
        self.config[CONFIG_KEY_S3_ACCESS_KEY] = conf[CONFIG_KEY_S3_ACCESS_KEY]
        self.config[CONFIG_KEY_S3_SECRET_KEY] = conf[CONFIG_KEY_S3_SECRET_KEY]
        self.config[CONFIG_KEY_S3_CREDS_PROVIDER] = conf[CONFIG_KEY_S3_CREDS_PROVIDER]
        self.config[CONFIG_KEY_S3_LOGS_DIR] = conf[CONFIG_KEY_S3_LOGS_DIR]
