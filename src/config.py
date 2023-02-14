#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Spark History Server configuration."""

from typing import Any, Dict

from constants import (
    CONFIG_KEY_S3_ACCESS_KEY,
    CONFIG_KEY_S3_ENDPOINT,
    CONFIG_KEY_S3_LOGS_DIR,
    CONFIG_KEY_S3_SECRET_KEY,
    CONFIG_KEY_S3_SSL_ENABLED,
    CONFIG_KEY_S3_CREDS_PROVIDER
)
from utils import WithLogging


class SparkHistoryServerConfig(WithLogging):
    """Spark History Server Configuration."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.spark_conf = {}

    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        self.spark_conf = {
            "spark.hadoop.fs.s3a.endpoint": self.config[CONFIG_KEY_S3_ENDPOINT],
            "spark.hadoop.fs.s3a.access.key": self.config[CONFIG_KEY_S3_ACCESS_KEY],
            "spark.hadoop.fs.s3a.secret.key": self.config[CONFIG_KEY_S3_SECRET_KEY],
            "spark.eventLog.dir": self.config[CONFIG_KEY_S3_LOGS_DIR],
            "spark.history.fs.logDirectory": self.config[CONFIG_KEY_S3_LOGS_DIR],
            "spark.hadoop.fs.s3a.aws.credentials.provider": self.config.get(
                CONFIG_KEY_S3_CREDS_PROVIDER,
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            ),
            "spark.hadoop.fs.s3a.connection.ssl.enabled": self.config[CONFIG_KEY_S3_SSL_ENABLED],
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.eventLog.enabled": "true",
        }
        return "\n".join(
            [f"{key}={value}" for key, value in self.spark_conf.items() if value is not None]
        )
