#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Spark History Server configuration."""

from typing import Any, Dict

from constants import (
    CONFIG_KEY_S3_ACCESS_KEY,
    CONFIG_KEY_S3_BUCKET,
    CONFIG_KEY_S3_CREDS_PROVIDER,
    CONFIG_KEY_S3_ENDPOINT,
    CONFIG_KEY_S3_LOGS_DIR,
    CONFIG_KEY_S3_SECRET_KEY,
    CONFIG_KEY_S3_SSL_ENABLED,
)
from utils import WithLogging


class SparkHistoryServerConfig(WithLogging):
    """Spark History Server Configuration."""

    def __init__(self, conn_config: Dict[str, Any], model_config: Dict[str, Any]):
        self.conn_config = conn_config
        self.model_config = model_config

    def update_conn_config(self, s3_integrator_conf: Dict[str, str]):
        """Update credentials in config cache."""
        # self.config.update(conf)

        # update incoming credentials if present
        if CONFIG_KEY_S3_ENDPOINT in s3_integrator_conf:
            self.conn_config[CONFIG_KEY_S3_ENDPOINT] = s3_integrator_conf[CONFIG_KEY_S3_ENDPOINT]
        if CONFIG_KEY_S3_ACCESS_KEY in s3_integrator_conf:
            self.conn_config[CONFIG_KEY_S3_ACCESS_KEY] = s3_integrator_conf[
                CONFIG_KEY_S3_ACCESS_KEY
            ]
        if CONFIG_KEY_S3_SECRET_KEY in s3_integrator_conf:
            self.conn_config[CONFIG_KEY_S3_SECRET_KEY] = s3_integrator_conf[
                CONFIG_KEY_S3_SECRET_KEY
            ]
        if CONFIG_KEY_S3_BUCKET in s3_integrator_conf:
            self.conn_config[CONFIG_KEY_S3_BUCKET] = s3_integrator_conf[CONFIG_KEY_S3_BUCKET]
        if CONFIG_KEY_S3_LOGS_DIR in s3_integrator_conf:
            self.conn_config[CONFIG_KEY_S3_LOGS_DIR] = s3_integrator_conf[CONFIG_KEY_S3_LOGS_DIR]

    def purge_conn_config(self):
        """Purge credentials config cache, not the extra config."""
        # self.conn_config.clear()
        self.conn_config[CONFIG_KEY_S3_ENDPOINT] = ""
        self.conn_config[CONFIG_KEY_S3_ACCESS_KEY] = ""
        self.conn_config[CONFIG_KEY_S3_SECRET_KEY] = ""
        self.conn_config[CONFIG_KEY_S3_BUCKET] = ""
        self.conn_config[CONFIG_KEY_S3_LOGS_DIR] = ""

    def verify_conn_config(self, input: Dict[str, str]) -> bool:
        """Verify incoming credentials."""
        return all(
            x in input and input.get(x, "MISSING") != "MISSING"
            for x in [
                CONFIG_KEY_S3_ENDPOINT,
                CONFIG_KEY_S3_ACCESS_KEY,
                CONFIG_KEY_S3_SECRET_KEY,
                CONFIG_KEY_S3_BUCKET,
                CONFIG_KEY_S3_LOGS_DIR,
            ]
        )

    @property
    def s3_log_dir(self) -> str:
        """Return the fully constructed S3 path to be used."""
        return f"s3a://{self.conn_config.get(CONFIG_KEY_S3_BUCKET, '')}/{self.conn_config.get(CONFIG_KEY_S3_LOGS_DIR, '')}"

    @property
    def spark_conf(self):
        """Return the dict representation of the configuration file."""
        s3_log_dir = self.s3_log_dir
        return {
            "spark.hadoop.fs.s3a.endpoint": self.conn_config.get(CONFIG_KEY_S3_ENDPOINT, ""),
            "spark.hadoop.fs.s3a.access.key": self.conn_config.get(CONFIG_KEY_S3_ACCESS_KEY, ""),
            "spark.hadoop.fs.s3a.secret.key": self.conn_config.get(CONFIG_KEY_S3_SECRET_KEY, ""),
            "spark.eventLog.dir": s3_log_dir,
            "spark.history.fs.logDirectory": s3_log_dir,
            "spark.hadoop.fs.s3a.aws.credentials.provider": self.model_config.get(
                CONFIG_KEY_S3_CREDS_PROVIDER,
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            ),
            "spark.hadoop.fs.s3a.connection.ssl.enabled": self.model_config.get(
                CONFIG_KEY_S3_SSL_ENABLED, "false"
            ),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.eventLog.enabled": "true",
        }

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        return "\n".join(
            [f"{key}={value}" for key, value in self.spark_conf.items() if value is not None]
        )
