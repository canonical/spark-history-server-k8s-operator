#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Spark History Server configuration."""

from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError
from charms.data_platform_libs.v0.s3 import (
    S3Requirer,
)

from constants import (
    CONFIG_KEY_S3_ACCESS_KEY,
    CONFIG_KEY_S3_BUCKET,
    CONFIG_KEY_S3_ENDPOINT,
    CONFIG_KEY_S3_LOGS_DIR,
    CONFIG_KEY_S3_SECRET_KEY,
)
from utils import WithLogging


class SparkHistoryServerConfig(WithLogging):
    """Spark History Server Configuration."""

    def __init__(self, s3_creds_client: S3Requirer, model_config: Dict[str, Any]):
        self.s3_creds_client = s3_creds_client
        self.model_config = model_config

    def verify_conn_config(self) -> bool:
        """Verify S3 credentials."""
        conn_config = self.s3_creds_client.get_s3_connection_info()
        if not all(
            x in conn_config and conn_config.get(x, "MISSING") != "MISSING"
            for x in [CONFIG_KEY_S3_ACCESS_KEY, CONFIG_KEY_S3_SECRET_KEY, CONFIG_KEY_S3_BUCKET]
        ):
            return False

        session = boto3.session.Session(
            aws_access_key_id=conn_config[CONFIG_KEY_S3_ACCESS_KEY],
            aws_secret_access_key=conn_config[CONFIG_KEY_S3_SECRET_KEY],
        )

        s3 = session.client(
            "s3", endpoint_url=conn_config.get(CONFIG_KEY_S3_ENDPOINT, "https://s3.amazonaws.com")
        )

        try:
            s3.list_buckets()
        except ClientError:
            self.logger.error("Invalid S3 credentials...")
            return False

        return True

    @property
    def s3_log_dir(self) -> str:
        """Return the fully constructed S3 path to be used."""
        conn_config = self.s3_creds_client.get_s3_connection_info()
        if CONFIG_KEY_S3_BUCKET not in conn_config:
            return "s3a://"
        else:
            return f"s3a://{conn_config[CONFIG_KEY_S3_BUCKET]}/{conn_config.get(CONFIG_KEY_S3_LOGS_DIR, '')}"

    @property
    def spark_conf(self):
        """Return the dict representation of the configuration file."""
        s3_log_dir = self.s3_log_dir
        conn_config = self.s3_creds_client.get_s3_connection_info()
        return {
            "spark.hadoop.fs.s3a.endpoint": conn_config.get(
                CONFIG_KEY_S3_ENDPOINT, "https://s3.amazonaws.com"
            ),
            "spark.hadoop.fs.s3a.access.key": conn_config.get(CONFIG_KEY_S3_ACCESS_KEY, ""),
            "spark.hadoop.fs.s3a.secret.key": conn_config.get(CONFIG_KEY_S3_SECRET_KEY, ""),
            "spark.eventLog.dir": s3_log_dir,
            "spark.history.fs.logDirectory": s3_log_dir,
            # to come from relations later
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.eventLog.enabled": "true",
        }

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        return "\n".join(
            [f"{key}={value}" for key, value in self.spark_conf.items() if value is not None]
        )
