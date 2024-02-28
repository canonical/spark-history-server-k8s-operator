#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Spark History Server configuration."""

import re
from typing import Optional

from models import S3ConnectionInfo
from utils import WithLogging


class SparkHistoryServerConfig(WithLogging):
    """Spark History Server Configuration."""

    _ingress_pattern = re.compile("http://.*?/|https://.*?/")

    def __init__(self, s3_connection_info: Optional[S3ConnectionInfo], ingress_url: Optional[str]):
        self.s3_connection_info = s3_connection_info
        self.ingress_url = ingress_url

    _base_conf: dict[str, str] = {
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.eventLog.enabled": "true",
    }

    def _ssl_enabled(self) -> str:
        """Check if ssl is enabled."""
        if self.s3_connection_info:
            if not self.s3_connection_info.endpoint:
                return "true"
            else:
                if (
                    self.s3_connection_info.endpoint.startswith("https:")
                    or ":443" in self.s3_connection_info.endpoint
                ):
                    return "true"

        return "false"

    @property
    def _ingress_proxy_conf(self) -> dict[str, str]:
        return (
            {
                "spark.ui.proxyBase": self._ingress_pattern.sub("/", self.ingress_url),
                "spark.ui.proxyRedirectUri": self._ingress_pattern.match(self.ingress_url).group(),
            }
            if self.ingress_url
            else {}
        )

    @property
    def _s3_conf(self) -> dict[str, str]:
        if self.s3_connection_info:
            return {
                "spark.hadoop.fs.s3a.endpoint": self.s3_connection_info.endpoint
                or "https://s3.amazonaws.com",
                "spark.hadoop.fs.s3a.access.key": self.s3_connection_info.access_key,
                "spark.hadoop.fs.s3a.secret.key": self.s3_connection_info.secret_key,
                "spark.eventLog.dir": self.s3_connection_info.log_dir,
                "spark.history.fs.logDirectory": self.s3_connection_info.log_dir,
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": self._ssl_enabled,  # type: ignore
            }
        return {}

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return self._base_conf | self._s3_conf | self._ingress_proxy_conf

    @property
    def contents(self) -> str:
        """Return configuration contents formatted to be consumed by pebble layer."""
        dict_content = self.to_dict()

        return "\n".join(
            [
                f"{key}={value}"
                for key in sorted(dict_content.keys())
                if (value := dict_content[key])
            ]
        )
