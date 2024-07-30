#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""History Server manager."""

import re

from common.utils import WithLogging
from core.context import AUTH_PROXY_HEADERS, IngressUrl, S3ConnectionInfo
from core.domain import AzureStorageConnectionInfo
from core.workload import SparkHistoryWorkloadBase
from managers.azure_storage import AzureStorageManager
from managers.s3 import S3Manager
from managers.tls import TLSManager


class HistoryServerConfig(WithLogging):
    """Class representing the Spark Properties configuration file."""

    _ingress_pattern = re.compile("http://.*?/|https://.*?/")

    _base_conf: dict[str, str] = {
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.eventLog.enabled": "true",
    }

    def __init__(
        self,
        s3: S3Manager | None,
        azure: AzureStorageConnectionInfo | None,
        ingress: IngressUrl | None,
        authorized_users: str | None,
    ):
        self.s3 = s3
        self.azure_storage = AzureStorageManager(azure) if azure else None
        self.ingress = ingress
        self.authorized_users = authorized_users

    @staticmethod
    def _ssl_enabled(endpoint: str | None) -> str:
        """Check if ssl is enabled."""
        if not endpoint or endpoint.startswith("https:") or ":443" in endpoint:
            return "true"

        return "false"

    @property
    def _ingress_proxy_conf(self) -> dict[str, str]:
        return (
            {
                "spark.ui.proxyBase": self._ingress_pattern.sub("/", ingress.url),
                "spark.ui.proxyRedirectUri": self._ingress_pattern.match(ingress.url).group(),
            }
            if (ingress := self.ingress)
            else {}
        )

    @property
    def _s3_conf(self) -> dict[str, str]:
        if (s3 := self.s3) and s3.verify():
            return {
                "spark.hadoop.fs.s3a.endpoint": s3.config.endpoint or "https://s3.amazonaws.com",
                "spark.hadoop.fs.s3a.access.key": s3.config.access_key,
                "spark.hadoop.fs.s3a.secret.key": s3.config.secret_key,
                "spark.eventLog.dir": s3.config.log_dir,
                "spark.history.fs.logDirectory": s3.config.log_dir,
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": self._ssl_enabled(
                    s3.config.endpoint
                ),
            }
        return {}

    @property
    def _azure_storage_conf(self) -> dict[str, str]:
        if azure_storage := self.azure_storage:
            confs = {
                "spark.eventLog.enabled": "true",
                "spark.eventLog.dir": azure_storage.config.log_dir,
                "spark.history.fs.logDirectory": azure_storage.config.log_dir,
            }
            connection_protocol = azure_storage.config.connection_protocol
            if connection_protocol.lower() in ("abfss", "abfs"):
                confs.update(
                    {
                        f"spark.hadoop.fs.azure.account.key.{azure_storage.config.storage_account}.dfs.core.windows.net": azure_storage.config.secret_key
                    }
                )
            elif connection_protocol.lower() in ("wasb", "wasbs"):
                confs.update(
                    {
                        f"spark.hadoop.fs.azure.account.key.{azure_storage.config.storage_account}.blob.core.windows.net": azure_storage.config.secret_key
                    }
                )
            return confs
        return {}

    @property
    def _auth_conf(self) -> dict[str, str]:
        return (
            {
                "spark.ui.filters": "com.canonical.charmedspark.history.AuthorizationServletFilter",
                "spark.com.canonical.charmedspark.history.AuthorizationServletFilter.param.authorizedParameter": AUTH_PROXY_HEADERS[
                    1
                ],
                "spark.com.canonical.charmedspark.history.AuthorizationServletFilter.param.authorizedEntities": users,
            }
            if (users := self.authorized_users)
            else {}
        )

    def to_dict(self) -> dict[str, str]:
        """Return the dict representation of the configuration file."""
        return (
            self._base_conf
            | self._s3_conf
            | self._azure_storage_conf
            | self._ingress_proxy_conf
            | self._auth_conf
        )

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


class HistoryServerManager(WithLogging):
    """Class exposing general functionalities of the SparkHistoryServer workload."""

    def __init__(self, workload: SparkHistoryWorkloadBase):
        self.workload = workload

        self.tls = TLSManager(workload)

    def update(
        self,
        s3: S3ConnectionInfo | None,
        azure: AzureStorageConnectionInfo | None,
        ingress: IngressUrl | None,
        authorized_users: str | None,
    ) -> None:
        """Update the Spark History server service if needed."""
        if not self.workload.ready():
            return

        self.workload.stop()

        s3_manager = S3Manager(s3) if s3 else None
        config = HistoryServerConfig(s3_manager, azure, ingress, authorized_users)

        self.workload.write(config.contents, str(self.workload.paths.spark_properties))
        self.workload.set_environment(
            {"SPARK_PROPERTIES_FILE": str(self.workload.paths.spark_properties)}
        )

        self.tls.reset()

        if (not s3_manager or not s3_manager.verify()) and not azure:
            self.logger.info("Nor s3 or azure are ready")
            return
        if s3:
            if tls_ca_chain := s3.tls_ca_chain:
                self.tls.import_ca("\n".join(tls_ca_chain))
                self.workload.set_environment(
                    {
                        "SPARK_HISTORY_OPTS": f"-Djavax.net.ssl.trustStore={self.workload.paths.truststore} "
                        f"-Djavax.net.ssl.trustStorePassword={self.tls.truststore_password}"
                    }
                )
            else:
                self.workload.set_environment({"SPARK_HISTORY_OPTS": ""})

        self.workload.start()
