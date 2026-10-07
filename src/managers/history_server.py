#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""History Server manager."""

import os
from urllib.parse import ParseResult, urlparse, urlunparse

from common.utils import WithLogging, is_proxy_skipped
from core.context import AUTH_PROXY_HEADERS, OAUTH2_PROXY_HEADERS, Context
from core.workload import SparkHistoryWorkloadBase
from managers.tls import TLSManager


class HistoryServerConfig(WithLogging):
    """Class representing the Spark Properties configuration file."""

    _base_conf: dict[str, str] = {
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.eventLog.enabled": "true",
    }

    def __init__(
        self,
        context: Context,
    ):
        self.context = context

    @staticmethod
    def _ssl_enabled(endpoint: str | None) -> str:
        """Check if ssl is enabled."""
        if not endpoint or endpoint.startswith("https:") or ":443" in endpoint:
            return "true"

        return "false"

    @property
    def _ingress_proxy_conf(self) -> dict[str, str]:
        if not (ingress := self.context.ingress):
            return {}

        parsed_ingress = urlparse(str(ingress.url))
        redirect_uri = urlunparse((parsed_ingress.scheme, parsed_ingress.netloc, "", "", "", ""))
        ingress_properties = {"spark.ui.proxyRedirectUri": redirect_uri}

        if base := parsed_ingress.path.strip("/"):
            ingress_properties["spark.ui.proxyBase"] = base

        return ingress_properties

    @property
    def _s3_conf(self) -> dict[str, str]:
        if not (s3 := self.context.s3):
            return {}

        base_s3_conf = {
            "spark.hadoop.fs.s3a.endpoint": s3.endpoint or "https://s3.amazonaws.com",
            "spark.hadoop.fs.s3a.access.key": s3.access_key,
            "spark.hadoop.fs.s3a.secret.key": s3.secret_key,
            "spark.eventLog.dir": s3.log_dir,
            "spark.history.fs.logDirectory": s3.log_dir,
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": self._ssl_enabled(s3.endpoint),
        }

        s3_scheme = urlparse(s3.endpoint).scheme
        proxy_url = {
            "http": os.environ.get("JUJU_CHARM_HTTP_PROXY", ""),
            "https": os.environ.get("JUJU_CHARM_HTTPS_PROXY", ""),
        }.get(s3_scheme, os.environ.get("JUJU_CHARM_HTTP_PROXY", ""))

        if is_proxy_skipped(s3.endpoint):
            proxy_conf: dict[str, str] = {}
        else:
            match urlparse(proxy_url):
                case ParseResult(
                    username=str(username),
                    password=str(password),
                    hostname=str(hostname),
                    port=port,
                    scheme=scheme,
                ) if scheme in ("http", "https"):
                    port_str = str(port) if port else {"http": "80", "https": "443"}[scheme]
                    proxy_conf = {
                        "spark.hadoop.fs.s3a.proxy.host": hostname,
                        "spark.hadoop.fs.s3a.proxy.ssl.enabled": "true"
                        if scheme == "https"
                        else "false",
                        "spark.hadoop.fs.s3a.proxy.port": port_str,
                        "spark.hadoop.fs.s3a.proxy.username": username,
                        "spark.hadoop.fs.s3a.proxy.password": password,
                    }

                case ParseResult(
                    username=None,
                    password=None,
                    hostname=str(hostname),
                    port=port,
                    scheme=scheme,
                ) if scheme in ("http", "https"):
                    port_str = str(port) if port else {"http": "80", "https": "443"}[scheme]
                    proxy_conf = {
                        "spark.hadoop.fs.s3a.proxy.host": hostname,
                        "spark.hadoop.fs.s3a.proxy.ssl.enabled": "true"
                        if scheme == "https"
                        else "false",
                        "spark.hadoop.fs.s3a.proxy.port": port_str,
                    }

                case _:
                    proxy_conf = {}

        return base_s3_conf | proxy_conf

    @property
    def _azure_storage_conf(self) -> dict[str, str]:
        if not (azure_storage := self.context.azure_storage):
            return {}

        confs = {
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": azure_storage.log_dir,
            "spark.history.fs.logDirectory": azure_storage.log_dir,
        }
        connection_protocol = azure_storage.connection_protocol
        if connection_protocol.lower() in ("abfss", "abfs"):
            confs.update(
                {
                    f"spark.hadoop.fs.azure.account.key.{azure_storage.storage_account}.dfs.core.windows.net": azure_storage.secret_key
                }
            )
        elif connection_protocol.lower() in ("wasb", "wasbs"):
            confs.update(
                {
                    f"spark.hadoop.fs.azure.account.key.{azure_storage.storage_account}.blob.core.windows.net": azure_storage.secret_key
                }
            )
        return confs

    @property
    def _auth_conf(self) -> dict[str, str]:
        return (
            {
                "spark.ui.filters": "com.canonical.charmedspark.history.AuthorizationServletFilter",
                "spark.com.canonical.charmedspark.history.AuthorizationServletFilter.param.authorizedParameter": AUTH_PROXY_HEADERS[
                    1
                ]
                if (self.context.oathkeeper_relation)
                else OAUTH2_PROXY_HEADERS[1],
                "spark.com.canonical.charmedspark.history.AuthorizationServletFilter.param.authorizedEntities": users,
            }
            if (users := self.context.authorized_users)
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

    def __init__(self, context: Context, workload: SparkHistoryWorkloadBase):
        self.context = context
        self.workload = workload

        self.tls = TLSManager(workload)

    def update(self) -> None:
        """Update the Spark History server service if needed."""
        if not self.workload.ready():
            return

        s3 = self.context.s3
        azure = self.context.azure_storage

        config = HistoryServerConfig(self.context)

        self.workload.write(config.contents, str(self.workload.paths.spark_properties))
        self.workload.set_environment(
            {"SPARK_PROPERTIES_FILE": str(self.workload.paths.spark_properties)}
        )

        self.tls.reset()

        if not s3 and not azure:
            self.logger.info("Neither S3 nor Azure Storage are ready")
            self.workload.stop()
            return

        if s3 and (tls_ca_chain := s3.tls_ca_chain):
            self.tls.import_ca("\n".join(tls_ca_chain))
            self.workload.set_environment(
                {
                    "SPARK_HISTORY_OPTS": f"-Djavax.net.ssl.trustStore={self.workload.paths.truststore} "
                    f"-Djavax.net.ssl.trustStorePassword={self.tls.truststore_password}"
                }
            )

        self.workload.restart()
