#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""

from typing import Optional

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from charms.oathkeeper.v0.auth_proxy import AuthProxyConfig, AuthProxyRequirer
from charms.traefik_k8s.v2.ingress import (
    IngressPerAppReadyEvent,
    IngressPerAppRequirer,
    IngressPerAppRevokedEvent,
)
from ops.charm import (
    CharmBase,
    InstallEvent,
)
from ops.main import main
from ops.model import StatusBase

from config import SparkHistoryServerConfig
from constants import CONTAINER, INGRESS_REL, PEBBLE_USER, S3_INTEGRATOR_REL
from models import S3ConnectionInfo, Status, User
from utils import WithLogging
from workload import IOMode, SparkHistoryServer

AUTH_PROXY_ALLOWED_ENDPOINTS = ["version"]
AUTH_PROXY_HEADERS = ["X-User"]


class SparkHistoryServerCharm(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        self.framework.observe(self.on.install, self._update_event)

        self.framework.observe(
            self.on.spark_history_server_pebble_ready,
            self._on_spark_history_server_pebble_ready,
        )
        self.framework.observe(self.on.update_status, self._update_event)
        self.framework.observe(self.on.install, self._on_install)
        self.s3_requirer = S3Requirer(self, S3_INTEGRATOR_REL)
        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(self.s3_requirer.on.credentials_gone, self._on_s3_credential_gone)

        self.ingress = IngressPerAppRequirer(
            self, relation_name=INGRESS_REL, port=18080, strip_prefix=True
        )
        self.framework.observe(self.ingress.on.ready, self._on_ingress_ready)
        self.framework.observe(self.ingress.on.revoked, self._on_ingress_revoked)

        self.workload = SparkHistoryServer(
            self.unit.get_container(CONTAINER), User(name=PEBBLE_USER[0], group=PEBBLE_USER[1])
        )

        # oauth
        self.auth_proxy_relation_name = "auth-proxy"

        self.auth_proxy = AuthProxyRequirer(
            self, self.auth_proxy_config, self.auth_proxy_relation_name
        )

    @property
    def auth_proxy_config(self) -> AuthProxyConfig:
        """Configure the auth proxy relation."""
        return AuthProxyConfig(
            protected_urls=[
                self.ingress.url if self.ingress.url is not None else "https://some-test-url.com"
            ],
            headers=AUTH_PROXY_HEADERS,
            allowed_endpoints=AUTH_PROXY_ALLOWED_ENDPOINTS,
        )

    def _configure_auth_proxy(self):
        self.auth_proxy.update_auth_proxy_config(auth_proxy_config=self.auth_proxy_config)

    @property
    def s3_connection_info(self) -> Optional[S3ConnectionInfo]:
        """Parse a S3ConnectionInfo object from relation data."""
        if not self.s3_requirer.relations:
            return None

        raw = self.s3_requirer.get_s3_connection_info()

        return S3ConnectionInfo(
            **{key.replace("-", "_"): value for key, value in raw.items() if key != "data"}
        )

    def get_status(self, s3: Optional[S3ConnectionInfo]) -> StatusBase:
        """Compute and return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        if not s3:
            return Status.MISSING_S3_RELATION.value

        if not s3.verify():
            return Status.INVALID_CREDENTIALS.value

        return Status.ACTIVE.value

    def update_service(self, s3: Optional[S3ConnectionInfo], ingress_url: Optional[str]) -> bool:
        """Update the Spark History server service if needed."""
        status = self.log_result(lambda _: f"Status: {_}")(self.get_status(s3))

        self.unit.status = status

        # TODO: to avoid disruption (although minimal) if you could the logic below
        # conditionally depending on whether the Spark configuration content had changed
        with self.workload.get_spark_configuration_file(IOMode.WRITE) as fid:
            spark_config = SparkHistoryServerConfig(s3, ingress_url)
            fid.write(spark_config.contents)

        if status is not Status.ACTIVE.value:
            self.logger.info(f"Cannot start service because of status {status}")
            self.workload.stop()
            return False

        self.workload.start()
        return True

    def _on_spark_history_server_pebble_ready(self, event):
        self.logger.info("Pebble ready")
        self.update_service(self.s3_connection_info, self.ingress.url)

    def _on_ingress_ready(self, event: IngressPerAppReadyEvent):
        self.logger.info("This app's ingress URL: %s", event.url)
        self.update_service(self.s3_connection_info, event.url)
        # auth proxy config
        self._configure_auth_proxy()

    def _on_ingress_revoked(self, event: IngressPerAppRevokedEvent):
        self.log_result("This app no longer has ingress")(
            self.update_service(self.s3_connection_info, None)
        )

    def _on_install(self, event: InstallEvent) -> None:
        """Handle the `on_install` event."""
        self.unit.status = Status.WAITING_PEBBLE.value

    def _on_s3_credential_changed(self, event: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.update_service(self.s3_connection_info, self.ingress.url)

    def _on_s3_credential_gone(self, event: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.update_service(None, self.ingress.url)

    def _update_event(self, _):
        self.unit.status = self.get_status(self.s3_connection_info)


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
