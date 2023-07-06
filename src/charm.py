#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""

from typing import Optional

from ops.charm import (
    CharmBase,
    InstallEvent,
)
from ops.main import main

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from charms.traefik_k8s.v2.ingress import (
    IngressPerAppReadyEvent,
    IngressPerAppRequirer,
    IngressPerAppRevokedEvent,
)
from config import SparkHistoryServerConfig
from constants import (
    CONTAINER,
    S3_INTEGRATOR_REL,
)
from models import S3ConnectionInfo, User, Status
from utils import WithLogging
from workload import SparkHistoryServer


class SparkHistoryServerCharm(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)
        self.framework.observe(
            self.on.spark_history_server_pebble_ready,
            self._on_spark_history_server_pebble_ready,
        )
        self.framework.observe(self.on.install, self._on_install)
        self.s3_requirer = S3Requirer(self, S3_INTEGRATOR_REL)
        self.framework.observe(
            self.s3_requirer.on.credentials_changed, self._on_s3_credential_changed
        )
        self.framework.observe(
            self.s3_requirer.on.credentials_gone, self._on_s3_credential_gone
        )

        self.ingress = IngressPerAppRequirer(self, port=18080, strip_prefix=True)
        self.framework.observe(self.ingress.on.ready, self._on_ingress_ready)
        self.framework.observe(self.ingress.on.revoked, self._on_ingress_revoked)

        self.workload = SparkHistoryServer(
            self.unit.get_container(CONTAINER),
            User(name="spark", user_id=185, group="spark", group_id=185)
        )

    @property
    def s3_connection_info(self) -> Optional[S3ConnectionInfo]:
        if not self.s3_requirer.relations:
            return None

        connection = S3ConnectionInfo(**self.s3_requirer.get_s3_connection_info())

        assert connection.verify()

        return connection

    @property
    def spark_config(self):
        return SparkHistoryServerConfig(self.s3_connection_info, self.ingress.url)

    def get_status(self):
        if not self.workload.ready():
            return Status.WAITING_PEBBLE

        if not self.s3_connection_info:
            return Status.MISSING_S3_RELATION

        if not self.s3_connection_info.verify():
            return Status.INVALID_CREDENTIALS

        return Status.ACTIVE

    def update_service(self) -> bool:
        status = self.get_status()

        if status is not Status.ACTIVE:
            self.logger.info(f"Cannot start service because of status {status}")
            return False

        with self.workload.spark_configuration_file as fid:
            fid.write(self.spark_config.contents)

        self.workload.start()
        return True

    def _on_spark_history_server_pebble_ready(self, event):
        self.logger.info("Pebble ready")
        self.update_service()

    def _on_ingress_ready(self, event: IngressPerAppReadyEvent):
        self.logger.info("This app's ingress URL: %s", event.url)
        if not self.update_service():
            event.defer()

    def _on_ingress_revoked(self, event: IngressPerAppRevokedEvent):
        self.logger.info("This app no longer has ingress")
        if not self.update_service():
            event.defer()

    def _on_install(self, event: InstallEvent) -> None:
        """Handle the `on_install` event."""
        self.unit.status = Status.WAITING_PEBBLE

    def _on_s3_credential_changed(self, event: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        if not self.update_service():
            event.defer()

    def _on_s3_credential_gone(self, event: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.update_service()


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
