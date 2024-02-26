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
from charms.oathkeeper.v0.auth_proxy import (
    AuthProxyConfig,
    AuthProxyRelationRemovedEvent,
    AuthProxyRequirer,
)
from charms.traefik_k8s.v2.ingress import (
    IngressPerAppReadyEvent,
    IngressPerAppRequirer,
    IngressPerAppRevokedEvent,
)
from ops import RelationChangedEvent
from ops.charm import CharmBase, ConfigChangedEvent, InstallEvent
from ops.main import main
from ops.model import StatusBase

from config import SparkHistoryServerConfig
from constants import (
    AUTH_PARAMETER,
    CONTAINER,
    INGRESS_REL,
    OATHKEEPER_REL,
    PEBBLE_USER,
    S3_INTEGRATOR_REL,
)
from models import S3ConnectionInfo, Status, User
from utils import WithLogging
from workload import IOMode, SparkHistoryServer

AUTH_PROXY_HEADERS = ["X-User", AUTH_PARAMETER]


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

        self.auth_proxy = AuthProxyRequirer(self, self.auth_proxy_config, OATHKEEPER_REL)
        self.framework.observe(
            self.auth_proxy.on.auth_proxy_relation_removed, self._on_auth_proxy_removed
        )
        self.framework.observe(
            self.on[OATHKEEPER_REL].relation_changed, self._on_auth_proxy_changed
        )

        self.framework.observe(self.on.config_changed, self._on_config_changed)

    @property
    def auth_proxy_config(self) -> Optional[AuthProxyConfig]:
        """Configure the auth proxy relation."""
        if self.ingress.url:
            return AuthProxyConfig(
                protected_urls=[self.ingress.url],
                headers=AUTH_PROXY_HEADERS,
                allowed_endpoints=[],
            )
        else:
            return None

    @property
    def authorized_users_info(self) -> Optional[str]:
        """Checks if oathkeeper is related and returns allowed users."""
        relations = list(self.model.relations[OATHKEEPER_REL])
        assert len(relations) <= 1
        if len(relations) == 0:
            return None
        return self.model.config["authorized-users"].strip()

    @property
    def s3_connection_info(self) -> Optional[S3ConnectionInfo]:
        """Parse a S3ConnectionInfo object from relation data."""
        if not self.s3_requirer.relations:
            return None

        raw = self.s3_requirer.get_s3_connection_info()

        return S3ConnectionInfo(
            **{key.replace("-", "_"): value for key, value in raw.items() if key != "data"}
        )

    def get_status(
        self,
        s3: Optional[S3ConnectionInfo],
        ingress_url: Optional[str],
        authorized_users: Optional[str],
    ) -> StatusBase:
        """Compute and return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        if not s3:
            return Status.MISSING_S3_RELATION.value

        if not s3.verify():
            return Status.INVALID_CREDENTIALS.value

        if authorized_users:
            if not ingress_url:
                return Status.MISSING_INGRESS_RELATION.value

        return Status.ACTIVE.value

    def update_service(
        self,
        s3: Optional[S3ConnectionInfo],
        ingress_url: Optional[str],
        authorized_users: Optional[str],
    ) -> bool:
        """Update the Spark History server service if needed."""
        status = self.log_result(lambda _: f"Status: {_}")(
            self.get_status(s3, ingress_url, authorized_users)
        )

        self.unit.status = status

        # TODO: to avoid disruption (although minimal) if you could the logic below
        # conditionally depending on whether the Spark configuration content had changed
        with self.workload.get_spark_configuration_file(IOMode.WRITE) as fid:
            spark_config = SparkHistoryServerConfig(s3, ingress_url, authorized_users)
            self.logger.info(
                f"Updating configuration: s3:{s3} ingress_url:{ingress_url} oathkeeper:{authorized_users}"
            )
            self.logger.info(f"{spark_config.contents}")
            fid.write(spark_config.contents)

        if status is not Status.ACTIVE.value:
            self.logger.info(f"Cannot start service because of status {status}")
            self.workload.stop()
            return False

        self.workload.start()
        return True

    def _on_spark_history_server_pebble_ready(self, event):
        """Handle on Pebble ready event."""
        self.logger.info("Pebble ready")
        self.update_service(self.s3_connection_info, self.ingress.url, self.authorized_users_info)

    def _on_ingress_ready(self, event: IngressPerAppReadyEvent):
        """Handle the `IngressPerAppReadyEvent`."""
        self.logger.info("This app's ingress URL: %s", event.url)
        # auth proxy config
        self.auth_proxy.update_auth_proxy_config(auth_proxy_config=self.auth_proxy_config)
        self.update_service(self.s3_connection_info, event.url, self.authorized_users_info)

    def _on_ingress_revoked(self, _: IngressPerAppRevokedEvent):
        """Handle the `IngressPerAppRevokedEvent`."""
        self.log_result("This app no longer has ingress")(
            self.update_service(self.s3_connection_info, None, self.authorized_users_info)
        )

    def _on_install(self, event: InstallEvent) -> None:
        """Handle the `on_install` event."""
        self.unit.status = Status.WAITING_PEBBLE.value

    def _on_s3_credential_changed(self, _: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.update_service(self.s3_connection_info, self.ingress.url, self.authorized_users_info)

    def _on_s3_credential_gone(self, _: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.update_service(None, self.ingress.url, self.authorized_users_info)

    def _on_auth_proxy_changed(self, _: RelationChangedEvent):
        """Handle the changes of the AuthProxy."""
        self.logger.info("AuthProxy configuration change")
        self.update_service(self.s3_connection_info, self.ingress.url, self.authorized_users_info)

    def _on_auth_proxy_removed(self, _: AuthProxyRelationRemovedEvent):
        """Handle the removal of the AuthProxy."""
        self.logger.info("AuthProxy configuration gone")
        self.update_service(self.s3_connection_info, self.ingress.url, None)

    def _update_event(self, _):
        """Handle the update event hook."""
        self.unit.status = self.get_status(
            self.s3_connection_info, self.ingress.url, self.authorized_users_info
        )

    def _on_config_changed(self, event: ConfigChangedEvent):
        """Handle the on config changed event."""
        if not self.workload.ready:
            self.logger.info("On config changed event: pebble not ready -> defer")
            event.defer()
            return
        self.update_service(self.s3_connection_info, self.ingress.url, self.authorized_users_info)


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
