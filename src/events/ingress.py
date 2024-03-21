#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Ingress related event handlers."""

from charms.oathkeeper.v0.auth_proxy import (
    AuthProxyRelationRemovedEvent,
    AuthProxyRequirer,
)
from charms.traefik_k8s.v2.ingress import (
    IngressPerAppReadyEvent,
    IngressPerAppRequirer,
    IngressPerAppRevokedEvent,
)
from ops import CharmBase

from common.utils import WithLogging
from core.context import INGRESS, OATHKEEPER, Context
from core.workload import SparkHistoryWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.history_server import HistoryServerManager


class IngressEvents(BaseEventHandler, WithLogging):
    """Class implementing ingress-related event hooks."""

    def __init__(self, charm: CharmBase, context: Context, workload: SparkHistoryWorkloadBase):
        super().__init__(charm, "ingress")

        self.charm = charm
        self.context = context
        self.workload = workload

        self.history_server = HistoryServerManager(self.workload)

        self.ingress = IngressPerAppRequirer(
            charm, relation_name=INGRESS, port=18080, strip_prefix=True
        )
        self.framework.observe(self.ingress.on.ready, self._on_ingress_ready)
        self.framework.observe(self.ingress.on.revoked, self._on_ingress_revoked)

        self.auth_proxy = AuthProxyRequirer(charm, self.context.auth_proxy_config, OATHKEEPER)
        self.framework.observe(
            self.auth_proxy.on.auth_proxy_relation_removed, self._on_auth_proxy_removed
        )

    @compute_status
    def _on_ingress_ready(self, event: IngressPerAppReadyEvent):
        """Handle the `IngressPerAppReadyEvent`."""
        self.logger.info("This app's ingress URL: %s", event.url)

        self.history_server.update(self.context.s3, self.context.ingress)

        # auth proxy config
        self.auth_proxy.update_auth_proxy_config(auth_proxy_config=self.context.auth_proxy_config)

    def _on_ingress_revoked(self, _: IngressPerAppRevokedEvent):
        """Handle the `IngressPerAppRevokedEvent`."""
        self.log_result("This app no longer has ingress")(
            self.history_server.update(self.context.s3, None)
        )

        self.charm.unit.status = self.get_app_status(
            self.context.s3, None, self.context.auth_proxy_config
        )
        if self.charm.unit.is_leader():
            self.charm.app.status = self.get_app_status(
                self.context.s3, None, self.context.auth_proxy_config
            )

    def _on_auth_proxy_removed(self, _: AuthProxyRelationRemovedEvent):
        """Handle the removal of the AuthProxy."""
        self.logger.info("AuthProxy configuration gone")
        self.history_server.update(self.context.s3, self.context.ingress)

        self.charm.unit.status = self.get_app_status(self.context.s3, self.context.ingress, None)
        if self.charm.unit.is_leader():
            self.charm.app.status = self.get_app_status(
                self.context.s3, self.context.ingress, None
            )
