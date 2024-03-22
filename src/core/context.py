#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from enum import Enum

from charms.data_platform_libs.v0.data_interfaces import RequirerData
from charms.oathkeeper.v0.auth_proxy import AuthProxyConfig
from charms.traefik_k8s.v2.ingress import IngressProviderAppData, IngressUrl
from ops import ActiveStatus, BlockedStatus, CharmBase, MaintenanceStatus, ModelError, Relation

from common.utils import WithLogging
from core.domain import S3ConnectionInfo

S3 = "s3-credentials"
INGRESS = "ingress"
OATHKEEPER = "auth-proxy"
AUTHORIZED_USERS = "authorized-users"
AUTH_PROXY_HEADERS = ["X-User", "X-Email"]


class Context(WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):

        self.charm = charm
        self.model = charm.model

        self.s3_endpoint = RequirerData(
            self.charm.model, S3
        )  # TODO: It would be nice if we had something that is more general (e.g. without extra-user-roles)

    # --------------
    # --- CONFIG ---
    # --------------
    @property
    def authorized_users(self) -> str | None:
        """The comma-separated list of authorized users."""
        return self.charm.config[AUTHORIZED_USERS] if self._oathkeeper_relation else None

    # -----------------
    # --- RELATIONS ---
    # -----------------

    @property
    def _s3_relation_id(self) -> int | None:
        """The S3 relation."""
        return relation.id if (relation := self.charm.model.get_relation(S3)) else None

    @property
    def _s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.charm.model.get_relation(S3)

    @property
    def _ingress_relation(self) -> Relation | None:
        """The ingress relation."""
        return self.charm.model.get_relation(INGRESS)

    @property
    def _oathkeeper_relation(self) -> Relation | None:
        """Checks if oathkeeper is related."""
        relations = list(self.model.relations[OATHKEEPER])

        if len(relations) > 1:
            # This should be prevented by endpoint specification which limits
            # number of units to 1
            raise ValueError("Cannot handle more than one oathkeeper relation")

        return relations[0] if relations else None

    # --- DOMAIN OBJECTS ---

    @property
    def s3(self) -> S3ConnectionInfo | None:
        """The server state of the current running Unit."""
        return S3ConnectionInfo(rel, rel.app) if (rel := self._s3_relation) else None

    @property
    def ingress(self) -> IngressUrl | None:
        """Return the Ingress information when available."""
        relation = self._ingress_relation
        if not relation or not relation.app:
            return None

        # fetch the provider's app databag
        try:
            databag = relation.data[relation.app]
        except ModelError as e:
            self.logger.debug(
                f"Error {e} attempting to read remote app data; "
                f"probably we are in a relation_departed hook"
            )
            return None

        if not databag:  # not ready yet
            return None

        return IngressProviderAppData.load(databag).ingress

    @property
    def auth_proxy_config(self) -> AuthProxyConfig | None:
        """Configure the auth proxy relation."""
        if self._oathkeeper_relation:
            return AuthProxyConfig(
                protected_urls=[self.ingress.url] if self.ingress else [],
                headers=AUTH_PROXY_HEADERS,
                allowed_endpoints=[],
            )
        else:
            return None


class Status(Enum):
    """Class bundling all statuses that the charm may fall into."""

    WAITING_PEBBLE = MaintenanceStatus("Waiting for Pebble")
    MISSING_S3_RELATION = BlockedStatus("Missing S3 relation")
    INVALID_CREDENTIALS = BlockedStatus("Invalid S3 credentials")
    MISSING_INGRESS_RELATION = BlockedStatus("Missing INGRESS relation")
    NOT_RUNNING = BlockedStatus("History server not running. Please check logs.")
    ACTIVE = ActiveStatus("")
