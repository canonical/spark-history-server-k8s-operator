#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charm Context definition and parsing logic."""

from typing import cast

from charms.oathkeeper.v0.auth_proxy import AuthProxyConfig as OathkeeperAuthProxyConfig
from charms.oauth2_proxy_k8s.v0.auth_proxy import AuthProxyConfig
from charms.traefik_k8s.v2.ingress import IngressProviderAppData, IngressUrl
from data_platform_helpers.advanced_statuses.components import StatusesState
from data_platform_helpers.advanced_statuses.protocol import StatusesStateProtocol
from object_storage import AzureStorageRequirer, S3Requirer
from ops import CharmBase, ModelError, Relation

from common.utils import WithLogging
from constants import AZURE_RELATION_NAME, S3_RELATION_NAME, STATUS_RELATION_NAME
from core.domain import AzureStorageConnectionInfo, S3ConnectionInfo

OATHKEEPER = "auth-proxy"
INGRESS = "ingress"
OAUTH2_PROXY = "oauth2-proxy"
AUTHORIZED_USERS = "authorized-users"
AUTH_PROXY_HEADERS = ["X-User", "X-Email"]
OAUTH2_PROXY_HEADERS = ["X-Auth-Request-User", "X-Auth-Request-Email"]


class Context(WithLogging, StatusesStateProtocol):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):
        self.charm = charm
        self.model = charm.model

        self.s3_requirer = S3Requirer(self.charm, S3_RELATION_NAME)
        self.azure_storage_requirer = AzureStorageRequirer(self.charm, AZURE_RELATION_NAME)
        self.statuses = StatusesState(self.charm, STATUS_RELATION_NAME)

    # --------------
    # --- CONFIG ---
    # --------------
    @property
    def authorized_users(self) -> str | None:
        """The comma-separated list of authorized users."""
        return (
            str(self.charm.config[AUTHORIZED_USERS])
            if (self.oathkeeper_relation or self.oauth2_proxy_relation)
            else None
        )

    # -----------------
    # --- RELATIONS ---
    # -----------------

    @property
    def _s3_relation_id(self) -> int | None:
        """The S3 relation."""
        return (
            relation.id if (relation := self.charm.model.get_relation(S3_RELATION_NAME)) else None
        )

    @property
    def s3_relation(self) -> Relation | None:
        """The S3 relation."""
        return self.charm.model.get_relation(S3_RELATION_NAME)

    @property
    def ingress_relation(self) -> Relation | None:
        """The ingress relation."""
        return self.charm.model.get_relation(INGRESS)

    @property
    def oathkeeper_relation(self) -> Relation | None:
        """Checks if oathkeeper is related."""
        relations = list(self.model.relations[OATHKEEPER])
        if len(relations) > 1:
            # This should be prevented by endpoint specification which limits
            # number of units to 1
            raise ValueError("Cannot handle more than one oathkeeper relation")

        return relations[0] if relations else None

    @property
    def oauth2_proxy_relation(self) -> Relation | None:
        """Checks if oauth2_proxy is related."""
        relations = list(self.model.relations[OAUTH2_PROXY])

        if len(relations) > 1:
            # This should be prevented by endpoint specification which limits
            # number of units to 1
            raise ValueError("Cannot handle more than one oauth2_proxy relation")

        return relations[0] if relations else None

    # --- DOMAIN OBJECTS ---

    @property
    def s3(self) -> S3ConnectionInfo | None:
        """The server state of the current running Unit."""
        relation_data = (
            self.s3_requirer.get_storage_connection_info(self.s3_relation)
            if self.s3_relation
            else None
        )
        return S3ConnectionInfo(cast(dict, relation_data)) if relation_data else None

    @property
    def ingress(self) -> IngressUrl | None:
        """Return the Ingress information when available."""
        relation = self.ingress_relation
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
    def oauth2_proxy_config(self) -> AuthProxyConfig | None:
        """Configure the auth proxy relation."""
        if self.oauth2_proxy_relation:
            return AuthProxyConfig(
                protected_urls=[str(self.ingress.url)] if self.ingress else [],
                allowed_endpoints=[],
                headers=OAUTH2_PROXY_HEADERS,
            )
        else:
            return None

    @property
    def auth_proxy_config(self) -> OathkeeperAuthProxyConfig | None:
        """Configure the auth proxy relation."""
        if self.oathkeeper_relation:
            return OathkeeperAuthProxyConfig(
                protected_urls=[str(self.ingress.url)] if self.ingress else [],
                headers=AUTH_PROXY_HEADERS,
                allowed_endpoints=[],
            )
        else:
            return None

    @property
    def _azure_storage_relation_id(self) -> int | None:
        """The Azure relation ID."""
        return (
            relation.id
            if (relation := self.charm.model.get_relation(AZURE_RELATION_NAME))
            else None
        )

    @property
    def azure_storage_relation(self) -> Relation | None:
        """The Azure relation."""
        return self.charm.model.get_relation(AZURE_RELATION_NAME)

    @property
    def azure_storage(self) -> AzureStorageConnectionInfo | None:
        """The server state of the current running Unit."""
        relation_data = (
            self.azure_storage_requirer.get_storage_connection_info(self.azure_storage_relation)
            if self.azure_storage_relation
            else None
        )
        return AzureStorageConnectionInfo(relation_data) if relation_data else None
