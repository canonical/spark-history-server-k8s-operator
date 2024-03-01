from enum import Enum

from ops import (
    Object, CharmBase, Relation, ModelError,
    MaintenanceStatus, ActiveStatus, BlockedStatus
)

from charms.data_platform_libs.v0.data_interfaces import DataRequires
from charms.oathkeeper.v0.auth_proxy import AuthProxyConfig
from charms.traefik_k8s.v2.ingress import IngressUrl, IngressProviderAppData
from common.utils import WithLogging
from core.domain import S3ConnectionInfo
from common.models import DataDict

S3 = "s3"
INGRESS = "ingress"
OATHKEEPER = "auth-proxy"

AUTH_PROXY_HEADERS = ["X-User"]


class State(Object, WithLogging):
    """Properties and relations of the charm."""

    def __init__(self, charm: CharmBase):
        super().__init__(parent=charm, key="charm_state")

        self.charm = charm

        self.s3_endpoint = DataRequires(self.charm, S3)

    # --------------
    # --- CONFIG ---
    # --------------
    # We don't have config yet in SHS
    # --------------

    # -----------------
    # --- RELATIONS ---
    # -----------------

    @property
    def _s3_relation_id(self) -> int | None:
        """The S3 relation."""
        return relation.id if (relation := self.charm.model.get_relation(S3)) \
            else None

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
        return S3ConnectionInfo.from_dict(
            DataDict(self.s3_endpoint, _id)
        ) if (_id := self._s3_relation_id) else None

    @property
    def ingress(self) -> IngressUrl | None:
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
        if self.ingress and self._oathkeeper_relation:
            return AuthProxyConfig(
                protected_urls=[self.ingress.url],
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
    ACTIVE = ActiveStatus("")
