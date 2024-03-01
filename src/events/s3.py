from ops import CharmBase

from charms.data_platform_libs.v0.s3 import (
    CredentialsChangedEvent,
    CredentialsGoneEvent,
    S3Requirer,
)
from common.utils import WithLogging
from core.state import State
from core.workload import SparkHistoryWorkloadBase
from events.base import BaseEventHandler, compute_status
from managers.history_server import HistoryServerManager


class S3Events(BaseEventHandler, WithLogging):

    def __init__(self, charm: CharmBase,
                 state: State, workload: SparkHistoryWorkloadBase
                 ):
        super().__init__(charm, "s3")

        self.charm = charm
        self.state = state
        self.workload = workload

        self.history_server = HistoryServerManager(self.workload)

        self.s3_requirer = S3Requirer(
            charm, self.state.s3_endpoint.relation_name
        )
        self.framework.observe(
            self.s3_requirer.on.credentials_changed,
            self._on_s3_credential_changed
        )
        self.framework.observe(
            self.s3_requirer.on.credentials_gone, self._on_s3_credential_gone
        )

    @compute_status
    def _on_s3_credential_changed(self, _: CredentialsChangedEvent):
        """Handle the `CredentialsChangedEvent` event from S3 integrator."""
        self.logger.info("S3 Credentials changed")
        self.history_server.update(self.state.s3, self.state.ingress)

    def _on_s3_credential_gone(self, _: CredentialsGoneEvent):
        """Handle the `CredentialsGoneEvent` event for S3 integrator."""
        self.logger.info("S3 Credentials gone")
        self.history_server.update(None, self.state.ingress)

        self.charm.app.status = self.get_app_status(
            None, self.state.ingress, self.state.auth_proxy_config
        )
