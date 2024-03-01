from typing import Callable

from ops import CharmBase, EventBase, Object, StatusBase

from core.state import AuthProxyConfig, IngressUrl, S3ConnectionInfo, State, Status
from core.workload import SparkHistoryWorkloadBase
from managers.s3 import S3Manager

from functools import wraps

class BaseEventHandler(Object):

    workload: SparkHistoryWorkloadBase
    charm: CharmBase
    state: State

    def get_app_status(
            self,
            s3: S3ConnectionInfo | None,
            ingress: IngressUrl | None,
            oathkeeper: AuthProxyConfig | None,
    ) -> StatusBase:
        """Compute and return the status of the charm."""
        if not self.workload.ready():
            return Status.WAITING_PEBBLE.value

        s3 = s3

        if not s3:
            return Status.MISSING_S3_RELATION.value

        s3_manager = S3Manager(s3)
        if not s3_manager.verify():
            return Status.INVALID_CREDENTIALS.value

        if not self.workload.active():
            return Status.NOT_RUNNING.value

        if oathkeeper and not ingress:
            return Status.MISSING_INGRESS_RELATION.value

        return Status.ACTIVE.value


def compute_status(
        hook: Callable[[BaseEventHandler, EventBase], None]
) -> Callable[[BaseEventHandler, EventBase], None]:
    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        res = hook(event_handler, event)
        if event_handler.charm.unit.is_leader():
            event_handler.charm.app.status = event_handler.get_app_status(
                event_handler.state.s3,
                event_handler.state.ingress,
                event_handler.state.auth_proxy_config
            )
        event_handler.charm.unit.status = event_handler.get_app_status(
            event_handler.state.s3,
            event_handler.state.ingress,
            event_handler.state.auth_proxy_config
        )
        return res

    return wrapper_hook
