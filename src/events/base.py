#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Base utilities exposing common functionalities for all Events classes."""

from functools import wraps
from typing import Callable

from ops import CharmBase, EventBase, Object

from core.context import Context
from core.workload import SparkHistoryWorkloadBase


class BaseEventHandler(Object):
    """Base class for all Event Handler classes in the Spark History Server."""

    workload: SparkHistoryWorkloadBase
    charm: CharmBase
    context: Context


def defer_when_not_ready(
    hook: Callable,
) -> Callable[[BaseEventHandler, EventBase], None]:
    """Decorator to automatically compute statuses at the end of the hook."""

    @wraps(hook)
    def wrapper_hook(event_handler: BaseEventHandler, event: EventBase):
        """Return output after resetting statuses."""
        if not event_handler.workload.ready():
            event.defer()
            return None
        return hook(event_handler, event)

    return wrapper_hook
