#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.

"""Charmed Kubernetes Operator for Apache Spark History Server."""

from ops import CharmBase
from ops.main import main

from common.utils import WithLogging
from constants import CONTAINER, PEBBLE_USER
from core.domain import User
from core.state import State
from events.history_server import HistoryServerEvents
from events.ingress import IngressEvents
from events.s3 import S3Events
from workload import SparkHistoryServer


class SparkHistoryServerCharm(CharmBase, WithLogging):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args)

        state = State(self)

        workload = SparkHistoryServer(
            self.unit.get_container(CONTAINER),
            User(name=PEBBLE_USER[0], group=PEBBLE_USER[1])
        )

        self.ingress = IngressEvents(self, state, workload)
        self.s3 = S3Events(self, state, workload)
        self.history_server = HistoryServerEvents(self, state, workload)


if __name__ == "__main__":  # pragma: nocover
    main(SparkHistoryServerCharm)
