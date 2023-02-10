# Copyright 2023 Abhishek Verma
# See LICENSE file for licensing details.
#
# Learn more about testing at: https://juju.is/docs/sdk/testing

import unittest
from unittest.mock import patch

import ops.testing
from charm import SparkHistoryServerCharm
from src.constants import *
from ops.model import ActiveStatus, WaitingStatus
from ops.testing import Harness


class TestCharm(unittest.TestCase):
    def setUp(self):
        # Enable more accurate simulation of container networking.
        # For more information, see https://juju.is/docs/sdk/testing#heading--simulate-can-connect
        ops.testing.SIMULATE_CAN_CONNECT = True
        self.addCleanup(setattr, ops.testing, "SIMULATE_CAN_CONNECT", False)

        self.harness = Harness(SparkHistoryServerCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_pebble_ready(self):
        # Expected plan after Pebble ready with default config
        expected_plan = {
            "services": {
                CONTAINER: {
                    "summary": "spark history server",
                    "startup": "enabled",
                    "override": "replace",
                    "command": SPARK_HISTORY_SERVER_LAUNCH_CMD,
                    "environment": {"SPARK_NO_DAEMONIZE": "true"},
                    "user": "spark",
                    "group": "spark",
                }
            },
        }
        # Simulate the container coming up and emission of pebble-ready event
        self.harness.container_pebble_ready(CONTAINER)
        # Get the plan now we've run PebbleReady
        updated_plan = self.harness.get_container_pebble_plan(CONTAINER).to_dict()
        # Check we've got the plan we expected
        self.assertEqual(expected_plan, updated_plan)
        # Check the service was started
        service = self.harness.model.unit.get_container(CONTAINER).get_service(
            CONTAINER
        )
        self.assertTrue(service.is_running())
        # Ensure we set an ActiveStatus with no message
        self.assertEqual(
            self.harness.model.unit.status,
            WaitingStatus("Pebble ready, waiting for Spark Configuration"),
        )

    def test_config_changed(self):
        with patch("ops.model.Container.list_files", return_value=None):
            # Ensure the simulated Pebble API is reachable
            self.harness.set_can_connect(CONTAINER, True)
            # Trigger a config-changed event with an updated value
            self.harness.update_config({CONFIG_KEY_S3_ENDPOINT: "http://192.168.1.7:9000"})
            self.harness.update_config({CONFIG_KEY_S3_ACCESS_KEY: "5mHwHrovJXVTMsQV"})
            self.harness.update_config({CONFIG_KEY_S3_SECRET_KEY: "dKQ9DyhcPltC3U4jSqlHsBhykiflT5kR"})
            self.harness.update_config({CONFIG_KEY_S3_LOGS_DIR: "s3a://history-server/spark-events/"})

            self.assertEqual(
                self.harness.model.unit.status, ActiveStatus("s3a://history-server/spark-events/")
            )
