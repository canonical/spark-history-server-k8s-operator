# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

import unittest
from unittest import mock
from unittest.mock import patch

import ops.testing
from ops.model import ActiveStatus, BlockedStatus, WaitingStatus
from ops.testing import Harness
from src.constants import (
    CONFIG_KEY_S3_ACCESS_KEY,
    CONFIG_KEY_S3_BUCKET,
    CONFIG_KEY_S3_ENDPOINT,
    CONFIG_KEY_S3_LOGS_DIR,
    CONFIG_KEY_S3_SECRET_KEY,
    CONTAINER,
    S3_INTEGRATOR_REL,
    SPARK_HISTORY_SERVER_LAUNCH_CMD,
    STATUS_MSG_MISSING_S3_RELATION,
    STATUS_MSG_WAITING_PEBBLE,
)

from charm import SparkHistoryServerCharm
from config import SparkHistoryServerConfig


class TestCharm(unittest.TestCase):
    def setUp(self):
        # Enable more accurate simulation of container networking.
        # For more information, see https://juju.is/docs/sdk/testing#heading--simulate-can-connect
        ops.testing.SIMULATE_CAN_CONNECT = True
        self.addCleanup(setattr, ops.testing, "SIMULATE_CAN_CONNECT", False)

        self.harness = Harness(SparkHistoryServerCharm)
        self.addCleanup(self.harness.cleanup)
        self.harness.begin()

    def test_install(self):
        self.harness.charm.on.install.emit()
        self.assertEqual(
            self.harness.model.unit.status,
            WaitingStatus(STATUS_MSG_WAITING_PEBBLE),
        )

        self.assertFalse(self.harness.charm.verify_s3_credentials_in_relation())

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
        service = self.harness.model.unit.get_container(CONTAINER).get_service(CONTAINER)
        self.assertTrue(service.is_running())
        # Ensure we set an ActiveStatus with no message
        self.assertEqual(
            self.harness.model.unit.status,
            BlockedStatus(STATUS_MSG_MISSING_S3_RELATION),
        )

    def test_pebble_not_ready_during_config_update(self):
        # Check the service was started
        self.harness.container_pebble_ready(CONTAINER)
        service = self.harness.model.unit.get_container(CONTAINER).get_service(CONTAINER)
        self.assertTrue(service.is_running())

        self.assertEqual(
            self.harness.model.unit.status,
            BlockedStatus(STATUS_MSG_MISSING_S3_RELATION),
        )

        self.harness.set_can_connect(CONTAINER, False)
        self.harness.charm.on.config_changed.emit()
        self.assertEqual(
            self.harness.model.unit.status,
            WaitingStatus(STATUS_MSG_WAITING_PEBBLE),
        )

    def test_config(self):
        mock_s3_info = mock.Mock()
        mock_s3_info.get_s3_connection_info.return_value = {
            CONFIG_KEY_S3_ACCESS_KEY: "DUMMY_ACCESS_KEY",
            CONFIG_KEY_S3_SECRET_KEY: "DUMMY_SECRET_KEY",
            CONFIG_KEY_S3_BUCKET: "DUMMY_BUCKET",
            CONFIG_KEY_S3_LOGS_DIR: "DUMMY_LOG_DIR",
        }
        config = SparkHistoryServerConfig(mock_s3_info, {})
        self.assertTrue(config.verify_conn_config())

        self.assertEqual(config.s3_log_dir, "s3a://DUMMY_BUCKET/DUMMY_LOG_DIR")
        self.assertEqual(
            config.spark_conf.get("spark.hadoop.fs.s3a.access.key", "MISSING"), "DUMMY_ACCESS_KEY"
        )
        self.assertEqual(
            config.spark_conf.get("spark.hadoop.fs.s3a.secret.key", "MISSING"), "DUMMY_SECRET_KEY"
        )
        self.assertEqual(
            config.spark_conf.get("spark.hadoop.fs.s3a.aws.credentials.provider", "MISSING"),
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        self.assertEqual(config.spark_conf["spark.hadoop.fs.s3a.connection.ssl.enabled"], "false")

    def test_config_no_bucket(self):
        mock_s3_info = mock.Mock()
        mock_s3_info.get_s3_connection_info.return_value = {
            CONFIG_KEY_S3_ACCESS_KEY: "DUMMY_ACCESS_KEY",
            CONFIG_KEY_S3_SECRET_KEY: "DUMMY_SECRET_KEY",
        }
        config = SparkHistoryServerConfig(mock_s3_info, {})
        self.assertTrue(config.verify_conn_config())

        self.assertEqual(config.s3_log_dir, "s3a://")
        self.assertEqual(
            config.spark_conf.get("spark.hadoop.fs.s3a.access.key", "MISSING"), "DUMMY_ACCESS_KEY"
        )
        self.assertEqual(
            config.spark_conf.get("spark.hadoop.fs.s3a.secret.key", "MISSING"), "DUMMY_SECRET_KEY"
        )

    def test_config_no_keys(self):
        mock_s3_info = mock.Mock()
        mock_s3_info.get_s3_connection_info.return_value = {
            CONFIG_KEY_S3_BUCKET: "DUMMY_BUCKET",
            CONFIG_KEY_S3_LOGS_DIR: "DUMMY_LOG_DIR",
        }
        config = SparkHistoryServerConfig(mock_s3_info, {})
        self.assertFalse(config.verify_conn_config())

    @patch("charms.data_platform_libs.v0.s3.S3Requirer.get_s3_connection_info")
    @patch("ops.model.Container.list_files")
    def test_s3_relation_add(self, mock_container_list_files, mock_s3_info):
        # mocks
        mock_container_list_files.return_value = None
        mock_s3_info.return_value = {
            CONFIG_KEY_S3_ACCESS_KEY: "DUMMY_ACCESS_KEY",
            CONFIG_KEY_S3_SECRET_KEY: "DUMMY_SECRET_KEY",
            CONFIG_KEY_S3_ENDPOINT: "https://dummyendpoint.com",
            CONFIG_KEY_S3_BUCKET: "DUMMY_BUCKET",
            CONFIG_KEY_S3_LOGS_DIR: "DUMMY_LOG_DIR",
        }

        # Ensure the simulated Pebble API is reachable
        self.harness.set_can_connect(CONTAINER, True)

        self.harness.charm.s3_creds_client.get_s3_connection_info = mock_s3_info
        relation_id = self.harness.add_relation(S3_INTEGRATOR_REL, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator",
            mock_s3_info.return_value,
        )

        self.assertTrue(self.harness.charm.verify_s3_credentials_in_relation())

        self.assertEqual(self.harness.model.unit.status, ActiveStatus())
        self.assertEqual(
            self.harness.charm.spark_config.spark_conf.get(
                "spark.hadoop.fs.s3a.access.key", "MISSING"
            ),
            "DUMMY_ACCESS_KEY",
        )
        self.assertEqual(
            self.harness.charm.spark_config.spark_conf.get(
                "spark.hadoop.fs.s3a.secret.key", "MISSING"
            ),
            "DUMMY_SECRET_KEY",
        )
        self.assertEqual(
            self.harness.charm.spark_config.spark_conf.get(
                "spark.hadoop.fs.s3a.endpoint", "MISSING"
            ),
            "https://dummyendpoint.com",
        )

        self.assertEqual(
            self.harness.charm.spark_config.s3_log_dir, "s3a://DUMMY_BUCKET/DUMMY_LOG_DIR"
        )

    @patch("ops.model.Container.list_files")
    def test_credentials_changed(self, mock_container_list_files):
        # mocks
        mock_container_list_files.return_value = None

        # Ensure the simulated Pebble API is reachable
        self.harness.set_can_connect(CONTAINER, True)

        relation_id = self.harness.add_relation(S3_INTEGRATOR_REL, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator",
            {
                CONFIG_KEY_S3_ACCESS_KEY: "DUMMY_ACCESS_KEY",
                CONFIG_KEY_S3_SECRET_KEY: "DUMMY_SECRET_KEY",
                CONFIG_KEY_S3_ENDPOINT: "https://dummyendpoint.com",
                CONFIG_KEY_S3_BUCKET: "DUMMY_BUCKET",
                CONFIG_KEY_S3_LOGS_DIR: "DUMMY_LOG_DIR",
            },
        )

        self.assertTrue(self.harness.charm.verify_s3_credentials_in_relation())

        self.assertEqual(self.harness.model.unit.status, ActiveStatus())

        self.assertEqual(
            self.harness.charm.spark_config.s3_log_dir, "s3a://DUMMY_BUCKET/DUMMY_LOG_DIR"
        )

        self.harness.update_relation_data(
            relation_id,
            "s3-integrator",
            {
                CONFIG_KEY_S3_ACCESS_KEY: "DUMMY_ACCESS_KEY_2",
                CONFIG_KEY_S3_SECRET_KEY: "DUMMY_SECRET_KEY_2",
                CONFIG_KEY_S3_ENDPOINT: "https://dummyendpoint2.com",
                CONFIG_KEY_S3_BUCKET: "DUMMY_BUCKET_2",
                CONFIG_KEY_S3_LOGS_DIR: "DUMMY_LOG_DIR_2",
            },
        )

        self.assertEqual(
            self.harness.charm.spark_config.s3_log_dir, "s3a://DUMMY_BUCKET_2/DUMMY_LOG_DIR_2"
        )

        self.harness.update_relation_data(
            relation_id,
            "s3-integrator",
            {
                CONFIG_KEY_S3_ACCESS_KEY: "DUMMY_ACCESS_KEY_3",
                CONFIG_KEY_S3_SECRET_KEY: "DUMMY_SECRET_KEY_3",
                CONFIG_KEY_S3_ENDPOINT: "https://dummyendpoint3.com",
                CONFIG_KEY_S3_BUCKET: "DUMMY_BUCKET_3",
                CONFIG_KEY_S3_LOGS_DIR: "DUMMY_LOG_DIR_3",
            },
        )

        self.assertEqual(
            self.harness.charm.spark_config.s3_log_dir, "s3a://DUMMY_BUCKET_3/DUMMY_LOG_DIR_3"
        )

    @patch("ops.model.Container.list_files")
    def test_credentials_gone(self, mock_container_list_files):
        # mocks
        mock_container_list_files.return_value = None

        # Ensure the simulated Pebble API is reachable
        self.harness.set_can_connect(CONTAINER, True)

        relation_id = self.harness.add_relation(S3_INTEGRATOR_REL, "s3-integrator")
        self.harness.add_relation_unit(relation_id, "s3-integrator/0")
        self.harness.update_relation_data(
            relation_id,
            "s3-integrator",
            {
                CONFIG_KEY_S3_ACCESS_KEY: "DUMMY_ACCESS_KEY",
                CONFIG_KEY_S3_SECRET_KEY: "DUMMY_SECRET_KEY",
                CONFIG_KEY_S3_ENDPOINT: "https://dummyendpoint.com",
                CONFIG_KEY_S3_BUCKET: "DUMMY_BUCKET",
                CONFIG_KEY_S3_LOGS_DIR: "DUMMY_LOG_DIR",
            },
        )

        self.assertEqual(self.harness.model.unit.status, ActiveStatus())

        self.assertEqual(
            self.harness.charm.spark_config.s3_log_dir, "s3a://DUMMY_BUCKET/DUMMY_LOG_DIR"
        )

        self.harness.remove_relation(relation_id)

        self.assertEqual(
            self.harness.model.unit.status, BlockedStatus(STATUS_MSG_MISSING_S3_RELATION)
        )

    @patch("ops.model.Container.exists")
    def test_container_failure(self, mock_container_file_exists):
        # mocks
        mock_container_file_exists.return_value = False
        self.harness.set_can_connect(CONTAINER, False)
        self.harness.charm.on.config_changed.emit()
        self.assertEqual(
            self.harness.model.unit.status,
            WaitingStatus(STATUS_MSG_WAITING_PEBBLE),
        )

        self.harness.set_can_connect(CONTAINER, True)

        self.harness.charm.on.config_changed.emit()

        self.assertEqual(
            self.harness.model.unit.status,
            BlockedStatus(STATUS_MSG_MISSING_S3_RELATION),
        )
