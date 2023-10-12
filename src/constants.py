#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Literals and constants."""

CONTAINER = "spark-history-server"
CONTAINER_LAYER = "history-server"
HISTORY_SERVER_SERVICE = "history-server"
CONFIG_KEY_S3_ENDPOINT = "endpoint"
CONFIG_KEY_S3_ACCESS_KEY = "access-key"
CONFIG_KEY_S3_SECRET_KEY = "secret-key"
CONFIG_KEY_S3_BUCKET = "bucket"
CONFIG_KEY_S3_LOGS_DIR = "path"

SPARK_USER_WORKDIR = "/opt/spark"
SPARK_PROPERTIES_FILE = "/etc/spark/conf/spark-properties.conf"

PEBBLE_USER = ("_daemon_", "_daemon_")

PEER = "spark-history-server-peers"
S3_INTEGRATOR_REL = "s3-credentials"

STATUS_MSG_WAITING_PEBBLE = "Waiting for Pebble"
STATUS_MSG_MISSING_S3_RELATION = "Missing S3 relation"
STATUS_MSG_INVALID_CREDENTIALS = "Invalid S3 credentials"
STATUS_MSG_ACTIVE = ""
