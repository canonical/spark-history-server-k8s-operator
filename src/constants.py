#!/usr/bin/env python3
# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Literals and constants."""

SPARK_PROPERTIES_FILE = "/opt/spark/conf/spark-properties.conf"
CONTAINER = "spark-history-server"
CONTAINER_LAYER = "spark-history-server"
CONFIG_KEY_S3_ENDPOINT = "endpoint"
CONFIG_KEY_S3_ACCESS_KEY = "access-key"
CONFIG_KEY_S3_SECRET_KEY = "secret-key"
CONFIG_KEY_S3_BUCKET = "bucket"
CONFIG_KEY_S3_LOGS_DIR = "path"
SPARK_USER = "spark"
SPARK_USER_GROUP = "spark"
SPARK_USER_UID = 185
SPARK_USER_GID = 185
SPARK_USER_WORKDIR = "/opt/spark"
SPARK_HISTORY_SERVER_SCRIPT = "/opt/spark/sbin/start-history-server.sh"
SPARK_HISTORY_SERVER_LAUNCH_CMD = (
    f"{SPARK_HISTORY_SERVER_SCRIPT} --properties-file {SPARK_PROPERTIES_FILE}"
)
PEER = "spark-history-server-peers"
S3_INTEGRATOR_REL = "s3-credentials"
S3_INTEGRATOR_CHARM_NAME = "s3-integrator"

STATUS_MSG_WAITING_PEBBLE = "Waiting for Pebble"
STATUS_MSG_MISSING_S3_RELATION = "Missing S3 relation"
