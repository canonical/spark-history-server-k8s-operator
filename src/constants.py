#!/usr/bin/env python3
# Copyright 2024 Canonical Limited
# See LICENSE file for licensing details.
#
# Learn more at: https://juju.is/docs/sdk

"""Literals and constants."""

CONTAINER = "spark-history-server"
PEBBLE_USER = ("_daemon_", "_daemon_")

HISTORY_SERVER_PORT = 18080
JMX_EXPORTER_PORT = 9101
JMX_CC_PORT = 9102
METRICS_RULES_DIR = "./src/alert_rules/prometheus"

S3_RELATION_NAME = "s3-credentials"
AZURE_RELATION_NAME = "azure-storage-credentials"
SERVICE_MESH_RELATION_NAME = "service-mesh"
METRICS_RELATION_NAME = "metrics-endpoint"
STATUS_RELATION_NAME = "status-peers"
