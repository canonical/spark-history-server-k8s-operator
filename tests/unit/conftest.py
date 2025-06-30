# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

from dataclasses import replace

import pytest
from ops import pebble
from ops.testing import Container, Context, Model, Mount, Relation

# from scenario.state import next_relation_id
from charm import SparkHistoryServerCharm
from constants import AZURE_RELATION_NAME, CONTAINER
from core.context import INGRESS, S3


@pytest.fixture
def history_server_charm():
    """Provide fixture for the SparkHistoryServer charm."""
    yield SparkHistoryServerCharm


@pytest.fixture
def history_server_ctx(history_server_charm):
    """Provide fixture for scenario context based on the SparkHistoryServer charm."""
    return Context(charm_type=history_server_charm)


@pytest.fixture
def model():
    """Provide fixture for the testing Juju model."""
    return Model(name="test-model")


@pytest.fixture
def history_server_container(tmp_path):
    """Provide fixture for the History Server workload container."""
    layer = pebble.Layer(
        {
            "summary": "Charmed Spark Layer",
            "description": "Pebble base layer in Charmed Spark OCI Image",
            "services": {
                "history-server": {
                    "override": "replace",
                    "summary": "This is the Spark History Server service",
                    "command": "/bin/bash /opt/pebble/charmed-spark-history-server.sh",
                    "startup": "disabled",
                    "environment": {
                        "SPARK_PROPERTIES_FILE": "/etc/spark/conf/spark-properties.conf"
                    },
                },
            },
        }
    )

    etc = Mount(location="/etc/", source=tmp_path)
    jmx = Mount(location="/opt/spark/jars/jmx_prometheus_javaagent_0.20.jar", source=tmp_path)
    return Container(
        name=CONTAINER,
        can_connect=True,
        layers={"base": layer},
        service_statuses={"history-server": pebble.ServiceStatus.ACTIVE},
        mounts={"etc": etc, "jmx": jmx},
    )


@pytest.fixture
def s3_relation():
    """Provide fixture for the S3 relation."""
    relation = Relation(
        endpoint=S3,
        interface="s3",
        remote_app_name="s3-integrator",
    )
    relation_id = relation.id

    return replace(
        relation,
        local_app_data={"bucket": f"relation-{relation_id}"},
        remote_app_data={
            "access-key": "access-key",
            "bucket": "my-bucket",
            "data": f'{{"bucket": "relation-{relation_id}"}}',
            "endpoint": "https://s3.endpoint",
            "path": "spark-events",
            "secret-key": "secret-key",
        },
    )


@pytest.fixture
def s3_relation_tls():
    """Provide fixture for the S3 relation."""
    relation = Relation(
        endpoint=S3,
        interface="s3",
        remote_app_name="s3-integrator",
    )
    relation_id = relation.id

    return replace(
        relation,
        local_app_data={"bucket": f"relation-{relation_id}"},
        remote_app_data={
            "access-key": "access-key",
            "bucket": "my-bucket",
            "data": f'{{"bucket": "relation-{relation_id}"}}',
            "endpoint": "https://s3.endpoint",
            "path": "spark-events",
            "secret-key": "secret-key",
            "tls-ca-chain": '["certificate"]',
        },
    )


@pytest.fixture
def ingress_relation():
    """Provide fixture for the ingress relation."""
    return Relation(
        endpoint=INGRESS,
        interface="ingress",
        remote_app_name="traefik-k8s",
        local_app_data={
            "model": '"spark"',
            "name": '"spark-history-server-k8s"',
            "port": "18080",
            "redirect-https": "false",
            "scheme": '"http"',
            "strip-prefix": "true",
        },
        remote_app_data={
            "ingress": '{"url": "http://spark.deusebio.com/spark-spark-history-server-k8s"}'
        },
    )


@pytest.fixture
def ingress_subdomain_relation():
    """Provide fixture for the ingress relation."""
    return Relation(
        endpoint=INGRESS,
        interface="ingress",
        remote_app_name="traefik-k8s",
        local_app_data={
            "model": '"spark"',
            "name": '"spark-history-server-k8s"',
            "port": "18080",
            "redirect-https": "false",
            "scheme": '"http"',
            "strip-prefix": "true",
        },
        remote_app_data={
            "ingress": '{"url": "http://spark-history-server-k8s.spark.deusebio.com"}'
        },
    )


@pytest.fixture
def azure_storage_relation():
    """Provide fixture for the Azure storage relation."""
    relation = Relation(
        endpoint=AZURE_RELATION_NAME,
        interface="azure_storage",
        remote_app_name="azure-storage-integrator",
    )
    relation_id = relation.id

    return replace(
        relation,
        local_app_data={"container": f"relation-{relation_id}"},
        remote_app_data={
            "container": "my-bucket",
            "data": f'{{"container": "relation-{relation_id}"}}',
            "path": "spark-events",
            "storage-account": "test-storage-account",
            "connection-protocol": "abfss",
            "secret-key": "some-secret",
        },
    )
