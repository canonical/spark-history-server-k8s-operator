# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

import pytest
from ops import pebble
from scenario import Container, Context, Model, Mount, Relation
from scenario.state import next_relation_id

from charm import SparkHistoryServerCharm
from constants import CONTAINER, INGRESS_REL, S3_INTEGRATOR_REL


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
                        "SPARK_PROPERTIES_FILE": "/etc/spark8t/conf/spark-defaults.conf"
                    },
                },
            },
        }
    )

    opt = Mount("/opt/", tmp_path)

    return Container(
        name=CONTAINER,
        can_connect=True,
        layers={"base": layer},
        service_status={"history-server": pebble.ServiceStatus.ACTIVE},
        mounts={"opt": opt},
    )


@pytest.fixture
def s3_relation():
    """Provide fixture for the S3 relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=S3_INTEGRATOR_REL,
        interface="s3",
        remote_app_name="s3-integrator",
        relation_id=relation_id,
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
def ingress_relation():
    """Provide fixture for the ingress relation."""
    relation_id = next_relation_id(update=True)

    return Relation(
        endpoint=INGRESS_REL,
        interface="ingress",
        remote_app_name="traefik-k8s",
        relation_id=relation_id,
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
