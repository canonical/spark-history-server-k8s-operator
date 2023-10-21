# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

from pathlib import Path
from unittest.mock import patch

from scenario import Container, State

from constants import CONTAINER


def parse_spark_properties(out: State, tmp_path: Path) -> dict[str, str]:

    spark_properties_path = (
        out.get_container(CONTAINER)
        .layers["charm"]
        .services["history-server"]
        .environment["SPARK_PROPERTIES_FILE"]
    )

    file_path = tmp_path / Path(spark_properties_path).relative_to("/opt")

    assert file_path.exists()

    with file_path.open("r") as fid:
        return dict(
            row.rsplit("=", maxsplit=1) for line in fid.readlines() if (row := line.strip())
        )


def test_start_history_server(history_server_ctx):
    state = State(
        config={},
        containers=[Container(name=CONTAINER, can_connect=False)],
    )
    out = history_server_ctx.run("install", state)
    assert out.unit_status.name == "maintenance"
    assert out.unit_status.message == "Waiting for Pebble"


def test_pebble_ready(history_server_ctx, history_server_container):
    state = State(
        containers=[history_server_container],
    )
    out = history_server_ctx.run(history_server_container.pebble_ready_event, state)
    assert out.unit_status.name == "blocked"
    assert out.unit_status.message == "Missing S3 relation"


@patch("models.S3ConnectionInfo.verify", return_value=True)
def test_s3_relation_connection_ok(
    _, tmp_path, history_server_ctx, history_server_container, s3_relation
):
    state = State(
        relations=[s3_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(s3_relation.changed_event, state)
    assert out.unit_status.name == "active"

    # Check containers modifications
    assert len(out.get_container(CONTAINER).layers) == 2

    spark_properties = parse_spark_properties(out, tmp_path)

    # Assert one of the keys
    assert "spark.hadoop.fs.s3a.endpoint" in spark_properties
    assert (
        spark_properties["spark.hadoop.fs.s3a.endpoint"] == s3_relation.remote_app_data["endpoint"]
    )


@patch("models.S3ConnectionInfo.verify", return_value=False)
def test_s3_relation_connection_ko(
    _, tmp_path, history_server_ctx, history_server_container, s3_relation
):
    state = State(
        relations=[s3_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(s3_relation.changed_event, state)
    assert out.unit_status.name == "blocked"
    assert out.unit_status.message == "Invalid S3 credentials"


@patch("models.S3ConnectionInfo.verify", return_value=True)
def test_s3_relation_broken(
    _, history_server_ctx, history_server_container, s3_relation, tmp_path
):
    state = State(
        relations=[s3_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(
        s3_relation.broken_event, history_server_ctx.run(s3_relation.changed_event, state)
    )
    assert out.unit_status.name == "blocked"
    assert out.unit_status.message == "Missing S3 relation"

    spark_properties = parse_spark_properties(out, tmp_path)

    # Assert one of the keys
    assert "spark.hadoop.fs.s3a.endpoint" not in spark_properties


def test_ingress_relation_creation(
    tmp_path,
    history_server_ctx,
    history_server_container,
    ingress_relation,
):
    state = State(
        leader=True,
        relations=[ingress_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(ingress_relation.changed_event, state)
    assert out.unit_status.name == "blocked"
    assert out.unit_status.message == "Missing S3 relation"


@patch("models.S3ConnectionInfo.verify", return_value=True)
def test_with_ingress(
    _, tmp_path, history_server_ctx, history_server_container, ingress_relation, s3_relation
):
    state = State(
        relations=[s3_relation, ingress_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(ingress_relation.changed_event, state)

    assert out.unit_status.name == "active"

    spark_properties = parse_spark_properties(out, tmp_path)

    assert "spark.ui.proxyRedirectUri" in spark_properties


@patch("models.S3ConnectionInfo.verify", return_value=True)
def test_remove_ingress(
    _, tmp_path, history_server_ctx, history_server_container, ingress_relation, s3_relation
):
    state = State(
        relations=[s3_relation, ingress_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(ingress_relation.broken_event, state)

    assert out.unit_status.name == "active"

    spark_properties = parse_spark_properties(out, tmp_path)

    assert "spark.ui.proxyRedirectUri" not in spark_properties
