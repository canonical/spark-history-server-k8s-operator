# Copyright 2023 Canonical Limited
# See LICENSE file for licensing details.

from pathlib import Path
from unittest.mock import patch

from ops import ActiveStatus, BlockedStatus, MaintenanceStatus
from scenario import Container, State

from constants import CONTAINER


def parse_spark_properties(out: State, tmp_path: Path) -> dict[str, str]:

    spark_properties_path = (
        out.get_container(CONTAINER)
        .layers["base"]
        .services["history-server"]
        .environment["SPARK_PROPERTIES_FILE"]
    )

    file_path = tmp_path / Path(spark_properties_path).relative_to("/etc")

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
    assert out.unit_status == MaintenanceStatus("Waiting for Pebble")


@patch("workload.SparkHistoryServer.exec")
def test_pebble_ready(exec_calls, history_server_ctx, history_server_container):
    state = State(
        containers=[history_server_container],
    )
    out = history_server_ctx.run(history_server_container.pebble_ready_event, state)
    assert out.unit_status == BlockedStatus("Missing relation with storage (s3 or azure storage)")


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
def test_s3_relation_connection_ok(
    exec_calls, verify_call, tmp_path, history_server_ctx, history_server_container, s3_relation
):
    state = State(
        relations=[s3_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(s3_relation.changed_event, state)
    assert out.unit_status == ActiveStatus("")

    # Check containers modifications
    assert len(out.get_container(CONTAINER).layers) == 2

    # Check that "SPARK_HISTORY_OPTS" is not in the envs
    envs = (
        out.get_container(CONTAINER)
        .layers["spark-history-server"]
        .services["history-server"]
        .environment
    )

    assert "SPARK_HISTORY_OPTS" in envs
    assert "SPARK_PROPERTIES_FILE" in envs

    spark_properties = parse_spark_properties(out, tmp_path)

    # Assert one of the keys
    assert "spark.hadoop.fs.s3a.endpoint" in spark_properties
    assert (
        spark_properties["spark.hadoop.fs.s3a.endpoint"] == s3_relation.remote_app_data["endpoint"]
    )


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
def test_s3_relation_connection_ok_tls(
    exec_calls,
    verify_call,
    tmp_path,
    history_server_ctx,
    history_server_container,
    s3_relation_tls,
    s3_relation,
):
    state = State(
        relations=[s3_relation_tls],
        containers=[history_server_container],
    )
    inter = history_server_ctx.run(s3_relation_tls.changed_event, state)
    assert inter.unit_status == ActiveStatus("")

    # Check containers modifications
    assert len(inter.get_container(CONTAINER).layers) == 2

    # Check that "SPARK_HISTORY_OPTS" is not in the envs
    envs = (
        inter.get_container(CONTAINER)
        .layers["spark-history-server"]
        .services["history-server"]
        .environment
    )

    assert "SPARK_HISTORY_OPTS" in envs
    assert len(envs["SPARK_HISTORY_OPTS"]) > 0

    spark_properties = parse_spark_properties(inter, tmp_path)

    # Assert one of the keys
    assert "spark.hadoop.fs.s3a.endpoint" in spark_properties
    assert (
        spark_properties["spark.hadoop.fs.s3a.endpoint"]
        == s3_relation_tls.remote_app_data["endpoint"]
    )

    out = history_server_ctx.run(
        s3_relation_tls.changed_event, inter.replace(relations=[s3_relation])
    )

    assert len(out.get_container(CONTAINER).layers) == 2

    # Check that "SPARK_HISTORY_OPTS" is not in the envs
    envs = (
        out.get_container(CONTAINER)
        .layers["spark-history-server"]
        .services["history-server"]
        .environment
    )

    assert "SPARK_HISTORY_OPTS" in envs
    assert len(envs["SPARK_HISTORY_OPTS"]) == 0


@patch("managers.s3.S3Manager.verify", return_value=False)
@patch("workload.SparkHistoryServer.exec")
def test_s3_relation_connection_ko(
    exec_calls, verify_call, tmp_path, history_server_ctx, history_server_container, s3_relation
):
    state = State(
        relations=[s3_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(s3_relation.changed_event, state)
    assert out.unit_status == BlockedStatus("Invalid S3 credentials")


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
def test_s3_relation_broken(
    exec_calls, verify_call, history_server_ctx, history_server_container, s3_relation, tmp_path
):
    initial_state = State(
        relations=[s3_relation],
        containers=[history_server_container],
    )

    state_after_relation_changed = history_server_ctx.run(s3_relation.changed_event, initial_state)
    state_after_relation_broken = history_server_ctx.run(
        s3_relation.broken_event, state_after_relation_changed
    )

    assert state_after_relation_broken.unit_status == BlockedStatus(
        "Missing relation with storage (s3 or azure storage)"
    )

    spark_properties = parse_spark_properties(state_after_relation_broken, tmp_path)

    # Assert one of the keys
    assert "spark.hadoop.fs.s3a.endpoint" not in spark_properties


@patch("workload.SparkHistoryServer.exec")
def test_ingress_relation_creation(
    exec_calls,
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
    assert out.unit_status == BlockedStatus("Missing relation with storage (s3 or azure storage)")


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
def test_with_ingress(
    exec_calls,
    verify_call,
    tmp_path,
    history_server_ctx,
    history_server_container,
    ingress_relation,
    s3_relation,
):
    state = State(
        relations=[s3_relation, ingress_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(ingress_relation.changed_event, state)

    assert out.unit_status == ActiveStatus("")

    spark_properties = parse_spark_properties(out, tmp_path)

    assert "spark.ui.proxyRedirectUri" in spark_properties
    assert "spark.ui.proxyBase" in spark_properties


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
def test_with_ingress_subdomain(
    exec_calls,
    verify_call,
    tmp_path,
    history_server_ctx,
    history_server_container,
    ingress_subdomain_relation,
    s3_relation,
):
    state = State(
        relations=[s3_relation, ingress_subdomain_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(ingress_subdomain_relation.changed_event, state)

    assert out.unit_status == ActiveStatus("")

    spark_properties = parse_spark_properties(out, tmp_path)

    assert "spark.ui.proxyRedirectUri" in spark_properties
    assert "spark.ui.proxyBase" not in spark_properties


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
def test_remove_ingress(
    exec_calls,
    verify_call,
    tmp_path,
    history_server_ctx,
    history_server_container,
    ingress_relation,
    s3_relation,
):
    state = State(
        relations=[s3_relation, ingress_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(ingress_relation.broken_event, state)

    assert out.unit_status == ActiveStatus("")

    spark_properties = parse_spark_properties(out, tmp_path)

    assert "spark.ui.proxyRedirectUri" not in spark_properties


@patch("managers.azure_storage.AzureStorageManager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
@patch("ops.JujuVersion.has_secrets", return_value=True)
def test_azure_storage_relation(
    mock_has_secrets,
    exec_calls,
    verify_call,
    tmp_path,
    history_server_ctx,
    history_server_container,
    azure_storage_relation,
):
    state = State(
        relations=[azure_storage_relation],
        containers=[history_server_container],
    )

    out = history_server_ctx.run(azure_storage_relation.changed_event, state)
    assert out.unit_status == ActiveStatus("")

    # Check containers modifications
    assert len(out.get_container(CONTAINER).layers) == 2

    envs = (
        out.get_container(CONTAINER)
        .layers["spark-history-server"]
        .services["history-server"]
        .environment
    )

    assert "SPARK_PROPERTIES_FILE" in envs

    spark_properties = parse_spark_properties(out, tmp_path)

    # Assert one of the keys
    storage_account = azure_storage_relation.remote_app_data["storage-account"]
    assert (
        f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
        in spark_properties
    )
    assert (
        spark_properties[
            f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
        ]
        == azure_storage_relation.remote_app_data["secret-key"]
    )


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
@patch("ops.JujuVersion.has_secrets", return_value=True)
def test_azure_storage_relation_broken(
    mock_has_secrets,
    exec_calls,
    verify_call,
    tmp_path,
    history_server_ctx,
    history_server_container,
    azure_storage_relation,
):
    state = State(
        relations=[azure_storage_relation],
        containers=[history_server_container],
    )

    state_after_relation_changed = history_server_ctx.run(
        azure_storage_relation.changed_event, state
    )
    state_after_relation_broken = history_server_ctx.run(
        azure_storage_relation.broken_event, state_after_relation_changed
    )

    assert state_after_relation_broken.unit_status == BlockedStatus(
        "Missing relation with storage (s3 or azure storage)"
    )

    spark_properties = parse_spark_properties(state_after_relation_broken, tmp_path)

    storage_account = azure_storage_relation.remote_app_data["storage-account"]
    assert (
        f"spark.hadoop.fs.azure.account.key.{storage_account}.dfs.core.windows.net"
        not in spark_properties
    )


@patch("managers.s3.S3Manager.verify", return_value=True)
@patch("workload.SparkHistoryServer.exec")
@patch("ops.JujuVersion.has_secrets", return_value=True)
def test_both_azure_storage_and_s3_relation_together(
    mock_has_secrets,
    exec_calls,
    verify_call,
    tmp_path,
    history_server_ctx,
    history_server_container,
    s3_relation,
    azure_storage_relation,
):
    state = State(
        relations=[s3_relation, azure_storage_relation],
        containers=[history_server_container],
    )
    out = history_server_ctx.run(azure_storage_relation.changed_event, state)
    assert out.unit_status == BlockedStatus(
        "Spark History Server can be related to only one storage backend at a time."
    )
