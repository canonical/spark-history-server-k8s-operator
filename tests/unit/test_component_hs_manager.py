# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details

from unittest import mock

from pytest import MonkeyPatch

from managers.history_server import HistoryServerConfig


def test_s3_proxy_credentials(monkeypatch: MonkeyPatch) -> None:
    """Proxy credentials are properly extracted and passed to the spark properties."""
    # Given
    monkeypatch.setenv("JUJU_CHARM_HTTP_PROXY", "http://username:password@10.152.193.234:80")
    s3_manager_testing = mock.MagicMock()
    s3_manager_testing.connection_info.endpoint = mock.PropertyMock(
        side_effect="https://192.168.1.1"
    )

    config = HistoryServerConfig(None, s3_manager_testing, None, None, None)  # type: ignore

    # When
    s3_proxy_conf = config._s3_conf

    # Then
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.username", "") == "username"
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.password", "") == "password"


def test_s3_proxy_plain_ip(monkeypatch: MonkeyPatch) -> None:
    """Proper scheme and port are passed to the spark properties.

    Even if https_proxy is pointing to an http domain.
    """
    # Given
    proxy_host = "10.152.193.234"
    monkeypatch.setenv("JUJU_CHARM_HTTPS_PROXY", f"http://{proxy_host}")
    s3_manager_testing = mock.MagicMock()
    s3_manager_testing.connection_info.endpoint = mock.PropertyMock(
        side_effect="https://192.168.1.1"
    )

    config = HistoryServerConfig(None, s3_manager_testing, None, None, None)  # type: ignore

    # When
    s3_proxy_conf = config._s3_conf

    # Then
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.host", "") == proxy_host
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.ssl.enabled", "") == "false"
    assert s3_proxy_conf.get("spark.hadoop.fs.s3a.proxy.port", "0") == "80"
