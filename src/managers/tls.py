#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Spark History Server TLS configuration."""

import subprocess
from functools import cached_property

from ops.pebble import ExecError

from common.utils import WithLogging
from core.workload import SparkHistoryWorkloadBase


class TLSManager(WithLogging):
    """Manager for building necessary files for Java TLS auth."""

    def __init__(self, workload: SparkHistoryWorkloadBase):
        self.workload = workload

    # This could eventually go in a peer relation databag when/if it will
    # be implemented
    @cached_property
    def truststore_password(self) -> str:
        """Return the password of the truststore."""
        _tmp_file = "/tmp/password"

        if self.workload.exists(_tmp_file):
            return self.workload.read(_tmp_file)[0]

        password = self.workload.generate_password()
        self.workload.write(password, _tmp_file)
        return password

    def import_ca(self, certificate: str):
        """Import a certificate into the truststore.

        Args:
            certificate: string representing the certificate
        """
        self.workload.write(certificate, self.workload.paths.cert)

        command = [
            self.workload.paths.keytool,
            "-import",
            "-v",
            "-aliasca-file",
            self.workload.paths.cert,
            "-keystore",
            self.workload.paths.truststore,
            "-storepass",
            self.truststore_password,
            "-noprompt",
        ]

        try:
            self.workload.exec(command=command, working_dir=str(self.workload.paths.conf_path))
            self.workload.exec(
                [
                    "chown",
                    "-R",
                    f"{self.workload.user.name}:{self.workload.user.group}",
                    self.workload.paths.truststore,
                ]
            )
            self.workload.exec(["chmod", "-R", "660", self.workload.paths.truststore])
        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                return
            self.logger.error(e.stdout)
            raise e

    def reset(self):
        """Remove all files related to TLS configuration."""
        self.workload.exec(["rm", "-f", self.workload.paths.truststore])
        self.workload.exec(["rm", "-f", self.workload.paths.cert])
