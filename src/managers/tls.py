#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka TLS configuration."""

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
        _tmp_file = "/tmp/password"

        if self.workload.exists(_tmp_file):
            return self.workload.read(_tmp_file)[0]

        password = self.workload.generate_password()
        self.workload.write(password, _tmp_file)
        return password

    def import_ca(self, certificate: str):
        self.workload.write(certificate, self.workload.paths.cert)

        command = f"{self.workload.paths.keytool} -import -v -alias ca -file {self.workload.paths.cert} -keystore {self.workload.paths.truststore} -storepass {self.truststore_password} -noprompt"

        try:
            self.workload.exec(command=command,
                               working_dir=str(self.workload.paths.conf_path))
            self.workload.exec(
                f"chown -R {self.workload.user.name}:{self.workload.user.group} {self.workload.paths.truststore}"
            )
            self.workload.exec(f"chmod -R 660 {self.workload.paths.truststore}")
        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                return
            self.logger.error(e.stdout)
            raise e

    def reset(self):
        self.workload.exec(f"rm -f {self.workload.paths.truststore}")
        self.workload.exec(f"rm -f {self.workload.paths.cert}")
