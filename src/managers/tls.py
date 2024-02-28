#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Manager for handling Kafka TLS configuration."""

import logging
import secrets
import string
import subprocess

from ops import CharmBase
from ops.pebble import ExecError

from constants import PEBBLE_USER
from utils import WithLogging
from workload import SparkHistoryServer

logger = logging.getLogger(__name__)


class TLSManager(WithLogging):
    """Manager for building necessary files for Java TLS auth."""

    def __init__(self, charm: CharmBase, workload: SparkHistoryServer):
        self.charm = charm
        self.workload = workload

    def configure_truststore(self) -> None:
        """Configure custom JVM truststore."""
        trustore_password = self.generate_password()

        command = f"keytool -import -v -alias ca -file {self.workload.SPARK_CERT} -keystore {self.workload.SPARK_TRUSTSTORE} -storepass {trustore_password} -noprompt"

        try:
            self.workload.exec(command=command, working_dir=self.workload.SPARK_CONF)
            self.workload.exec(
                f"chown -R {PEBBLE_USER[0]}:{PEBBLE_USER[1]} {self.workload.SPARK_TRUSTSTORE}"
            )
            self.workload.exec(f"chmod -R 770 {self.workload.SPARK_TRUSTSTORE}")
        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                return
            self.logger.error(e.stdout)
            raise e
        self.workload.spark_history_server_java_config = f"-Djavax.net.ssl.trustStore={self.workload.SPARK_TRUSTSTORE} -Djavax.net.ssl.trustStorePassword={trustore_password}"

    def remove_truststore(self) -> None:
        """Manage the removal of the truststore."""
        try:
            self.workload.exec(f"rm -f {self.workload.SPARK_TRUSTSTORE}")
        except (subprocess.CalledProcessError, ExecError) as e:
            # in case this reruns and fails
            if e.stdout and "already exists" in e.stdout:
                return
            self.logger.error(e.stdout)
            raise e
        self.SPARK_HISTORY_JAVA_PROPERTIES = ""

    @staticmethod
    def generate_password() -> str:
        """Creates randomized string for use as app passwords.

        Returns:
            String of 32 randomized letter+digit characters
        """
        return "".join([secrets.choice(string.ascii_letters + string.digits) for _ in range(32)])
