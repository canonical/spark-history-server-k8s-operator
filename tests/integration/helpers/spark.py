#!/usr/bin/env python3
# Copyright 2026 Canonical Limited
# See LICENSE file for licensing details.

import json
import logging
import subprocess

from .history_server import get_history_server_image_version
from ..types import AzureInfo, S3Info

logger = logging.getLogger(__name__)


def get_spark_version():
    """Get the Spark version from the Spark History Server image metadata."""
    image_version = get_history_server_image_version()
    logger.info(f"Spark History Server image version: {image_version}")

    shell_output = subprocess.check_output(
        f"./tests/integration/setup/get_image_metadata.sh {image_version}", shell=True
    ).decode("utf-8")
    logger.info(shell_output)

    image_metadata = json.loads(shell_output)
    spark_version = image_metadata["org.opencontainers.image.version"]
    logger.info(f"Spark version: {spark_version}")
    return spark_version


def setup_spark_job(
    s3_bucket_and_creds: S3Info | None = None,
    azure_storage_credentials: AzureInfo | None = None,
):
    """Set up a service account for running Spark jobs with the specified storage backend."""
    image_version = get_history_server_image_version()
    if s3_bucket_and_creds is not None:
        logger.info("Setting up Spark job with S3 storage")
        access_key = s3_bucket_and_creds["access_key"]
        secret_key = s3_bucket_and_creds["secret_key"]
        endpoint = s3_bucket_and_creds["endpoint"]
        setup_spark_output = subprocess.check_output(
            f"./tests/integration/setup/setup_spark.sh {endpoint} {access_key} {secret_key} {image_version}",
            shell=True,
            stderr=None,
        ).decode("utf-8")
    elif azure_storage_credentials is not None:
        logger.info("Setting up Spark job with Azure storage")
        container = azure_storage_credentials["container"]
        path = azure_storage_credentials["path"]
        storage_account = azure_storage_credentials["storage-account"]
        secret_key = azure_storage_credentials["secret-key"]
        setup_spark_output = subprocess.check_output(
            (
                f"./tests/integration/setup/setup_spark_azure.sh {container} {path} {storage_account} {secret_key} {image_version}"
            ),
            shell=True,
            stderr=None,
        ).decode("utf-8")
    else:
        raise ValueError(
            "Either s3_bucket_and_creds or azure_storage_credentials must be provided."
        )
    return setup_spark_output


def run_spark_job(tls_ca: str | None = None) -> str:
    """Run a Spark job, optionally with TLS configuration."""
    logger.info("Executing Spark job...")
    spark_version = get_spark_version()
    output = ""
    if tls_ca:
        output = subprocess.check_output(
            f"./tests/integration/setup/run_spark_job_tls.sh  {spark_version} {tls_ca}",
            shell=True,
            stderr=None,
        ).decode("utf-8")
    else:
        output = subprocess.check_output(
            f"./tests/integration/setup/run_spark_job.sh {spark_version}",
            shell=True,
            stderr=None,
            timeout=10 * 60,
        ).decode("utf-8")
    logger.info(f"Run spark output:\n{output}")
    return output
