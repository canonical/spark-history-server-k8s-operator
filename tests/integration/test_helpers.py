#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.

import logging
from time import sleep
from typing import Dict

import boto3
from botocore.client import Config
from juju.unit import Unit

logger = logging.getLogger(__name__)


async def fetch_action_sync_s3_credentials(unit: Unit, access_key: str, secret_key: str) -> Dict:
    """Helper to run an action to sync credentials.

    Args:
        unit: The juju unit on which to run the get-password action for credentials
        access_key: the access_key to access the s3 compatible endpoint
        secret_key: the secret key to access the s3 compatible endpoint
    Returns:
        A dictionary with the server config username and password
    """
    parameters = {"access-key": access_key, "secret-key": secret_key}
    action = await unit.run_action(action_name="sync-s3-credentials", **parameters)
    result = await action.wait()

    return result.results


def setup_s3_bucket_for_history_server(
    endpoint_url: str, aws_access_key: str, aws_secret_key: str, bucket_str: str
):
    config = Config(connect_timeout=60, retries={"max_attempts": 0})
    session = boto3.session.Session(
        aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key
    )
    s3 = session.client("s3", endpoint_url=endpoint_url, config=config)
    # delete test bucket and its content if it already exist
    buckets = s3.list_buckets()
    for bucket in buckets["Buckets"]:
        bucket_name = bucket["Name"]
        if bucket_name == bucket_str:
            logger.info(f"Deleting bucket: {bucket_name}")
            objects = s3.list_objects_v2(Bucket=bucket_str)["Contents"]
            objs = [{"Key": x["Key"]} for x in objects]
            s3.delete_objects(Bucket=bucket_str, Delete={"Objects": objs})
            s3.delete_bucket(Bucket=bucket_str)

    logger.info("create bucket in minio")
    for i in range(0, 30):
        try:
            s3.create_bucket(Bucket=bucket_str)
            break
        except Exception as e:
            if i >= 30:
                logger.error(f"create bucket failed....exiting....\n{str(e)}")
                raise
            else:
                logger.warning(f"create bucket failed....retrying in 10 secs.....\n{str(e)}")
                sleep(10)
                continue

    s3.put_object(Bucket=bucket_str, Key=("spark-events/"))
    logger.debug(s3.list_buckets())
