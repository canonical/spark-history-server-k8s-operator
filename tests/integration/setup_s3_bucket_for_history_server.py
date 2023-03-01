from sys import argv

import boto3
from botocore.client import Config

config = Config(connect_timeout=60, retries={"max_attempts": 0})
session = boto3.session.Session(aws_access_key_id=argv[2], aws_secret_access_key=argv[3])
s3 = session.client("s3", endpoint_url=argv[1], config=config)
s3.create_bucket(Bucket="history-server")
s3.put_object(Bucket="history-server", Key=("spark-events/"))
print(s3.list_buckets())
