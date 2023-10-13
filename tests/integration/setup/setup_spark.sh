#!/bin/bash

sudo snap remove --purge spark-client
sudo snap install spark-client
mkdir -p ~/.kube
sudo microk8s config | tee ~/.kube/config

spark-client.service-account-registry delete --username hello

spark-client.service-account-registry create --username hello \
                --conf spark.hadoop.fs.s3a.access.key=$2 \
                --conf spark.hadoop.fs.s3a.secret.key=$3 \
                --conf spark.hadoop.fs.s3a.endpoint=$1 \
                --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
                --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.eventLog.enabled=true \
                --conf spark.eventLog.dir=s3a://history-server/spark-events/ \
                --conf spark.history.fs.logDirectory=s3a://history-server/spark-events/

spark-client.service-account-registry get-config --username hello