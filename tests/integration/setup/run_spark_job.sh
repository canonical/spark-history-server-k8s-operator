#!/bin/bash

SPARK_VERSION=$1

spark-client.spark-submit --username hello \
  --conf spark.kubernetes.container.image=ghcr.io/canonical/charmed-spark@sha256:22eae73b12cda8b7c89a7dc2c49eda557f211d29b21dff5f704b641e662e4b2d \
  --conf spark.kubernetes.executor.request.cores=0.1 \
  --class org.apache.spark.examples.SparkPi \
  local:///opt/spark/examples/jars/spark-examples_2.13-${SPARK_VERSION}.jar 1000 
echo "Print logs"
kubectl logs -l spark-version=${SPARK_VERSION}
echo "Kubectl get pods -A"
kubectl get pods -A
