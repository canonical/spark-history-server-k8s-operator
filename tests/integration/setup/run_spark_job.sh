#!/bin/bash

SPARK_VERSION=$1

spark-client.spark-submit --username hello --conf spark.kubernetes.executor.request.cores=0.1 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-${SPARK_VERSION}.jar 1000
echo "Print logs"
kubectl logs -l spark-version=${SPARK_VERSION}
echo "Kubectl get pods -A"
kubectl get pods -A