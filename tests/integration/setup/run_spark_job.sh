#!/bin/bash

spark-client.spark-submit --username hello --conf spark.kubernetes.executor.request.cores=0.1 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 100
echo "Print logs"
kubectl logs -l app.kubernetes.io/managed-by:spark8t