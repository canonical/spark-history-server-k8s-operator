#!/bin/bash

spark-client.spark-submit --username hello --conf spark.kubernetes.executor.request.cores=0.1 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-3.4.2.jar 1000
echo "Print logs"
kubectl logs -l spark-version=3.4.2
echo "Kubectl get pods -A"
kubectl get pods -A