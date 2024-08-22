#!/bin/bash

spark-client.spark-submit --username hello --conf spark.kubernetes.executor.request.cores=0.1 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.13-4.0.0-preview1.jar --conf spark.kubernetes.container.image=ghcr.io/welpaolo/charmed-spark:4.0.0-preview1_edge 100