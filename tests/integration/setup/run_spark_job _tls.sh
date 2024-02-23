#!/bin/bash

keytool -import -alias ceph-cert -file $1 -storetype JKS -keystore cacerts -storepass changeit -noprompt
mv cacerts spark.truststore
sudo microk8s.kubectl create secret generic spark-truststore --from-file spark.truststore
spark-client.service-account-registry add-config --username hello \
    --conf spark.executor.extraJavaOptions="-Djavax.net.ssl.trustStore=/spark-truststore/spark.truststore -Djavax.net.ssl.trustStorePassword=changeit" \
    --conf spark.driver.extraJavaOptions="-Djavax.net.ssl.trustStore=/spark-truststore/spark.truststore -Djavax.net.ssl.trustStorePassword=changeit" \
    --conf spark.kubernetes.executor.secrets.spark-truststore=/spark-truststore \
    --conf spark.kubernetes.driver.secrets.spark-truststore=/spark-truststore 

spark-client.spark-submit --username hello --conf --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true spark.kubernetes.executor.request.cores=0.1 --conf --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.12-3.4.1.jar 100