#!/bin/bash
set -eux
sudo apt install s3cmd -y
echo "Generate truststore"
keytool -import -alias ceph-cert -file $1 -storetype JKS -keystore cacerts -storepass changeit -noprompt
mv cacerts spark.truststore
echo "Create secret for truststore"
sudo microk8s.kubectl create secret generic spark-truststore --from-file spark.truststore
echo "Configure Service account"
spark-client.service-account-registry add-config --username hello \
    --conf spark.executor.extraJavaOptions="-Djavax.net.ssl.trustStore=/spark-truststore/spark.truststore -Djavax.net.ssl.trustStorePassword=changeit" \
    --conf spark.driver.extraJavaOptions="-Djavax.net.ssl.trustStore=/spark-truststore/spark.truststore -Djavax.net.ssl.trustStorePassword=changeit" \
    --conf spark.kubernetes.executor.secrets.spark-truststore=/spark-truststore \
    --conf spark.kubernetes.driver.secrets.spark-truststore=/spark-truststore 
echo "Run Spark job"
spark-client.spark-submit --username hello --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true --conf spark.kubernetes.executor.request.cores=0.1 --class org.apache.spark.examples.SparkPi local:///opt/spark/examples/jars/spark-examples_2.13-4.0.0-preview1.jar --conf spark.kubernetes.container.image=ghcr.io/welpaolo/charmed-spark:4.0.0-preview1_edge 100
set +e