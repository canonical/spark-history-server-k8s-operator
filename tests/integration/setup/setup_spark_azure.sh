#!/bin/bash

sudo snap remove --purge spark-client
sudo snap install spark-client --edge
sudo snap install azcli
mkdir -p ~/.kube
sudo microk8s config | tee ~/.kube/config

spark-client.service-account-registry delete --username hello

container=$1
path=$2
account_name=$3
secret_key=$4
folder="abfss://$container@$account_name.dfs.core.windows.net/$path"

spark-client.service-account-registry create --username hello \
    --conf spark.hadoop.fs.azure.account.key.$account_name.dfs.core.windows.net=$secret_key \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=$folder \
    --conf spark.history.fs.logDirectory=$folder

spark-client.service-account-registry get-config --username hello