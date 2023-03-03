#!/bin/bash

sudo microk8s disable minio
sudo microk8s enable minio

sudo microk8s status --wait-ready

pip install boto3

attempt=1
while [ $attempt -le 10 ]
do
  echo "s3 params setup attempt=$attempt"
  access_key=$(sudo microk8s.kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_ACCESS_KEY}' | base64 -d)
  #echo "access_key=$access_key"
  secret_key=$(sudo microk8s.kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_SECRET_KEY}' | base64 -d)
  #echo "secret_key=$secret_key"
  endpoint_ip=$(sudo microk8s.kubectl get services -n minio-operator | grep minio | awk '{ print $3 }')
  endpoint="http://$endpoint_ip:80"
  #echo "endpoint=$endpoint"

  if [ -z "$access_key" ] || [ -z "$secret_key" ] || [ -z "$endpoint_ip" ]
  then
        sleep 3
        let "attempt+=1"
  else
        echo "s3 params setup complete..."
        break
  fi
done

echo "$endpoint,$access_key,$secret_key"