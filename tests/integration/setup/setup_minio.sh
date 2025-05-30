#!/bin/bash

attempt=1
while [ $attempt -le 10 ]
do
  echo "s3 params setup attempt=$attempt"
  if [ -z "$access_key" ]; then
    access_key=$(sudo microk8s.kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_ACCESS_KEY}' | base64 -d)
    sudo microk8s.kubectl get secret -n minio-operator microk8s-user-1
    if [ $? -eq 0 ]; then
      echo "Use access-key from secret"
    else
      echo "use default access-key"
      access_key="minio"
    fi
    echo "access_key=$access_key"
  fi
  if [ -z "$secret_key" ]; then
    secret_key=$(sudo microk8s.kubectl get secret -n minio-operator microk8s-user-1 -o jsonpath='{.data.CONSOLE_SECRET_KEY}' | base64 -d)
    sudo microk8s.kubectl get secret -n minio-operator microk8s-user-1
    if [ $? -eq 0 ]; then
      echo "secret_key=$secret_key"
    else
      echo "Use default secret-key"
      secret_key="minio123"
    fi
    echo "secret_key=$secret_key"
  fi
  if [ -z "$endpoint_ip" ]; then
    endpoint_ip=$(sudo microk8s.kubectl get service minio -n minio-operator -o jsonpath='{.spec.clusterIP}' )
    endpoint="http://$endpoint_ip:80"
    echo "endpoint=$endpoint"
  fi
  echo "AK: $access_key"
  echo "AK: $secret_key"
  echo "EP: $endpoint_ip"

  if [ -z "$access_key" ] || [ -z "$secret_key" ] || [ -z "$endpoint_ip" ]
  then
        if [ $attempt -ge 10 ];then
            echo "ERROR: s3 params setup failure, aborting." >&2
            exit 1
        fi

        echo "[$attempt] s3 params are still missing (see above), retrying in 3 secs..."
        sleep 3
        let "attempt+=1"
  else
        echo "s3 params setup complete..."
        break
  fi
done

echo "$endpoint,$access_key,$secret_key"