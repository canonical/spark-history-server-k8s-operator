#!/bin/bash

IMAGE=$1

docker pull $IMAGE >/dev/null 2>/dev/null
METADATA=$(docker image inspect $IMAGE 2>/dev/null | jq '.[0].Config.Labels')
# docker rmi $IMAGE

echo $METADATA
