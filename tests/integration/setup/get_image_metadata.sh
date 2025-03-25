#!/bin/bash

IMAGE=$1

docker pull $IMAGE >/dev/null 2>/dev/null
METADATA=$(docker image inspect ghcr.io/canonical/charmed-spark:3.4-22.04_edge 2>/dev/null | jq '.[0].Config.Labels')
# docker rmi $IMAGE

echo $METADATA
