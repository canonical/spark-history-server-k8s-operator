#!/bin/bash

IMAGE=$1

METADATA=$(rockcraft.skopeo inspect "docker://${IMAGE}" 2>/dev/null | jq '.Labels')

echo $METADATA
