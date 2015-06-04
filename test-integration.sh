#!/usr/bin/env bash

set -e
set -x

CLUSTER_NAME="integration-test"

./flintrock launch "$CLUSTER_NAME" --num-slaves 1
./flintrock describe "$CLUSTER_NAME"
# ./flintrock login "$CLUSTER_NAME"  # How do you test this automatically? 
./flintrock destroy "$CLUSTER_NAME" --assume-yes
