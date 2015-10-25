#!/usr/bin/env bash

# TODO: Convert this to Python so we can run it on Windows too.

set -e

function test_echo () {
    echo ""
    echo ""
    echo ""
    echo " ==="
    echo "| $1"
    echo " -"
}


CLUSTER_NAME="integration-test"

test_echo "Launch a cluster."
./flintrock launch "$CLUSTER_NAME" --num-slaves 1

test_echo "Describe a running cluster."
./flintrock describe "$CLUSTER_NAME"

test_echo "Make sure we can't launch a cluster with a duplicate name."
set +e
./flintrock launch "$CLUSTER_NAME"

if [ "$?" -ne 1 ]; then
    exit 1
fi
set -e

test_echo "Run a command on a cluster."
./flintrock run-command "$CLUSTER_NAME" -- ls -l

test_echo "Stop a cluster."
./flintrock stop "$CLUSTER_NAME" --assume-yes

test_echo "Describe a stopped cluster."
./flintrock describe "$CLUSTER_NAME"

test_echo "Make sure that a stopped cluster is still detected as a duplicate."
set +e
./flintrock launch "$CLUSTER_NAME"

if [ "$?" -ne 1 ]; then
    exit 1
fi
set -e

test_echo "Start a stopped cluster and make sure Spark is still working."
./flintrock start "$CLUSTER_NAME"

test_echo "Copy a file up to a cluster."
dummy_file="/tmp/flintrock.dummy"
test ! -f "$dummy_file"  # make sure it doesn't already exist
truncate --size 1KB "$dummy_file"
./flintrock copy-file "$CLUSTER_NAME" "$dummy_file" /tmp/
rm "$dummy_file"

# How do you test this automatically?
# ./flintrock login "$CLUSTER_NAME"

test_echo "Destroy a cluster."
./flintrock destroy "$CLUSTER_NAME" --assume-yes

test_echo "Check available clusters."
./flintrock describe
