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

function check_failure () {
    if [ "$?" -ne 1 ]; then
        exit 1
    fi
}

CLUSTER_NAME="integration-test"


test_echo "Try stuff against a non-existent cluster."
set +e
./flintrock describe sike
check_failure
./flintrock stop sike
check_failure
./flintrock start sike
check_failure
./flintrock login sike
check_failure
./flintrock destroy sike
check_failure
./flintrock run-command sike 'pwd'
check_failure
./flintrock copy-file sike "$0" /tmp/
check_failure
set -e

test_echo "Launch a cluster."
./flintrock launch "$CLUSTER_NAME" --num-slaves 1

test_echo "Describe a running cluster."
./flintrock describe "$CLUSTER_NAME"

test_echo "Make sure we can't launch a cluster with a duplicate name."
set +e
./flintrock launch "$CLUSTER_NAME"
check_failure
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
check_failure
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
