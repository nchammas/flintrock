#!/usr/bin/env bash

export SPARK_LOCAL_DIRS="{spark_scratch_dir}"

# Standalone cluster options
export SPARK_MASTER_OPTS="{spark_master_opts}"
export SPARK_WORKER_INSTANCES="1"
export SPARK_WORKER_CORES="$(nproc)"

export HADOOP_HOME=""
export SPARK_MASTER_IP="{master_host}"
export MASTER="spark://{master_host}:7077"

export SPARK_SUBMIT_LIBRARY_PATH="$SPARK_SUBMIT_LIBRARY_PATH"
export SPARK_SUBMIT_CLASSPATH="$SPARK_SUBMIT_CLASSPATH"

# TODO: Make this non-EC2-specific.
# Bind Spark's web UIs to this machine's public EC2 hostname
export SPARK_PUBLIC_DNS="$(curl --silent http://169.254.169.254/latest/meta-data/public-hostname)"

# TODO: Set a high ulimit for large shuffles
# Need to find a way to do this, since "sudo ulimit..." doesn't fly.
# Probably need to edit some Linux config file.
# ulimit -n 1000000
