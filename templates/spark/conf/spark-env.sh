#!/usr/bin/env bash

export SPARK_LOCAL_DIRS="{spark_local_dirs}"

# Standalone cluster options
export SPARK_MASTER_OPTS="{spark_master_opts}"
export SPARK_WORKER_INSTANCES="{spark_worker_instances}"
export SPARK_WORKER_CORES="{spark_worker_cores}"

export HADOOP_HOME=""
export SPARK_MASTER_IP="{active_master}"
export MASTER="spark://{active_master}:7077"

export SPARK_SUBMIT_LIBRARY_PATH="$SPARK_SUBMIT_LIBRARY_PATH"
export SPARK_SUBMIT_CLASSPATH="$SPARK_SUBMIT_CLASSPATH"

# Bind Spark's web UIs to this machine's public EC2 hostname
export SPARK_PUBLIC_DNS="$(curl --silent http://169.254.169.254/latest/meta-data/public-hostname)"

# Set a high ulimit for large shuffles
# ulimit -n 1000000
