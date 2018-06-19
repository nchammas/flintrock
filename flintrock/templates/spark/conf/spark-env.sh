#!/usr/bin/env bash

export SPARK_LOCAL_DIRS="{spark_root_ephemeral_dirs}"

# Standalone cluster options
export SPARK_EXECUTOR_INSTANCES="{spark_executor_instances}"
export SPARK_EXECUTOR_CORES="$(($(nproc) / {spark_executor_instances}))"
export SPARK_WORKER_CORES="$(nproc)"

export SPARK_MASTER_HOST="{master_host}"

export HADOOP_HOME="$HOME/hadoop"
export LD_LIBRARY_PATH="$HADOOP_HOME/lib/native:$LD_LIBRARY_PATH"

# TODO: Make this dependent on HDFS install.
export HADOOP_CONF_DIR="$HADOOP_HOME/hadoop/conf"

export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

# TODO: Make this non-EC2-specific.
# Bind Spark's web UIs to this machine's public EC2 hostname if it has, otherwise bind to private name
HTTP_STATUS_PUBLIC_NAME=$(curl --head --silent --output /dev/null --write-out "%{{http_code}}" http://169.254.169.254/latest/meta-data/public-hostname)
HTTP_STATUS_PRIVATE_NAME=$(curl --head --silent --output /dev/null --write-out "%{{http_code}}" http://169.254.169.254/latest/meta-data/hostname)
if [ $HTTP_STATUS_PUBLIC_NAME -eq 200 ]; then
    export SPARK_PUBLIC_DNS="$(curl --silent http://169.254.169.254/latest/meta-data/public-hostname)"
elif [ $HTTP_STATUS_PRIVATE_NAME -eq 200  ]; then
    export SPARK_PUBLIC_DNS="$(curl --silent http://169.254.169.254/latest/meta-data/hostname)"
fi

# TODO: Set a high ulimit for large shuffles
# Need to find a way to do this, since "sudo ulimit..." doesn't fly.
# Probably need to edit some Linux config file.
# ulimit -n 1000000
