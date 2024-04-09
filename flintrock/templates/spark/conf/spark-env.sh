#!/usr/bin/env bash

export SPARK_LOCAL_DIRS="{spark_root_ephemeral_dirs}"

# Standalone cluster options
export SPARK_EXECUTOR_INSTANCES="{spark_executor_instances}"
export SPARK_EXECUTOR_CORES="$(($(nproc) / {spark_executor_instances}))"
export SPARK_WORKER_CORES="$(nproc)"

export SPARK_MASTER_HOST="{master_private_host}"

# TODO: Make this dependent on HDFS install.
export HADOOP_CONF_DIR="$HOME/hadoop/conf"

# TODO: Make this non-EC2-specific.
# Bind Spark's web UIs to this machine's public EC2 hostname
spark_public_hostname="$(curl --silent http://169.254.169.254/latest/meta-data/public-hostname)" #IMDSv1 check
if [[ -z "$spark_public_hostname" ]] || [[ "$spark_public_hostname" == *"DOCTYPE"*"html"*"head"*"body"* ]]
then
      TOKEN="$(curl --silent -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")"
      spark_public_hostname="$(curl --silent -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-hostname)" #IMDSv2 check
      if [[ -z "$spark_public_hostname" ]] || [[ "$spark_public_hostname" == *"DOCTYPE"*"html"*"head"*"body"* ]]
      then
          true #skip setting SPARK_PUBLIC_DNS
      else
          export SPARK_PUBLIC_DNS="$spark_public_hostname"
      fi
else
      export SPARK_PUBLIC_DNS="$spark_public_hostname"
fi

# TODO: Set a high ulimit for large shuffles
# Need to find a way to do this, since "sudo ulimit..." doesn't fly.
# Probably need to edit some Linux config file.
# ulimit -n 1000000

# Should this be made part of a Python service somehow?
export PYSPARK_PYTHON="python3"
